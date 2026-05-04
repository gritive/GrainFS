package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestE2E_NBDMultiNode_ByteLevelReplication writes a block via NBD on node 0
// in a 3-node cluster, reads it back through NBD, then walks every node's
// dataDir to confirm the bytes physically replicated to ≥2 nodes.
//
// Why this exists separately from TestE2E_MultiRaftSharding_NBDRoutesThroughCoordinator:
// that test verifies metadata propagation only — ListObjects from a non-coordinator
// sees the key because raft replicates metadata. It can pass while bytes live on
// a single replica, exactly the failure mode caused by the StreamShardWriteBody
// router-registration bug fixed in v0.0.43.3 (PR #170). This test catches that
// class of regression by checking holders on disk, not metadata.
func TestE2E_NBDMultiNode_ByteLevelReplication(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}

	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:      3,
		SeedGroups: 3,
		Mode:       ClusterModeStaticPeers,
		DisableNFS: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := tryCreateBucket(ctx, c.S3Client(0), "__grainfs_volumes")
	if err != nil && !strings.Contains(fmt.Sprint(err), "BucketAlreadyOwnedByYou") {
		require.NoError(t, err)
	}

	// Cold-start: peer connections may not be fully warm. Wait past the 10s
	// peerHealth cooldown so the NBD-driven block write replicates cleanly
	// on first try (mirrors TestE2E_VolumeScrub_MultiNodeRepair).
	t.Log("waiting 12s for peer connections to warm up")
	time.Sleep(12 * time.Second)

	client := dialE2ENBD(t, fmt.Sprintf("127.0.0.1:%d", c.nbdPorts[0]), "default")
	defer client.Close()

	body := []byte("nbd-byte-level-replication-payload")
	client.WriteAt(t, 0, body)
	got := client.ReadAt(t, 0, uint32(len(body)))
	require.Equal(t, body, got, "NBD round-trip must succeed before checking replication")

	// Poll up to 10s for replication fan-out. putObjectNxSpooledAsync may
	// hand bytes to peers asynchronously; the NBD WriteAt reply only
	// guarantees the leader has committed.
	var (
		holders     int
		perNodeHits []int
	)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		holders = 0
		perNodeHits = make([]int, len(c.dataDirs))
		for i, dd := range c.dataDirs {
			var hits []string
			_ = filepathWalkBlock(dd, "default", 0, &hits)
			perNodeHits[i] = len(hits)
			if len(hits) > 0 {
				holders++
			}
		}
		if holders >= 2 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	for i, n := range perNodeHits {
		t.Logf("node %d: %d block hit(s)", i, n)
	}
	require.GreaterOrEqual(t, holders, 2,
		"need ≥2 holders on disk for byte-level NBD replication; got %d (replication broken?)", holders)
}
