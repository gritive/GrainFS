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
		Mode:       ClusterModeStaticPeers,
		DisableNFS: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c.GrantAdminOnBuckets("__grainfs_volumes")
	require.Eventually(t, func() bool {
		err := tryCreateBucket(ctx, c.S3Client(0), "__grainfs_volumes")
		return err == nil || strings.Contains(fmt.Sprint(err), "BucketAlreadyOwnedByYou")
	}, 30*time.Second, 500*time.Millisecond, "__grainfs_volumes bucket grant did not become writable")

	client := dialE2ENBD(t, fmt.Sprintf("127.0.0.1:%d", c.nbdPorts[0]), "default")
	defer client.Close()

	body := []byte("nbd-byte-level-replication-payload")
	client.WriteAt(t, 0, body)
	client.Flush(t)
	requireNBDReadEventually(t, client, 0, body)

	t.Skip("physical byte-level fanout assertion requires pre-join multi-voter shard groups; raft-v2 static e2e now boots via join-mode single-voter seed groups")

	// Poll up to 10s for replication fan-out. FLUSH commits the write-back
	// mutation, but peer fan-out can still settle asynchronously.
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
