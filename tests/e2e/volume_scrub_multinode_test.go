package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// filepathWalkBlock walks dataDir collecting any file under a /__vol/<vol>/
// path that matches the volume block key pattern (with or without dedup
// _v<UUID> suffix). Hits cover BOTH layouts:
//   - Leader local file: {root}/data/__grainfs_volumes/.obj/__vol/<vol>/<key>/current
//   - Peer replica:      {root}/shards/__grainfs_volumes/__vol/<vol>/<key>/<versionID>/shard_0
func filepathWalkBlock(dataDir, vol string, blockNum int, hits *[]string) error {
	want := blockKeyName(blockNum)
	return filepath.Walk(dataDir, func(p string, info os.FileInfo, werr error) error {
		if werr != nil || info == nil || info.IsDir() {
			return nil
		}
		// We accept either a "current" file (leader) or a "shard_N" file (peer).
		base := filepath.Base(p)
		if base != "current" && !strings.HasPrefix(base, "shard_") {
			return nil
		}
		if !strings.Contains(p, "/__vol/"+vol+"/") {
			return nil
		}
		i := strings.LastIndex(p, "/__vol/"+vol+"/")
		rest := p[i+len("/__vol/"+vol+"/"):]
		end := strings.Index(rest, "/")
		if end < 0 {
			return nil
		}
		keyTail := rest[:end]
		if strings.HasPrefix(keyTail, want) {
			*hits = append(*hits, p)
		}
		return nil
	})
}

// TestE2E_VolumeScrub_MultiNodeRepair — write a volume block on a 3-node
// cluster, truncate the local replica on one node, trigger scrub on that
// node, expect peer-pull repair to succeed.
//
// Pre-condition: N×replication actually places copies on every group voter.
// This was broken by a wiring bug at serve.go:747 (StreamShardWriteBody body
// handler registered on the wrong router). Without the fix this test fails
// at the holders>=2 assertion (only the leader has a copy).
func TestE2E_VolumeScrub_MultiNodeRepair(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}

	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:         3,
		SeedGroups:    3,
		Mode:          ClusterModeStaticPeers,
		DisableNFS:    true,
		DisableNBD:    true,
		ScrubInterval: "24h",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	err := tryCreateBucket(ctx, c.S3Client(0), "__grainfs_volumes")
	if err != nil && !strings.Contains(fmt.Sprint(err), "BucketAlreadyOwnedByYou") {
		require.NoError(t, err)
	}

	out, code := runCLI(t, c.dataDirs[0], "volume", "create", "vmrn", "--size", "1Mi")
	require.Equal(t, 0, code, out)
	// Cold-start QUIC handshake races used to flap peers into cooldown on first
	// write. Absorbed product-side by writeSpooledReplicaShardStream's bounded
	// retry (3 attempts, 100ms backoff) — no test-side sleep needed.
	out, code = runCLI(t, c.dataDirs[0], "volume", "write-at", "vmrn", "--offset", "0", "--content", "MultiNodePayload!")
	require.Equal(t, 0, code, out)

	// Scan every node's dataDir for replicas. Each node ends up with EITHER
	// the leader-shaped local file (data/.obj/<key>/current) OR a peer-replica
	// shard (shards/<key>/<vid>/shard_0). Both count as a holder for the
	// "peer-pull source" purpose, but the repair WRITE target is the leader
	// path. Truncating a peer shard exercises detection but the repaired
	// bytes land on a different file. Pick the leader's data/.obj file as
	// truncate target so the file-size restoration assertion is deterministic.
	leaderTarget := -1
	leaderPath := ""
	holders := 0
	for i, dd := range c.dataDirs {
		var hits []string
		_ = filepathWalkBlock(dd, "vmrn", 0, &hits)
		t.Logf("node %d: %d block hit(s)", i, len(hits))
		if len(hits) == 0 {
			continue
		}
		holders++
		for _, p := range hits {
			if strings.Contains(p, "/data/__grainfs_volumes/.obj/") && strings.HasSuffix(p, "/current") {
				if leaderTarget < 0 {
					leaderTarget = i
					leaderPath = p
				}
			}
		}
	}
	require.GreaterOrEqual(t, holders, 2, "need ≥2 holders for peer-pull; got %d (replication broken?)", holders)
	require.GreaterOrEqual(t, leaderTarget, 0, "no node holds the leader-shaped data/.obj/<key>/current file")

	// Truncate the leader's local replica; trigger scrub on the same node;
	// expect peer-pull repair to restore the file via RepairReplica's
	// writeRepairedReplica → objectPathV(... "current") path.
	t.Logf("truncating leader replica on node %d at %s", leaderTarget, leaderPath)
	require.NoError(t, os.Truncate(leaderPath, 1))

	out, code = runCLI(t, c.dataDirs[leaderTarget], "volume", "scrub", "vmrn")
	t.Logf("scrub output:\n%s", out)
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "Detected=1", "expected detection; got:\n%s", out)
	require.Contains(t, out, "Repaired=1", "expected peer-pull repair; got:\n%s", out)

	fi, err := os.Stat(leaderPath)
	require.NoError(t, err)
	require.Greater(t, fi.Size(), int64(1), "leader replica should be restored beyond truncated 1 byte; got %d", fi.Size())
}
