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
	skipIfShort(t, "skipping e2e test in -short mode")

	c := startE2ECluster(t, e2eClusterOptions{
		Nodes:         3,
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
	// write. The EC shard writer retries transiently — no test-side sleep needed.
	out, code = runCLI(t, c.dataDirs[0], "volume", "write-at", "vmrn", "--offset", "0", "--content", "MultiNodePayload!")
	require.Equal(t, 0, code, out)

	// Scan every node's dataDir for shard holders. Volume blocks are EC objects
	// now, so the deterministic repair target is the local shard owned by the
	// node whose scrub we trigger.
	targetNode := -1
	targetPath := ""
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
			if strings.Contains(p, "/shards/__grainfs_volumes/") && strings.HasPrefix(filepath.Base(p), "shard_") {
				if targetNode < 0 {
					targetNode = i
					targetPath = p
				}
			}
		}
	}
	require.GreaterOrEqual(t, holders, 2, "need ≥2 holders for peer-pull; got %d (replication broken?)", holders)
	require.GreaterOrEqual(t, targetNode, 0, "no node holds a local EC shard for the volume block")

	// Truncate the local shard; trigger scrub on the same node; expect EC
	// repair to restore that shard from the surviving peers.
	t.Logf("truncating local shard on node %d at %s", targetNode, targetPath)
	require.NoError(t, os.Truncate(targetPath, 1))

	out, code = runCLI(t, c.dataDirs[targetNode], "volume", "scrub", "vmrn")
	t.Logf("scrub output:\n%s", out)
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "Detected=1", "expected detection; got:\n%s", out)
	require.Contains(t, out, "Repaired=1", "expected EC shard repair; got:\n%s", out)
}
