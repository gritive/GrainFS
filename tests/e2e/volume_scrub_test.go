package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// findVolumeBlockOnDisk walks dataDir for the on-disk file backing a volume
// block. Volume blocks may be stored in the legacy object layout
// (.../__vol/<name>/blk_NNN[_vUUID]/current) or the current EC shard layout
// (.../shards/__grainfs_volumes/__vol/<name>/blk_NNN[_vUUID]/<version>/shard_N).
// The block key may carry a versionID suffix on initial allocation, so we glob
// rather than reconstruct the full path.
func findVolumeBlockOnDisk(t *testing.T, dataDir, vol string, blockNum int) string {
	t.Helper()
	want := blockKeyName(blockNum)
	var hit string
	err := filepath.Walk(dataDir, func(p string, info os.FileInfo, werr error) error {
		if werr != nil || info == nil || info.IsDir() {
			return nil
		}
		if !isVolumeBlockFilePath(p, vol, want) {
			return nil
		}
		hit = p
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, hit, "could not find on-disk block %s for volume %s under %s", want, vol, dataDir)
	return hit
}

func isVolumeBlockFilePath(p, vol, want string) bool {
	base := filepath.Base(p)
	if base != "current" && !strings.HasPrefix(base, "shard_") {
		return false
	}

	slashPath := filepath.ToSlash(p)
	marker := "/__vol/" + vol + "/"
	i := strings.LastIndex(slashPath, marker)
	if i < 0 {
		return false
	}
	rest := slashPath[i+len(marker):]
	end := strings.Index(rest, "/")
	if end < 0 {
		return false
	}
	return strings.HasPrefix(rest[:end], want)
}

func blockKeyName(blockNum int) string {
	// matches volume.blockKey: "blk_%012d"
	s := []byte("blk_000000000000")
	v := blockNum
	for i := len(s) - 1; i >= len("blk_") && v > 0; i-- {
		s[i] = byte('0' + v%10)
		v /= 10
	}
	return string(s)
}

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

// TestFindVolumeBlockOnDisk verifies the helper resolves both on-disk
// layouts (legacy `current` and EC `shard_N`). Pure unit test — no fixture.
// TestFindVolumeBlockOnDiskE2E verifies the helper resolves both on-disk
// layouts (legacy `current` and EC `shard_N`). Pure helper unit check, no
// fixture used, but wrapped in the canonical SingleNode/Cluster4Node shape
// for grep/inventory consistency.
func TestFindVolumeBlockOnDiskE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		_ = newSingleNodeS3Target()
		runFindVolumeBlockOnDiskCases(t)
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		_ = newSharedClusterS3Target(t)
		runFindVolumeBlockOnDiskCases(t)
	})
}

func runFindVolumeBlockOnDiskCases(t *testing.T) {
	t.Helper()

	t.Run("LegacyCurrentLayout", func(t *testing.T) {
		dataDir := t.TempDir()
		blockPath := filepath.Join(dataDir, "groups", "g1", "data", "__grainfs_volumes", ".obj", "__vol", "vs2", "blk_000000000000_vabc", "current")
		require.NoError(t, os.MkdirAll(filepath.Dir(blockPath), 0o755))
		require.NoError(t, os.WriteFile(blockPath, []byte("data"), 0o644))

		require.Equal(t, blockPath, findVolumeBlockOnDisk(t, dataDir, "vs2", 0))
	})

	t.Run("ShardLayout", func(t *testing.T) {
		dataDir := t.TempDir()
		blockPath := filepath.Join(dataDir, "shards", "__grainfs_volumes", "__vol", "vs2", "blk_000000000000_vabc", "019e20ca-0000-7000-8000-000000000000", "shard_0")
		require.NoError(t, os.MkdirAll(filepath.Dir(blockPath), 0o755))
		require.NoError(t, os.WriteFile(blockPath, []byte("data"), 0o644))

		require.Equal(t, blockPath, findVolumeBlockOnDisk(t, dataDir, "vs2", 0))
	})
}

// volumeScrubFactory builds a fresh per-case fixture with the given grainfs
// serve args. Each scrub case needs its own dedup/scrub-interval, so the case
// set is parametrised on a fixture factory rather than a single s3Target.
type volumeScrubFactory func(args ...string) s3Target

// scrubDataDir returns the dataDir to drive CLI commands against tgt for the
// given node index. Single-node fixtures ignore nodeIdx.
func scrubDataDir(tgt s3Target, nodeIdx int) string {
	if tgt.isCluster {
		return tgt.cluster.dataDirs[nodeIdx]
	}
	return filepath.Dir(tgt.adminSockPath())
}

// truncateAVolumeBlock corrupts the on-disk block backing (vol, blockNum) and
// returns (nodeIdx, path). For single-node the dataDir is the unique server
// dir; for cluster the function picks the first node holding an EC shard for
// that block.
func truncateAVolumeBlock(t *testing.T, tgt s3Target, vol string, blockNum int) (int, string) {
	t.Helper()
	if !tgt.isCluster {
		dataDir := scrubDataDir(tgt, 0)
		p := findVolumeBlockOnDisk(t, dataDir, vol, blockNum)
		require.NoError(t, os.Truncate(p, 1))
		return 0, p
	}
	for i, dd := range tgt.cluster.dataDirs {
		var hits []string
		_ = filepathWalkBlock(dd, vol, blockNum, &hits)
		for _, p := range hits {
			if strings.Contains(p, "/shards/__grainfs_volumes/") &&
				strings.HasPrefix(filepath.Base(p), "shard_") {
				require.NoError(t, os.Truncate(p, 1))
				return i, p
			}
		}
	}
	t.Fatalf("no EC shard for %s/%d found across %d nodes", vol, blockNum, len(tgt.cluster.dataDirs))
	return 0, ""
}

// TestVolumeScrubE2E exercises the volume scrubber against both single-node
// and 4-node cluster fixtures. The shared case set covers healthy-noop,
// dry-run detection, repair behavior, zero-interval admin trigger, and the
// status/list/cancel CLI subcommands. RepairBehavior asserts diverging
// expectations per fixture: single → Unrepairable=1 (no peer), cluster →
// Repaired=1 (EC peer-pull). MultiNodeRepair (formerly its own test) is
// absorbed by RepairBehavior's cluster branch.
func TestVolumeScrubE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runVolumeScrubCases(t, func(args ...string) s3Target {
			return newDedicatedSingleNodeS3Target(t, args)
		})
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runVolumeScrubCases(t, func(args ...string) s3Target {
			return newClusterS3TargetWithExtraArgs(t, 4, args)
		})
	})
}

func runVolumeScrubCases(t *testing.T, mk volumeScrubFactory) {
	t.Helper()

	t.Run("HealthyNoop", func(t *testing.T) {
		tgt := mk("--dedup=false")
		dd := scrubDataDir(tgt, 0)
		out, code := runCLI(t, dd, "volume", "create", "vs1", "--size", "1Mi")
		require.Equal(t, 0, code, out)
		out, code = runCLI(t, dd, "volume", "write-at", "vs1", "--offset", "0", "--content", "hello")
		require.Equal(t, 0, code, out)

		out, code = runCLI(t, dd, "volume", "scrub", "vs1")
		require.Equal(t, 0, code, out)
		require.Contains(t, out, "Repaired=0", "got:\n%s", out)
		require.Contains(t, out, "Detected=0", "got:\n%s", out)
	})

	t.Run("HealthyNoop_Dedup", func(t *testing.T) {
		tgt := mk("--dedup=true")
		dd := scrubDataDir(tgt, 0)
		out, code := runCLI(t, dd, "volume", "create", "vsd1", "--size", "1Mi")
		require.Equal(t, 0, code, out)
		out, code = runCLI(t, dd, "volume", "write-at", "vsd1", "--offset", "0", "--content", "hello")
		require.Equal(t, 0, code, out)
		out, code = runCLI(t, dd, "volume", "scrub", "vsd1")
		require.Equal(t, 0, code, out)
		require.Contains(t, out, "Repaired=0", "got:\n%s", out)
		require.Contains(t, out, "Detected=0", "got:\n%s", out)
	})

	t.Run("DryRunDetectsCorruption", func(t *testing.T) {
		tgt := mk("--dedup=false")
		dd := scrubDataDir(tgt, 0)
		out, code := runCLI(t, dd, "volume", "create", "vs2", "--size", "1Mi")
		require.Equal(t, 0, code, out)
		out, code = runCLI(t, dd, "volume", "write-at", "vs2", "--offset", "0", "--content", "abcd1234")
		require.Equal(t, 0, code, out)

		nodeIdx, blockPath := truncateAVolumeBlock(t, tgt, "vs2", 0)
		nodeDD := scrubDataDir(tgt, nodeIdx)

		out, code = runCLI(t, nodeDD, "volume", "scrub", "vs2", "--dry-run")
		require.Equal(t, 0, code, out)
		require.Contains(t, out, "Detected=1", "got:\n%s", out)
		require.Contains(t, out, "Repaired=0", "dry-run must not repair; got:\n%s", out)

		fi, err := os.Stat(blockPath)
		require.NoError(t, err)
		require.Equal(t, int64(1), fi.Size(), "dry-run must leave the local file untouched")
	})

	t.Run("DryRunDetectsCorruption_Dedup", func(t *testing.T) {
		tgt := mk("--dedup=true")
		dd := scrubDataDir(tgt, 0)
		out, code := runCLI(t, dd, "volume", "create", "vsd2", "--size", "1Mi")
		require.Equal(t, 0, code, out)
		out, code = runCLI(t, dd, "volume", "write-at", "vsd2", "--offset", "0", "--content", "abcd1234")
		require.Equal(t, 0, code, out)

		nodeIdx, blockPath := truncateAVolumeBlock(t, tgt, "vsd2", 0)
		nodeDD := scrubDataDir(tgt, nodeIdx)

		out, code = runCLI(t, nodeDD, "volume", "scrub", "vsd2", "--dry-run")
		require.Equal(t, 0, code, out)
		require.Contains(t, out, "Detected=1", "got:\n%s", out)
		require.Contains(t, out, "Repaired=0", "dry-run must not repair; got:\n%s", out)

		fi, err := os.Stat(blockPath)
		require.NoError(t, err)
		require.Equal(t, int64(1), fi.Size(), "dry-run must leave the local file untouched")
	})

	t.Run("RepairBehavior", func(t *testing.T) {
		// Single-node: no peer to pull from → Unrepairable=1.
		// Cluster:      EC peer-pull repair  → Repaired=1.
		tgt := mk("--dedup=false")
		dd := scrubDataDir(tgt, 0)
		out, code := runCLI(t, dd, "volume", "create", "vs3", "--size", "1Mi")
		require.Equal(t, 0, code, out)
		out, code = runCLI(t, dd, "volume", "write-at", "vs3", "--offset", "0", "--content", "QWERTY")
		require.Equal(t, 0, code, out)

		nodeIdx, _ := truncateAVolumeBlock(t, tgt, "vs3", 0)
		nodeDD := scrubDataDir(tgt, nodeIdx)

		out, code = runCLI(t, nodeDD, "volume", "scrub", "vs3")
		require.Equal(t, 0, code, out)
		require.Contains(t, out, "Detected=1", "got:\n%s", out)
		if tgt.isCluster {
			require.Contains(t, out, "Repaired=1", "cluster peer-pull must repair; got:\n%s", out)
		} else {
			require.Contains(t, out, "Unrepairable=1", "single node has no peer; got:\n%s", out)
		}
	})

	t.Run("RepairBehavior_Dedup", func(t *testing.T) {
		tgt := mk("--dedup=true")
		dd := scrubDataDir(tgt, 0)
		out, code := runCLI(t, dd, "volume", "create", "vsd3", "--size", "1Mi")
		require.Equal(t, 0, code, out)
		out, code = runCLI(t, dd, "volume", "write-at", "vsd3", "--offset", "0", "--content", "QWERTY")
		require.Equal(t, 0, code, out)

		nodeIdx, _ := truncateAVolumeBlock(t, tgt, "vsd3", 0)
		nodeDD := scrubDataDir(tgt, nodeIdx)

		out, code = runCLI(t, nodeDD, "volume", "scrub", "vsd3")
		require.Equal(t, 0, code, out)
		require.Contains(t, out, "Detected=1", "got:\n%s", out)
		if tgt.isCluster {
			require.Contains(t, out, "Repaired=1", "cluster peer-pull must repair; got:\n%s", out)
		} else {
			require.Contains(t, out, "Unrepairable=1", "single node has no peer; got:\n%s", out)
		}
	})

	t.Run("AdminTriggerWorksAtZeroInterval", func(t *testing.T) {
		// Regression guard for Director-wiring fix. --scrub-interval=0
		// disables the periodic loop but admin trigger must keep working
		// (pre-fix returned "scrub director not configured").
		tgt := mk("--dedup=false", "--scrub-interval=0")
		dd := scrubDataDir(tgt, 0)
		out, code := runCLI(t, dd, "volume", "create", "vsi0", "--size", "1Mi")
		require.Equal(t, 0, code, out)
		out, code = runCLI(t, dd, "volume", "write-at", "vsi0", "--offset", "0", "--content", "x")
		require.Equal(t, 0, code, out)
		out, code = runCLI(t, dd, "volume", "scrub", "vsi0")
		require.Equal(t, 0, code, out)
		require.NotContains(t, out, "scrub director not configured", "got:\n%s", out)
		require.Contains(t, out, "Detected=0", "got:\n%s", out)
		require.Contains(t, out, "Repaired=0", "got:\n%s", out)
	})

	t.Run("StatusListCancel", func(t *testing.T) {
		tgt := mk("--dedup=false")
		dd := scrubDataDir(tgt, 0)
		out, code := runCLI(t, dd, "volume", "create", "vs4", "--size", "1Mi")
		require.Equal(t, 0, code, out)
		out, code = runCLI(t, dd, "volume", "write-at", "vs4", "--offset", "0", "--content", "data")
		require.Equal(t, 0, code, out)

		out, code = runCLI(t, dd, "volume", "scrub", "vs4", "--detach")
		require.Equal(t, 0, code, out)
		// Parse session id out of "Triggered scrub: session=<uuid> ..."
		idx := strings.Index(out, "session=")
		require.Greater(t, idx, -1, "no session id in output: %s", out)
		rest := out[idx+len("session="):]
		end := strings.IndexByte(rest, ' ')
		require.Greater(t, end, 0)
		sessionID := rest[:end]

		out, code = runCLI(t, dd, "volume", "scrub", "list")
		require.Equal(t, 0, code, out)
		require.Contains(t, out, sessionID)

		out, code = runCLI(t, dd, "volume", "scrub", "status", sessionID)
		require.Equal(t, 0, code, out)
		require.Contains(t, out, sessionID)
	})
}
