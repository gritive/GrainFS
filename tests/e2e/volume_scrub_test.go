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

func TestFindVolumeBlockOnDiskFindsLegacyCurrentLayout(t *testing.T) {
	dataDir := t.TempDir()
	blockPath := filepath.Join(dataDir, "groups", "g1", "data", "__grainfs_volumes", ".obj", "__vol", "vs2", "blk_000000000000_vabc", "current")
	require.NoError(t, os.MkdirAll(filepath.Dir(blockPath), 0o755))
	require.NoError(t, os.WriteFile(blockPath, []byte("data"), 0o644))

	require.Equal(t, blockPath, findVolumeBlockOnDisk(t, dataDir, "vs2", 0))
}

func TestFindVolumeBlockOnDiskFindsShardLayout(t *testing.T) {
	dataDir := t.TempDir()
	blockPath := filepath.Join(dataDir, "shards", "__grainfs_volumes", "__vol", "vs2", "blk_000000000000_vabc", "019e20ca-0000-7000-8000-000000000000", "shard_0")
	require.NoError(t, os.MkdirAll(filepath.Dir(blockPath), 0o755))
	require.NoError(t, os.WriteFile(blockPath, []byte("data"), 0o644))

	require.Equal(t, blockPath, findVolumeBlockOnDisk(t, dataDir, "vs2", 0))
}

// TestE2E_VolumeScrub_HealthyNoop — clean scrub on a freshly-written volume
// reports zero detections. Replication-only path (dedup off).
func TestE2E_VolumeScrub_HealthyNoop(t *testing.T) {
	dataDir, _, _ := startTestServer(t, "--dedup=false")

	out, code := runCLI(t, dataDir, "volume", "create", "vs1", "--size", "1Mi")
	require.Equal(t, 0, code, out)
	out, code = runCLI(t, dataDir, "volume", "write-at", "vs1", "--offset", "0", "--content", "hello")
	require.Equal(t, 0, code, out)

	out, code = runCLI(t, dataDir, "volume", "scrub", "vs1")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "Repaired=0", "no corruption => Repaired=0; got:\n%s", out)
	require.Contains(t, out, "Detected=0")
}

// TestE2E_VolumeScrub_HealthyNoop_Dedup — same shape with dedup enabled.
// Pins the source/verifier behavior under content-addressed keys
// (blk_NNN_v<UUID>); regression guard since v0.0.43 shipped without
// dedup-mode coverage.
func TestE2E_VolumeScrub_HealthyNoop_Dedup(t *testing.T) {
	dataDir, _, _ := startTestServer(t, "--dedup=true")

	out, code := runCLI(t, dataDir, "volume", "create", "vsd1", "--size", "1Mi")
	require.Equal(t, 0, code, out)
	out, code = runCLI(t, dataDir, "volume", "write-at", "vsd1", "--offset", "0", "--content", "hello")
	require.Equal(t, 0, code, out)

	out, code = runCLI(t, dataDir, "volume", "scrub", "vsd1")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "Repaired=0", "no corruption => Repaired=0; got:\n%s", out)
	require.Contains(t, out, "Detected=0", "no corruption => Detected=0; got:\n%s", out)
}

// TestE2E_VolumeScrub_DryRunDetectsCorruption — truncating a block on disk
// makes scrub --dry-run report the detection without attempting repair.
func TestE2E_VolumeScrub_DryRunDetectsCorruption(t *testing.T) {
	dataDir, _, _ := startTestServer(t, "--dedup=false")

	out, code := runCLI(t, dataDir, "volume", "create", "vs2", "--size", "1Mi")
	require.Equal(t, 0, code, out)
	out, code = runCLI(t, dataDir, "volume", "write-at", "vs2", "--offset", "0", "--content", "abcd1234")
	require.Equal(t, 0, code, out)

	blockPath := findVolumeBlockOnDisk(t, dataDir, "vs2", 0)
	require.NoError(t, os.Truncate(blockPath, 1), "truncate %s", blockPath)

	out, code = runCLI(t, dataDir, "volume", "scrub", "vs2", "--dry-run")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "Detected=1", "corrupt block must be detected; got:\n%s", out)
	require.Contains(t, out, "Repaired=0", "dry-run must not repair; got:\n%s", out)

	fi, err := os.Stat(blockPath)
	require.NoError(t, err)
	require.Equal(t, int64(1), fi.Size(), "dry-run must leave the local file untouched")
}

// TestE2E_VolumeScrub_DryRunDetectsCorruption_Dedup — same as the base test
// but with dedup enabled, exercising verifier MD5 against the canonical
// _v<UUID> block on disk.
func TestE2E_VolumeScrub_DryRunDetectsCorruption_Dedup(t *testing.T) {
	dataDir, _, _ := startTestServer(t, "--dedup=true")

	out, code := runCLI(t, dataDir, "volume", "create", "vsd2", "--size", "1Mi")
	require.Equal(t, 0, code, out)
	out, code = runCLI(t, dataDir, "volume", "write-at", "vsd2", "--offset", "0", "--content", "abcd1234")
	require.Equal(t, 0, code, out)

	blockPath := findVolumeBlockOnDisk(t, dataDir, "vsd2", 0)
	require.NoError(t, os.Truncate(blockPath, 1), "truncate %s", blockPath)

	out, code = runCLI(t, dataDir, "volume", "scrub", "vsd2", "--dry-run")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "Detected=1", "corrupt block must be detected; got:\n%s", out)
	require.Contains(t, out, "Repaired=0", "dry-run must not repair; got:\n%s", out)

	fi, err := os.Stat(blockPath)
	require.NoError(t, err)
	require.Equal(t, int64(1), fi.Size(), "dry-run must leave the local file untouched")
}

// TestE2E_VolumeScrub_SingleNodeRepairUnrepairable — single-node cluster has
// no peer to pull from, so a real scrub records the corruption but cannot
// repair it. Multi-node repair is covered when the cluster broadcast E2E
// lands (TODOS).
func TestE2E_VolumeScrub_SingleNodeRepairUnrepairable(t *testing.T) {
	dataDir, _, _ := startTestServer(t, "--dedup=false")

	out, code := runCLI(t, dataDir, "volume", "create", "vs3", "--size", "1Mi")
	require.Equal(t, 0, code, out)
	out, code = runCLI(t, dataDir, "volume", "write-at", "vs3", "--offset", "0", "--content", "QWERTY")
	require.Equal(t, 0, code, out)

	blockPath := findVolumeBlockOnDisk(t, dataDir, "vs3", 0)
	require.NoError(t, os.Truncate(blockPath, 1))

	out, code = runCLI(t, dataDir, "volume", "scrub", "vs3")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "Detected=1")
	require.Contains(t, out, "Unrepairable=1", "single node has no peer => repair fails; got:\n%s", out)
}

// TestE2E_VolumeScrub_SingleNodeRepairUnrepairable_Dedup — same shape with
// dedup enabled. Exercises RepairReplica's HEAD/peer-pull path against
// canonical _v<UUID> keys; on a single-node cluster the peer pool is empty
// so repair must report Unrepairable rather than crash.
func TestE2E_VolumeScrub_SingleNodeRepairUnrepairable_Dedup(t *testing.T) {
	dataDir, _, _ := startTestServer(t, "--dedup=true")

	out, code := runCLI(t, dataDir, "volume", "create", "vsd3", "--size", "1Mi")
	require.Equal(t, 0, code, out)
	out, code = runCLI(t, dataDir, "volume", "write-at", "vsd3", "--offset", "0", "--content", "QWERTY")
	require.Equal(t, 0, code, out)

	blockPath := findVolumeBlockOnDisk(t, dataDir, "vsd3", 0)
	require.NoError(t, os.Truncate(blockPath, 1))

	out, code = runCLI(t, dataDir, "volume", "scrub", "vsd3")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "Detected=1")
	require.Contains(t, out, "Unrepairable=1", "single node has no peer => repair fails; got:\n%s", out)
}

// TestE2E_VolumeScrub_AdminTriggerWorksAtZeroInterval — regression guard
// for the Director-wiring fix. With --scrub-interval=0 the periodic scrub is
// disabled, but the admin trigger (`grainfs volume scrub <name>`) must still
// work. Pre-fix it returned "scrub director not configured".
func TestE2E_VolumeScrub_AdminTriggerWorksAtZeroInterval(t *testing.T) {
	dataDir, _, _ := startTestServer(t, "--dedup=false", "--scrub-interval=0")

	out, code := runCLI(t, dataDir, "volume", "create", "vsi0", "--size", "1Mi")
	require.Equal(t, 0, code, out)
	out, code = runCLI(t, dataDir, "volume", "write-at", "vsi0", "--offset", "0", "--content", "x")
	require.Equal(t, 0, code, out)

	out, code = runCLI(t, dataDir, "volume", "scrub", "vsi0")
	require.Equal(t, 0, code, out)
	require.NotContains(t, out, "scrub director not configured", "interval=0 must not disable admin trigger; got:\n%s", out)
	require.Contains(t, out, "Detected=0", "healthy volume scrub should detect nothing; got:\n%s", out)
	require.Contains(t, out, "Repaired=0")
}

// TestE2E_VolumeScrub_StatusListCancel — exercise the auxiliary subcommands
// against a known-good session.
func TestE2E_VolumeScrub_StatusListCancel(t *testing.T) {
	dataDir, _, _ := startTestServer(t, "--dedup=false")

	out, code := runCLI(t, dataDir, "volume", "create", "vs4", "--size", "1Mi")
	require.Equal(t, 0, code, out)
	out, code = runCLI(t, dataDir, "volume", "write-at", "vs4", "--offset", "0", "--content", "data")
	require.Equal(t, 0, code, out)

	out, code = runCLI(t, dataDir, "volume", "scrub", "vs4", "--detach")
	require.Equal(t, 0, code, out)
	// Parse session id out of "Triggered scrub: session=<uuid> ..."
	idx := strings.Index(out, "session=")
	require.Greater(t, idx, -1, "no session id in output: %s", out)
	rest := out[idx+len("session="):]
	end := strings.IndexByte(rest, ' ')
	require.Greater(t, end, 0)
	sessionID := rest[:end]

	out, code = runCLI(t, dataDir, "volume", "scrub", "list")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, sessionID)

	out, code = runCLI(t, dataDir, "volume", "scrub", "status", sessionID)
	require.Equal(t, 0, code, out)
	require.Contains(t, out, sessionID)
}
