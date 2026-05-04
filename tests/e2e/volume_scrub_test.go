package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// findVolumeBlockOnDisk walks dataDir for the on-disk file backing a volume
// block. The actual layout is per-data-group sharded
// ({dataDir}/groups/<gid>/data/__grainfs_volumes/.obj/__vol/<name>/blk_NNN[_vUUID]/current)
// and the block key may carry a versionID suffix on initial allocation, so
// we glob rather than reconstruct.
func findVolumeBlockOnDisk(t *testing.T, dataDir, vol string, blockNum int) string {
	t.Helper()
	want := blockKeyName(blockNum)
	var hit string
	err := filepath.Walk(dataDir, func(p string, info os.FileInfo, werr error) error {
		if werr != nil || info == nil || info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(p, "/current") {
			return nil
		}
		// path ends in "/__vol/<vol>/<keyTail>/current"; keyTail starts with want.
		if !strings.Contains(p, "/__vol/"+vol+"/") {
			return nil
		}
		// extract keyTail
		i := strings.LastIndex(p, "/__vol/"+vol+"/")
		rest := p[i+len("/__vol/"+vol+"/"):]
		end := strings.Index(rest, "/")
		if end < 0 {
			return nil
		}
		keyTail := rest[:end]
		if strings.HasPrefix(keyTail, want) {
			hit = p
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, hit, "could not find on-disk block %s for volume %s under %s", want, vol, dataDir)
	return hit
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
