package e2e

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterPlacementCLI_NoPlacement verifies that `grainfs cluster
// placement` on a fresh test server (no shard groups configured) prints
// the expected fallback message rather than failing.
func TestClusterPlacementCLI_NoPlacement(t *testing.T) {
	binary := getBinary()
	sock := filepath.Join(testServerDataDir, "admin.sock")

	out, err := exec.Command(binary, "cluster",
		"--endpoint", sock,
		"placement",
	).Output()
	require.NoError(t, err, "placement command must succeed")

	output := string(out)
	// Either the fallback text or the topology-derived placement report is
	// acceptable depending on harness mode.
	hasFallback := false
	for _, want := range []string{"single-node mode", "no shard groups configured", "SHARD GROUPS", "Desired policy:"} {
		if strings.Contains(output, want) {
			hasFallback = true
			break
		}
	}
	assert.True(t, hasFallback, "expected one of fallback or table render; got: %q", output)
}

// TestClusterPlacementCLI_UnknownBucket verifies `placement <bucket>` for
// an unconfigured bucket prints the not-assigned message and exits 0.
func TestClusterPlacementCLI_UnknownBucket(t *testing.T) {
	binary := getBinary()
	sock := filepath.Join(testServerDataDir, "admin.sock")

	out, err := exec.Command(binary, "cluster",
		"--endpoint", sock,
		"placement", "no-such-bucket",
	).Output()
	require.NoError(t, err)

	output := string(out)
	// On local/empty harness the "no shard groups" path runs first.
	assert.True(t,
		strings.Contains(output, "not assigned") ||
			strings.Contains(output, "single-node mode") ||
			strings.Contains(output, "no shard groups configured") ||
			strings.Contains(output, "Bucket: no-such-bucket"),
		"expected not-assigned or fallback; got: %q", output)
}
