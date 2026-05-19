package e2e

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterPlacementCLIE2E exercises the `grainfs cluster placement`
// admin CLI. Shared single + shared cluster fixtures — the helper accepts
// either the fallback "single-node mode" / "no shard groups" message or
// the topology-derived placement table, so the same assertions hold on
// both fixtures.
func TestClusterPlacementCLIE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runClusterPlacementCLICases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runClusterPlacementCLICases(t, newSharedClusterS3Target(t))
	})
}

func runClusterPlacementCLICases(t *testing.T, tgt s3Target) {
	t.Helper()
	binary := getBinary()
	sock := tgt.adminSockPath()

	t.Run("NoPlacement", func(t *testing.T) {
		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"placement",
		).Output()
		require.NoError(t, err, "placement command must succeed")

		output := string(out)
		hasFallback := false
		for _, want := range []string{"single-node mode", "no shard groups configured", "SHARD GROUPS", "Desired policy:"} {
			if strings.Contains(output, want) {
				hasFallback = true
				break
			}
		}
		assert.True(t, hasFallback, "expected one of fallback or table render; got: %q", output)
	})

	t.Run("UnknownBucket", func(t *testing.T) {
		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"placement", "no-such-bucket",
		).Output()
		require.NoError(t, err)

		output := string(out)
		assert.True(t,
			strings.Contains(output, "not assigned") ||
				strings.Contains(output, "single-node mode") ||
				strings.Contains(output, "no shard groups configured") ||
				strings.Contains(output, "Bucket: no-such-bucket"),
			"expected not-assigned or fallback; got: %q", output)
	})
}
