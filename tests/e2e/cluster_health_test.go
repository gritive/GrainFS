package e2e

import (
	"encoding/json"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterHealthCLIE2E verifies `grainfs cluster health` against both
// deployment shapes. Returns 200 and renders quorum + issues regardless
// of singleton vs cluster. With no configured peers the local-mode rule
// does not fire, so issues should be empty (apart from EC degraded if
// backend is degraded).
func runClusterAdminCLIHealth(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runClusterHealthCLICases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runClusterHealthCLICases(t, newSharedClusterS3Target(t))
	})
}

func runClusterHealthCLICases(t *testing.T, tgt s3Target) {
	t.Helper()
	binary := getBinary()
	sock := tgt.adminSockPath()

	t.Run("JSON", func(t *testing.T) {
		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"health", "--format", "json",
		).Output()
		require.NoError(t, err, "cluster health command must succeed")

		var h map[string]any
		require.NoError(t, json.Unmarshal(out, &h), "output must be valid JSON")
		assert.Contains(t, []any{"cluster", "local"}, h["mode"])
		_, ok := h["quorum"].(map[string]any)
		assert.True(t, ok, "quorum object expected")
	})

	t.Run("TextRender", func(t *testing.T) {
		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"health",
		).Output()
		require.NoError(t, err)

		output := string(out)
		assert.Contains(t, output, "mode:")
		assert.Contains(t, output, "ISSUES")
	})
}
