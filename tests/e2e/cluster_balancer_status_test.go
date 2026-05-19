package e2e

import (
	"encoding/json"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterBalancerStatusCLIE2E verifies `cluster balancer status` on
// both single-node and 4-node fixtures. Balancer may be active or not
// depending on harness; both should produce structured output.
func runClusterAdminCLIBalancerStatus(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runClusterBalancerStatusCLICases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runClusterBalancerStatusCLICases(t, newSharedClusterS3Target(t))
	})
}

func runClusterBalancerStatusCLICases(t *testing.T, tgt s3Target) {
	t.Helper()
	binary := getBinary()
	sock := tgt.adminSockPath()

	t.Run("JSON", func(t *testing.T) {
		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"balancer", "status", "--format", "json",
		).Output()
		require.NoError(t, err, "balancer status command must succeed")

		var b map[string]any
		require.NoError(t, json.Unmarshal(out, &b), "output must be valid JSON")
		_, ok := b["available"]
		assert.True(t, ok, "available field expected: %v", b)
	})

	t.Run("TextRender", func(t *testing.T) {
		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"balancer", "status",
		).Output()
		require.NoError(t, err)

		output := string(out)
		assert.Contains(t, output, "balancer:")
	})
}
