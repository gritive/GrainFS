package e2e

import (
	"encoding/json"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterStatusCLIE2E verifies `grainfs cluster status` against both
// deployment shapes. After the DistributedBackend unification, mode is
// always "cluster"; the deployment shape is distinguished by peer count
// (singleton = 0 peers, cluster = N-1 peers).
//
// The cluster CLI uses the admin Unix socket (mode 0660 + admin-group)
// since v0.0.89; HTTP /api/cluster/status remains for the dashboard.
func TestClusterStatusCLIE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runClusterStatusCLICases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		skipIfShort(t, "shared cluster fixture skipped in -short mode")
		runClusterStatusCLICases(t, newSharedClusterS3Target(t))
	})
}

func runClusterStatusCLICases(t *testing.T, tgt s3Target) {
	t.Helper()
	binary := getBinary()
	sock := tgt.adminSockPath()
	expectedPeers := 0
	if tgt.isCluster {
		expectedPeers = tgt.nodes - 1
	}

	t.Run("JSON", func(t *testing.T) {
		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"status", "--format", "json",
		).Output()
		require.NoError(t, err, "cluster status command must succeed")

		var status map[string]any
		require.NoError(t, json.Unmarshal(out, &status), "output must be valid JSON")

		assert.Equal(t, "cluster", status["mode"], "unified path must report mode=cluster")
		peers, _ := status["peers"].([]any)
		assert.Len(t, peers, expectedPeers, "peer count must match deployment shape")
	})

	t.Run("HumanReadable", func(t *testing.T) {
		out, err := exec.Command(binary, "cluster",
			"--endpoint", sock,
			"status", "--format", "text",
		).Output()
		require.NoError(t, err, "cluster status command must succeed")

		output := string(out)
		assert.Contains(t, output, "mode", "human-readable output must include mode")
		assert.Contains(t, output, "cluster", "unified path shows cluster mode")
	})
}
