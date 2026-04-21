package e2e

import (
	"encoding/json"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterStatusCLI_NoPeers verifies that `grainfs cluster status` outputs
// the cluster state for a no-peers (singleton) server. After the
// unification, all servers run DistributedBackend so mode is always "cluster";
// singletons are distinguished by peers=0.
func TestClusterStatusCLI_NoPeers(t *testing.T) {
	binary := getBinary()

	out, err := exec.Command(binary, "cluster", "status",
		"--endpoint", testServerURL,
	).Output()
	require.NoError(t, err, "cluster status command must succeed")

	var status map[string]any
	require.NoError(t, json.Unmarshal(out, &status), "output must be valid JSON")

	assert.Equal(t, "cluster", status["mode"], "unified path must report mode=cluster")
	peers, _ := status["peers"].([]any)
	assert.Empty(t, peers, "no-peers server must have empty peer list")
}

// TestClusterStatusCLI_HumanReadable verifies default human-readable output
// when --json is not specified.
func TestClusterStatusCLI_HumanReadable(t *testing.T) {
	binary := getBinary()

	out, err := exec.Command(binary, "cluster", "status",
		"--endpoint", testServerURL,
		"--format", "text",
	).Output()
	require.NoError(t, err, "cluster status command must succeed")

	output := string(out)
	assert.Contains(t, output, "mode", "human-readable output must include mode")
	assert.Contains(t, output, "cluster", "unified path shows cluster mode")
	assert.Contains(t, output, "peers:     0", "singleton reports zero peers")
}
