package e2e

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterBalancerStatusCLI verifies `cluster balancer status` returns
// 200 + a valid response on the test server (balancer may be active or
// not depending on harness; both should produce structured output).
func TestClusterBalancerStatusCLI(t *testing.T) {
	binary := getBinary()
	sock := filepath.Join(testServerDataDir, "admin.sock")

	out, err := exec.Command(binary, "cluster",
		"--endpoint", sock,
		"balancer", "status", "--format", "json",
	).Output()
	require.NoError(t, err, "balancer status command must succeed")

	var b map[string]any
	require.NoError(t, json.Unmarshal(out, &b), "output must be valid JSON")
	_, ok := b["available"]
	assert.True(t, ok, "available field expected: %v", b)
}

// TestClusterBalancerStatusCLI_TextRender verifies text output renders
// one of the expected status lines (active/disabled/not available).
func TestClusterBalancerStatusCLI_TextRender(t *testing.T) {
	binary := getBinary()
	sock := filepath.Join(testServerDataDir, "admin.sock")

	out, err := exec.Command(binary, "cluster",
		"--endpoint", sock,
		"balancer", "status",
	).Output()
	require.NoError(t, err)

	output := string(out)
	assert.Contains(t, output, "balancer:")
}
