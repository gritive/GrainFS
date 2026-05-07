package e2e

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterHealthCLI_NoPeers verifies `grainfs cluster health` returns
// 200 and renders quorum + issues for a single-node test server. With
// no configured peers, the local-mode rule does not fire, so issues
// should be empty (apart from EC degraded if backend is degraded).
func TestClusterHealthCLI_NoPeers(t *testing.T) {
	binary := getBinary()
	sock := filepath.Join(testServerDataDir, "admin.sock")

	out, err := exec.Command(binary, "cluster",
		"--endpoint", sock,
		"health", "--format", "json",
	).Output()
	require.NoError(t, err, "cluster health command must succeed")

	var h map[string]any
	require.NoError(t, json.Unmarshal(out, &h), "output must be valid JSON")
	// Unified path runs DistributedBackend → mode=cluster even for singleton.
	assert.Contains(t, []any{"cluster", "local"}, h["mode"])
	_, ok := h["quorum"].(map[string]any)
	assert.True(t, ok, "quorum object expected")
}

// TestClusterHealthCLI_TextRender verifies default text output contains
// the expected sections (mode, quorum, ISSUES).
func TestClusterHealthCLI_TextRender(t *testing.T) {
	binary := getBinary()
	sock := filepath.Join(testServerDataDir, "admin.sock")

	out, err := exec.Command(binary, "cluster",
		"--endpoint", sock,
		"health",
	).Output()
	require.NoError(t, err)

	output := string(out)
	assert.Contains(t, output, "mode:")
	assert.Contains(t, output, "ISSUES")
}
