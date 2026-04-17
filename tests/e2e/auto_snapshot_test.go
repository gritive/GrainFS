package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAutoSnapshot_CreatesSnapshotAutomatically verifies that when started with
// --snapshot-interval, the server creates snapshots automatically.
func TestAutoSnapshot_CreatesSnapshotAutomatically(t *testing.T) {
	binary := getBinary()
	dir, err := os.MkdirTemp("", "grainfs-autosnap-e2e-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	port := freePort()
	cmd := exec.Command(binary, "serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", port),
		"--nfs-port", "0",
		"--snapshot-interval", "500ms",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer cmd.Process.Kill()

	waitForPort(port, 5*time.Second)

	// Wait for at least 2 auto-snapshots (>1s)
	time.Sleep(1500 * time.Millisecond)

	// List snapshots via API
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	resp, err := http.Get(endpoint + "/admin/snapshots")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Parse snapshot list from JSON object
	var result struct {
		Snapshots []map[string]any `json:"snapshots"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.GreaterOrEqual(t, len(result.Snapshots), 2,
		"at least 2 auto-snapshots should have been created after 1.5s with 500ms interval")
}
