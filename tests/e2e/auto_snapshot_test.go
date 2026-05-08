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
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--snapshot-interval", "500ms",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	waitForPort(t, port, 5*time.Second)

	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	snapshots := waitForAutoSnapshots(t, endpoint, 2, 10*time.Second)
	assert.GreaterOrEqual(t, len(snapshots), 2,
		"at least 2 auto-snapshots should have been created with 500ms interval")
}

func waitForAutoSnapshots(t *testing.T, endpoint string, want int, timeout time.Duration) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := http.Get(endpoint + "/admin/snapshots")
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}

		var result struct {
			Snapshots []map[string]any `json:"snapshots"`
		}
		if resp.StatusCode == http.StatusOK {
			err = json.NewDecoder(resp.Body).Decode(&result)
		} else {
			err = fmt.Errorf("status %d", resp.StatusCode)
		}
		_ = resp.Body.Close()

		if err == nil && len(result.Snapshots) >= want {
			return result.Snapshots
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("snapshots=%d want=%d", len(result.Snapshots), want)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("auto snapshots did not reach %d within %s: %v", want, timeout, lastErr)
	return nil
}
