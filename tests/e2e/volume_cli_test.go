package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// startTestServer launches `grainfs serve` against a freshly-created /tmp data
// directory (Unix-socket paths on macOS are capped at 104 chars, so /var/folders
// TempDir can't host the admin socket). Returns the data dir + HTTP port + a
// teardown closure.
func startTestServer(t *testing.T, extraArgs ...string) (dataDir string, httpPort int, stop func()) {
	return startTestServerOnPort(t, 0, extraArgs...)
}

func startTestServerOnPort(t *testing.T, port int, extraArgs ...string) (dataDir string, httpPort int, stop func()) {
	t.Helper()
	binary := getBinary()
	if _, err := os.Stat(binary); err != nil {
	}

	dir, err := os.MkdirTemp("/tmp", "grainfs-vol-cli-")
	require.NoError(t, err)

	httpPort = port
	if httpPort == 0 {
		httpPort = freePort()
	}
	args := []string{
		"serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", httpPort),
		"--nfs4-port", fmt.Sprintf("%d", freePort()),
		"--nbd-port", fmt.Sprintf("%d", freePort()),
		"--cluster-key", "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
	}
	args = append(args, extraArgs...)

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	stop = func() {
		cancel()
		_ = cmd.Wait()
		_ = os.RemoveAll(dir)
	}
	t.Cleanup(stop)

	waitForPort(t, httpPort, 5*time.Second)
	// Wait for admin.sock too.
	deadline := time.Now().Add(5 * time.Second)
	sock := filepath.Join(dir, "admin.sock")
	for time.Now().Before(deadline) {
		info, err := os.Stat(sock)
		if err == nil && info.Mode()&os.ModeSocket != 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
		if time.Now().After(deadline) {
			t.Fatalf("admin.sock not ready at %s", sock)
		}
	}
	// Wait for the volume bucket's data group to elect a leader. Without
	// leadership, Manager.List forwards through the cluster coordinator and
	// fails with "forward: no reachable peer" on a single-node cluster.
	waitForVolumeReady(t, dir, 30*time.Second)
	return dir, httpPort, stop
}

// waitForVolumeReady retries `volume list` until it returns 0 (cluster ready
// to serve volume bucket queries) or the deadline expires.
func waitForVolumeReady(t *testing.T, dataDir string, timeout time.Duration) {
	t.Helper()
	binary, err := filepath.Abs(getBinary())
	require.NoError(t, err)
	deadline := time.Now().Add(timeout)
	var lastOut string
	for time.Now().Before(deadline) {
		cmd := exec.Command(binary, "volume", "list", "--endpoint", filepath.Join(dataDir, "admin.sock"))
		out, err := cmd.CombinedOutput()
		lastOut = string(out)
		if err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("volume bucket never became ready within %v: last output:\n%s", timeout, lastOut)
}

func runCLI(t testing.TB, dataDir string, args ...string) (stdout string, exitCode int) {
	t.Helper()
	full := append([]string{}, args...)
	if !containsFlag(full, "--endpoint") {
		full = append(full, "--endpoint", filepath.Join(dataDir, "admin.sock"))
	}
	cmd := exec.Command(getBinary(), full...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return string(out), ee.ExitCode()
	}
	t.Fatalf("CLI run failed unexpectedly: %v\n%s", err, out)
	return "", 0
}

func containsFlag(args []string, flag string) bool {
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}

// TestVolumeCLIGuardsE2E groups negative-path checks on the volume CLI /
// data-plane surface that complement TestVolumeE2E's happy-path coverage:
//
//   - CLIHintWhenNoEndpoint: invoking the binary without --endpoint in an
//     empty cwd prints the actionable hint instead of a stack trace. The
//     binary computes this before any server connection, so the assertion
//     is identical on both fixtures — kept under both branches for shape
//     parity with the rest of the suite.
//   - DataPlaneVolumesPathHidden (A6 regression): /volumes/* admin endpoints
//     were removed from the data plane; /volumes/ now falls through to the
//     S3 bucket handler and must not return admin-shaped JSON.
func TestVolumeCLIGuardsE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runVolumeCLIGuardsCases(t, newSingleNodeS3Target())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runVolumeCLIGuardsCases(t, newSharedClusterS3Target(t))
	})
}

func runVolumeCLIGuardsCases(t *testing.T, tgt s3Target) {
	t.Helper()

	t.Run("CLIHintWhenNoEndpoint", func(t *testing.T) {
		cwd, err := os.MkdirTemp("/tmp", "grainfs-noctx-")
		require.NoError(t, err)
		defer os.RemoveAll(cwd)

		binary, err := filepath.Abs(getBinary())
		require.NoError(t, err)
		cmd := exec.Command(binary, "volume", "list")
		cmd.Dir = cwd
		out, err := cmd.CombinedOutput()
		require.Error(t, err)
		require.Contains(t, string(out), "admin endpoint not configured")
		require.Contains(t, string(out), "Hint")
	})

	t.Run("DataPlaneVolumesPathHidden", func(t *testing.T) {
		resp, err := http.Get(tgt.endpoint(0) + "/volumes/")
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		require.NotContains(t, string(body), `"volumes":`,
			"data plane should no longer expose the admin volumes endpoint")
	})
}
