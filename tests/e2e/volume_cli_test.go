package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
		t.Skipf("grainfs binary not found at %s — run `make build`", binary)
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
		cmd := exec.Command(binary, "volume", "list", "--data", dataDir)
		out, err := cmd.CombinedOutput()
		lastOut = string(out)
		if err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("volume bucket never became ready within %v: last output:\n%s", timeout, lastOut)
}

func runCLI(t *testing.T, dataDir string, args ...string) (stdout string, exitCode int) {
	t.Helper()
	full := append([]string{}, args...)
	if !containsFlag(full, "--data") && !containsFlag(full, "--endpoint") {
		full = append(full, "--data", dataDir)
	}
	cmd := exec.Command(getBinary(), full...)
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

func TestE2E_VolumeCLI_FullLifecycle(t *testing.T) {
	dataDir, _, _ := startTestServer(t)

	// 1. list — server may or may not have a "default" volume from NBD wiring.
	// Just confirm it returns 0 (cluster ready and admin reachable).
	out, code := runCLI(t, dataDir, "volume", "list")
	require.Equal(t, 0, code, out)

	// 2. create
	out, code = runCLI(t, dataDir, "volume", "create", "v1", "--size", "1Mi")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, `created "v1"`)

	// 3. info
	out, code = runCLI(t, dataDir, "volume", "info", "v1")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "name:             v1")

	// 4. resize grow
	out, code = runCLI(t, dataDir, "volume", "resize", "v1", "--size", "2Mi")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "resized")

	// 5. snapshot create
	out, code = runCLI(t, dataDir, "volume", "snapshot", "create", "v1")
	require.Equal(t, 0, code, out)
	require.Contains(t, out, "created")

	// 6. delete refused
	_, code = runCLI(t, dataDir, "volume", "delete", "v1")
	require.NotEqual(t, 0, code, "delete with snapshots should fail")

	// 7. delete --force succeeds
	out, code = runCLI(t, dataDir, "volume", "delete", "v1", "--force")
	require.Equal(t, 0, code, out)
}

func TestE2E_VolumeCLI_ShrinkRejected(t *testing.T) {
	dataDir, _, _ := startTestServer(t)

	out, code := runCLI(t, dataDir, "volume", "create", "v1", "--size", "10Mi")
	require.Equal(t, 0, code, out)

	out, code = runCLI(t, dataDir, "volume", "resize", "v1", "--size", "5Mi")
	require.NotEqual(t, 0, code, out)
	require.Contains(t, out, "shrink not supported")
}

func TestE2E_VolumeCLI_NotFound(t *testing.T) {
	dataDir, _, _ := startTestServer(t)
	_, code := runCLI(t, dataDir, "volume", "info", "ghost")
	require.NotEqual(t, 0, code, "info on missing volume should fail")
}

func TestE2E_VolumeCLI_AutoDiscoveryFailureMessage(t *testing.T) {
	// Run CLI without --data, no GRAINFS_DATA, no grainfs.toml in cwd.
	t.Setenv("GRAINFS_DATA", "")
	cwd, err := os.MkdirTemp("/tmp", "grainfs-noctx-")
	require.NoError(t, err)
	defer os.RemoveAll(cwd)

	binary, err := filepath.Abs(getBinary())
	require.NoError(t, err)
	cmd := exec.Command(binary, "volume", "list")
	cmd.Dir = cwd
	out, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(out), "admin socket not found")
	require.Contains(t, string(out), "Hint")
}

func TestE2E_VolumeCLI_NoVolumesViaDataPlane(t *testing.T) {
	// Regression: data-plane /volumes/* admin endpoints must be removed (A6).
	// /volumes/ now falls through to the S3 bucket handler (it matches
	// /:bucket/), so it should NOT return JSON shaped like admin output.
	// We assert the response is not the admin "{"volumes":[...]}" shape.
	_, port, _ := startTestServer(t)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/volumes/", port))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.NotContains(t, string(body), `"volumes":`,
		"data plane should no longer expose the admin volumes endpoint")
}

func TestE2E_Dashboard_TokenURLAndRotate(t *testing.T) {
	publicPort := freePort()
	dataDir, port, _ := startTestServerOnPort(t, publicPort,
		"--public-url", fmt.Sprintf("http://127.0.0.1:%d", publicPort),
	)
	require.Equal(t, publicPort, port)

	// Get token via CLI.
	out1, code := runCLI(t, dataDir, "dashboard", "--json")
	require.Equal(t, 0, code, out1)
	var resp1 struct {
		Token string `json:"token"`
		URL   string `json:"url"`
	}
	require.NoError(t, json.Unmarshal([]byte(out1), &resp1))
	require.NotEmpty(t, resp1.Token)
	require.Contains(t, resp1.URL, "#token="+resp1.Token)

	// Old token works.
	require.True(t, callUI(t, port, resp1.Token) == http.StatusOK)

	// No token → 401.
	require.True(t, callUI(t, port, "") == http.StatusUnauthorized)

	// Rotate.
	out2, code := runCLI(t, dataDir, "dashboard", "--rotate", "--json")
	require.Equal(t, 0, code, out2)
	var resp2 struct {
		Token string `json:"token"`
	}
	require.NoError(t, json.Unmarshal([]byte(out2), &resp2))
	require.NotEqual(t, resp1.Token, resp2.Token)

	// Old token is dead.
	require.True(t, callUI(t, port, resp1.Token) == http.StatusUnauthorized)
	// New token works.
	require.True(t, callUI(t, port, resp2.Token) == http.StatusOK)
}

func callUI(t *testing.T, port int, token string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/ui/api/volumes", port), nil)
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// --- Helpers shared with other e2e tests ---

func freePortStart(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// avoid duplicate decl with existing freePort/getBinary helpers
var _ = freePortStart

// containsAdminSock provides a quick boolean for parsing CLI output that lists
// the dashboard token path.
func containsAdminSock(s string) bool {
	return strings.Contains(s, "admin.sock")
}

var _ = containsAdminSock
