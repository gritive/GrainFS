package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestE2E_RotateKey_HappyPath drives a single-node cluster through a full
// online rotation:
//   - generate a new 32-byte PSK
//   - submit `cluster rotate-key begin --new-key=...`
//   - poll `status` until phase returns to steady (1)
//   - verify keys.d/current.key now contains the NEW key and previous.key has OLD
//
// Total wall time bounded by RotationPhaseGrace (5s × 2 phases) + slack.
func TestE2E_RotateKey_HappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip("rotation test waits ~15s for auto-progress; skip in -short")
	}
	dir := shortTempDir(t)
	httpPort := freePort()
	raftPort := freePort()
	oldKey := strings.Repeat("a", 64)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	args := []string{
		"serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", httpPort),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", raftPort),
		"--node-id", "rotate-test",
		"--cluster-key", oldKey,
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--no-encryption",
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}
	logFile, err := os.CreateTemp("", "rotate-test-*.log")
	require.NoError(t, err)
	defer os.Remove(logFile.Name())

	srv := exec.CommandContext(ctx, getBinary(), args...)
	srv.Stdout = logFile
	srv.Stderr = logFile
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		cancel()
		_ = srv.Wait()
	})

	// Wait for HTTP + rotate.sock to be ready.
	waitHTTPReady(t, httpPort, 20*time.Second)
	waitSocketReady(t, filepath.Join(dir, "rotate.sock"), 10*time.Second)

	// Initial status: must be steady.
	st := runRotateKeyCLI(t, dir, "status")
	require.Equal(t, 1, st.Phase, "initial phase should be steady, got %d", st.Phase)

	// Begin rotation with --generate so the test doesn't hardcode a key.
	beginOut := runRotateKeyCLIBeginGenerate(t, dir)
	require.Empty(t, beginOut.Error, "begin error: %s", beginOut.Error)
	require.NotEmpty(t, beginOut.RotationID)
	require.NotEmpty(t, beginOut.NewSPKI)
	require.NotEqual(t, beginOut.OldSPKI, beginOut.NewSPKI, "OLD and NEW SPKI must differ")

	// Poll until steady-on-NEW (phase=1, OldSPKI = previous NEW). Auto-progress
	// is RotationPhaseGrace=5s per transition; allow 30s slack.
	deadline := time.Now().Add(45 * time.Second)
	var final rotationCLIResp
	for time.Now().Before(deadline) {
		final = runRotateKeyCLI(t, dir, "status")
		if final.Phase == 1 && final.OldSPKI == beginOut.NewSPKI {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.Equal(t, 1, final.Phase, "expected steady after auto-progress; got phase=%d (rotation_id=%s)", final.Phase, final.RotationID)
	require.Equal(t, beginOut.NewSPKI, final.OldSPKI, "active SPKI should now be NEW")

	// Verify keystore on disk reflects the rotation.
	currentKeyBytes, err := os.ReadFile(filepath.Join(dir, "keys.d", "current.key"))
	require.NoError(t, err)
	currentKey := strings.TrimSpace(string(currentKeyBytes))
	require.Len(t, currentKey, 64, "current.key should be 64 hex chars")
	require.NotEqual(t, oldKey, currentKey, "current.key should have rotated away from initial key")

	previousKeyBytes, err := os.ReadFile(filepath.Join(dir, "keys.d", "previous.key"))
	require.NoError(t, err, "previous.key should exist after Drop")
	previousKey := strings.TrimSpace(string(previousKeyBytes))
	require.Equal(t, oldKey, previousKey, "previous.key should hold the OLD PSK")
}

// TestE2E_RotateKey_StatusOnlyOnSoloMode verifies the rotate.sock is reachable
// even on a solo (peers=[]) node and reports steady. Quick smoke before the
// longer happy-path test.
func TestE2E_RotateKey_StatusOnlyOnSoloMode(t *testing.T) {
	dir := shortTempDir(t)
	httpPort := freePort()
	raftPort := freePort()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	args := []string{
		"serve",
		"--data", dir,
		"--port", fmt.Sprintf("%d", httpPort),
		"--raft-addr", fmt.Sprintf("127.0.0.1:%d", raftPort),
		"--node-id", "rotate-status-test",
		"--cluster-key", strings.Repeat("c", 64),
		"--nfs4-port", "0",
		"--nbd-port", "0",
		"--no-encryption",
		"--snapshot-interval", "0",
		"--scrub-interval", "0",
		"--lifecycle-interval", "0",
	}
	logFile, _ := os.CreateTemp("", "rotate-status-*.log")
	defer os.Remove(logFile.Name())

	srv := exec.CommandContext(ctx, getBinary(), args...)
	srv.Stdout = logFile
	srv.Stderr = logFile
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		cancel()
		_ = srv.Wait()
	})

	waitHTTPReady(t, httpPort, 15*time.Second)
	waitSocketReady(t, filepath.Join(dir, "rotate.sock"), 10*time.Second)

	st := runRotateKeyCLI(t, dir, "status")
	require.Equal(t, 1, st.Phase, "solo node should report steady")
	require.Empty(t, st.RotationID, "no in-flight rotation")
}

// rotationCLIResp mirrors the CLI's rotationSocketResponse but lives here to
// avoid importing internal cmd packages.
type rotationCLIResp struct {
	Phase      int    `json:"phase"`
	RotationID string `json:"rotation_id,omitempty"`
	OldSPKI    string `json:"old_spki,omitempty"`
	NewSPKI    string `json:"new_spki,omitempty"`
	Error      string `json:"error,omitempty"`
}

// runRotateKeyCLI is a thin wrapper that talks to rotate.sock directly with
// JSON; we'd use the CLI binary but parsing its human-readable output is more
// brittle than just hitting the socket.
func runRotateKeyCLI(t *testing.T, dataDir, action string, extra ...func(map[string]any)) rotationCLIResp {
	t.Helper()
	conn, err := net.DialTimeout("unix", filepath.Join(dataDir, "rotate.sock"), 5*time.Second)
	require.NoError(t, err)
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(45 * time.Second))
	req := map[string]any{"action": action}
	for _, f := range extra {
		f(req)
	}
	require.NoError(t, json.NewEncoder(conn).Encode(req))
	var resp rotationCLIResp
	require.NoError(t, json.NewDecoder(conn).Decode(&resp))
	return resp
}

// runRotateKeyCLIBeginGenerate uses the actual CLI binary so we exercise the
// --generate path end-to-end. Output parsing is minimal: we care that the
// rotation started and the response carried the expected fields.
func runRotateKeyCLIBeginGenerate(t *testing.T, dataDir string) rotationCLIResp {
	t.Helper()
	cmd := exec.Command(getBinary(), "cluster", "rotate-key", "begin", "--generate", "--data", dataDir)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	require.NoError(t, err, "cli begin failed: stdout=%s stderr=%s", stdout.String(), stderr.String())
	out := stdout.String()
	// Expected shape:
	//   Generated new PSK: <hex>
	//   Save this securely...
	//   Rotation started: phase=2 rotation_id=<hex>
	//     OLD SPKI: <hex>
	//     NEW SPKI: <hex>
	//   Cluster will auto-progress...
	resp := rotationCLIResp{Phase: 2}
	for _, line := range strings.Split(out, "\n") {
		switch {
		case strings.HasPrefix(line, "Rotation started: "):
			parts := strings.Fields(line)
			for _, p := range parts {
				if strings.HasPrefix(p, "rotation_id=") {
					resp.RotationID = strings.TrimPrefix(p, "rotation_id=")
				}
			}
		case strings.HasPrefix(strings.TrimSpace(line), "OLD SPKI:"):
			resp.OldSPKI = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "OLD SPKI:"))
		case strings.HasPrefix(strings.TrimSpace(line), "NEW SPKI:"):
			resp.NewSPKI = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "NEW SPKI:"))
		}
	}
	return resp
}

// shortTempDir returns a tempdir at /tmp/<short-prefix> rather than using
// t.TempDir() which on macOS produces /var/folders/.../<long-test-name>/...
// paths that exceed the 104-byte UNIX_PATH_MAX. rotate.sock + a long path
// silently fails listen with "invalid argument".
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "rk-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func waitHTTPReady(t *testing.T, port int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 1*time.Second)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("HTTP port %d not ready within %s", port, timeout)
}

func waitSocketReady(t *testing.T, sockPath string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("unix", sockPath, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("rotate.sock at %s not ready within %s", sockPath, timeout)
}
