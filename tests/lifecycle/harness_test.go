//go:build lifecycle

package lifecycle_test

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/gritive/GrainFS/internal/adminapi"
)

// testingHandle is the narrow interface satisfied by both *testing.T
// and ginkgo.GinkgoT()'s return value. Avoids testing.TB (whose
// unexported private() method bars GinkgoT()).
type testingHandle interface {
	TempDir() string
	Cleanup(func())
	Helper()
}

// Harness drives one external grainfs binary for a single-node test.
// Plan 2 extends this with StartCluster for multi-node bringup.
type Harness struct {
	t        testingHandle
	binary   string
	dataDir  string
	port     int
	proc     *exec.Cmd
	endpoint string
}

// NewHarness allocates state and a free port. Pass GinkgoT() in Ginkgo
// specs; pass *testing.T in plain-testing files.
func NewHarness(t testingHandle) *Harness {
	t.Helper()
	return &Harness{
		t:       t,
		binary:  binaryPath(),
		dataDir: t.TempDir(),
		port:    mustFreePort(),
	}
}

// binaryPath honors the GRAINFS_BINARY env var (set by `make test-lifecycle`),
// falling back to <module-root>/bin/grainfs.
func binaryPath() string {
	if b := os.Getenv("GRAINFS_BINARY"); b != "" {
		return b
	}
	// Walk up from cwd to find the module root (directory containing go.mod),
	// then return <root>/bin/grainfs. This keeps the fallback working when
	// go test sets cwd to the package directory (tests/lifecycle/).
	dir, err := os.Getwd()
	if err == nil {
		for {
			if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
				return filepath.Join(dir, "bin", "grainfs")
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}
	return "bin/grainfs"
}

// mustFreePort grabs a free TCP port for the local server.
func mustFreePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("free port: %v", err))
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// randomHex32 generates a fresh random 32-byte hex string suitable for
// --cluster-key.
func randomHex32() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("random cluster key: %v", err))
	}
	return hex.EncodeToString(b)
}

// StartLocal launches `grainfs serve --data <dir> --port <port>` and
// waits until /api/cluster/status answers 200.
func (h *Harness) StartLocal() error {
	cmd := exec.Command(h.binary, "serve",
		"--data", h.dataDir,
		"--port", fmt.Sprintf("%d", h.port),
		"--cluster-key", randomHex32(),
		"--nfs4-port", "0",
		"--nbd-port", "0",
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start grainfs: %w", err)
	}
	h.proc = cmd
	h.endpoint = fmt.Sprintf("http://127.0.0.1:%d", h.port)
	return h.waitReady(15 * time.Second)
}

func (h *Harness) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(h.endpoint + "/api/cluster/status")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("grainfs not ready after %v", timeout)
}

// Status fetches /api/cluster/status and decodes it.
func (h *Harness) Status() (adminapi.Status, error) {
	resp, err := http.Get(h.endpoint + "/api/cluster/status")
	if err != nil {
		return adminapi.Status{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return adminapi.Status{}, fmt.Errorf("status http %d", resp.StatusCode)
	}
	var s adminapi.Status
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return adminapi.Status{}, fmt.Errorf("decode status: %w", err)
	}
	return s, nil
}

// EndpointURL returns the http://host:port base for S3 / admin clients.
func (h *Harness) EndpointURL() string { return h.endpoint }

// DataDir returns the harness's scratch directory (cleaned up by t.TempDir).
func (h *Harness) DataDir() string { return h.dataDir }

// Stop signals the grainfs process to exit, then reaps it. Cleanup
// helper — call via DeferCleanup(h.Stop) in Ginkgo or t.Cleanup in plain
// testing.
func (h *Harness) Stop() {
	if h.proc == nil || h.proc.Process == nil {
		return
	}
	_ = h.proc.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- h.proc.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = h.proc.Process.Kill()
		<-done
	}
	h.proc = nil
}

// portString returns the harness port as a decimal string for env-var use.
func (h *Harness) portString() string { return fmt.Sprintf("%d", h.port) }

// repoRelPath resolves a repo-root-relative path by walking up from cwd
// until it finds go.mod. Robust against -trimpath builds (which strip
// runtime.Caller paths to module-relative form and would break the
// previous Dir(..)/.. approach).
func repoRelPath(rel string) string {
	cwd, err := os.Getwd()
	if err != nil {
		panic("repoRelPath: getwd: " + err.Error())
	}
	for d := cwd; ; {
		if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
			return filepath.Join(d, rel)
		}
		parent := filepath.Dir(d)
		if parent == d {
			panic("repoRelPath: go.mod not found upward from " + cwd)
		}
		d = parent
	}
}
