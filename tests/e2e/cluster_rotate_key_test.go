package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("Cluster rotate key", func() {
	ginkgo.Context("Cluster3Node", func() {
		ginkgo.It("rotates the cluster key online", func() {
			t := ginkgo.GinkgoTB()

			dir := shortTempDir(t)
			httpPort := freePort()
			raftPort := freePort()
			oldKey := strings.Repeat("a", 64)
			encKeyFile := makeSharedEncryptionKeyFile(t)

			ctx, cancel := context.WithCancel(context.Background())
			ginkgo.DeferCleanup(cancel)

			args := []string{
				"serve",
				"--data", dir,
				"--port", fmt.Sprintf("%d", httpPort),
				"--raft-addr", fmt.Sprintf("127.0.0.1:%d", raftPort),
				"--node-id", "rotate-test",
				"--cluster-key", oldKey,
				"--nfs4-port", "0",
				"--nbd-port", "0",
				"--encryption-key-file", encKeyFile,
				"--scrub-interval", "0",
				"--lifecycle-interval", "0",
			}
			logFile, err := os.CreateTemp("", "rotate-test-*.log")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			ginkgo.DeferCleanup(os.Remove, logFile.Name())

			srv := exec.CommandContext(ctx, getBinary(), args...)
			srv.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			srv.Stdout = logFile
			srv.Stderr = logFile
			gomega.Expect(srv.Start()).To(gomega.Succeed())
			ginkgo.DeferCleanup(func() {
				cancel()
				_ = srv.Wait()
			})

			// Wait for HTTP + rotate.sock to be ready.
			waitHTTPReady(t, httpPort, 20*time.Second)
			waitSocketReady(t, filepath.Join(dir, "rotate.sock"), 10*time.Second)

			// Initial status: must be steady.
			st := runRotateKeyCLI(t, dir, "status")
			gomega.Expect(st.State).To(gomega.Equal("steady"), "initial state should be steady, got %s", st.State)

			// Begin rotation with --generate so the test doesn't hardcode a key.
			beginOut := runRotateKeyCLIBeginGenerate(t, dir)
			gomega.Expect(beginOut.Error).To(gomega.BeEmpty(), "begin error: %s", beginOut.Error)
			gomega.Expect(beginOut.RotationID).NotTo(gomega.BeEmpty())
			gomega.Expect(beginOut.NewSPKI).NotTo(gomega.BeEmpty())
			gomega.Expect(beginOut.NewSPKI).NotTo(gomega.Equal(beginOut.OldSPKI), "OLD and NEW SPKI must differ")

			// Poll until steady-on-NEW (state=steady, OldSPKI = previous NEW). Auto-progress
			// is RotationPhaseGrace=5s per transition; allow 30s slack.
			deadline := time.Now().Add(45 * time.Second)
			var final rotationCLIResp
			for time.Now().Before(deadline) {
				final = runRotateKeyCLI(t, dir, "status")
				if final.State == "steady" && final.OldSPKI == beginOut.NewSPKI {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			gomega.Expect(final.State).To(gomega.Equal("steady"), "expected steady after auto-progress; got state=%s (rotation_id=%s)", final.State, final.RotationID)
			gomega.Expect(final.OldSPKI).To(gomega.Equal(beginOut.NewSPKI), "active SPKI should now be NEW")

			// Verify keystore on disk reflects the rotation.
			currentKeyBytes, err := os.ReadFile(filepath.Join(dir, "keys.d", "current.key"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			currentKey := strings.TrimSpace(string(currentKeyBytes))
			gomega.Expect(currentKey).To(gomega.HaveLen(64), "current.key should be 64 hex chars")
			gomega.Expect(currentKey).NotTo(gomega.Equal(oldKey), "current.key should have rotated away from initial key")

			previousKeyBytes, err := os.ReadFile(filepath.Join(dir, "keys.d", "previous.key"))
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "previous.key should exist after Drop")
			previousKey := strings.TrimSpace(string(previousKeyBytes))
			gomega.Expect(previousKey).To(gomega.Equal(oldKey), "previous.key should hold the OLD PSK")
		})
	})

	ginkgo.Context("SingleNode", func() {
		ginkgo.It("reports steady status on a solo node", func() {
			t := ginkgo.GinkgoTB()

			dir := shortTempDir(t)
			httpPort := freePort()
			raftPort := freePort()
			encKeyFile := makeSharedEncryptionKeyFile(t)

			ctx, cancel := context.WithCancel(context.Background())
			ginkgo.DeferCleanup(cancel)

			args := []string{
				"serve",
				"--data", dir,
				"--port", fmt.Sprintf("%d", httpPort),
				"--raft-addr", fmt.Sprintf("127.0.0.1:%d", raftPort),
				"--node-id", "rotate-status-test",
				"--cluster-key", strings.Repeat("c", 64),
				"--nfs4-port", "0",
				"--nbd-port", "0",
				"--encryption-key-file", encKeyFile,
				"--scrub-interval", "0",
				"--lifecycle-interval", "0",
			}
			logFile, _ := os.CreateTemp("", "rotate-status-*.log")
			ginkgo.DeferCleanup(os.Remove, logFile.Name())

			srv := exec.CommandContext(ctx, getBinary(), args...)
			srv.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			srv.Stdout = logFile
			srv.Stderr = logFile
			gomega.Expect(srv.Start()).To(gomega.Succeed())
			ginkgo.DeferCleanup(func() {
				cancel()
				_ = srv.Wait()
			})

			waitHTTPReady(t, httpPort, 15*time.Second)
			waitSocketReady(t, filepath.Join(dir, "rotate.sock"), 10*time.Second)

			st := runRotateKeyCLI(t, dir, "status")
			gomega.Expect(st.State).To(gomega.Equal("steady"), "solo node should report steady")
			gomega.Expect(st.RotationID).To(gomega.BeEmpty(), "no in-flight rotation")
		})
	})
})

// rotationCLIResp mirrors the CLI's rotationSocketResponse but lives here to
// avoid importing internal cmd packages.
type rotationCLIResp struct {
	State      string `json:"state"`
	RotationID string `json:"rotation_id,omitempty"`
	OldSPKI    string `json:"old_spki,omitempty"`
	NewSPKI    string `json:"new_spki,omitempty"`
	Error      string `json:"error,omitempty"`
}

// runRotateKeyCLI is a thin wrapper that talks to rotate.sock via HTTP/UDS
// (Hertz handlers); we'd use the CLI binary but parsing its human-readable
// output is more brittle than just hitting the socket.
func runRotateKeyCLI(t testing.TB, dataDir, action string, extra ...func(map[string]any)) rotationCLIResp {
	t.Helper()
	sock := filepath.Join(dataDir, "rotate.sock")
	cli := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
		Timeout: 45 * time.Second,
	}

	var resp *http.Response
	var err error
	switch action {
	case "status":
		resp, err = cli.Get("http://unix/v1/rotate-key/status")
	case "begin", "abort":
		body := map[string]any{}
		for _, f := range extra {
			f(body)
		}
		buf, _ := json.Marshal(body)
		resp, err = cli.Post("http://unix/v1/rotate-key/"+action, "application/json", bytes.NewReader(buf))
	default:
		ginkgo.Fail(fmt.Sprintf("unknown rotate-key action %q", action))
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(resp.Body.Close)
	var out rotationCLIResp
	gomega.Expect(json.NewDecoder(resp.Body).Decode(&out)).To(gomega.Succeed())
	return out
}

// runRotateKeyCLIBeginGenerate uses the actual CLI binary so we exercise the
// --generate path end-to-end. Output parsing is minimal: we care that the
// rotation started and the response carried the expected fields.
func runRotateKeyCLIBeginGenerate(t testing.TB, dataDir string) rotationCLIResp {
	t.Helper()
	cmd := exec.Command(getBinary(), "cluster", "rotate-key", "begin", "--generate", "--endpoint", filepath.Join(dataDir, "rotate.sock"))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "cli begin failed: stdout=%s stderr=%s", stdout.String(), stderr.String())
	out := stdout.String()
	// Expected shape:
	//   Generated new PSK: <hex>
	//   Save this securely...
	//   Rotation started: state=begun rotation_id=<hex>
	//     OLD SPKI: <hex>
	//     NEW SPKI: <hex>
	//   Cluster will auto-progress...
	resp := rotationCLIResp{State: "begun"}
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
func shortTempDir(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "rk-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func waitHTTPReady(t testing.TB, port int, timeout time.Duration) {
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
	ginkgo.Fail(fmt.Sprintf("HTTP port %d not ready within %s", port, timeout))
}

func waitSocketReady(t testing.TB, sockPath string, timeout time.Duration) {
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
	ginkgo.Fail(fmt.Sprintf("rotate.sock at %s not ready within %s", sockPath, timeout))
}
