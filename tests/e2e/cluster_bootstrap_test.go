package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// joinViaUDS sends POST /v1/cluster/join to the admin socket and returns the
// HTTP status code and decoded response body.
func joinViaUDS(t testing.TB, sock, peerAddr string) (int, map[string]string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"peer_addr": peerAddr})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost,
		"http://unix/v1/cluster/join", bytes.NewReader(body))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	req.Header.Set("Content-Type", "application/json")

	resp, err := iamUDSClient(sock).Do(req)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.DeferCleanup(resp.Body.Close)

	raw, err := io.ReadAll(resp.Body)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var out map[string]string
	gomega.Expect(json.Unmarshal(raw, &out)).To(gomega.Succeed())
	return resp.StatusCode, out
}

var _ = ginkgo.Describe("Cluster bootstrap join", func() {
	ginkgo.Context("Cluster3Node", func() {
		ginkgo.It("blocks join when solo node already has data", func() {
			runClusterBootstrapDataPresentBlocksJoin(ginkgo.GinkgoTB())
		})

		ginkgo.It("keeps the join CLI idempotent", func() {
			runClusterBootstrapJoinCLIIdempotent(ginkgo.GinkgoTB())
		})

		ginkgo.It("returns already_member for a joined node via UDS", func() {
			runClusterBootstrapJoinUDSAlreadyMember(ginkgo.GinkgoTB())
		})
	})
})

func runClusterBootstrapJoinUDSAlreadyMember(t testing.TB) {
	t.Helper()
	{
		c := startE2ECluster(t, e2eClusterOptions{
			Nodes:      2,
			Mode:       ClusterModeDynamicJoin,
			ClusterKey: "E2E-BOOTSTRAP-KEY",
			LogPrefix:  "grainfs-bootstrap-member",
			DisableNFS: true,
			DisableNBD: true,
		})

		sock := filepath.Join(c.dataDirs[0], "admin.sock")
		code, body := joinViaUDS(t, sock, c.raftAddr(1))
		gomega.Expect(code).To(gomega.Equal(200), "unexpected status; body: %v", body)
		gomega.Expect(body["status"]).To(gomega.Equal("already_member"),
			"expected already_member for a joined node; got %v", body)
	}
}

// TestBootstrapJoinCLIIdempotentE2E verifies that running `grainfs join`
// twice for the same peer on an already-joined node prints "already_member"
// and exits zero both times.
func runClusterBootstrapJoinCLIIdempotent(t testing.TB) {
	t.Helper()
	{
		c := startE2ECluster(t, e2eClusterOptions{
			Nodes:      2,
			Mode:       ClusterModeDynamicJoin,
			ClusterKey: "E2E-BOOTSTRAP-CLI-KEY",
			LogPrefix:  "grainfs-bootstrap-cli",
			DisableNFS: true,
			DisableNBD: true,
		})

		sock := filepath.Join(c.dataDirs[0], "admin.sock")
		peerAddr := c.raftAddr(1)

		for i := 1; i <= 2; i++ {
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				out, err := runGrainFSJoin(ctx, sock, peerAddr)
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "grainfs join attempt %d: %s", i, out)
				gomega.Expect(out).To(gomega.ContainSubstring("already_member"),
					"attempt %d: expected already_member in output: %s", i, out)
			}()
		}
	}
}

// runGrainFSJoin runs `grainfs join <peerAddr> --endpoint <sock>` and returns
// combined stdout+stderr output.
func runGrainFSJoin(ctx context.Context, sock, peerAddr string) (string, error) {
	cmd := exec.CommandContext(ctx, getBinary(), "join", peerAddr, "--endpoint", sock, "--confirm-staged-keys")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := cmd.CombinedOutput()
	return fmt.Sprintf("%s", out), err
}

// TestBootstrapDataPresentBlocksJoinE2E verifies that a solo node with
// existing user data rejects a join request with 409 data_present when
// force=false (the default).
func runClusterBootstrapDataPresentBlocksJoin(t testing.TB) {
	t.Helper()
	{
		// Nodes:1 + ClusterModeDynamicJoin → single solo node with admin SA bootstrapped.
		c := startE2ECluster(t, e2eClusterOptions{
			Nodes:      1,
			Mode:       ClusterModeDynamicJoin,
			ClusterKey: "E2E-DATA-GUARD-KEY",
			LogPrefix:  "grainfs-data-guard",
			DisableNFS: true,
			DisableNBD: true,
		})

		// Create a bucket so HasUserData() → true.
		gomega.Expect(adminCreateBucketWithPolicyAttachAny(c.dataDirs, c.saID, "guard-test-bucket", 30*time.Second)).To(gomega.Succeed())

		// Any non-self, non-empty host:port works — the data guard fires before
		// the handler tries to reach the peer.
		sock := filepath.Join(c.dataDirs[0], "admin.sock")
		code, body := joinViaUDS(t, sock, "127.0.0.1:19999")
		gomega.Expect(code).To(gomega.Equal(409), "expected 409 when solo has data and force=false; body: %v", body)
		gomega.Expect(body["status"]).To(gomega.Equal("data_present"))
		gomega.Expect(body["message"]).To(gomega.ContainSubstring("force=true"),
			"message must hint at --force; got: %s", body["message"])
	}
}
