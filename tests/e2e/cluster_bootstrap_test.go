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
	"github.com/stretchr/testify/require"
)

// joinViaUDS sends POST /v1/cluster/join to the admin socket and returns the
// HTTP status code and decoded response body.
func joinViaUDS(t testing.TB, sock, peerAddr string) (int, map[string]string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"peer_addr": peerAddr})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost,
		"http://unix/v1/cluster/join", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := iamUDSClient(sock).Do(req)
	require.NoError(t, err)
	ginkgo.DeferCleanup(resp.Body.Close)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out map[string]string
	require.NoError(t, json.Unmarshal(raw, &out))
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
		require.Equal(t, 200, code, "unexpected status; body: %v", body)
		require.Equal(t, "already_member", body["status"],
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
				require.NoError(t, err, "grainfs join attempt %d: %s", i, out)
				require.Contains(t, out, "already_member",
					"attempt %d: expected already_member in output: %s", i, out)
			}()
		}
	}
}

// runGrainFSJoin runs `grainfs join <peerAddr> --endpoint <sock>` and returns
// combined stdout+stderr output.
func runGrainFSJoin(ctx context.Context, sock, peerAddr string) (string, error) {
	cmd := exec.CommandContext(ctx, getBinary(), "join", peerAddr, "--endpoint", sock)
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
		require.NoError(t, adminCreateBucketWithPolicyAttachAny(c.dataDirs, c.saID, "guard-test-bucket", 30*time.Second))

		// Any non-self, non-empty host:port works — the data guard fires before
		// the handler tries to reach the peer.
		sock := filepath.Join(c.dataDirs[0], "admin.sock")
		code, body := joinViaUDS(t, sock, "127.0.0.1:19999")
		require.Equal(t, 409, code, "expected 409 when solo has data and force=false; body: %v", body)
		require.Equal(t, "data_present", body["status"])
		require.Contains(t, body["message"], "force=true",
			"message must hint at --force; got: %s", body["message"])
	}
}
