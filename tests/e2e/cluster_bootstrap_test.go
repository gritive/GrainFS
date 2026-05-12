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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// joinViaUDS sends POST /v1/cluster/join to the admin socket and returns the
// HTTP status code and decoded response body.
func joinViaUDS(t *testing.T, sock, peerAddr string) (int, map[string]string) {
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
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out map[string]string
	require.NoError(t, json.Unmarshal(raw, &out))
	return resp.StatusCode, out
}

// TestE2E_Bootstrap_JoinUDS_AlreadyMember verifies that calling the admin UDS
// /v1/cluster/join endpoint on a node that is already part of a multi-node
// cluster returns status "already_member" (no-op, idempotent).
func TestE2E_Bootstrap_JoinUDS_AlreadyMember(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
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

// TestE2E_Bootstrap_JoinCLI_Idempotent verifies that running `grainfs join`
// twice for the same peer on an already-joined node prints "already_member"
// and exits zero both times.
func TestE2E_Bootstrap_JoinCLI_Idempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e")
	}
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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		out, err := runGrainFSJoin(ctx, sock, peerAddr)
		cancel()
		require.NoError(t, err, "grainfs join attempt %d: %s", i, out)
		require.Contains(t, out, "already_member",
			"attempt %d: expected already_member in output: %s", i, out)
	}
}

// runGrainFSJoin runs `grainfs join <peerAddr> --endpoint <sock>` and returns
// combined stdout+stderr output.
func runGrainFSJoin(ctx context.Context, sock, peerAddr string) (string, error) {
	cmd := exec.CommandContext(ctx, getBinary(), "join", peerAddr, "--endpoint", sock)
	out, err := cmd.CombinedOutput()
	return fmt.Sprintf("%s", out), err
}
