package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestRegisterClusterAdminUDS_RoutesRespond verifies that all three routes
// (status, remove-peer, eventlog) register and respond on a UDS Hertz
// instance. With minimal Server{} (no cluster, no evStore, no membership):
//   - status      → 200 with mode="local"
//   - eventlog    → 200 with empty array
//   - remove-peer → 503 (membership not configured)
func TestRegisterClusterAdminUDS_RoutesRespond(t *testing.T) {
	sock := udsTestSocket(t)
	cli := startUDSAdminTestServer(t, sock)

	// /v1/cluster/status (cluster=nil → mode=local)
	resp, err := cli.Get("http://unix/v1/cluster/status")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode, "status body: %s", body)
	var status map[string]any
	require.NoError(t, json.Unmarshal(body, &status))
	require.Equal(t, "local", status["mode"])

	// /v1/cluster/eventlog (evStore=nil → empty array)
	resp, err = cli.Get("http://unix/v1/cluster/eventlog")
	require.NoError(t, err)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode, "eventlog body: %s", body)
	var events []any
	require.NoError(t, json.Unmarshal(body, &events))
	require.Empty(t, events)

	// /v1/cluster/remove-peer (membership=nil → 503)
	resp, err = cli.Post("http://unix/v1/cluster/remove-peer", "application/json",
		bytes.NewReader([]byte(`{"id":"node-x"}`)))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// TestRegisterClusterAdminUDS_TransferLeader_NoCluster verifies that with
// cluster=nil the route returns 503 (cluster mode not configured).
func TestRegisterClusterAdminUDS_TransferLeader_NoCluster(t *testing.T) {
	sock := udsTestSocket(t)
	cli := startUDSAdminTestServer(t, sock)

	resp, err := cli.Post("http://unix/v1/cluster/transfer-leader", "application/json",
		bytes.NewReader([]byte(`{}`)))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// TestRegisterClusterAdminUDS_TransferLeader_NotLeader verifies 409 +
// leader_id when the node is not currently the leader.
func TestRegisterClusterAdminUDS_TransferLeader_NotLeader(t *testing.T) {
	s := &Server{cluster: &fakeTransferLeader{
		fakeClusterInfo: &fakeClusterInfo{leaderID: "node-2", nodeID: "node-1"},
		isLeader:        false,
	}}
	sock := udsTestSocket(t)
	cli := startUDSAdminTestServerWithSrv(t, sock, s)

	resp, err := cli.Post("http://unix/v1/cluster/transfer-leader", "application/json",
		bytes.NewReader([]byte(`{}`)))
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))
	require.Equal(t, "node-2", parsed["leader_id"])
}

// TestRegisterClusterAdminUDS_TransferLeader_NoPeers verifies 503 when
// the leader has no peers (single-node cluster).
func TestRegisterClusterAdminUDS_TransferLeader_NoPeers(t *testing.T) {
	s := &Server{cluster: &fakeTransferLeader{
		fakeClusterInfo: &fakeClusterInfo{leaderID: "node-1", nodeID: "node-1", term: 5},
		isLeader:        true,
		err:             raft.ErrNoPeers,
	}}
	sock := udsTestSocket(t)
	cli := startUDSAdminTestServerWithSrv(t, sock, s)

	resp, err := cli.Post("http://unix/v1/cluster/transfer-leader", "application/json",
		bytes.NewReader([]byte(`{}`)))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// TestRegisterClusterAdminUDS_TransferLeader_Race verifies 409 + retry=true
// when raft.ErrNotLeader is returned (race during transfer).
func TestRegisterClusterAdminUDS_TransferLeader_Race(t *testing.T) {
	s := &Server{cluster: &fakeTransferLeader{
		fakeClusterInfo: &fakeClusterInfo{leaderID: "node-3", nodeID: "node-1", term: 5},
		isLeader:        true,
		err:             raft.ErrNotLeader,
	}}
	sock := udsTestSocket(t)
	cli := startUDSAdminTestServerWithSrv(t, sock, s)

	resp, err := cli.Post("http://unix/v1/cluster/transfer-leader", "application/json",
		bytes.NewReader([]byte(`{}`)))
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))
	require.Equal(t, true, parsed["retry"])
}

// TestRegisterClusterAdminUDS_TransferLeader_Happy verifies 200 + payload.
func TestRegisterClusterAdminUDS_TransferLeader_Happy(t *testing.T) {
	s := &Server{cluster: &fakeTransferLeader{
		fakeClusterInfo: &fakeClusterInfo{leaderID: "node-1", nodeID: "node-1", term: 8},
		isLeader:        true,
	}}
	sock := udsTestSocket(t)
	cli := startUDSAdminTestServerWithSrv(t, sock, s)

	resp, err := cli.Post("http://unix/v1/cluster/transfer-leader", "application/json",
		bytes.NewReader([]byte(`{}`)))
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var parsed map[string]any
	require.NoError(t, json.Unmarshal(body, &parsed))
	require.Equal(t, "node-1", parsed["old_leader"])
	require.Equal(t, float64(8), parsed["term"])
}

// fakeTransferLeader composes fakeClusterInfo and adds the optional
// IsLeader/TransferLeadership methods so the handler can detect and
// invoke them via clusterTransferLeader interface.
type fakeTransferLeader struct {
	*fakeClusterInfo
	isLeader bool
	err      error
}

func (f *fakeTransferLeader) IsLeader() bool            { return f.isLeader }
func (f *fakeTransferLeader) TransferLeadership() error { return f.err }

// TestRegisterClusterAdminUDS_NoLocalhostOnly verifies the UDS routes
// don't apply localhostOnly() middleware. If it were applied,
// remove-peer would return 403; we expect 503 (membership unwired)
// instead — proving the middleware was bypassed.
func TestRegisterClusterAdminUDS_NoLocalhostOnly(t *testing.T) {
	sock := udsTestSocket(t)
	cli := startUDSAdminTestServer(t, sock)

	resp, err := cli.Post("http://unix/v1/cluster/remove-peer", "application/json",
		bytes.NewReader([]byte(`{"id":"node-x"}`)))
	require.NoError(t, err)
	resp.Body.Close()
	require.NotEqual(t, http.StatusForbidden, resp.StatusCode,
		"localhostOnly() should not be applied to UDS routes")
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// startUDSAdminTestServer brings up a minimal Hertz instance bound to
// the given UDS path with RegisterClusterAdminUDS wired. Returns an
// http.Client that dials the socket. The server is shut down at test end.
func startUDSAdminTestServer(t *testing.T, sock string) *http.Client {
	return startUDSAdminTestServerWithSrv(t, sock, &Server{})
}

// startUDSAdminTestServerWithSrv is like startUDSAdminTestServer but
// allows callers to inject a *Server populated with custom fields
// (e.g., a fake cluster) so handler logic that branches on s.cluster
// can be exercised end-to-end through the UDS routes.
func startUDSAdminTestServerWithSrv(t *testing.T, sock string, s *Server) *http.Client {
	t.Helper()

	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)

	h := server.New(
		server.WithListener(ln),
		server.WithTransport(standard.NewTransporter),
		server.WithHostPorts(""),
	)
	s.RegisterClusterAdminUDS(h)

	go h.Spin() //nolint:errcheck
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = h.Shutdown(ctx)
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, dialErr := net.Dial("unix", sock)
		if dialErr == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
		Timeout: 5 * time.Second,
	}
}

func udsTestSocket(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "gs-uds-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return filepath.Join(d, "a.sock")
}
