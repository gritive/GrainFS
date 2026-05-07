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
	t.Helper()

	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)

	h := server.New(
		server.WithListener(ln),
		server.WithTransport(standard.NewTransporter),
		server.WithHostPorts(""),
	)
	s := &Server{}
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
