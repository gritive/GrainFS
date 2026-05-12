package serveruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
)

// fakeClusterNodes implements clusterNodes for tests.
type fakeClusterNodes struct {
	nodes []cluster.MetaNodeEntry
}

func (f *fakeClusterNodes) Nodes() []cluster.MetaNodeEntry { return f.nodes }

func soloNodes() *fakeClusterNodes {
	return &fakeClusterNodes{nodes: []cluster.MetaNodeEntry{{ID: "n1"}}}
}

func multiNodes() *fakeClusterNodes {
	return &fakeClusterNodes{nodes: []cluster.MetaNodeEntry{{ID: "n1"}, {ID: "n2"}}}
}

// startJoinHandlerTestServer spins up a Hertz UDS server with JoinHandler
// registered and returns an HTTP client that dials over the socket.
func startJoinHandlerTestServer(t *testing.T, h *JoinHandler) *http.Client {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "gs-join-uds-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	sock := filepath.Join(d, "a.sock")

	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)

	srv := hzserver.New(
		hzserver.WithListener(ln),
		hzserver.WithTransport(standard.NewTransporter),
		hzserver.WithHostPorts(""),
	)
	srv.POST("/v1/cluster/join", h.Handle)
	go srv.Spin() //nolint:errcheck
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
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

func joinPost(t *testing.T, cli *http.Client, body string) (int, JoinResponse) {
	t.Helper()
	resp, err := cli.Post("http://local/v1/cluster/join", "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	var jr JoinResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&jr))
	return resp.StatusCode, jr
}

func TestJoinHandler_Handle_InvalidJSON(t *testing.T) {
	dataDir := t.TempDir()
	h := &JoinHandler{dataDir: dataDir, raftAddr: "127.0.0.1:7001", cancel: func() {}, nodes: soloNodes()}
	cli := startJoinHandlerTestServer(t, h)

	code, jr := joinPost(t, cli, "not-json{{{")
	assert.Equal(t, 400, code)
	assert.Equal(t, "error", jr.Status)
}

func TestJoinHandler_Handle_EmptyPeerAddr(t *testing.T) {
	dataDir := t.TempDir()
	h := &JoinHandler{dataDir: dataDir, raftAddr: "127.0.0.1:7001", cancel: func() {}, nodes: soloNodes()}
	cli := startJoinHandlerTestServer(t, h)

	body, _ := json.Marshal(JoinRequest{PeerAddr: "   "})
	code, jr := joinPost(t, cli, string(body))
	assert.Equal(t, 400, code)
	assert.Equal(t, "error", jr.Status)
}

func TestJoinHandler_Handle_AlreadyMember(t *testing.T) {
	dataDir := t.TempDir()
	h := &JoinHandler{dataDir: dataDir, raftAddr: "127.0.0.1:7001", cancel: func() {}, nodes: multiNodes()}
	cli := startJoinHandlerTestServer(t, h)

	body, _ := json.Marshal(JoinRequest{PeerAddr: "127.0.0.1:8001"})
	code, jr := joinPost(t, cli, string(body))
	assert.Equal(t, 200, code)
	assert.Equal(t, "already_member", jr.Status)
}

func TestJoinHandler_Handle_Self(t *testing.T) {
	dataDir := t.TempDir()
	h := &JoinHandler{dataDir: dataDir, raftAddr: "127.0.0.1:7001", cancel: func() {}, nodes: soloNodes()}
	cli := startJoinHandlerTestServer(t, h)

	body, _ := json.Marshal(JoinRequest{PeerAddr: "127.0.0.1:7001"})
	code, jr := joinPost(t, cli, string(body))
	assert.Equal(t, 200, code)
	assert.Equal(t, "self", jr.Status)
}

func TestJoinHandler_Handle_HappyPath(t *testing.T) {
	dataDir := t.TempDir()
	cancelCalled := make(chan struct{}, 1)
	h := &JoinHandler{
		dataDir:  dataDir,
		raftAddr: "127.0.0.1:7001",
		cancel:   func() { cancelCalled <- struct{}{} },
		nodes:    soloNodes(),
	}
	cli := startJoinHandlerTestServer(t, h)

	body, _ := json.Marshal(JoinRequest{PeerAddr: "127.0.0.1:8001"})
	code, jr := joinPost(t, cli, string(body))

	assert.Equal(t, 200, code)
	assert.Equal(t, "restart_initiated", jr.Status)

	// .join-pending file must be written with the peer address.
	data, err := os.ReadFile(filepath.Join(dataDir, joinPendingFile))
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:8001", string(bytes.TrimSpace(data)))

	// cancel() should be called (within 500ms for the 150ms goroutine sleep).
	select {
	case <-cancelCalled:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cancel() was not called after restart_initiated")
	}
}

func TestJoinHandler_Handle_WriteFileFails(t *testing.T) {
	// Use a non-existent/non-writable dataDir to force WriteFile failure.
	h := &JoinHandler{
		dataDir:  "/nonexistent/path/that/cannot/exist",
		raftAddr: "127.0.0.1:7001",
		cancel:   func() {},
		nodes:    soloNodes(),
	}
	// Start server with a real temp dir for the socket.
	cli := startJoinHandlerTestServer(t, h)

	body, _ := json.Marshal(JoinRequest{PeerAddr: "127.0.0.1:8001"})
	code, jr := joinPost(t, cli, string(body))
	assert.Equal(t, 500, code)
	assert.Equal(t, "error", jr.Status)
}

func TestJoinHandler_isSelf(t *testing.T) {
	h := &JoinHandler{raftAddr: "127.0.0.1:8301"}
	assert.True(t, h.isSelf("127.0.0.1:8301"))
	assert.False(t, h.isSelf("127.0.0.1:8302"))
	assert.False(t, h.isSelf("192.168.1.1:8301"))
}

func TestWipeSoloRaftState(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "meta_raft", "data"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "raft", "raft-v2"), 0o755))

	require.NoError(t, wipeSoloRaftState(dir))

	_, err := os.Stat(filepath.Join(dir, "meta_raft"))
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(dir, "meta_raft.pre-join-backup"))
	assert.NoError(t, err)
}

func TestWipeSoloRaftState_Idempotent(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "meta_raft"), 0o755))

	require.NoError(t, wipeSoloRaftState(dir))
	require.NoError(t, wipeSoloRaftState(dir))
}
