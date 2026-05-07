package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startUDSStubServer brings up a stub HTTP server bound to a Unix socket.
// Returns the socket path; cleanup is registered via t.Cleanup. CLI tests
// use this so the clusterEndpointFromCmd validation (which rejects http://)
// receives a UDS path rather than an httptest URL.
func startUDSStubServer(t *testing.T, h http.Handler) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "rp-")
	require.NoError(t, err)
	sock := filepath.Join(d, "admin.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	go http.Serve(ln, h) //nolint:errcheck
	t.Cleanup(func() {
		_ = ln.Close()
		_ = os.RemoveAll(d)
	})
	return sock
}

// Thin invocation tests: orchestration logic lives in internal/clusteradmin
// and is tested there. These tests only verify the cobra command wires
// flags + std streams through to clusteradmin.RemovePeer.

func runRemovePeer(t *testing.T, base string, args []string, stdin string) (string, error) {
	t.Helper()
	cmd := clusterRemovePeerCmd()
	// In production --endpoint is a PersistentFlag on clusterCmd; here we
	// register it locally so the standalone subcommand sees the flag value
	// during the test.
	cmd.Flags().String("endpoint", "", "")
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetIn(strings.NewReader(stdin))
	cmd.SetArgs(append([]string{"--endpoint", base}, args...))
	err := cmd.Execute()
	return out.String(), err
}

type stubRemoveServer struct {
	statusBody  map[string]any
	removeBody  map[string]any
	removeCalls atomic.Int32
	lastBody    atomic.Value // map[string]any
}

func removePeerLiveSnapshot(ids ...string) []map[string]any {
	rows := make([]map[string]any, 0, len(ids))
	for i, id := range ids {
		identity := "resolved"
		reason := "raft_append_success"
		if i == 0 {
			identity = "self"
			reason = "self"
		}
		rows = append(rows, map[string]any{
			"peer_id":        id,
			"identity_state": identity,
			"liveness_state": "live",
			"reason":         reason,
		})
	}
	return rows
}

func removePeerSnapshotWithCooldown(self string, livePeers []string, cooldownPeers ...string) []map[string]any {
	rows := removePeerLiveSnapshot(append([]string{self}, livePeers...)...)
	for _, id := range cooldownPeers {
		rows = append(rows, map[string]any{
			"peer_id":        id,
			"identity_state": "resolved",
			"liveness_state": "health_cooldown",
			"reason":         "peer_health_cooldown",
		})
	}
	return rows
}

func (s *stubRemoveServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(s.statusBody)
	})
	mux.HandleFunc("/v1/cluster/remove-peer", func(w http.ResponseWriter, r *http.Request) {
		s.removeCalls.Add(1)
		body, _ := io.ReadAll(r.Body)
		parsed := map[string]any{}
		_ = json.Unmarshal(body, &parsed)
		s.lastBody.Store(parsed)
		_ = json.NewEncoder(w).Encode(s.removeBody)
	})
	return mux
}

func TestCmdRemovePeer_FlagsReachOptions(t *testing.T) {
	stub := &stubRemoveServer{
		statusBody: map[string]any{
			"mode":      "cluster",
			"node_id":   "n1",
			"state":     "Leader",
			"leader_id": "n1",
			"peers":     []string{"n2", "n3"},
			"peer_snapshot": removePeerLiveSnapshot(
				"n1",
				"n2",
				"n3",
			),
		},
		removeBody: map[string]any{"status": "removed", "id": "n3"},
	}
	sock := startUDSStubServer(t, stub.handler())

	out, err := runRemovePeer(t, sock, []string{"n3", "--yes"}, "")
	require.NoError(t, err, out)
	assert.Contains(t, out, "removed n3")
	assert.Equal(t, int32(1), stub.removeCalls.Load(), "POST must reach the server")

	body := stub.lastBody.Load().(map[string]any)
	assert.Equal(t, "n3", body["id"], "args[0] must be wired into id")
	assert.Equal(t, false, body["force"], "default --force=false must propagate")
}

func TestCmdRemovePeer_ForceFlagPropagates(t *testing.T) {
	stub := &stubRemoveServer{
		statusBody: map[string]any{
			"mode":       "cluster",
			"node_id":    "n1",
			"state":      "Leader",
			"leader_id":  "n1",
			"peers":      []string{"n2", "n3"},
			"down_nodes": []string{"n3"},
			"peer_snapshot": removePeerSnapshotWithCooldown(
				"n1",
				[]string{"n2"},
				"n3",
			),
		},
		removeBody: map[string]any{"status": "removed", "id": "n2"},
	}
	sock := startUDSStubServer(t, stub.handler())

	_, err := runRemovePeer(t, sock, []string{"n2", "--force", "--yes"}, "")
	require.NoError(t, err)
	body := stub.lastBody.Load().(map[string]any)
	assert.Equal(t, true, body["force"])
}
