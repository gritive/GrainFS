package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/storage"
)

// fakeClusterInfo is a static stand-in for the raft adapter. Set fields
// directly per test case.
type fakeClusterInfo struct {
	nodeID    string
	state     string
	term      uint64
	leaderID  string
	peers     []string
	livePeers []string
}

func (f *fakeClusterInfo) NodeID() string      { return f.nodeID }
func (f *fakeClusterInfo) State() string       { return f.state }
func (f *fakeClusterInfo) Term() uint64        { return f.term }
func (f *fakeClusterInfo) LeaderID() string    { return f.leaderID }
func (f *fakeClusterInfo) Peers() []string     { return f.peers }
func (f *fakeClusterInfo) LivePeers() []string { return f.livePeers }

// fakeMembership records calls and returns a configured error.
type fakeMembership struct {
	mu       sync.Mutex
	calls    []string
	err      error
	blockFor time.Duration
}

func (f *fakeMembership) RemoveVoter(ctx context.Context, id string) error {
	if f.blockFor > 0 {
		select {
		case <-time.After(f.blockFor):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	f.calls = append(f.calls, id)
	f.mu.Unlock()
	return f.err
}

func (f *fakeMembership) called() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

type removePeerHarness struct {
	baseURL string
	ci      *fakeClusterInfo
	mem     *fakeMembership
	gate    *MutationGate
	events  *eventstore.Store
}

func setupRemovePeerServer(t *testing.T, ci *fakeClusterInfo, mem ClusterMembership) *removePeerHarness {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	bopts := badger.DefaultOptions(t.TempDir()).WithLogger(nil)
	db, err := badger.Open(bopts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	evStore := eventstore.New(db)

	gate := NewMutationGate(nil)

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	opts := []Option{WithMutationGate(gate), WithEventStore(evStore)}
	if ci != nil {
		opts = append(opts, WithClusterInfo(ci))
	}
	if mem != nil {
		opts = append(opts, WithClusterMembership(mem))
	}

	srv := New(addr, backend, opts...)
	go srv.Run() //nolint:errcheck
	for i := 0; i < 100; i++ {
		conn, dialErr := net.Dial("tcp", addr)
		if dialErr == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	return &removePeerHarness{
		baseURL: "http://" + addr,
		ci:      ci,
		gate:    gate,
		events:  evStore,
	}
}

// postRemovePeer sends POST /api/cluster/remove-peer with a JSON body and
// returns the status and parsed body.
func postRemovePeer(t *testing.T, base, id string, force bool) (int, map[string]any) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"id": id, "force": force})
	resp, err := http.Post(base+"/api/cluster/remove-peer", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	out := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return resp.StatusCode, out
}

func TestRemovePeer_HappyPath(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},       // remote-only (semantics of cluster.Peers)
		livePeers: []string{"n1", "n2", "n3"}, // includes self per backend.LiveNodes
	}
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, ci, mem)

	status, body := postRemovePeer(t, h.baseURL, "n3", false)
	assert.Equal(t, http.StatusOK, status, "happy path must return 200, body=%v", body)
	assert.Equal(t, "removed", body["status"])
	assert.Equal(t, "n3", body["id"])
	assert.Equal(t, []string{"n3"}, mem.called(), "engine RemoveVoter must be invoked exactly once with target id")
}

func TestRemovePeer_PeerNotInCluster_Returns404(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},       // remote-only (semantics of cluster.Peers)
		livePeers: []string{"n1", "n2", "n3"}, // includes self per backend.LiveNodes
	}
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, ci, mem)

	status, body := postRemovePeer(t, h.baseURL, "n99", false)
	assert.Equal(t, http.StatusNotFound, status, "unknown peer must return 404, body=%v", body)
	assert.Empty(t, mem.called(), "engine must NOT be invoked for unknown peer")
}

func TestRemovePeer_NotLeader_Returns409WithLeaderID(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n2",
		state:     "Follower",
		leaderID:  "n1",
		peers:     []string{"n1", "n3"}, // n2's remote view: peers minus self
		livePeers: []string{"n1", "n2", "n3"},
	}
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, ci, mem)

	status, body := postRemovePeer(t, h.baseURL, "n3", false)
	assert.Equal(t, http.StatusConflict, status, "follower must return 409, body=%v", body)
	assert.Equal(t, "n1", body["leader_id"], "client must learn where to retry")
	assert.Empty(t, mem.called(), "engine must NOT be invoked on follower")
}

func TestRemovePeer_PreflightQuorumBlock(t *testing.T) {
	// 3 voters, 2 alive (n3 dead). Removing n2 (alive) leaves 1 alive in a
	// 2-voter post-config: alive_after=1 < new_quorum=2. Must block.
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},
		livePeers: []string{"n1", "n2"}, // n3 dead
	}
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, ci, mem)

	status, body := postRemovePeer(t, h.baseURL, "n2", false)
	assert.Equal(t, http.StatusConflict, status, "quorum-breaking remove must be blocked, body=%v", body)
	assert.Contains(t, body["error"], "quorum")
	assert.Empty(t, mem.called(), "pre-flight must abort BEFORE engine call")
}

func TestRemovePeer_PreflightAllowsRemovingDeadPeer(t *testing.T) {
	// 3 voters, n3 dead. Removing n3 leaves alive_after=2 in 2-voter post-config:
	// 2 >= new_quorum=2. Must succeed.
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},
		livePeers: []string{"n1", "n2"}, // n3 dead
	}
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, ci, mem)

	status, body := postRemovePeer(t, h.baseURL, "n3", false)
	assert.Equal(t, http.StatusOK, status, "removing dead peer in healthy quorum must succeed, body=%v", body)
	assert.Equal(t, []string{"n3"}, mem.called())
}

func TestRemovePeer_ForceBypassesPreflight(t *testing.T) {
	// Same shape as the quorum-block case, but force=true must let it through.
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},
		livePeers: []string{"n1", "n2"}, // n3 dead
	}
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, ci, mem)

	status, body := postRemovePeer(t, h.baseURL, "n2", true)
	// With force, the pre-flight must NOT block. Engine receives the call.
	// (Whether it succeeds is a raft concern; we assert the handler hands off.)
	assert.Equal(t, http.StatusOK, status, "force must bypass pre-flight, body=%v", body)
	assert.Equal(t, []string{"n2"}, mem.called())
}

func TestRemovePeer_NoMembershipController_Returns503(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},       // remote-only (semantics of cluster.Peers)
		livePeers: []string{"n1", "n2", "n3"}, // includes self per backend.LiveNodes
	}
	h := setupRemovePeerServer(t, ci, nil) // membership controller not wired

	status, body := postRemovePeer(t, h.baseURL, "n3", false)
	assert.Equal(t, http.StatusServiceUnavailable, status, "missing controller must surface as 503, body=%v", body)
}

func TestRemovePeer_MutationGateBlocks(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},       // remote-only (semantics of cluster.Peers)
		livePeers: []string{"n1", "n2", "n3"}, // includes self per backend.LiveNodes
	}
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, ci, mem)

	// Engage the recovery-read-only gate.
	h.gate.SetBlocked(ErrMutationDisabled)

	status, body := postRemovePeer(t, h.baseURL, "n3", false)
	assert.Equal(t, http.StatusServiceUnavailable, status, "mutation-gated server must reject, body=%v", body)
	assert.Empty(t, mem.called(), "engine must NOT run while gate is closed")
}

func TestRemovePeer_EnginePropagatesError(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},       // remote-only (semantics of cluster.Peers)
		livePeers: []string{"n1", "n2", "n3"}, // includes self per backend.LiveNodes
	}
	mem := &fakeMembership{err: errors.New("conf change in progress")}
	h := setupRemovePeerServer(t, ci, mem)

	status, body := postRemovePeer(t, h.baseURL, "n3", false)
	assert.Equal(t, http.StatusInternalServerError, status, "engine error must surface, body=%v", body)
	assert.Contains(t, fmt.Sprintf("%v", body["error"]), "conf change")
}

func TestRemovePeer_MalformedBody_Returns400(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},       // remote-only (semantics of cluster.Peers)
		livePeers: []string{"n1", "n2", "n3"}, // includes self per backend.LiveNodes
	}
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, ci, mem)

	resp, err := http.Post(h.baseURL+"/api/cluster/remove-peer", "application/json", bytes.NewReader([]byte("{not json")))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "malformed JSON must be 400")
}

func TestRemovePeer_EmptyID_Returns400(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},       // remote-only (semantics of cluster.Peers)
		livePeers: []string{"n1", "n2", "n3"}, // includes self per backend.LiveNodes
	}
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, ci, mem)

	status, body := postRemovePeer(t, h.baseURL, "", false)
	assert.Equal(t, http.StatusBadRequest, status, "empty id must be 400, body=%v", body)
	assert.Empty(t, mem.called())
}

func TestRemovePeer_HappyPath_EmitsAuditEvent(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},       // remote-only (semantics of cluster.Peers)
		livePeers: []string{"n1", "n2", "n3"}, // includes self per backend.LiveNodes
	}
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, ci, mem)

	status, _ := postRemovePeer(t, h.baseURL, "n3", false)
	require.Equal(t, http.StatusOK, status)

	// Pull recent events from the store. cluster events CLI relies on this.
	// Allow async event consumer goroutine to drain.
	deadline := time.Now().Add(2 * time.Second)
	var events []eventstore.Event
	for time.Now().Before(deadline) {
		got, qerr := h.events.Query(time.Now().Add(-time.Minute), time.Now().Add(time.Minute), 100, nil)
		require.NoError(t, qerr)
		events = got
		if len(events) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	found := false
	for _, e := range events {
		if e.Action == eventstore.EventActionClusterRemovePeer {
			found = true
			break
		}
	}
	assert.True(t, found, "remove-peer must emit a cluster-remove-peer audit event for cluster events CLI to surface")
}
