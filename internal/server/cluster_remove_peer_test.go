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

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/eventstore"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

// fakeClusterInfo is a static stand-in for the raft adapter. Set fields
// directly per test case.
type fakeClusterInfo struct {
	nodeID     string
	state      string
	term       uint64
	leaderID   string
	peers      []string
	livePeers  []string
	peerAddrs  map[string]string
	peerStates map[string]string
	snapshot   []cluster.PeerLivenessRow
}

func (f *fakeClusterInfo) NodeID() string      { return f.nodeID }
func (f *fakeClusterInfo) State() string       { return f.state }
func (f *fakeClusterInfo) Term() uint64        { return f.term }
func (f *fakeClusterInfo) LeaderID() string    { return f.leaderID }
func (f *fakeClusterInfo) Peers() []string     { return f.peers }
func (f *fakeClusterInfo) LivePeers() []string { return f.livePeers }
func (f *fakeClusterInfo) Snapshot() cluster.ClusterStatus {
	peerAddrs := make(map[string]string, len(f.peerAddrs))
	for k, v := range f.peerAddrs {
		peerAddrs[k] = v
	}
	peerStates := make(map[string]string, len(f.peerStates))
	for k, v := range f.peerStates {
		peerStates[k] = v
	}
	return cluster.ClusterStatus{
		PeerSnapshot: append([]cluster.PeerLivenessRow(nil), f.snapshot...),
		PeerAddrs:    peerAddrs,
		PeerStates:   peerStates,
	}
}
func (f *fakeClusterInfo) ObjectIndexSummary(string) cluster.ObjectIndexSummary {
	return cluster.ObjectIndexSummary{}
}
func (f *fakeClusterInfo) PlacementReport(string, string, int) cluster.PlacementReport {
	return cluster.PlacementReport{}
}
func (f *fakeClusterInfo) CapabilityEvidence() map[string]map[string]bool {
	return map[string]map[string]bool{}
}

type fakeTopologyClusterInfo struct {
	*fakeClusterInfo
	assignments map[string]string
	groups      []cluster.ShardGroupEntry
	leaders     map[string]string
}

func (f *fakeTopologyClusterInfo) Snapshot() cluster.ClusterStatus {
	snap := f.fakeClusterInfo.Snapshot()
	snap.BucketAssignments = make(map[string]string, len(f.assignments))
	for k, v := range f.assignments {
		snap.BucketAssignments[k] = v
	}
	snap.ShardGroups = make([]cluster.ShardGroupEntry, 0, len(f.groups))
	for _, group := range f.groups {
		snap.ShardGroups = append(snap.ShardGroups, cluster.ShardGroupEntry{
			ID:      group.ID,
			PeerIDs: append([]string(nil), group.PeerIDs...),
		})
	}
	snap.ShardGroupLeaders = make(map[string]string, len(f.leaders))
	for groupID, leaderID := range f.leaders {
		snap.ShardGroupLeaders[groupID] = leaderID
	}
	return snap
}

type fakeClusterInfoWithoutSnapshot struct {
	nodeID    string
	state     string
	term      uint64
	leaderID  string
	peers     []string
	livePeers []string
}

func (f *fakeClusterInfoWithoutSnapshot) NodeID() string      { return f.nodeID }
func (f *fakeClusterInfoWithoutSnapshot) State() string       { return f.state }
func (f *fakeClusterInfoWithoutSnapshot) Term() uint64        { return f.term }
func (f *fakeClusterInfoWithoutSnapshot) LeaderID() string    { return f.leaderID }
func (f *fakeClusterInfoWithoutSnapshot) Peers() []string     { return f.peers }
func (f *fakeClusterInfoWithoutSnapshot) LivePeers() []string { return f.livePeers }
func (f *fakeClusterInfoWithoutSnapshot) Snapshot() cluster.ClusterStatus {
	return cluster.ClusterStatus{}
}
func (f *fakeClusterInfoWithoutSnapshot) ObjectIndexSummary(string) cluster.ObjectIndexSummary {
	return cluster.ObjectIndexSummary{}
}
func (f *fakeClusterInfoWithoutSnapshot) PlacementReport(string, string, int) cluster.PlacementReport {
	return cluster.PlacementReport{}
}
func (f *fakeClusterInfoWithoutSnapshot) CapabilityEvidence() map[string]map[string]bool {
	return map[string]map[string]bool{}
}

func liveMetaSnapshot(ids ...string) []cluster.PeerLivenessRow {
	rows := make([]cluster.PeerLivenessRow, 0, len(ids))
	for i, id := range ids {
		row := cluster.PeerLivenessRow{
			PeerID:        id,
			IdentityState: cluster.PeerIdentityResolved,
			LivenessState: cluster.PeerLivenessLive,
			Reason:        "raft_append_success",
		}
		if i == 0 {
			row.IdentityState = cluster.PeerIdentitySelf
			row.Reason = "self"
		}
		rows = append(rows, row)
	}
	return rows
}

func snapshotWithCooldown(self string, livePeers []string, cooldownPeers ...string) []cluster.PeerLivenessRow {
	rows := liveMetaSnapshot(append([]string{self}, livePeers...)...)
	for _, id := range cooldownPeers {
		rows = append(rows, cluster.PeerLivenessRow{
			PeerID:        id,
			IdentityState: cluster.PeerIdentityResolved,
			LivenessState: cluster.PeerLivenessHealthCooldown,
			Reason:        "peer_health_cooldown",
		})
	}
	return rows
}

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
	ci      ClusterInfo
	mem     *fakeMembership
	gate    *MutationGate
	events  *eventstore.Store
}

func setupRemovePeerServer(t *testing.T, ci ClusterInfo, mem ClusterMembership) *removePeerHarness {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	bopts := badgerutil.SmallOptions(t.TempDir())
	db, err := badger.Open(bopts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	evStore := eventstore.New(db)

	gate := NewMutationGate(nil)

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	opts := []Option{WithMutationGate(gate), WithEventStore(evStore), withEventQueueSize(testEventQueueSize)}
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

func TestClusterStatusIncludesBucketTopology(t *testing.T) {
	ci := &fakeTopologyClusterInfo{
		fakeClusterInfo: &fakeClusterInfo{
			nodeID:    "node-1",
			state:     "Leader",
			leaderID:  "node-1",
			peers:     []string{"node-2"},
			livePeers: []string{"node-2"},
		},
		assignments: map[string]string{"bench": "group-7"},
		groups: []cluster.ShardGroupEntry{
			{ID: "group-7", PeerIDs: []string{"node-1", "node-2", "node-3"}},
		},
		leaders: map[string]string{"group-7": "node-2"},
	}
	h := setupRemovePeerServer(t, ci, nil)

	resp, err := http.Get(h.baseURL + "/api/cluster/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		BucketAssignments map[string]string `json:"bucket_assignments"`
		ShardGroups       []struct {
			ID       string   `json:"id"`
			PeerIDs  []string `json:"peer_ids"`
			LeaderID string   `json:"leader_id"`
		} `json:"shard_groups"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, map[string]string{"bench": "group-7"}, body.BucketAssignments)
	require.Len(t, body.ShardGroups, 1)
	require.Equal(t, "group-7", body.ShardGroups[0].ID)
	require.Equal(t, []string{"node-1", "node-2", "node-3"}, body.ShardGroups[0].PeerIDs)
	require.Equal(t, "node-2", body.ShardGroups[0].LeaderID)
}

func TestClusterStatus_IncludesPeerAddresses(t *testing.T) {
	h := setupRemovePeerServer(t, &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2"},
		livePeers: []string{"n1", "n2"},
		peerAddrs: map[string]string{"n2": "10.0.0.2:7001"},
	}, nil)

	resp, err := http.Get(h.baseURL + "/api/cluster/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, map[string]any{"n2": "10.0.0.2:7001"}, got["peer_addrs"])
}

func TestClusterStatus_IncludesPeerStates(t *testing.T) {
	h := setupRemovePeerServer(t, &fakeClusterInfo{
		nodeID:     "n1",
		state:      "Leader",
		leaderID:   "n1",
		peers:      []string{"10.0.0.9:7001"},
		livePeers:  []string{"n1", "10.0.0.9:7001"},
		peerStates: map[string]string{"10.0.0.9:7001": "unresolved_legacy"},
	}, nil)

	resp, err := http.Get(h.baseURL + "/api/cluster/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, map[string]any{"10.0.0.9:7001": "unresolved_legacy"}, got["peer_states"])
}

func TestClusterStatus_DerivesLegacyPeerFieldsFromSnapshot(t *testing.T) {
	h := setupRemovePeerServer(t, &fakeClusterInfo{
		nodeID:   "n1",
		state:    "Leader",
		leaderID: "n1",
		peers:    []string{"stale-peer"},
		peerAddrs: map[string]string{
			"stale-peer": "stale-addr",
		},
		peerStates: map[string]string{
			"stale-peer": "down",
		},
		livePeers: []string{"n1", "stale-peer"},
		snapshot: []cluster.PeerLivenessRow{
			{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive, Reason: "self"},
			{PeerID: "n2", RaftAddr: "10.0.0.2:7001", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessConfigured, Reason: "configured"},
			{PeerID: "n3", RaftAddr: "10.0.0.3:7001", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessHealthCooldown, Reason: "peer_health_cooldown"},
			{PeerID: "10.0.0.9:7001", RaftAddr: "10.0.0.9:7001", IdentityState: cluster.PeerIdentityUnresolvedLegacy, LivenessState: cluster.PeerLivenessConfigured, Reason: "identity_unresolved"},
		},
	}, nil)

	resp, err := http.Get(h.baseURL + "/api/cluster/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, []any{"n2", "n3", "10.0.0.9:7001"}, got["peers"])
	require.Equal(t, map[string]any{"n2": "10.0.0.2:7001", "n3": "10.0.0.3:7001", "10.0.0.9:7001": "10.0.0.9:7001"}, got["peer_addrs"])
	require.Equal(t, map[string]any{"n2": "configured", "n3": "health_cooldown", "10.0.0.9:7001": "unresolved_legacy"}, got["peer_states"])
	require.Equal(t, []any{"n3"}, got["down_nodes"], "configured and unresolved legacy rows are not display-down")

	snapshot, ok := got["peer_snapshot"].([]any)
	require.True(t, ok)
	require.Len(t, snapshot, 4, "peer_snapshot includes self")
}

func TestRemovePeer_HappyPath(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},       // remote-only (semantics of cluster.Peers)
		livePeers: []string{"n1", "n2", "n3"}, // includes self per backend.LiveNodes
		snapshot:  liveMetaSnapshot("n1", "n2", "n3"),
	}
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, ci, mem)

	status, body := postRemovePeer(t, h.baseURL, "n3", false)
	assert.Equal(t, http.StatusOK, status, "happy path must return 200, body=%v", body)
	assert.Equal(t, "removed", body["status"])
	assert.Equal(t, "n3", body["id"])
	assert.Equal(t, []string{"n3"}, mem.called(), "engine RemoveVoter must be invoked exactly once with target id")
}

func TestRemovePeer_PreflightCanonicalizesLegacyRaftAddressVoters(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:   "n1",
		state:    "Leader",
		leaderID: "n1",
		// Dynamic-join Raft internals may still expose remote voters by raft
		// address while the peer snapshot has already resolved canonical node
		// IDs for the public admin API.
		peers: []string{"10.0.0.2:7001", "10.0.0.3:7001"},
		snapshot: []cluster.PeerLivenessRow{
			{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive, Reason: "self"},
			{PeerID: "n2", RaftAddr: "10.0.0.2:7001", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive, Reason: "probe_live"},
			{PeerID: "n3", RaftAddr: "10.0.0.3:7001", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive, Reason: "probe_live"},
		},
	}
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, ci, mem)

	status, body := postRemovePeer(t, h.baseURL, "n2", false)
	assert.Equal(t, http.StatusOK, status, "resolved node ID remove must pass preflight, body=%v", body)
	assert.Equal(t, []string{"n2"}, mem.called(), "membership adapter receives the requested canonical node ID")
}

func TestRemovePeer_PeerNotInCluster_Returns404(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},       // remote-only (semantics of cluster.Peers)
		livePeers: []string{"n1", "n2", "n3"}, // includes self per backend.LiveNodes
		snapshot:  liveMetaSnapshot("n1", "n2", "n3"),
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
		snapshot:  snapshotWithCooldown("n1", []string{"n2"}, "n3"),
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
		snapshot:  snapshotWithCooldown("n1", []string{"n2"}, "n3"),
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
		snapshot:  snapshotWithCooldown("n1", []string{"n2"}, "n3"),
	}
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, ci, mem)

	status, body := postRemovePeer(t, h.baseURL, "n2", true)
	// With force, the pre-flight must NOT block. Engine receives the call.
	// (Whether it succeeds is a raft concern; we assert the handler hands off.)
	assert.Equal(t, http.StatusOK, status, "force must bypass pre-flight, body=%v", body)
	assert.Equal(t, []string{"n2"}, mem.called())
}

func TestClusterRemovePeer_UsesSnapshotPreflightForConfiguredUnknown(t *testing.T) {
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, &fakeClusterInfo{
		nodeID:   "n1",
		state:    "Leader",
		leaderID: "n1",
		peers:    []string{"n2", "n3"},
		snapshot: []cluster.PeerLivenessRow{
			{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "n2", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessConfigured},
			{PeerID: "n3", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive},
		},
	}, mem)

	status, body := postRemovePeer(t, h.baseURL, "n3", false)

	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, "quorum would break", body["error"])
	require.Equal(t, float64(2), body["voters_after"])
	require.Equal(t, float64(1), body["alive_after"])
	require.Equal(t, float64(2), body["new_quorum"])
	require.Empty(t, mem.called())
}

func TestClusterRemovePeer_BlocksOnUnresolvedLegacyUnlessTarget(t *testing.T) {
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, &fakeClusterInfo{
		nodeID:   "n1",
		state:    "Leader",
		leaderID: "n1",
		peers:    []string{"n2", "10.0.0.9:7001"},
		snapshot: []cluster.PeerLivenessRow{
			{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "n2", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "10.0.0.9:7001", IdentityState: cluster.PeerIdentityUnresolvedLegacy, LivenessState: cluster.PeerLivenessConfigured},
		},
	}, mem)

	status, body := postRemovePeer(t, h.baseURL, "n2", false)

	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, "membership identity unresolved", body["error"])
	require.Equal(t, []any{"10.0.0.9:7001"}, body["blocking_peers"])
	require.Empty(t, mem.called())
}

func TestClusterRemovePeer_ForceDoesNotBypassUnresolvedLegacyBlocker(t *testing.T) {
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, &fakeClusterInfo{
		nodeID:   "n1",
		state:    "Leader",
		leaderID: "n1",
		peers:    []string{"n2", "10.0.0.9:7001"},
		snapshot: []cluster.PeerLivenessRow{
			{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "n2", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "10.0.0.9:7001", IdentityState: cluster.PeerIdentityUnresolvedLegacy, LivenessState: cluster.PeerLivenessConfigured},
		},
	}, mem)

	status, body := postRemovePeer(t, h.baseURL, "n2", true)

	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, "membership identity unresolved", body["error"])
	require.Equal(t, []any{"10.0.0.9:7001"}, body["blocking_peers"])
	require.Empty(t, mem.called())
}

func TestClusterRemovePeer_AllowsUnresolvedLegacyTarget(t *testing.T) {
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, &fakeClusterInfo{
		nodeID:   "n1",
		state:    "Leader",
		leaderID: "n1",
		peers:    []string{"n2", "10.0.0.9:7001"},
		snapshot: []cluster.PeerLivenessRow{
			{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "n2", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "10.0.0.9:7001", IdentityState: cluster.PeerIdentityUnresolvedLegacy, LivenessState: cluster.PeerLivenessConfigured},
		},
	}, mem)

	status, body := postRemovePeer(t, h.baseURL, "10.0.0.9:7001", false)

	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "removed", body["status"])
	require.Equal(t, []string{"10.0.0.9:7001"}, mem.called())
}

func TestClusterRemovePeer_ForceBypassesSnapshotSafetyButNotMissingTarget(t *testing.T) {
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, &fakeClusterInfo{
		nodeID:   "n1",
		state:    "Leader",
		leaderID: "n1",
		peers:    []string{"n2", "n3"},
		snapshot: []cluster.PeerLivenessRow{
			{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "n2", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessConfigured},
			{PeerID: "n3", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive},
		},
	}, mem)

	status, body := postRemovePeer(t, h.baseURL, "n3", true)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "removed", body["status"])
	require.Equal(t, []string{"n3"}, mem.called())

	status, body = postRemovePeer(t, h.baseURL, "n9", true)
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, "peer not in cluster", body["error"])
	require.Equal(t, []string{"n3"}, mem.called())
}

func TestClusterRemovePeer_BlocksWhenSnapshotUnavailable(t *testing.T) {
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, &fakeClusterInfoWithoutSnapshot{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},
		livePeers: []string{"n1", "n2", "n3"},
	}, mem)

	status, body := postRemovePeer(t, h.baseURL, "n3", true)

	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, "peer snapshot unavailable", body["error"])
	require.Equal(t, "cluster liveness snapshot is required before membership mutation", body["hint"])
	require.Empty(t, mem.called())
}

func TestRemovePeer_NoMembershipController_Returns503(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},       // remote-only (semantics of cluster.Peers)
		livePeers: []string{"n1", "n2", "n3"}, // includes self per backend.LiveNodes
		snapshot:  liveMetaSnapshot("n1", "n2", "n3"),
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
		snapshot:  liveMetaSnapshot("n1", "n2", "n3"),
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
		snapshot:  liveMetaSnapshot("n1", "n2", "n3"),
	}
	mem := &fakeMembership{err: errors.New("conf change in progress")}
	h := setupRemovePeerServer(t, ci, mem)

	status, body := postRemovePeer(t, h.baseURL, "n3", false)
	assert.Equal(t, http.StatusInternalServerError, status, "engine error must surface, body=%v", body)
	assert.Contains(t, fmt.Sprintf("%v", body["error"]), "conf change")
}

// TestRemovePeer_EngineErrNotLeaderRemapsTo409 — leadership can change between
// the up-front state check and the engine call (the leader steps down or the
// network partitions). The engine returns raft.ErrNotLeader; the handler must
// surface that as the same 409 + leader_id shape as the up-front check so the
// CLI client doesn't see two different error shapes for the same condition.
func TestRemovePeer_EngineErrNotLeaderRemapsTo409(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},
		livePeers: []string{"n1", "n2", "n3"},
		snapshot:  liveMetaSnapshot("n1", "n2", "n3"),
	}
	mem := &fakeMembership{err: raft.ErrNotLeader}
	h := setupRemovePeerServer(t, ci, mem)

	status, body := postRemovePeer(t, h.baseURL, "n3", false)
	assert.Equal(t, http.StatusConflict, status, "ErrNotLeader from engine must remap to 409, body=%v", body)
	assert.Equal(t, "n1", body["leader_id"], "leader_id must accompany the 409 so the CLI can redirect")
}

func TestRemovePeer_MalformedBody_Returns400(t *testing.T) {
	ci := &fakeClusterInfo{
		nodeID:    "n1",
		state:     "Leader",
		leaderID:  "n1",
		peers:     []string{"n2", "n3"},       // remote-only (semantics of cluster.Peers)
		livePeers: []string{"n1", "n2", "n3"}, // includes self per backend.LiveNodes
		snapshot:  liveMetaSnapshot("n1", "n2", "n3"),
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
		snapshot:  liveMetaSnapshot("n1", "n2", "n3"),
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
		snapshot:  liveMetaSnapshot("n1", "n2", "n3"),
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
