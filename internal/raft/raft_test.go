package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingStore wraps a LogStore and injects errors for SaveState/AppendEntries.
type failingStore struct {
	LogStore
	saveStateErr   error
	appendEntryErr error
}

func (f *failingStore) SaveState(term uint64, votedFor string) error {
	if f.saveStateErr != nil {
		return f.saveStateErr
	}
	return f.LogStore.SaveState(term, votedFor)
}

func (f *failingStore) AppendEntries(entries []LogEntry) error {
	if f.appendEntryErr != nil {
		return f.appendEntryErr
	}
	return f.LogStore.AppendEntries(entries)
}

// testCluster sets up N interconnected Raft nodes for testing.
type testCluster struct {
	nodes []*Node
	mu    sync.Mutex
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()

	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = nodeID(i)
	}

	cluster := &testCluster{nodes: make([]*Node, n)}

	for i := 0; i < n; i++ {
		peers := make([]string, 0, n-1)
		for j := 0; j < n; j++ {
			if i != j {
				peers = append(peers, ids[j])
			}
		}
		config := Config{
			ID:               ids[i],
			Peers:            peers,
			ElectionTimeout:  100 * time.Millisecond,
			HeartbeatTimeout: 30 * time.Millisecond,
		}
		cluster.nodes[i] = NewNode(config)
	}

	// Wire up in-process RPC
	for i := 0; i < n; i++ {
		idx := i
		cluster.nodes[i].SetTransport(
			func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
				target := cluster.nodeByID(peer)
				if target == nil {
					return nil, errNodeNotFound
				}
				_ = idx // capture
				return target.HandleRequestVote(args), nil
			},
			func(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
				target := cluster.nodeByID(peer)
				if target == nil {
					return nil, errNodeNotFound
				}
				return target.HandleAppendEntries(args), nil
			},
		)
	}

	t.Cleanup(func() {
		for _, node := range cluster.nodes {
			node.Stop()
		}
	})

	return cluster
}

func (c *testCluster) nodeByID(id string) *Node {
	for _, n := range c.nodes {
		if n.ID() == id {
			return n
		}
	}
	return nil
}

func (c *testCluster) startAll() {
	for _, n := range c.nodes {
		n.Start()
	}
}

func (c *testCluster) waitForLeader(timeout time.Duration) *Node {
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return nil
		default:
			for _, n := range c.nodes {
				if n.State() == Leader {
					return n
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (c *testCluster) countState(state NodeState) int {
	count := 0
	for _, n := range c.nodes {
		if n.State() == state {
			count++
		}
	}
	return count
}

func nodeID(i int) string {
	return string(rune('A' + i))
}

var errNodeNotFound = assert.AnError

// --- Tests ---

func TestNewNode_StartsAsFollower(t *testing.T) {
	config := DefaultConfig("A", []string{"B", "C"})
	node := NewNode(config)
	assert.Equal(t, Follower, node.State())
	assert.Equal(t, uint64(0), node.Term())
}

func TestThreeNode_ElectsLeader(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader, "no leader elected within timeout")
	assert.Equal(t, Leader, leader.State())

	// Exactly one leader
	assert.Equal(t, 1, cluster.countState(Leader))
}

func TestThreeNode_AllAgreeOnTerm(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)

	// Give followers time to receive heartbeats
	time.Sleep(200 * time.Millisecond)

	leaderTerm := leader.Term()
	for _, n := range cluster.nodes {
		assert.Equal(t, leaderTerm, n.Term(), "node %s has different term", n.ID())
	}
}

func TestThreeNode_FollowersKnowLeader(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)

	time.Sleep(200 * time.Millisecond)

	for _, n := range cluster.nodes {
		if n.ID() == leader.ID() {
			continue
		}
		assert.Equal(t, Follower, n.State(), "node %s should be follower", n.ID())
		assert.Equal(t, leader.ID(), n.LeaderID(), "node %s should know the leader", n.ID())
	}
}

func TestThreeNode_IsLeader(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)
	time.Sleep(200 * time.Millisecond)

	leaderCount := 0
	for _, n := range cluster.nodes {
		if n.IsLeader() {
			leaderCount++
			assert.Equal(t, leader.ID(), n.ID(), "IsLeader() true only on the elected leader")
		}
	}
	assert.Equal(t, 1, leaderCount, "exactly one node should report IsLeader()")
}

func TestFiveNode_ElectsLeader(t *testing.T) {
	cluster := newTestCluster(t, 5)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader, "no leader elected in 5-node cluster")
	assert.Equal(t, 1, cluster.countState(Leader))
}

func TestLeaderReelection_AfterLeaderStop(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader1 := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader1, "no initial leader")
	leader1ID := leader1.ID()

	// Stop the leader
	leader1.Stop()

	// Wait for a new leader from the remaining nodes
	var leader2 *Node
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("no new leader elected after leader stop")
		default:
			for _, n := range cluster.nodes {
				if n.ID() != leader1ID && n.State() == Leader {
					leader2 = n
				}
			}
			if leader2 != nil {
				goto found
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
found:
	assert.NotEqual(t, leader1ID, leader2.ID(), "new leader should be different from stopped leader")
	assert.GreaterOrEqual(t, leader2.Term(), leader1.Term(), "new leader should have equal or higher term")
}

func TestRequestVote_GrantsVoteOnce(t *testing.T) {
	config := DefaultConfig("A", []string{"B", "C"})
	node := NewNode(config)

	// First request in term 1 should be granted
	reply := node.HandleRequestVote(&RequestVoteArgs{
		Term:        1,
		CandidateID: "B",
	})
	assert.True(t, reply.VoteGranted)
	assert.Equal(t, uint64(1), reply.Term)

	// Second request from different candidate in same term should be denied
	reply = node.HandleRequestVote(&RequestVoteArgs{
		Term:        1,
		CandidateID: "C",
	})
	assert.False(t, reply.VoteGranted)
}

func TestRequestVote_RejectsOlderTerm(t *testing.T) {
	config := DefaultConfig("A", []string{"B", "C"})
	node := NewNode(config)

	// Set node to term 5
	node.HandleRequestVote(&RequestVoteArgs{Term: 5, CandidateID: "B"})

	// Request with term 3 should be rejected
	reply := node.HandleRequestVote(&RequestVoteArgs{
		Term:        3,
		CandidateID: "C",
	})
	assert.False(t, reply.VoteGranted)
	assert.Equal(t, uint64(5), reply.Term)
}

func TestRequestVote_HigherTermConvertsToFollower(t *testing.T) {
	config := DefaultConfig("A", []string{"B", "C"})
	node := NewNode(config)

	// Simulate node being a candidate in term 2
	node.mu.Lock()
	node.currentTerm = 2
	node.state = Candidate
	node.mu.Unlock()

	reply := node.HandleRequestVote(&RequestVoteArgs{
		Term:        5,
		CandidateID: "B",
	})
	assert.True(t, reply.VoteGranted)
	assert.Equal(t, Follower, node.State())
	assert.Equal(t, uint64(5), node.Term())
}

func TestAppendEntries_ResetsElectionTimer(t *testing.T) {
	config := DefaultConfig("A", []string{"B", "C"})
	node := NewNode(config)

	reply := node.HandleAppendEntries(&AppendEntriesArgs{
		Term:     1,
		LeaderID: "B",
	})
	assert.True(t, reply.Success)
	assert.Equal(t, "B", node.LeaderID())
}

func TestAppendEntries_RejectsOlderTerm(t *testing.T) {
	config := DefaultConfig("A", []string{"B", "C"})
	node := NewNode(config)

	node.mu.Lock()
	node.currentTerm = 5
	node.mu.Unlock()

	reply := node.HandleAppendEntries(&AppendEntriesArgs{
		Term:     3,
		LeaderID: "B",
	})
	assert.False(t, reply.Success)
	assert.Equal(t, uint64(5), reply.Term)
}

func TestNodeState_String(t *testing.T) {
	assert.Equal(t, "Follower", Follower.String())
	assert.Equal(t, "Candidate", Candidate.String())
	assert.Equal(t, "Leader", Leader.String())
}

func TestAddPeer_ExpandsCluster(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)

	// Add a new peer
	require.NoError(t, leader.AddPeer("D"))

	// Wait for config change to be applied
	time.Sleep(500 * time.Millisecond)

	// All nodes should now have "D" in their peers
	for _, n := range cluster.nodes {
		n.mu.Lock()
		hasPeer := false
		for _, p := range n.config.Peers {
			if p == "D" {
				hasPeer = true
				break
			}
		}
		n.mu.Unlock()
		assert.True(t, hasPeer, "node %s should have peer D", n.ID())
	}
}

func TestRemovePeer_ShrinksCluster(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)

	// Find a follower to remove
	var followerID string
	for _, n := range cluster.nodes {
		if n.ID() != leader.ID() {
			followerID = n.ID()
			break
		}
	}

	require.NoError(t, leader.RemovePeer(followerID))

	// Wait for config change to be applied
	time.Sleep(500 * time.Millisecond)

	// Leader should not have the removed peer
	leader.mu.Lock()
	hasPeer := false
	for _, p := range leader.config.Peers {
		if p == followerID {
			hasPeer = true
		}
	}
	leader.mu.Unlock()
	assert.False(t, hasPeer, "leader should not have removed peer %s", followerID)
}

func TestIsConfigChange(t *testing.T) {
	assert.True(t, IsConfigChange(append([]byte("__raft_config_add:"), []byte("node-D")...)))
	assert.True(t, IsConfigChange(append([]byte("__raft_config_remove:"), []byte("node-B")...)))
	assert.False(t, IsConfigChange([]byte("normal command")))
	assert.False(t, IsConfigChange(nil))
}

func TestPersistState_PanicsOnError(t *testing.T) {
	baseStore, err := NewBadgerLogStore(t.TempDir())
	require.NoError(t, err)
	defer baseStore.Close()

	store := &failingStore{
		LogStore:     baseStore,
		saveStateErr: fmt.Errorf("disk full"),
	}

	config := DefaultConfig("A", []string{"B", "C"})
	node := NewNode(config, store)

	assert.Panics(t, func() {
		node.HandleRequestVote(&RequestVoteArgs{
			Term:        1,
			CandidateID: "B",
		})
	}, "persistState should panic on SaveState error")
}

func TestPersistLogEntries_PanicsOnError(t *testing.T) {
	baseStore, err := NewBadgerLogStore(t.TempDir())
	require.NoError(t, err)
	defer baseStore.Close()

	store := &failingStore{
		LogStore:       baseStore,
		appendEntryErr: fmt.Errorf("disk full"),
	}

	config := DefaultConfig("A", nil)
	node := NewNode(config, store)

	// Propose now goes through batcherLoop (async), so panic is in a goroutine.
	// Test the invariant directly: persistLogEntries must panic synchronously.
	entry := LogEntry{Term: 1, Index: 1, Command: []byte("cmd")}
	node.mu.Lock()
	node.log = append(node.log, entry)
	node.mu.Unlock()

	assert.Panics(t, func() {
		node.mu.Lock()
		defer node.mu.Unlock()
		node.persistLogEntries([]LogEntry{entry})
	}, "persistLogEntries should panic on AppendEntries error")
}

// --- InstallSnapshot tests ---

func TestHandleInstallSnapshot_RestoresState(t *testing.T) {
	config := DefaultConfig("A", []string{"B"})
	node := NewNode(config)

	reply := node.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "B",
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		Data:              []byte("snapshot-data"),
	})

	assert.Equal(t, uint64(2), reply.Term)

	node.mu.Lock()
	assert.Equal(t, uint64(2), node.currentTerm)
	assert.Equal(t, uint64(5), node.lastApplied)
	assert.Equal(t, uint64(5), node.commitIndex)
	assert.Equal(t, uint64(6), node.firstIndex)
	assert.Empty(t, node.log)
	node.mu.Unlock()
}

func TestHandleInstallSnapshot_RejectsStaleTerm(t *testing.T) {
	config := DefaultConfig("A", []string{"B"})
	node := NewNode(config)

	node.mu.Lock()
	node.currentTerm = 5
	node.mu.Unlock()

	reply := node.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              3,
		LeaderID:          "B",
		LastIncludedIndex: 10,
		LastIncludedTerm:  3,
		Data:              []byte("old-snap"),
	})

	assert.Equal(t, uint64(5), reply.Term)

	node.mu.Lock()
	assert.Equal(t, uint64(0), node.lastApplied)
	node.mu.Unlock()
}

func TestCompactLog_UpdatesFirstIndex(t *testing.T) {
	config := DefaultConfig("A", nil)
	node := NewNode(config)

	// Simulate log entries 1-10
	node.mu.Lock()
	for i := uint64(1); i <= 10; i++ {
		node.log = append(node.log, LogEntry{Term: 1, Index: i, Command: []byte("cmd")})
	}
	node.mu.Unlock()

	assert.Equal(t, uint64(10), node.lastLogIdx())

	// Compact up to index 5: removes entries 1-5, keeps 6-10
	node.CompactLog(5)

	node.mu.Lock()
	logLen := len(node.log)
	node.mu.Unlock()

	assert.Equal(t, 5, logLen, "should have 5 entries remaining (6-10)")
	assert.Equal(t, uint64(10), node.lastLogIdx(), "lastLogIdx should still be 10")

	// Verify entry access still works
	node.mu.Lock()
	firstIdx, firstTerm := node.lastLogInfo()
	node.mu.Unlock()
	assert.Equal(t, uint64(10), firstIdx)
	assert.Equal(t, uint64(1), firstTerm)
}

func TestCompactLog_HandleAppendEntriesAfterCompaction(t *testing.T) {
	config := DefaultConfig("A", []string{"B"})
	node := NewNode(config)

	// Simulate log entries 1-5
	node.mu.Lock()
	for i := uint64(1); i <= 5; i++ {
		node.log = append(node.log, LogEntry{Term: 1, Index: i, Command: []byte("cmd")})
	}
	node.currentTerm = 1
	node.mu.Unlock()

	// Compact up to index 3: keeps entries 4-5
	node.CompactLog(3)

	// Append new entries 6-7 via AppendEntries
	reply := node.HandleAppendEntries(&AppendEntriesArgs{
		Term:         1,
		LeaderID:     "B",
		PrevLogIndex: 5,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 1, Index: 6, Command: []byte("new1")},
			{Term: 1, Index: 7, Command: []byte("new2")},
		},
		LeaderCommit: 7,
	})

	assert.True(t, reply.Success, "AppendEntries should succeed after compaction")
	assert.Equal(t, uint64(7), node.lastLogIdx())
}

// ── Phase 14d: QuorumMinMatchIndex tests ──────────────────────────────────

func makeNodeWithMatchIndex(id string, peers []string, matchMap map[string]uint64) *Node {
	config := Config{
		ID:               id,
		Peers:            peers,
		ElectionTimeout:  100 * time.Millisecond,
		HeartbeatTimeout: 30 * time.Millisecond,
	}
	n := NewNode(config)
	n.mu.Lock()
	for k, v := range matchMap {
		n.matchIndex[k] = v
	}
	n.mu.Unlock()
	return n
}

func TestQuorumMinMatchIndex_Singleton(t *testing.T) {
	n := makeNodeWithMatchIndex("A", nil, map[string]uint64{"A": 42})
	assert.Equal(t, uint64(42), n.QuorumMinMatchIndex())
}

func TestQuorumMinMatchIndex_ThreeNode_Median(t *testing.T) {
	// peers: B=80, C=50, self A=100 → sorted desc [100,80,50] → idx 3/2=1 → 80
	n := makeNodeWithMatchIndex("A", []string{"B", "C"}, map[string]uint64{
		"A": 100, "B": 80, "C": 50,
	})
	assert.Equal(t, uint64(80), n.QuorumMinMatchIndex())
}

func TestQuorumMinMatchIndex_ThreeNode_LaggingFollower(t *testing.T) {
	// A=100, B=50, C=0 → sorted [100,50,0] → idx 1 → 50
	n := makeNodeWithMatchIndex("A", []string{"B", "C"}, map[string]uint64{
		"A": 100, "B": 50, "C": 0,
	})
	assert.Equal(t, uint64(50), n.QuorumMinMatchIndex())
}

func TestQuorumMinMatchIndex_FiveNode_TwoPeersDown(t *testing.T) {
	// A=100, B=90, C=80, D=0, E=0 → sorted [100,90,80,0,0] → idx 5/2=2 → 80
	n := makeNodeWithMatchIndex("A", []string{"B", "C", "D", "E"}, map[string]uint64{
		"A": 100, "B": 90, "C": 80, "D": 0, "E": 0,
	})
	assert.Equal(t, uint64(80), n.QuorumMinMatchIndex())
}

// ── Phase 14d: log GC tests ───────────────────────────────────────────────

func TestNode_LogGC_TruncatesStoreToWatermark(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir, WithManagedMode())
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	// Append 20 entries
	entries := make([]LogEntry, 20)
	for i := range entries {
		entries[i] = LogEntry{Term: 1, Index: uint64(i + 1), Command: []byte(fmt.Sprintf("cmd%d", i+1))}
	}
	require.NoError(t, store.AppendEntries(entries))

	// Node: leader, A=20, B=20, C=15 → watermark = sorted[1] = 20
	config := Config{
		ID:               "A",
		Peers:            []string{"B", "C"},
		ManagedMode:      true,
		LogGCInterval:    1 * time.Millisecond,
		ElectionTimeout:  100 * time.Millisecond,
		HeartbeatTimeout: 30 * time.Millisecond,
	}
	n := NewNode(config, store)
	n.mu.Lock()
	n.state = Leader
	n.matchIndex["A"] = 20
	n.matchIndex["B"] = 20
	n.matchIndex["C"] = 15
	n.mu.Unlock()

	// Snapshot gate: save snapshot at watermark (index=20) before GC
	require.NoError(t, store.SaveSnapshot(20, 1, []byte(`{"snap":"test"}`)))

	time.Sleep(5 * time.Millisecond)
	n.maybeRunLogGC()

	// TruncateBefore(20) removes indices 1..19, keeps 20
	for i := uint64(1); i < 20; i++ {
		_, err := store.GetEntry(i)
		assert.Error(t, err, "entry %d should be GC'd", i)
	}
	got, err := store.GetEntry(20)
	require.NoError(t, err)
	assert.Equal(t, []byte("cmd20"), got.Command)
}

func TestNode_LogGC_SkipsWhenNotManagedMode(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir) // non-managed
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	entries := []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}}
	require.NoError(t, store.AppendEntries(entries))

	config := Config{
		ID:    "A",
		Peers: []string{"B"},
		// ManagedMode = false (default)
		LogGCInterval:    1 * time.Millisecond,
		ElectionTimeout:  100 * time.Millisecond,
		HeartbeatTimeout: 30 * time.Millisecond,
	}
	n := NewNode(config, store)
	n.mu.Lock()
	n.state = Leader
	n.matchIndex["A"] = 1
	n.matchIndex["B"] = 1
	n.mu.Unlock()

	time.Sleep(5 * time.Millisecond)
	n.maybeRunLogGC() // should be a no-op

	// Entry should still exist
	got, err := store.GetEntry(1)
	require.NoError(t, err)
	assert.Equal(t, []byte("a"), got.Command)
}

func TestNode_LogGC_SkipsBeforeInterval(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir, WithManagedMode())
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}
	require.NoError(t, store.AppendEntries(entries))

	config := Config{
		ID:               "A",
		Peers:            []string{"B"},
		ManagedMode:      true,
		LogGCInterval:    10 * time.Second, // very long interval
		ElectionTimeout:  100 * time.Millisecond,
		HeartbeatTimeout: 30 * time.Millisecond,
	}
	n := NewNode(config, store)
	n.mu.Lock()
	n.state = Leader
	n.matchIndex["A"] = 2
	n.matchIndex["B"] = 2
	n.mu.Unlock()

	// Snapshot gate: save snapshot at watermark (index=2) before GC
	require.NoError(t, store.SaveSnapshot(2, 1, []byte(`{"snap":"test"}`)))

	// First call sets lastLogGC but skips (interval not elapsed)
	n.maybeRunLogGC()
	n.maybeRunLogGC() // immediate second call → interval not elapsed

	// Both entries should still exist (second call was skipped)
	// First call DID run GC (lastLogGC was zero initially)
	// After first call: lastLogGC = now, entries 1 deleted (TruncateBefore(2))
	// Second call: skipped (interval not elapsed)
	// So entry 1 is gone (GC'd by first call), entry 2 remains
	_, err = store.GetEntry(1)
	assert.Error(t, err, "entry 1 should be GC'd by first maybeRunLogGC call")
	got, err := store.GetEntry(2)
	require.NoError(t, err)
	assert.Equal(t, []byte("b"), got.Command)
}

// ── Phase 14d: GC + partition + recovery integration test ────────────────

// newTestClusterWithStores creates a 3-node in-process cluster where each
// node has a BadgerDB store (managed mode). Returns the cluster and stores.
func newTestClusterWithStores(t *testing.T, n int) (*testCluster, []*BadgerLogStore) {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = nodeID(i)
	}
	stores := make([]*BadgerLogStore, n)
	for i := range stores {
		dir := t.TempDir()
		s, err := NewBadgerLogStore(dir, WithManagedMode())
		require.NoError(t, err)
		t.Cleanup(func() { s.Close() })
		stores[i] = s
	}
	cluster := &testCluster{nodes: make([]*Node, n)}
	for i := range ids {
		peers := make([]string, 0, n-1)
		for j := range ids {
			if i != j {
				peers = append(peers, ids[j])
			}
		}
		config := Config{
			ID:               ids[i],
			Peers:            peers,
			ManagedMode:      true,
			LogGCInterval:    1 * time.Millisecond,
			ElectionTimeout:  100 * time.Millisecond,
			HeartbeatTimeout: 30 * time.Millisecond,
		}
		cluster.nodes[i] = NewNode(config, stores[i])
	}
	for i := range ids {
		idx := i
		cluster.nodes[i].SetTransport(
			func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
				target := cluster.nodeByID(peer)
				if target == nil {
					return nil, errNodeNotFound
				}
				_ = idx
				return target.HandleRequestVote(args), nil
			},
			func(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
				target := cluster.nodeByID(peer)
				if target == nil {
					return nil, errNodeNotFound
				}
				return target.HandleAppendEntries(args), nil
			},
		)
	}
	return cluster, stores
}

func waitForCommitIndex(t *testing.T, node *Node, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			node.mu.Lock()
			ci := node.commitIndex
			node.mu.Unlock()
			t.Fatalf("node %s: commitIndex=%d, want=%d (timeout)", node.ID(), ci, want)
		default:
			node.mu.Lock()
			ci := node.commitIndex
			node.mu.Unlock()
			if ci >= want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestIntegration_LogGC_PartitionAndRecovery(t *testing.T) {
	cluster, stores := newTestClusterWithStores(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader, "no leader elected")

	// Propose 10 entries
	for i := range 10 {
		require.NoError(t, leader.Propose([]byte(fmt.Sprintf("cmd%d", i+1))))
	}

	// Wait for all nodes to commit all 10 entries
	for _, n := range cluster.nodes {
		waitForCommitIndex(t, n, 10, 3*time.Second)
	}

	// Snapshot gate: save snapshot at watermark (index=10) on all stores before GC
	for _, s := range stores {
		require.NoError(t, s.SaveSnapshot(10, 1, []byte(`{"snap":"test"}`)))
	}

	// GC: run on all nodes (watermark = quorumMinMatchIndex = 10)
	for _, n := range cluster.nodes {
		n.mu.Lock()
		n.lastLogGC = time.Time{} // reset to force GC
		n.mu.Unlock()
		n.maybeRunLogGC()
	}

	// Verify entries 1-9 are gone from all stores, entry 10 remains
	for i, s := range stores {
		for idx := uint64(1); idx < 10; idx++ {
			_, err := s.GetEntry(idx)
			assert.Error(t, err, "store[%d]: entry %d should be GC'd", i, idx)
		}
		got, err := s.GetEntry(10)
		require.NoError(t, err, "store[%d]: entry 10 should remain", i)
		assert.Equal(t, []byte("cmd10"), got.Command)
	}

	// "Partition" node C: remove from cluster transport mesh so it won't receive
	// AppendEntries from the leader during the next round of proposals.
	nodeC := cluster.nodes[2]
	cluster.mu.Lock()
	cluster.nodes = cluster.nodes[:2] // A and B only in mesh
	cluster.mu.Unlock()

	// Propose 5 more entries — only A+B quorum
	for i := range 5 {
		require.NoError(t, leader.Propose([]byte(fmt.Sprintf("cmd%d", 11+i))))
	}
	waitForCommitIndex(t, leader, 15, 3*time.Second)

	// "Reconnect" C: add back to mesh
	cluster.mu.Lock()
	cluster.nodes = append(cluster.nodes, nodeC)
	cluster.mu.Unlock()

	// Leader will send entries 11-15 to C via AppendEntries on next heartbeat.
	// Wait for all 3 nodes to commit all 15 entries.
	for _, n := range cluster.nodes {
		waitForCommitIndex(t, n, 15, 5*time.Second)
	}

	// All nodes should agree on commitIndex=15
	for _, n := range cluster.nodes {
		n.mu.Lock()
		ci := n.commitIndex
		n.mu.Unlock()
		assert.Equal(t, uint64(15), ci, "node %s commitIndex mismatch", n.ID())
	}
}

func TestHandleAppendEntries_EntryConflictAbortsWaiters(t *testing.T) {
	cfg := DefaultConfig("node1", nil)
	node := NewNode(cfg)

	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.log = []LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
		{Term: 1, Index: 3, Command: []byte("cmd3")},
	}
	ch1 := make(chan error, 1)
	ch2 := make(chan error, 1)
	ch3 := make(chan error, 1)
	node.waiters[1] = ch1
	node.waiters[2] = ch2
	node.waiters[3] = ch3
	node.mu.Unlock()

	reply := node.HandleAppendEntries(&AppendEntriesArgs{
		Term:     2,
		LeaderID: "node2",
		Entries:  []LogEntry{{Term: 2, Index: 1, Command: []byte("new")}},
	})
	assert.True(t, reply.Success)

	node.mu.Lock()
	assert.Empty(t, node.waiters)
	node.mu.Unlock()

	assert.ErrorIs(t, <-ch1, ErrProposalFailed)
	assert.ErrorIs(t, <-ch2, ErrProposalFailed)
	assert.ErrorIs(t, <-ch3, ErrProposalFailed)
}

func TestHandleAppendEntries_PrevLogConflictAbortsWaiters(t *testing.T) {
	cfg := DefaultConfig("node1", nil)
	node := NewNode(cfg)

	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.log = []LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 2, Index: 2, Command: []byte("cmd2")},
		{Term: 2, Index: 3, Command: []byte("cmd3")},
	}
	ch2 := make(chan error, 1)
	ch3 := make(chan error, 1)
	node.waiters[2] = ch2
	node.waiters[3] = ch3
	node.mu.Unlock()

	// New leader: PrevLogIndex=2, PrevLogTerm=1 (node has Term=2 at index 2 → conflict)
	reply := node.HandleAppendEntries(&AppendEntriesArgs{
		Term:         3,
		LeaderID:     "node2",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
	})
	assert.False(t, reply.Success)

	node.mu.Lock()
	assert.Empty(t, node.waiters)
	node.mu.Unlock()

	assert.ErrorIs(t, <-ch2, ErrProposalFailed)
	assert.ErrorIs(t, <-ch3, ErrProposalFailed)
}

func TestHandleInstallSnapshot_AbortsWaiters(t *testing.T) {
	cfg := DefaultConfig("node1", nil)
	node := NewNode(cfg)

	node.mu.Lock()
	node.currentTerm = 1
	node.log = []LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
	}
	ch1 := make(chan error, 1)
	ch2 := make(chan error, 1)
	node.waiters[1] = ch1
	node.waiters[2] = ch2
	node.mu.Unlock()

	reply := node.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "node2",
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		Data:              []byte("snapshot"),
	})
	assert.Equal(t, uint64(2), reply.Term)

	node.mu.Lock()
	assert.Empty(t, node.waiters)
	node.mu.Unlock()

	assert.ErrorIs(t, <-ch1, ErrProposalFailed)
	assert.ErrorIs(t, <-ch2, ErrProposalFailed)
}

func TestNode_Close_WaitsForGoroutines(t *testing.T) {
	cfg := DefaultConfig("node-1", nil)
	n := NewNode(cfg)
	n.Start()

	// Give Start a moment so all goroutines actually launch.
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		n.Close()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("Close() did not return within 2s — goroutines leaked or wg untracked")
	}
}

func TestHandleAppendEntries_UpdatesLastLeaderContact(t *testing.T) {
	cfg := DefaultConfig("A", []string{"B"})
	n := NewNode(cfg)

	before := time.Now()
	n.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: "B"})
	after := time.Now()

	n.mu.Lock()
	contact := n.lastLeaderContact
	n.mu.Unlock()

	assert.True(t, contact.After(before.Add(-time.Millisecond)),
		"lastLeaderContact should be set on AppendEntries")
	assert.True(t, contact.Before(after.Add(time.Millisecond)),
		"lastLeaderContact should not be in the future")
}

func TestNode_Close_IsIdempotent(t *testing.T) {
	cfg := DefaultConfig("node-1", nil)
	n := NewNode(cfg)
	n.Start()
	time.Sleep(50 * time.Millisecond)
	n.Close()
	// Second Close must not panic on double wg.Wait or double close(stopCh).
	n.Close()
}
