package raft

import (
	"context"
	"errors"
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
	mu    sync.RWMutex
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
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, n := range c.nodes {
		if n.ID() == id {
			return n
		}
	}
	return nil
}

func (c *testCluster) startAll() {
	c.mu.RLock()
	nodes := make([]*Node, len(c.nodes))
	copy(nodes, c.nodes)
	c.mu.RUnlock()
	for _, n := range nodes {
		n.Start()
	}
}

func (c *testCluster) stopAll() {
	c.mu.RLock()
	nodes := make([]*Node, len(c.nodes))
	copy(nodes, c.nodes)
	c.mu.RUnlock()
	for _, n := range nodes {
		n.Close()
	}
}

func (c *testCluster) waitForLeader(timeout time.Duration) *Node {
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return nil
		default:
			c.mu.RLock()
			nodes := make([]*Node, len(c.nodes))
			copy(nodes, c.nodes)
			c.mu.RUnlock()
			for _, n := range nodes {
				if n.State() == Leader {
					return n
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (c *testCluster) countState(state NodeState) int {
	c.mu.RLock()
	nodes := make([]*Node, len(c.nodes))
	copy(nodes, c.nodes)
	c.mu.RUnlock()
	count := 0
	for _, n := range nodes {
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

// --- Membership change tests (§4.4) ---

func TestConfChange_AppliesOnAppendNotCommit(t *testing.T) {
	// §4.4: config must be applied when the entry is appended, not when committed.
	node := NewNode(DefaultConfig("A", []string{"B"}))
	node.mu.Lock()
	node.currentTerm = 1
	node.mu.Unlock()

	cmd := encodeConfChange(ConfChangeAddVoter, "C", "C")
	entry := LogEntry{Term: 1, Index: 1, Type: LogEntryConfChange, Command: cmd}

	// LeaderCommit = 0: the ConfChange entry is NOT committed yet.
	reply := node.HandleAppendEntries(&AppendEntriesArgs{
		Term:         1,
		LeaderID:     "B",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{entry},
		LeaderCommit: 0,
	})
	require.True(t, reply.Success)

	node.mu.Lock()
	hasPeer := false
	for _, p := range node.config.Peers {
		if p == "C" {
			hasPeer = true
		}
	}
	node.mu.Unlock()
	assert.True(t, hasPeer, "ConfChange must be applied on append, before commit")
}

func TestConfChange_AppliesOnAppendNotCommit_Leader(t *testing.T) {
	// §4.4: leader must apply ConfChange when the entry is appended (flushBatch),
	// not when it is committed.
	node := NewNode(DefaultConfig("A", []string{"B"}))
	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.nextIndex = map[string]uint64{"B": 1}
	node.matchIndex = map[string]uint64{"B": 0}
	node.mu.Unlock()
	defer node.Close()

	cmd := encodeConfChange(ConfChangeAddVoter, "C", "C")
	p := proposal{command: cmd, entryType: LogEntryConfChange, doneCh: make(chan proposalResult, 1), ctx: context.Background()}

	// LeaderCommit never advances (B hasn't acked), so C is not committed.
	node.flushBatch([]proposal{p})

	node.mu.Lock()
	hasPeer := false
	for _, peer := range node.config.Peers {
		if peer == "C" {
			hasPeer = true
		}
	}
	node.mu.Unlock()
	assert.True(t, hasPeer, "ConfChange must be applied on append (leader path), before commit")
}

func TestConfChange_MixedVersionRejected(t *testing.T) {
	node := NewNode(DefaultConfig("A", []string{"B", "C"}))
	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.nextIndex = map[string]uint64{"B": 1, "C": 1}
	node.matchIndex = map[string]uint64{"B": 0, "C": 0}
	node.mu.Unlock()

	node.SetMixedVersion(true)
	err := node.AddVoter("D", "D")
	assert.ErrorIs(t, err, ErrMixedVersionNoMembershipChange)
}

func TestConfChange_RejectsConcurrent(t *testing.T) {
	node := NewNode(DefaultConfig("A", []string{"B", "C"}))
	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.pendingConfChangeIndex = 1
	node.nextIndex = map[string]uint64{"B": 1, "C": 1}
	node.matchIndex = map[string]uint64{"B": 0, "C": 0}
	node.mu.Unlock()

	err := node.AddVoter("D", "D")
	assert.ErrorIs(t, err, ErrConfChangeInProgress)
}

func TestConfChange_RejectsConcurrentGoroutine(t *testing.T) {
	// Simulate the TOCTOU window: two ConfChange proposals arrive in the same
	// flushBatch call (both passed proposeConfChangeWait's pre-check before
	// either set pendingConfChangeIndex). The lock-protected filter in
	// flushBatch must reject the second one synchronously.
	node := NewNode(DefaultConfig("A", []string{"B", "C"}))
	node.mu.Lock()
	node.state = Leader
	node.currentTerm = 1
	node.nextIndex = map[string]uint64{"B": 1, "C": 1}
	node.matchIndex = map[string]uint64{"B": 0, "C": 0}
	node.mu.Unlock()
	defer node.Close()

	makeCC := func(id string) proposal {
		cmd := encodeConfChange(ConfChangeAddVoter, id, id)
		return proposal{command: cmd, entryType: LogEntryConfChange, doneCh: make(chan proposalResult, 1), ctx: context.Background()}
	}
	p1 := makeCC("D")
	p2 := makeCC("E")

	node.flushBatch([]proposal{p1, p2})

	// The rejected proposal's doneCh is populated synchronously during flushBatch.
	var rejected int
	for _, ch := range []chan proposalResult{p1.doneCh, p2.doneCh} {
		select {
		case r := <-ch:
			if errors.Is(r.err, ErrConfChangeInProgress) {
				rejected++
			}
		default:
		}
	}
	assert.Equal(t, 1, rejected, "exactly one ConfChange per batch must be rejected")
}

func TestConfChange_LearnerNotInQuorum(t *testing.T) {
	node := NewNode(DefaultConfig("A", []string{"B", "C"}))
	node.mu.Lock()
	initialPeerCount := len(node.config.Peers)

	cmd := encodeConfChange(ConfChangeAddLearner, "D", "D")
	entry := LogEntry{Term: 1, Index: 1, Type: LogEntryConfChange, Command: cmd}
	node.applyConfigChangeLocked(entry)

	assert.Equal(t, initialPeerCount, len(node.config.Peers), "AddLearner must not expand config.Peers")
	assert.Equal(t, uint64(1), node.pendingConfChangeIndex)
	node.mu.Unlock()
}

func TestConfChange_SnapshotPreservesConfig(t *testing.T) {
	node := NewNode(DefaultConfig("A", []string{"B"}))

	reply := node.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              2,
		LeaderID:          "B",
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		Data:              []byte("snapshot"),
		Servers: []Server{
			{ID: "A", Suffrage: Voter},
			{ID: "B", Suffrage: Voter},
			{ID: "C", Suffrage: Voter},
		},
	})
	require.Equal(t, uint64(2), reply.Term)

	node.mu.Lock()
	peers := make([]string, len(node.config.Peers))
	copy(peers, node.config.Peers)
	node.mu.Unlock()

	assert.ElementsMatch(t, []string{"B", "C"}, peers, "config.Peers must be restored from snapshot Servers, excluding self")
}

func TestConfChange_RebuildConfigAfterTruncation(t *testing.T) {
	// When conflicting entries are truncated, config must be rebuilt from initialPeers + remaining log.
	node := NewNode(DefaultConfig("A", []string{"B"}))

	cmd := encodeConfChange(ConfChangeAddVoter, "C", "C")
	node.mu.Lock()
	node.log = []LogEntry{
		{Term: 1, Index: 1, Type: LogEntryConfChange, Command: cmd},
	}
	node.config.Peers = []string{"B", "C"}

	// Simulate truncation: ConfChange entry is removed from the log.
	node.log = nil
	node.rebuildConfigFromLog()

	peers := make([]string, len(node.config.Peers))
	copy(peers, node.config.Peers)
	node.mu.Unlock()

	assert.ElementsMatch(t, []string{"B"}, peers, "after truncation config must revert to initialPeers")
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
	config := Config{ID: "A", TrailingLogs: 0} // TrailingLogs=0: remove all up to snapshotIndex
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

func TestHasQuorum_TrueWhenMajorityRecent(t *testing.T) {
	cfg := DefaultConfig("n0", []string{"n1", "n2"})
	n := NewNode(cfg)

	n.mu.Lock()
	n.checkQuorumAcks = map[string]time.Time{
		"n1": time.Now(),
		"n2": {},
	}
	result := n.hasQuorum()
	n.mu.Unlock()

	assert.True(t, result, "n0(self)+n1=2 of 3 is majority")
}

func TestHasQuorum_FalseWhenNoPeersResponded(t *testing.T) {
	cfg := DefaultConfig("n0", []string{"n1", "n2"})
	n := NewNode(cfg)

	n.mu.Lock()
	n.checkQuorumAcks = map[string]time.Time{
		"n1": {},
		"n2": {},
	}
	result := n.hasQuorum()
	n.mu.Unlock()

	assert.False(t, result, "only self=1 of 3, not majority")
}

func TestHandleRequestVote_PreVoteNoStateChange(t *testing.T) {
	cfg := DefaultConfig("n0", []string{"n1", "n2"})
	n := NewNode(cfg)

	n.mu.Lock()
	n.lastLeaderContact = time.Now()
	n.mu.Unlock()

	reply := n.HandleRequestVote(&RequestVoteArgs{
		Term:        1,
		CandidateID: "n1",
		PreVote:     true,
	})

	assert.False(t, reply.VoteGranted, "pre-vote must be rejected when leader is recent")

	n.mu.Lock()
	defer n.mu.Unlock()
	assert.Equal(t, uint64(0), n.currentTerm, "pre-vote must not update term")
	assert.Equal(t, Follower, n.state, "pre-vote must not change state")
	assert.Equal(t, "", n.votedFor, "pre-vote must not set votedFor")
}

func TestHandleRequestVote_LeaderStickyRejectsHigherTerm(t *testing.T) {
	cfg := DefaultConfig("n0", []string{"n1", "n2"})
	n := NewNode(cfg)

	n.mu.Lock()
	n.currentTerm = 5
	n.lastLeaderContact = time.Now()
	n.mu.Unlock()

	reply := n.HandleRequestVote(&RequestVoteArgs{
		Term:        6,
		CandidateID: "n1",
		PreVote:     false,
	})

	assert.False(t, reply.VoteGranted, "leader stickiness must reject higher-term vote")

	n.mu.Lock()
	defer n.mu.Unlock()
	assert.Equal(t, uint64(5), n.currentTerm,
		"leader stickiness must suppress term update from rogue node")
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

func TestHandleInstallSnapshot_UpdatesLastLeaderContact(t *testing.T) {
	cfg := DefaultConfig("A", []string{"B"})
	n := NewNode(cfg)

	before := time.Now()
	n.HandleInstallSnapshot(&InstallSnapshotArgs{Term: 1, LeaderID: "B"})
	after := time.Now()

	n.mu.Lock()
	contact := n.lastLeaderContact
	n.mu.Unlock()

	assert.True(t, contact.After(before.Add(-time.Millisecond)),
		"lastLeaderContact should be set on InstallSnapshot")
	assert.True(t, contact.Before(after.Add(time.Millisecond)),
		"lastLeaderContact should not be in the future")
}

func TestHasQuorum_TwoNodeCluster(t *testing.T) {
	cfg := DefaultConfig("n0", []string{"n1"})
	n := NewNode(cfg)

	n.mu.Lock()
	n.checkQuorumAcks = map[string]time.Time{"n1": time.Now()}
	withPeer := n.hasQuorum()
	n.checkQuorumAcks = map[string]time.Time{"n1": {}}
	withoutPeer := n.hasQuorum()
	n.mu.Unlock()

	assert.True(t, withPeer, "2-node: self+peer=2/2, quorum")
	assert.False(t, withoutPeer, "2-node: only self=1/2, not quorum")
}

func TestHandleRequestVote_LeaderRejectsPreVote(t *testing.T) {
	cfg := DefaultConfig("n0", []string{"n1", "n2"})
	n := NewNode(cfg)

	n.mu.Lock()
	n.state = Leader
	n.currentTerm = 3
	n.mu.Unlock()

	reply := n.HandleRequestVote(&RequestVoteArgs{
		Term:        4,
		CandidateID: "n1",
		PreVote:     true,
	})

	assert.False(t, reply.VoteGranted, "leader must reject pre-vote")

	n.mu.Lock()
	defer n.mu.Unlock()
	assert.Equal(t, Leader, n.state, "leader state must not change on pre-vote")
	assert.Equal(t, uint64(3), n.currentTerm, "term must not change on pre-vote")
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

// TestAEReply_ConflictTermJumpsCorrectly verifies that when the leader receives
// an AppendEntriesReply with ConflictTerm/ConflictIndex, it jumps nextIndex to
// skip all conflicting entries in one step instead of decrementing one-by-one.
func TestAEReply_ConflictTermJumpsCorrectly(t *testing.T) {
	// Leader has log: [T1@1, T1@2, T1@3, T2@4, T2@5]
	// Follower has:   [T1@1, T3@2]  — term conflict at index 2
	// Reply: ConflictTerm=3, ConflictIndex=2
	// Leader scans backwards: no T3 entry → nextIndex = ConflictIndex = 2
	n := NewNode(Config{ID: "leader", Peers: []string{"follower"}})
	n.mu.Lock()
	n.state = Leader
	n.currentTerm = 2
	n.log = []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 2},
		{Index: 5, Term: 2},
	}
	n.firstIndex = 1
	n.nextIndex = map[string]uint64{"follower": 6}
	n.matchIndex = map[string]uint64{"follower": 0}
	n.mu.Unlock()

	reply := &AppendEntriesReply{
		Term:          2,
		Success:       false,
		ConflictTerm:  3,
		ConflictIndex: 2,
	}
	n.mu.Lock()
	n.applyConflictHint("follower", reply, 6, []LogEntry{{Index: 5, Term: 2}})
	got := n.nextIndex["follower"]
	n.mu.Unlock()

	// ConflictTerm=3 not in leader log → use ConflictIndex=2
	assert.Equal(t, uint64(2), got)
}

// TestAEReply_FallbackToMinusOneForOldPeer verifies that when ConflictIndex=0
// (old peer without conflict hint support), nextIndex decrements by 1.
func TestAEReply_FallbackToMinusOneForOldPeer(t *testing.T) {
	n := NewNode(Config{ID: "leader", Peers: []string{"follower"}})
	n.mu.Lock()
	n.state = Leader
	n.currentTerm = 1
	n.log = []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}
	n.firstIndex = 1
	n.nextIndex = map[string]uint64{"follower": 4}
	n.matchIndex = map[string]uint64{"follower": 0}
	n.mu.Unlock()

	reply := &AppendEntriesReply{
		Term:          1,
		Success:       false,
		ConflictTerm:  0,
		ConflictIndex: 0, // old peer
	}
	n.mu.Lock()
	n.applyConflictHint("follower", reply, 4, []LogEntry{{Index: 3, Term: 1}})
	got := n.nextIndex["follower"]
	n.mu.Unlock()

	assert.Equal(t, uint64(3), got) // decremented by 1
}

// TestAESize_SplitsAtMaxEntries verifies that when MaxEntriesPerAE is set,
// replicateTo sends at most that many entries per RPC call.
func TestAESize_SplitsAtMaxEntries(t *testing.T) {
	sentCounts := make([]int, 0)
	n := NewNode(Config{
		ID:              "leader",
		Peers:           []string{"follower"},
		MaxEntriesPerAE: 3,
	})
	n.SetTransport(
		func(_ string, _ *RequestVoteArgs) (*RequestVoteReply, error) { return nil, nil },
		func(_ string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
			sentCounts = append(sentCounts, len(args.Entries))
			return &AppendEntriesReply{Term: 1, Success: true}, nil
		},
	)

	n.mu.Lock()
	n.state = Leader
	n.currentTerm = 1
	n.log = make([]LogEntry, 7)
	for i := range n.log {
		n.log[i] = LogEntry{Index: uint64(i + 1), Term: 1}
	}
	n.firstIndex = 1
	n.nextIndex = map[string]uint64{"follower": 1}
	n.matchIndex = map[string]uint64{"follower": 0}
	n.commitIndex = 7
	n.checkQuorumAcks = map[string]time.Time{}
	n.mu.Unlock()

	n.replicateTo("follower")

	require.NotEmpty(t, sentCounts)
	assert.LessOrEqual(t, sentCounts[0], 3, "first AE must send ≤ MaxEntriesPerAE entries")
}

// TestTrailingLogs_KeepsLastN verifies that CompactLog with a non-zero TrailingLogs
// retains the last TrailingLogs entries in memory so a slightly-lagged follower
// can catch up without InstallSnapshot.
func TestTrailingLogs_KeepsLastN(t *testing.T) {
	n := NewNode(Config{
		ID:           "n1",
		Peers:        nil,
		TrailingLogs: 3,
	})
	n.mu.Lock()
	n.log = []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
		{Index: 4, Term: 1},
		{Index: 5, Term: 1},
	}
	n.firstIndex = 1
	n.mu.Unlock()

	n.CompactLog(5) // snapshot at index 5, trailing=3 → keep [3,4,5]

	n.mu.Lock()
	fi := n.firstIndex
	li := n.lastLogIdx()
	si := n.snapshotIndex
	n.mu.Unlock()

	assert.Equal(t, uint64(3), fi, "firstIndex should be snapshotIndex+1-trailing = 3")
	assert.Equal(t, uint64(5), li, "lastLogIdx should still be 5")
	assert.Equal(t, uint64(5), si, "snapshotIndex should be 5")
}

// TestTrailingLogs_ZeroRemovesAll verifies that CompactLog with TrailingLogs=0
// (default unset) removes all entries up to snapshotIndex (original behavior).
func TestTrailingLogs_ZeroRemovesAll(t *testing.T) {
	n := NewNode(Config{ID: "n1", Peers: nil, TrailingLogs: 0})
	n.mu.Lock()
	n.log = []LogEntry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 1},
	}
	n.firstIndex = 1
	n.mu.Unlock()

	n.CompactLog(3)

	n.mu.Lock()
	fi := n.firstIndex
	ll := len(n.log)
	n.mu.Unlock()

	assert.Equal(t, uint64(4), fi)
	assert.Equal(t, 0, ll)
}

func TestConfiguration_ConcurrentReads(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()
	defer cluster.stopAll()

	require.Eventually(t, func() bool {
		for _, n := range cluster.nodes {
			if n.IsLeader() {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			cfg := cluster.nodes[0].Configuration()
			assert.NotEmpty(t, cfg.Servers, "servers must not be empty")
		}
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Configuration() concurrent reads timed out")
	}

	cfg := cluster.nodes[0].Configuration()
	assert.Len(t, cfg.Servers, 3)
	for _, s := range cfg.Servers {
		assert.Equal(t, Voter, s.Suffrage)
	}
}

func TestBootstrap_RejectsSecondCall(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	defer store.Close()

	cfg := DefaultConfig("node-0", []string{"node-1", "node-2"})
	node := NewNode(cfg, store)

	require.NoError(t, node.Bootstrap())
	err = node.Bootstrap()
	assert.ErrorIs(t, err, ErrAlreadyBootstrapped)
}

func TestBootstrap_AutoDetectsExistingStore(t *testing.T) {
	dir := t.TempDir()

	store1, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	require.NoError(t, store1.SaveState(1, "node-0"))
	require.NoError(t, store1.Close())

	store2, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	defer store2.Close()

	cfg := DefaultConfig("node-0", []string{"node-1", "node-2"})
	node := NewNode(cfg, store2)

	err = node.Bootstrap()
	assert.ErrorIs(t, err, ErrAlreadyBootstrapped,
		"Bootstrap must detect pre-existing hard state (term=1)")
}

func TestObserver_DeliversLeaderChange(t *testing.T) {
	cluster := newTestCluster(t, 3)
	ch := make(chan Event, 24)
	for _, n := range cluster.nodes {
		n.RegisterObserver(ch)
	}
	cluster.startAll()
	defer cluster.stopAll()

	require.Eventually(t, func() bool {
		for {
			select {
			case e := <-ch:
				if e.Type == EventLeaderChange && e.IsLeader {
					return true
				}
			default:
				return false
			}
		}
	}, 5*time.Second, 10*time.Millisecond, "observer must deliver EventLeaderChange with IsLeader=true")
}

// TestObserver_DeregisterStopsDelivery verifies that after DeregisterObserver
// no further events are delivered to the deregistered channel.
func TestObserver_DeregisterStopsDelivery(t *testing.T) {
	cfg := DefaultConfig("node1", nil)
	n := NewNode(cfg)

	ch := make(chan Event, 10)
	n.RegisterObserver(ch)

	e := Event{Type: EventLeaderChange, IsLeader: true, Term: 1}
	n.notifyObservers(e)
	require.Len(t, ch, 1, "event should be delivered before deregister")

	n.DeregisterObserver(ch)
	<-ch // drain

	n.notifyObservers(e)
	assert.Len(t, ch, 0, "no event should be delivered after deregister")
}

// TestObserver_FullChannelDrops verifies that a full observer channel causes
// the event to be dropped rather than blocking Raft.
func TestObserver_FullChannelDrops(t *testing.T) {
	cfg := DefaultConfig("node1", nil)
	n := NewNode(cfg)

	ch := make(chan Event, 1)
	n.RegisterObserver(ch)
	ch <- Event{} // fill the channel

	// This must not block.
	done := make(chan struct{})
	go func() {
		n.notifyObservers(Event{Type: EventLeaderChange, IsLeader: true, Term: 1})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("notifyObservers blocked on full channel")
	}
}

// TestBootstrap_NilStoreNoOp verifies that Bootstrap with a nil store is a no-op
// (returns nil error) — used when running with in-memory state for tests.
func TestBootstrap_NilStoreNoOp(t *testing.T) {
	cfg := DefaultConfig("node1", nil)
	node := NewNode(cfg) // no store argument → store is nil
	err := node.Bootstrap()
	assert.NoError(t, err)
}

// TestObserver_FailedHeartbeat verifies that EventFailedHeartbeat fires when
// replicateTo cannot send AppendEntries to a peer.
func TestObserver_FailedHeartbeat(t *testing.T) {
	cfg := DefaultConfig("node1", []string{"node2"})
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.HeartbeatTimeout = 30 * time.Millisecond
	node := NewNode(cfg)

	failCh := make(chan Event, 10)
	node.RegisterObserver(failCh)

	node.SetTransport(
		func(_ string, _ *RequestVoteArgs) (*RequestVoteReply, error) {
			return &RequestVoteReply{Term: 0, VoteGranted: true}, nil
		},
		func(_ string, _ *AppendEntriesArgs) (*AppendEntriesReply, error) {
			return nil, fmt.Errorf("simulated network failure")
		},
	)
	node.Start()
	defer node.Stop()
	go func() {
		for range node.ApplyCh() {
		}
	}()

	require.Eventually(t, func() bool {
		return node.State() == Leader
	}, 2*time.Second, 10*time.Millisecond)

	// Trigger replication by proposing.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go node.ProposeWait(ctx, []byte("cmd")) //nolint:errcheck

	require.Eventually(t, func() bool {
		for {
			select {
			case e := <-failCh:
				if e.Type == EventFailedHeartbeat && e.PeerID == "node2" {
					return true
				}
			default:
				return false
			}
		}
	}, 2*time.Second, 10*time.Millisecond, "EventFailedHeartbeat must be delivered for node2")
}

// TestObserver_StepDownLeaderIDEmpty verifies that when a leader steps down
// due to receiving a higher-term RequestVote (no new leader known yet), the
// EventLeaderChange has IsLeader=false and LeaderID="".
func TestObserver_StepDownLeaderIDEmpty(t *testing.T) {
	// Single-node leader that wins election immediately.
	cfg := DefaultConfig("node1", []string{"node2"})
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.HeartbeatTimeout = 30 * time.Millisecond
	node := NewNode(cfg)
	node.SetTransport(
		func(_ string, _ *RequestVoteArgs) (*RequestVoteReply, error) {
			return &RequestVoteReply{Term: 0, VoteGranted: true}, nil
		},
		func(_ string, _ *AppendEntriesArgs) (*AppendEntriesReply, error) {
			return &AppendEntriesReply{Term: 0, Success: true}, nil
		},
	)

	ch := make(chan Event, 16)
	node.RegisterObserver(ch)
	defer node.DeregisterObserver(ch)

	node.Start()
	defer node.Stop()
	go func() {
		for range node.ApplyCh() {
		}
	}()

	require.Eventually(t, func() bool {
		return node.State() == Leader
	}, 2*time.Second, 10*time.Millisecond)

	// Inject a higher-term RequestVote. HandleRequestVote clears leaderID to "".
	node.mu.Lock()
	higherTerm := node.currentTerm + 1
	node.mu.Unlock()

	node.HandleRequestVote(&RequestVoteArgs{
		Term:        higherTerm,
		CandidateID: "node2",
		// LastLogIndex/LastLogTerm left zero; vote not required — step-down alone is enough.
		LeaderTransfer: true, // bypass stickiness guard
	})

	// Should receive a step-down event with LeaderID == "".
	require.Eventually(t, func() bool {
		for {
			select {
			case e := <-ch:
				if e.Type == EventLeaderChange && !e.IsLeader {
					assert.Equal(t, "", e.LeaderID,
						"step-down event must have empty LeaderID")
					return true
				}
			default:
				return false
			}
		}
	}, 2*time.Second, 10*time.Millisecond, "step-down EventLeaderChange not received")
}

// applyCC applies a ConfChange directly to the node's state under lock.
// Only for testing — bypasses proposal pipeline.
func applyCC(t *testing.T, n *Node, op ConfChangeOp, id, addr string) {
	t.Helper()
	entry := LogEntry{
		Type:    LogEntryConfChange,
		Command: encodeConfChange(op, id, addr),
		Index:   1,
	}
	n.mu.Lock()
	n.applyConfigChangeLocked(entry)
	n.mu.Unlock()
}

func TestApplyConfigChange_AddVoterUsesAddress(t *testing.T) {
	cfg := DefaultConfig("self", []string{"existing:9000"})
	n := NewNode(cfg)
	applyCC(t, n, ConfChangeAddVoter, "node-3", "10.0.0.3:9000")

	n.mu.Lock()
	peers := append([]string{}, n.config.Peers...)
	n.mu.Unlock()

	require.Contains(t, peers, "10.0.0.3:9000", "peerKey must be cc.Address")
	require.NotContains(t, peers, "node-3", "cc.ID must not appear when cc.Address is set")
}

func TestApplyConfigChange_AddLearnerTracked(t *testing.T) {
	cfg := DefaultConfig("self", nil)
	n := NewNode(cfg)
	applyCC(t, n, ConfChangeAddLearner, "learner-1", "10.0.0.1:9001")

	n.mu.Lock()
	peerKey := n.learnerIDs["learner-1"]
	inVoters := false
	for _, p := range n.config.Peers {
		if p == "10.0.0.1:9001" {
			inVoters = true
			break
		}
	}
	n.mu.Unlock()

	require.Equal(t, "10.0.0.1:9001", peerKey, "learnerIDs[id] must be peerKey")
	require.False(t, inVoters, "learner must NOT be in voter peers")
}

func TestApplyConfigChange_PromoteLearnerToVoter(t *testing.T) {
	cfg := DefaultConfig("self", nil)
	n := NewNode(cfg)
	applyCC(t, n, ConfChangeAddLearner, "node-3", "10.0.0.3:9000")
	applyCC(t, n, ConfChangePromote, "node-3", "")

	n.mu.Lock()
	peers := append([]string{}, n.config.Peers...)
	_, stillLearner := n.learnerIDs["node-3"]
	n.mu.Unlock()

	require.Contains(t, peers, "10.0.0.3:9000", "promoted peer must appear in voters")
	require.False(t, stillLearner, "promoted peer must be removed from learnerIDs")
}

func TestApplyConfigChange_RemoveVoterByAddress(t *testing.T) {
	cfg := DefaultConfig("self", []string{"10.0.0.1:9000"})
	n := NewNode(cfg)
	applyCC(t, n, ConfChangeRemoveVoter, "10.0.0.1:9000", "")

	n.mu.Lock()
	peers := append([]string{}, n.config.Peers...)
	n.mu.Unlock()

	require.NotContains(t, peers, "10.0.0.1:9000")
}

func TestPeerMatchIndex_LearnerTracked(t *testing.T) {
	cfg := DefaultConfig("self", nil)
	n := NewNode(cfg)
	// PeerMatchIndex is a leader-side API; set leader state so applyCC populates matchIndex.
	n.mu.Lock()
	n.state = Leader
	n.mu.Unlock()
	applyCC(t, n, ConfChangeAddLearner, "learner-1", "10.0.0.1:9001")

	idx, ok := n.PeerMatchIndex("10.0.0.1:9001")
	require.True(t, ok, "learner must be in matchIndex after AddLearner")
	require.Equal(t, uint64(0), idx)
}

func TestPeerMatchIndex_UnknownPeer(t *testing.T) {
	cfg := DefaultConfig("self", nil)
	n := NewNode(cfg)
	_, ok := n.PeerMatchIndex("unknown:9000")
	require.False(t, ok)
}
