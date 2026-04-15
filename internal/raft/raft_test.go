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
	node.Start()
	defer node.Stop()

	require.Eventually(t, func() bool {
		return node.State() == Leader
	}, 3*time.Second, 10*time.Millisecond)

	assert.Panics(t, func() {
		node.Propose([]byte("cmd"))
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
