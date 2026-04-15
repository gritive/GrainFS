package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNode_PersistsAndRecoversState(t *testing.T) {
	dir := t.TempDir()

	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)

	// Create a 3-node cluster where one node uses a store
	cluster := newTestCluster(t, 3)
	// Replace node 0 with a store-backed node
	cluster.nodes[0].Stop()
	config := cluster.nodes[0].config
	cluster.nodes[0] = NewNode(config, store)

	// Re-wire transport for node 0
	cluster.nodes[0].SetTransport(
		func(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
			target := cluster.nodeByID(peer)
			if target == nil {
				return nil, errNodeNotFound
			}
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

	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)

	// Propose entries
	require.NoError(t, leader.Propose([]byte("persist-cmd-1")))
	require.NoError(t, leader.Propose([]byte("persist-cmd-2")))
	time.Sleep(500 * time.Millisecond)

	// Check node 0's stored state
	node0 := cluster.nodes[0]
	node0.mu.Lock()
	logLen := len(node0.log)
	currentTerm := node0.currentTerm
	node0.mu.Unlock()

	require.GreaterOrEqual(t, logLen, 2)
	require.Greater(t, currentTerm, uint64(0))

	// Stop all nodes
	for _, n := range cluster.nodes {
		n.Stop()
	}
	store.Close()

	// Reopen store and create a new node from it
	store2, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	defer store2.Close()

	recoveredNode := NewNode(config, store2)

	// Verify recovered state matches
	recoveredNode.mu.Lock()
	recoveredLogLen := len(recoveredNode.log)
	recoveredTerm := recoveredNode.currentTerm
	recoveredNode.mu.Unlock()

	assert.Equal(t, logLen, recoveredLogLen, "recovered log length should match")
	assert.Equal(t, currentTerm, recoveredTerm, "recovered term should match")

	// Verify actual log content
	if recoveredLogLen >= 2 {
		recoveredNode.mu.Lock()
		cmd1 := string(recoveredNode.log[0].Command)
		cmd2 := string(recoveredNode.log[1].Command)
		recoveredNode.mu.Unlock()
		assert.Equal(t, "persist-cmd-1", cmd1)
		assert.Equal(t, "persist-cmd-2", cmd2)
	}
}

func TestNode_RecoveredNodeJoinsCluster(t *testing.T) {
	dir := t.TempDir()

	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)

	// Simulate persisted state: term=3, votedFor="B", 2 log entries
	require.NoError(t, store.SaveState(3, "B"))
	require.NoError(t, store.AppendEntries([]LogEntry{
		{Term: 1, Index: 1, Command: []byte("old-cmd-1")},
		{Term: 2, Index: 2, Command: []byte("old-cmd-2")},
	}))
	store.Close()

	// Reopen
	store2, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	defer store2.Close()

	config := DefaultConfig("A", []string{"B", "C"})
	node := NewNode(config, store2)

	assert.Equal(t, uint64(3), node.Term())
	node.mu.Lock()
	assert.Equal(t, "B", node.votedFor)
	assert.Len(t, node.log, 2)
	assert.Equal(t, "old-cmd-1", string(node.log[0].Command))
	assert.Equal(t, "old-cmd-2", string(node.log[1].Command))
	node.mu.Unlock()
}
