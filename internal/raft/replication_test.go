package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeader_ProposeAndReplicate(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)

	// Propose a command
	err := leader.Propose([]byte("set x=1"))
	require.NoError(t, err)

	// Wait for commit to propagate
	time.Sleep(500 * time.Millisecond)

	// All nodes should have the entry in their logs
	for _, n := range cluster.nodes {
		n.mu.Lock()
		logLen := len(n.log)
		n.mu.Unlock()
		assert.GreaterOrEqual(t, logLen, 1, "node %s should have at least 1 log entry", n.ID())
	}
}

func TestLeader_CommitIndex_Advances(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)

	require.NoError(t, leader.Propose([]byte("cmd1")))
	require.NoError(t, leader.Propose([]byte("cmd2")))

	// Wait for replication and commit
	time.Sleep(500 * time.Millisecond)

	leader.mu.Lock()
	commitIdx := leader.commitIndex
	leader.mu.Unlock()
	assert.GreaterOrEqual(t, commitIdx, uint64(2), "leader commitIndex should be >= 2")
}

func TestLeader_ApplyCh_DeliversCommitted(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)

	require.NoError(t, leader.Propose([]byte("apply-me")))

	// Read from ApplyCh
	select {
	case entry := <-leader.ApplyCh():
		assert.Equal(t, "apply-me", string(entry.Command))
		assert.Equal(t, leader.Term(), entry.Term)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for applied entry")
	}
}

func TestFollower_ProposeRejected(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)

	// Find a follower
	var follower *Node
	for _, n := range cluster.nodes {
		if n.State() == Follower {
			follower = n
			break
		}
	}
	require.NotNil(t, follower)

	err := follower.Propose([]byte("should fail"))
	assert.Error(t, err, "propose on follower should fail")
}

func TestReplication_MultipleEntries(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader)

	for i := 0; i < 5; i++ {
		require.NoError(t, leader.Propose([]byte("entry")))
	}

	time.Sleep(800 * time.Millisecond)

	// All nodes should have all entries
	for _, n := range cluster.nodes {
		n.mu.Lock()
		logLen := len(n.log)
		n.mu.Unlock()
		assert.GreaterOrEqual(t, logLen, 5, "node %s should have >= 5 log entries, got %d", n.ID(), logLen)
	}
}

func TestReplication_LogConsistencyAfterLeaderChange(t *testing.T) {
	cluster := newTestCluster(t, 3)
	cluster.startAll()

	leader1 := cluster.waitForLeader(3 * time.Second)
	require.NotNil(t, leader1)

	require.NoError(t, leader1.Propose([]byte("before-failover")))
	time.Sleep(500 * time.Millisecond)

	// Stop leader
	leader1.Stop()

	// Wait for new leader
	var leader2 *Node
	deadline := time.After(5 * time.Second)
	for leader2 == nil {
		select {
		case <-deadline:
			t.Fatal("no new leader elected")
		default:
			for _, n := range cluster.nodes {
				if n.ID() != leader1.ID() && n.State() == Leader {
					leader2 = n
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Propose on new leader
	require.NoError(t, leader2.Propose([]byte("after-failover")))
	time.Sleep(500 * time.Millisecond)

	// Surviving nodes should have both entries
	for _, n := range cluster.nodes {
		if n.ID() == leader1.ID() {
			continue
		}
		n.mu.Lock()
		logLen := len(n.log)
		n.mu.Unlock()
		assert.GreaterOrEqual(t, logLen, 2, "surviving node %s should have >= 2 entries", n.ID())
	}
}
