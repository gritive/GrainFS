package chaos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

func TestChaosTransport_HappyPathRequestVote(t *testing.T) {
	tr := NewChaosTransport()

	cfgA := raft.DefaultConfig("A", []string{"B"})
	cfgB := raft.DefaultConfig("B", []string{"A"})
	a := raft.NewNode(cfgA)
	b := raft.NewNode(cfgB)
	t.Cleanup(func() { a.Close(); b.Close() })

	tr.Register(a)
	tr.Register(b)
	tr.Wire(a)
	tr.Wire(b)

	a.Start()
	b.Start()

	// Wait for a leader to be elected via real RequestVote/AppendEntries flow.
	require.Eventually(t, func() bool {
		return a.IsLeader() || b.IsLeader()
	}, 5*time.Second, 50*time.Millisecond, "no leader elected through ChaosTransport")

	// Both nodes must agree on which is leader.
	leaderID := a.LeaderID()
	if leaderID == "" {
		leaderID = b.LeaderID()
	}
	assert.NotEmpty(t, leaderID)
	assert.Equal(t, leaderID, a.LeaderID(), "A must know the leader")
	assert.Equal(t, leaderID, b.LeaderID(), "B must know the leader")
}

func TestChaosTransport_PartitionBlocksBothDirections(t *testing.T) {
	tr := NewChaosTransport()

	cfgA := raft.DefaultConfig("A", []string{"B"})
	cfgB := raft.DefaultConfig("B", []string{"A"})
	a := raft.NewNode(cfgA)
	b := raft.NewNode(cfgB)
	t.Cleanup(func() { a.Close(); b.Close() })

	tr.Register(a)
	tr.Register(b)
	tr.Wire(a)
	tr.Wire(b)

	a.Start()
	b.Start()

	require.Eventually(t, func() bool {
		return a.IsLeader() || b.IsLeader()
	}, 5*time.Second, 50*time.Millisecond)

	// Partition A — A cannot send to B, B cannot send to A.
	tr.PartitionPeer("A")

	allowed := tr.shouldDeliver("B", "A")
	assert.False(t, allowed, "partition must block B→A")

	allowed = tr.shouldDeliver("A", "B")
	assert.False(t, allowed, "partition must block A→B")

	allowed = tr.shouldDeliver("A", "A") // partitioned node self-loopback
	assert.False(t, allowed, "partition blocks A→A (partitioned node)")

	// Heal.
	tr.HealPartition("A")
	assert.True(t, tr.shouldDeliver("A", "B"), "heal must restore A→B")
	assert.True(t, tr.shouldDeliver("B", "A"), "heal must restore B→A")
}

func TestChaosTransport_DropMessageDecrementsCounter(t *testing.T) {
	tr := NewChaosTransport()

	// Drop next 3 messages from A to B.
	tr.DropMessage("A", "B", 3)

	assert.False(t, tr.shouldDeliver("A", "B"), "drop 1 of 3")
	assert.False(t, tr.shouldDeliver("A", "B"), "drop 2 of 3")
	assert.False(t, tr.shouldDeliver("A", "B"), "drop 3 of 3")
	assert.True(t, tr.shouldDeliver("A", "B"), "counter exhausted, deliver resumes")

	// DropMessage is directional — does not affect B→A.
	tr.DropMessage("A", "B", 1)
	assert.True(t, tr.shouldDeliver("B", "A"), "B→A unaffected by A→B drop")
}
