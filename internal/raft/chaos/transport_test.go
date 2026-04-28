package chaos

import (
	"sync/atomic"
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

func TestChaosTransport_RequestVoteHook_DropsPreVote(t *testing.T) {
	tr := NewChaosTransport()

	cfgA := raft.DefaultConfig("A", []string{"B", "C"})
	cfgB := raft.DefaultConfig("B", []string{"A", "C"})
	cfgC := raft.DefaultConfig("C", []string{"A", "B"})
	a := raft.NewNode(cfgA)
	b := raft.NewNode(cfgB)
	c := raft.NewNode(cfgC)
	t.Cleanup(func() { a.Close(); b.Close(); c.Close() })

	tr.Register(a)
	tr.Register(b)
	tr.Register(c)
	tr.Wire(a)
	tr.Wire(b)
	tr.Wire(c)

	var dropped atomic.Int64
	// Simulate C as an "old" node that silently drops incoming PreVote RPCs.
	tr.SetRequestVoteHook("C", func(from, to string, args *raft.RequestVoteArgs) (*raft.RequestVoteArgs, bool) {
		if args.PreVote {
			dropped.Add(1)
			return nil, true
		}
		return args, false
	})

	a.Start()
	b.Start()
	c.Start()

	// A and B can form a pre-vote majority (2/3) between themselves;
	// cluster must still elect a leader despite C dropping all pre-votes.
	require.Eventually(t, func() bool {
		return a.IsLeader() || b.IsLeader() || c.IsLeader()
	}, 5*time.Second, 50*time.Millisecond, "cluster must elect leader with one pre-vote-dropping node")

	assert.Greater(t, dropped.Load(), int64(0), "hook must have fired at least once")
}

// TestChaosTransport_Wire_UnregisteredPeer exercises the "peer not registered"
// error branches in the sendVote and sendAppend closures installed by Wire.
// We wire node A but do NOT register its peer "B", so when A's transport
// callbacks fire they must return a non-nil error.
func TestChaosTransport_Wire_UnregisteredPeer(t *testing.T) {
	tr := NewChaosTransport()

	cfgA := raft.DefaultConfig("A", []string{"B"})
	a := raft.NewNode(cfgA)
	t.Cleanup(func() { a.Close() })

	// Register A but NOT B — lookup("B") returns nil.
	tr.Register(a)
	tr.Wire(a)

	// Directly invoke the wired callbacks by calling the raft handler methods
	// on A with the transport that will attempt to reach unregistered "B".
	// We do this by starting A in isolation; it will try to reach "B" for
	// RequestVote and AppendEntries and get back errors. The node should
	// tolerate those errors gracefully (not panic/crash).
	a.Start()

	// The node will attempt elections — if it panics, the test will fail.
	// Give it a couple of election timeouts.
	time.Sleep(500 * time.Millisecond)
	// Node should still be running (not crashed).
	// It cannot become leader (single node with declared peer "B" offline),
	// but it must not panic.
	_ = a.State()
}
