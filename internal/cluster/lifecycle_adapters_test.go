package cluster

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// fakeRaftNode implements raftNodeAccess for tests. State is updated via
// SetState; RaftLeadership.Subscribe polls so transitions are detected.
type fakeRaftNode struct {
	state atomic.Int32 // raft.NodeState
}

func (f *fakeRaftNode) State() raft.NodeState { return raft.NodeState(f.state.Load()) }

func (f *fakeRaftNode) SetState(s raft.NodeState) { f.state.Store(int32(s)) }

func newFakeRaftNode(s raft.NodeState) *fakeRaftNode {
	f := &fakeRaftNode{}
	f.SetState(s)
	return f
}

// quickRaftLeadership wires a short poll interval so tests stay fast.
func quickRaftLeadership(f *fakeRaftNode) *RaftLeadership {
	return &RaftLeadership{Node: f, PollInterval: 5 * time.Millisecond}
}

func TestRaftLeadership_Subscribe_EmitsOnLeaderTransition(t *testing.T) {
	fake := newFakeRaftNode(raft.Follower)
	rl := quickRaftLeadership(fake)
	events, cancel := rl.Subscribe()
	defer cancel()

	fake.SetState(raft.Leader)

	select {
	case <-events:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected leader-change signal after transition to leader")
	}
}

func TestRaftLeadership_Subscribe_NoEmitWhenStateUnchanged(t *testing.T) {
	fake := newFakeRaftNode(raft.Follower)
	rl := quickRaftLeadership(fake)
	events, cancel := rl.Subscribe()
	defer cancel()

	select {
	case <-events:
		t.Fatal("must not emit when leadership state is unchanged")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestRaftLeadership_Subscribe_CancelClosesEventsChannel(t *testing.T) {
	fake := newFakeRaftNode(raft.Follower)
	rl := quickRaftLeadership(fake)
	events, cancel := rl.Subscribe()
	cancel()

	require.Eventually(t, func() bool {
		_, ok := <-events
		return !ok
	}, 200*time.Millisecond, 5*time.Millisecond, "events must close on cancel")
}

func TestRaftLeadership_IsLeader(t *testing.T) {
	fake := newFakeRaftNode(raft.Follower)
	rl := &RaftLeadership{Node: fake}
	require.False(t, rl.IsLeader())
	fake.SetState(raft.Leader)
	require.True(t, rl.IsLeader())
}
