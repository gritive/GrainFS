package cluster

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// fakeRaftNode implements raftNodeAccess for tests. Thread-safe access to the
// observer so the test goroutine can push events.
type fakeRaftNode struct {
	mu       sync.Mutex
	state    raft.NodeState
	observer chan<- raft.Event
}

func (f *fakeRaftNode) State() raft.NodeState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state
}
func (f *fakeRaftNode) RegisterObserver(ch chan<- raft.Event) {
	f.mu.Lock()
	f.observer = ch
	f.mu.Unlock()
}
func (f *fakeRaftNode) DeregisterObserver(ch chan<- raft.Event) {
	f.mu.Lock()
	f.observer = nil
	f.mu.Unlock()
}
func (f *fakeRaftNode) push(e raft.Event) {
	f.mu.Lock()
	ch := f.observer
	f.mu.Unlock()
	if ch != nil {
		ch <- e
	}
}

func TestRaftLeadership_Subscribe_EmitsOnLeaderChange(t *testing.T) {
	fake := &fakeRaftNode{state: raft.Leader}
	rl := &RaftLeadership{Node: fake}
	events, cancel := rl.Subscribe()
	defer cancel()

	fake.push(raft.Event{Type: raft.EventLeaderChange, IsLeader: true, Term: 1})

	select {
	case <-events:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected leader-change signal")
	}
}

func TestRaftLeadership_Subscribe_IgnoresOtherEventTypes(t *testing.T) {
	fake := &fakeRaftNode{}
	rl := &RaftLeadership{Node: fake}
	events, cancel := rl.Subscribe()
	defer cancel()

	// Use EventType(99) so the test is independent of which other events exist.
	fake.push(raft.Event{Type: raft.EventType(99)})

	select {
	case <-events:
		t.Fatal("must not emit on non-leader-change event")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestRaftLeadership_Subscribe_CancelClosesEventsChannel(t *testing.T) {
	fake := &fakeRaftNode{}
	rl := &RaftLeadership{Node: fake}
	events, cancel := rl.Subscribe()
	cancel()

	require.Eventually(t, func() bool {
		_, ok := <-events
		return !ok
	}, 200*time.Millisecond, 5*time.Millisecond, "events must close on cancel")
	fake.mu.Lock()
	obs := fake.observer
	fake.mu.Unlock()
	assert.Nil(t, obs, "raw observer must be deregistered")
}
