package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/raft"
)

// fakeLeadership is a mutable leadershipSource for tests. set(s) updates the
// stored state and emits an EventLeaderChange to every registered observer
// so reconcile is woken up exactly the way raft.Node would wake it in
// production.
type fakeLeadership struct {
	s         atomic.Int32
	mu        sync.Mutex
	observers []chan<- raft.Event
}

func (f *fakeLeadership) State() raft.NodeState { return raft.NodeState(f.s.Load()) }

func (f *fakeLeadership) set(s raft.NodeState) {
	f.s.Store(int32(s))
	isLeader := s == raft.Leader

	f.mu.Lock()
	obs := append([]chan<- raft.Event(nil), f.observers...)
	f.mu.Unlock()
	for _, ch := range obs {
		select {
		case ch <- raft.Event{Type: raft.EventLeaderChange, IsLeader: isLeader}:
		default:
		}
	}
}

func (f *fakeLeadership) RegisterObserver(ch chan<- raft.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observers = append(f.observers, ch)
}

func (f *fakeLeadership) DeregisterObserver(ch chan<- raft.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, o := range f.observers {
		if o == ch {
			f.observers = append(f.observers[:i], f.observers[i+1:]...)
			return
		}
	}
}

// newTestLifecycleManager builds a LifecycleManager with a fake leadership
// source and a worker over a real DistributedBackend's store. The worker
// interval is large so the periodic ticker never fires during short tests;
// the eager runCycle at start handles the "did the worker actually run"
// check, and lead.set() drives reconcile via the observer channel.
func newTestLifecycleManager(t *testing.T, lead *fakeLeadership) *LifecycleManager {
	t.Helper()
	b := newTestDistributedBackend(t)
	store := lifecycle.NewStore(b.FSMDB())
	worker := lifecycle.NewWorker(store, b, b, 10*time.Second)
	return &LifecycleManager{
		leadership: lead,
		worker:     worker,
		interval:   10 * time.Second,
		logger:     zerolog.Nop(),
	}
}

func waitUntil(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting: %s", msg)
}

func (m *LifecycleManager) isRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

// TestLifecycleManager_StartsOnLeader flips leadership from Follower→Leader
// and asserts the manager starts its worker.
func TestLifecycleManager_StartsOnLeader(t *testing.T) {
	lead := &fakeLeadership{}
	lead.set(raft.Follower)
	mgr := newTestLifecycleManager(t, lead)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		mgr.Run(ctx)
		close(done)
	}()

	// Not leader yet.
	time.Sleep(30 * time.Millisecond)
	assert.False(t, mgr.isRunning(), "worker must not be running while Follower")

	lead.set(raft.Leader)
	waitUntil(t, mgr.isRunning, "worker did not start after leadership gained")

	// Step down — worker must stop.
	lead.set(raft.Follower)
	waitUntil(t, func() bool { return !mgr.isRunning() }, "worker did not stop after losing leadership")

	cancel()
	<-done
}

// TestLifecycleManager_StartIsIdempotent proves that multiple start() calls
// while already running don't spawn extra goroutines.
func TestLifecycleManager_StartIsIdempotent(t *testing.T) {
	lead := &fakeLeadership{}
	lead.set(raft.Leader)
	mgr := newTestLifecycleManager(t, lead)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.start(ctx)
	require.True(t, mgr.isRunning())

	// Second start while running must be a no-op.
	mgr.start(ctx)
	assert.True(t, mgr.isRunning(), "still running")

	mgr.stop()
	assert.False(t, mgr.isRunning())
}

// TestLifecycleManager_RegistersAndDeregistersObserver proves Run subscribes
// to the leadership observer stream and tears the subscription down on exit
// so a long-lived raft.Node is not leaked an unbounded list of channels.
func TestLifecycleManager_RegistersAndDeregistersObserver(t *testing.T) {
	lead := &fakeLeadership{}
	lead.set(raft.Follower)
	mgr := newTestLifecycleManager(t, lead)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { mgr.Run(ctx); close(done) }()

	// Wait until at least one observer is registered.
	waitUntil(t, func() bool {
		lead.mu.Lock()
		defer lead.mu.Unlock()
		return len(lead.observers) >= 1
	}, "Run did not register an observer")

	cancel()
	<-done

	// After Run exits, the observer must be removed.
	lead.mu.Lock()
	count := len(lead.observers)
	lead.mu.Unlock()
	assert.Equal(t, 0, count, "observer must be deregistered after Run exits")
}

// TestLifecycleManager_IgnoresNonLeaderEvents proves Run does not call
// reconcile for raft events other than EventLeaderChange — a stray
// EventFailedHeartbeat must not flip the worker on/off.
func TestLifecycleManager_IgnoresNonLeaderEvents(t *testing.T) {
	lead := &fakeLeadership{}
	lead.set(raft.Follower)
	mgr := newTestLifecycleManager(t, lead)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { mgr.Run(ctx); close(done) }()

	// Wait for observer to register.
	waitUntil(t, func() bool {
		lead.mu.Lock()
		defer lead.mu.Unlock()
		return len(lead.observers) >= 1
	}, "Run did not register observer")

	// Push a non-leader-change event directly to the observers.
	lead.mu.Lock()
	obs := append([]chan<- raft.Event(nil), lead.observers...)
	lead.mu.Unlock()
	for _, ch := range obs {
		select {
		case ch <- raft.Event{Type: raft.EventFailedHeartbeat, PeerID: "peer-1"}:
		default:
		}
	}

	// Give the goroutine a chance to react. State must remain Follower
	// (no reconcile triggered = worker stays stopped).
	time.Sleep(50 * time.Millisecond)
	assert.False(t, mgr.isRunning(), "non-leader events must not start the worker")

	cancel()
	<-done
}

// TestLifecycleManager_ExitsOnCancel ensures Run returns and the worker is
// stopped when the parent ctx is cancelled.
func TestLifecycleManager_ExitsOnCancel(t *testing.T) {
	lead := &fakeLeadership{}
	lead.set(raft.Leader)
	mgr := newTestLifecycleManager(t, lead)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		mgr.Run(ctx)
		close(done)
	}()

	waitUntil(t, mgr.isRunning, "worker did not start while leader")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
	assert.False(t, mgr.isRunning(), "worker must be stopped after Run returns")
}
