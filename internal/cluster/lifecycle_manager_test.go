package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/raft"
)

// fakeLeadership is a mutable leadershipSource for tests.
type fakeLeadership struct{ s atomic.Int32 }

func (f *fakeLeadership) State() raft.NodeState { return raft.NodeState(f.s.Load()) }
func (f *fakeLeadership) set(s raft.NodeState)  { f.s.Store(int32(s)) }

// newTestLifecycleManager builds a LifecycleManager with a fake leadership
// source and a worker over a real DistributedBackend's store. We skip the
// eager worker run interval to keep tests fast — the worker.Run kicks off a
// runCycle immediately on start, and ListBuckets returning nothing makes the
// cycle a no-op.
func newTestLifecycleManager(t *testing.T, lead *fakeLeadership) *LifecycleManager {
	t.Helper()
	b := newTestDistributedBackend(t)
	store := lifecycle.NewStore(b.FSMDB())
	// 10s interval so the ticker never fires during short tests; the eager
	// runCycle at start handles the "did the worker actually run" check.
	worker := lifecycle.NewWorker(store, b, b, 10*time.Second)
	return &LifecycleManager{
		leadership: lead,
		worker:     worker,
		interval:   10 * time.Second,
		pollEvery:  10 * time.Millisecond,
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
