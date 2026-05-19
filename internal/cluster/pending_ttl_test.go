package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeTTLExecutor creates a MigrationExecutor with TTL sweep enabled.
func makeTTLExecutor(ttl time.Duration) (*MigrationExecutor, context.CancelFunc) {
	e := NewMigrationExecutorWithTTL(
		&noopMover{},
		&noopRaft{nodeID: "self"},
		2,
		ttl,
	)
	ctx, cancel := context.WithCancel(context.Background())
	e.Start(ctx)
	return e, cancel
}

// TestMigrationExecutor_PendingTTL: expired entry → cancel() called, removed from pending.
func TestMigrationExecutor_PendingTTL(t *testing.T) {
	ttl := 5 * time.Millisecond
	e, cancel := makeTTLExecutor(ttl)
	defer cancel()
	defer e.Stop()

	// Register a fake pending entry (simulate Execute() Phase 2 not yet completing).
	id := "bucket/key/"
	entryCtx, entryCancel := context.WithCancel(context.Background())
	e.registerPending(id, entryCancel)

	require.Eventually(t, func() bool {
		return entryCtx.Err() == context.Canceled && !e.hasPending(id)
	}, 100*time.Millisecond, time.Millisecond, "sweep must cancel and remove the expired entry")

	// Entry should be cancelled and removed.
	assert.ErrorIs(t, entryCtx.Err(), context.Canceled, "sweep must cancel the entry context")
	assert.False(t, e.hasPending(id), "sweep must remove expired entry")
}

// TestMigrationExecutor_TTLDuringPhase3: sweep fires after Phase 2 (proposedAt set)
// but before Raft commit → must NOT cancel on first sweep, must cancel after extension.
// Option A: 1 extension of TTL duration, then cancel.
// Timeline (TTL=10ms, sweep=5ms):
//
//	t=0:    register + markProposed (deadline = t+10ms)
//	t=5:    sweep1 — not expired → skip
//	t=10:   sweep2 — expired, proposedAt!=0 → extend to ~20ms
//	t=15:   sweep3 — not expired (deadline=20ms) → skip  ← check1 here
//	t=20:   sweep4 — extended deadline expired, extended=true → CANCEL ← check2 after this
func TestMigrationExecutor_TTLDuringPhase3(t *testing.T) {
	ttl := 10 * time.Millisecond
	e, cancel := makeTTLExecutor(ttl)
	defer cancel()
	defer e.Stop()

	id := "bucket/key/"
	entryCtx, entryCancel := context.WithCancel(context.Background())
	e.registerPending(id, entryCancel)

	// Simulate Phase 2: mark proposedAt immediately
	e.markProposed(id)

	time.Sleep(ttl + ttl/2)
	require.NoError(t, entryCtx.Err(), "sweep must NOT cancel entry immediately after proposedAt extension")
	assert.True(t, e.hasPending(id), "entry must still exist after first sweep with proposedAt")

	require.Eventually(t, func() bool {
		return entryCtx.Err() == context.Canceled
	}, 100*time.Millisecond, time.Millisecond, "sweep must eventually cancel after extension")
	assert.ErrorIs(t, entryCtx.Err(), context.Canceled, "sweep must eventually cancel after extension")
}

// TestMigrationExecutor_SweepLoopStopsOnCtxDone: sweepLoop exits cleanly on ctx cancel.
func TestMigrationExecutor_SweepLoopStopsOnCtxDone(t *testing.T) {
	e, cancel := makeTTLExecutor(100 * time.Millisecond)
	defer e.Stop()
	cancel() // cancel immediately

	require.Eventually(t, func() bool {
		return e.stopOnce.Load()
	}, 100*time.Millisecond, time.Millisecond, "sweepLoop goroutine should exit")
}

// TestMigrationExecutor_SweepDoesNotCloseCh: ensure sweep doesn't close channels (panic prevention).
func TestMigrationExecutor_SweepDoesNotCloseCh(t *testing.T) {
	ttl := 5 * time.Millisecond
	e, cancel := makeTTLExecutor(ttl)
	defer cancel()
	defer e.Stop()

	id := "bucket/key/"
	_, entryCancel := context.WithCancel(context.Background())
	e.registerPending(id, entryCancel)

	require.Eventually(t, func() bool {
		return !e.hasPending(id)
	}, 100*time.Millisecond, time.Millisecond, "sweep must remove expired entry")

	// Register again with same id — must not panic
	_, entryCancel2 := context.WithCancel(context.Background())
	assert.NotPanics(t, func() {
		e.registerPending(id, entryCancel2)
	})
}

// noopMover is a ShardMover that does nothing.
type noopMover struct{}

func (n *noopMover) ReadShard(_ context.Context, _, _, _ string, _ int) ([]byte, error) {
	return []byte("data"), nil
}
func (n *noopMover) WriteShard(_ context.Context, _, _, _ string, _ int, _ []byte) error { return nil }
func (n *noopMover) DeleteShards(_ context.Context, _, _, _ string) error                { return nil }

// noopRaft is a MigrationRaft that does nothing.
type noopRaft struct{ nodeID string }

func (n *noopRaft) Propose(_ []byte) error { return nil }
func (n *noopRaft) NodeID() string         { return n.nodeID }
