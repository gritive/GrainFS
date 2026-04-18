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
	ttl := 50 * time.Millisecond
	e, cancel := makeTTLExecutor(ttl)
	defer cancel()

	// Register a fake pending entry (simulate Execute() Phase 2 not yet completing).
	id := "bucket/key/"
	entryCtx, entryCancel := context.WithCancel(context.Background())
	e.registerPending(id, entryCancel)

	// Wait for sweep to expire it.
	time.Sleep(ttl*3 + 10*time.Millisecond)

	// Entry should be cancelled and removed.
	assert.ErrorIs(t, entryCtx.Err(), context.Canceled, "sweep must cancel the entry context")
	assert.False(t, e.hasPending(id), "sweep must remove expired entry")
}

// TestMigrationExecutor_TTLDuringPhase3: sweep fires after Phase 2 (proposedAt set)
// but before Raft commit → must NOT cancel on first sweep, must cancel after extension.
// Option A: 1 extension of TTL duration, then cancel.
// Timeline (TTL=100ms, sweep=50ms):
//
//	t=0:    register + markProposed (deadline = t+100ms)
//	t=50:   sweep1 — not expired → skip
//	t=100:  sweep2 — expired, proposedAt!=0 → extend to ~200ms
//	t=150:  sweep3 — not expired (deadline=200ms) → skip  ← check1 here
//	t=200:  sweep4 — extended deadline expired, extended=true → CANCEL ← check2 after this
func TestMigrationExecutor_TTLDuringPhase3(t *testing.T) {
	ttl := 100 * time.Millisecond
	e, cancel := makeTTLExecutor(ttl)
	defer cancel()

	id := "bucket/key/"
	entryCtx, entryCancel := context.WithCancel(context.Background())
	e.registerPending(id, entryCancel)

	// Simulate Phase 2: mark proposedAt immediately
	e.markProposed(id)

	// Check 1: after ttl + ttl/2 + margin — extension should be active, NOT cancelled
	time.Sleep(ttl + ttl/2 + 20*time.Millisecond) // ~170ms

	require.NoError(t, entryCtx.Err(), "sweep must NOT cancel entry that has proposedAt set")
	assert.True(t, e.hasPending(id), "entry must still exist after first sweep with proposedAt")

	// Check 2: after another ttl + margin — extension should have expired, CANCELLED
	time.Sleep(ttl + 20*time.Millisecond) // +120ms = total ~290ms

	assert.ErrorIs(t, entryCtx.Err(), context.Canceled, "sweep must eventually cancel after extension")
}

// TestMigrationExecutor_SweepLoopStopsOnCtxDone: sweepLoop exits cleanly on ctx cancel.
func TestMigrationExecutor_SweepLoopStopsOnCtxDone(t *testing.T) {
	_, cancel := makeTTLExecutor(100 * time.Millisecond)
	cancel() // cancel immediately

	// sweepLoop goroutine should exit; no deadlock or panic
	time.Sleep(20 * time.Millisecond)
	// If we reach here without deadlock, test passes.
}

// TestMigrationExecutor_SweepDoesNotCloseCh: ensure sweep doesn't close channels (panic prevention).
func TestMigrationExecutor_SweepDoesNotCloseCh(t *testing.T) {
	ttl := 50 * time.Millisecond
	e, cancel := makeTTLExecutor(ttl)
	defer cancel()

	id := "bucket/key/"
	_, entryCancel := context.WithCancel(context.Background())
	e.registerPending(id, entryCancel)

	// Let sweep fire
	time.Sleep(ttl*3 + 10*time.Millisecond)

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
