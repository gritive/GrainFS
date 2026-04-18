package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/metrics"
)

// MigrationTask describes a single shard migration request from Raft.
type MigrationTask struct {
	Bucket    string
	Key       string
	VersionID string
	SrcNode   string
	DstNode   string
}

func (t MigrationTask) id() string {
	return t.Bucket + "/" + t.Key + "/" + t.VersionID
}

// ShardMover abstracts shard I/O for testability.
type ShardMover interface {
	ReadShard(ctx context.Context, peer, bucket, key string, shardIdx int) ([]byte, error)
	WriteShard(ctx context.Context, peer, bucket, key string, shardIdx int, data []byte) error
	DeleteShards(ctx context.Context, peer, bucket, key string) error
}

// MigrationRaft is the Raft interface needed by MigrationExecutor.
type MigrationRaft interface {
	Propose(data []byte) error
	NodeID() string
}

// maxDoneHistory caps the idempotency set. When exceeded, the set is reset.
// Worst case: an already-done migration is re-executed, which is idempotent.
const maxDoneHistory = 10_000

// ttlEntry tracks a pending migration for TTL-based cancellation.
// Lock order: mu → pendingMu when both are needed.
// sweepExpired holds pendingMu and calls cancel() — cancel must NOT acquire mu.
type ttlEntry struct {
	cancel     func()
	deadline   time.Time    // when this entry expires; updated on extension
	proposedAt atomic.Int64 // unix nano; non-zero after Phase 2 (Raft proposal submitted)
	extended   atomic.Bool  // true after the single deadline extension (Option A)
}

// MigrationExecutor copies all shards from src→dst, proposes CmdMigrationDone,
// waits for FSM to confirm commit, then deletes from src.
type MigrationExecutor struct {
	mover     ShardMover
	node      MigrationRaft
	numShards int

	// mu protects done, committed, pending (commit channels).
	// Lock order: mu → pendingMu when both are needed.
	mu        sync.Mutex
	done      map[string]struct{}     // idempotency: taskID → done
	committed map[string]struct{}     // early commit arrivals: Raft committed before Execute()
	pending   map[string]chan struct{} // commit channels: taskID → chan

	// pendingMu protects ttlPending. sweepExpired holds pendingMu and calls cancel(),
	// which must NOT re-acquire mu.
	pendingMu  sync.Mutex
	ttlPending map[string]*ttlEntry // TTL-tracked entries: taskID → entry
	pendingTTL time.Duration        // 0 = TTL sweep disabled

	logger *slog.Logger
}

// NewMigrationExecutor creates an executor with the given shard count. TTL sweep disabled.
func NewMigrationExecutor(mover ShardMover, node MigrationRaft, numShards int) *MigrationExecutor {
	return newExecutor(mover, node, numShards, 0)
}

// NewMigrationExecutorWithTTL creates an executor with TTL-based pending sweep.
// Call Start(ctx) to begin the sweep loop.
func NewMigrationExecutorWithTTL(mover ShardMover, node MigrationRaft, numShards int, ttl time.Duration) *MigrationExecutor {
	return newExecutor(mover, node, numShards, ttl)
}

func newExecutor(mover ShardMover, node MigrationRaft, numShards int, ttl time.Duration) *MigrationExecutor {
	return &MigrationExecutor{
		mover:      mover,
		node:       node,
		numShards:  numShards,
		done:       make(map[string]struct{}),
		committed:  make(map[string]struct{}),
		pending:    make(map[string]chan struct{}),
		ttlPending: make(map[string]*ttlEntry),
		pendingTTL: ttl,
		logger:     slog.Default().With("component", "migration"),
	}
}

// Start launches the background TTL sweep loop. No-op if pendingTTL == 0.
func (e *MigrationExecutor) Start(ctx context.Context) {
	if e.pendingTTL == 0 {
		return
	}
	go e.sweepLoop(ctx)
}

// sweepLoop runs sweepExpired every pendingTTL/2, stopping when ctx is done.
func (e *MigrationExecutor) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(e.pendingTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.sweepExpired()
		}
	}
}

// sweepExpired cancels and removes TTL entries that have passed their deadline.
// For entries with proposedAt set (Phase 2 done, awaiting Raft commit), the
// deadline is extended by one TTL window (Option A). A second expiry cancels.
func (e *MigrationExecutor) sweepExpired() {
	now := time.Now()

	e.pendingMu.Lock()
	var toCancel []func()
	var toDelete []string
	for id, entry := range e.ttlPending {
		if now.Before(entry.deadline) {
			continue // not yet expired
		}
		// Entry has expired.
		if entry.proposedAt.Load() != 0 && !entry.extended.Load() {
			// Phase 2 complete: extend once instead of cancelling (Option A).
			entry.deadline = now.Add(e.pendingTTL)
			entry.extended.Store(true)
			continue
		}
		// Either not proposed yet, or already extended once: cancel.
		toCancel = append(toCancel, entry.cancel)
		toDelete = append(toDelete, id)
	}
	for _, id := range toDelete {
		delete(e.ttlPending, id)
	}
	e.pendingMu.Unlock()

	for _, cancel := range toCancel {
		cancel()
	}
	if len(toDelete) > 0 {
		metrics.BalancerMigrationPendingTTLExpiredTotal.Add(float64(len(toDelete)))
	}
}

// registerPending adds an entry to the TTL sweep map with deadline = now + pendingTTL.
func (e *MigrationExecutor) registerPending(id string, cancel func()) {
	e.pendingMu.Lock()
	e.ttlPending[id] = &ttlEntry{
		cancel:   cancel,
		deadline: time.Now().Add(e.pendingTTL),
	}
	e.pendingMu.Unlock()
}

// removePending removes an entry from the TTL sweep map (called after successful commit).
func (e *MigrationExecutor) removePending(id string) {
	e.pendingMu.Lock()
	delete(e.ttlPending, id)
	e.pendingMu.Unlock()
}

// markProposed sets proposedAt for id (called after Phase 2 succeeds).
func (e *MigrationExecutor) markProposed(id string) {
	e.pendingMu.Lock()
	entry, ok := e.ttlPending[id]
	e.pendingMu.Unlock()
	if ok {
		entry.proposedAt.Store(time.Now().UnixNano())
	}
}

// hasPending reports whether id is in the TTL sweep map.
func (e *MigrationExecutor) hasPending(id string) bool {
	e.pendingMu.Lock()
	_, ok := e.ttlPending[id]
	e.pendingMu.Unlock()
	return ok
}

// NotifyCommit is called by the FSM when CmdMigrationDone is applied.
// It unblocks any Execute() waiting for that task's commit.
// If Execute() has not yet registered its pending channel (early arrival), the task is
// recorded in committed so Execute() skips Phases 1–3 but still runs Phase 4 (delete src).
func (e *MigrationExecutor) NotifyCommit(bucket, key, versionID string) {
	id := bucket + "/" + key + "/" + versionID
	e.mu.Lock()
	ch, ok := e.pending[id]
	if ok {
		delete(e.pending, id)
	} else {
		// FSM applied CmdMigrationDone before Execute registered.
		// Mark committed so Execute() still runs Phase 4 (DeleteShards).
		e.markCommitted(id)
	}
	e.mu.Unlock()
	if ok {
		close(ch)
	}
}

// Run processes migration tasks from the channel until ctx is cancelled or ch is closed.
// Each task is executed concurrently.
func (e *MigrationExecutor) Run(ctx context.Context, tasks <-chan MigrationTask) {
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-tasks:
			if !ok {
				return
			}
			go func(t MigrationTask) {
				if err := e.Execute(ctx, t); err != nil {
					e.logger.Warn("migration execute failed", "task", t.id(), "err", err)
					metrics.BalancerMigrationsFailedTotal.Inc()
				}
			}(task)
		}
	}
}

// Execute performs the full migration sequence: copy → propose done → wait commit → delete src.
// It is idempotent: repeated calls with the same task id are no-ops.
// If a concurrent Execute is already in flight for the same id, this call waits for it to complete.
// If NotifyCommit arrived early (before Execute), earlyCommit=true skips Phases 1–3 and
// runs Phase 4 (delete src) directly — preventing orphan shards on the source node.
func (e *MigrationExecutor) Execute(ctx context.Context, task MigrationTask) error {
	id := task.id()

	e.mu.Lock()
	if _, already := e.done[id]; already {
		e.mu.Unlock()
		return nil
	}
	// If a concurrent Execute already registered a pending channel, wait for it instead of overwriting.
	if existingCh, inFlight := e.pending[id]; inFlight {
		e.mu.Unlock()
		select {
		case <-existingCh:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}
	// Check for early commit (NotifyCommit arrived before Execute was called).
	// In this case Phases 1–3 are already complete; only Phase 4 (delete src) remains.
	earlyCommit := false
	if _, ec := e.committed[id]; ec {
		delete(e.committed, id)
		earlyCommit = true
	}
	// Always register a pending channel — even on earlyCommit — so that a concurrent
	// Execute that arrives after we release mu (but before markDone) blocks here
	// instead of falling through to Phase 1 and re-copying already-committed shards.
	commitCh := make(chan struct{})
	e.pending[id] = commitCh
	e.mu.Unlock()

	if !earlyCommit {
		// Phase 1: copy all shards src → dst
		e.logger.Debug("migration phase start", "phase", "1", "task", id)
		copyStart := time.Now()
		for i := range e.numShards {
			data, err := e.mover.ReadShard(ctx, task.SrcNode, task.Bucket, task.Key, i)
			if err != nil {
				e.cleanupPending(id)
				return fmt.Errorf("migration read shard %d: %w", i, err)
			}
			if err := e.mover.WriteShard(ctx, task.DstNode, task.Bucket, task.Key, i, data); err != nil {
				metrics.BalancerShardWriteErrorsTotal.Inc()
				e.cleanupPending(id)
				return fmt.Errorf("migration write shard %d: %w", i, err)
			}
		}
		metrics.BalancerShardCopyDuration.Observe(time.Since(copyStart).Seconds())

		// Phase 2: propose CmdMigrationDone to Raft
		e.logger.Debug("migration phase start", "phase", "2", "task", id)
		if err := e.proposeDone(task); err != nil {
			e.cleanupPending(id)
			return err
		}
		e.markProposed(id)

		// Phase 3: wait for FSM to apply CmdMigrationDone (= Raft commit confirmed)
		e.logger.Debug("migration phase start", "phase", "3", "task", id)
		select {
		case <-commitCh:
		case <-ctx.Done():
			e.cleanupPending(id)
			return fmt.Errorf("migration: commit wait cancelled for %s/%s", task.Bucket, task.Key)
		}
	}

	// Mark done before Phase 4. For the earlyCommit path, also remove the sentinel
	// pending channel we registered and close it to unblock any concurrent Execute
	// that arrived after we released mu but before markDone.
	// For the normal path, NotifyCommit already removed the channel from pending.
	e.mu.Lock()
	e.markDone(id)
	if earlyCommit {
		delete(e.pending, id)
	}
	e.mu.Unlock()
	if earlyCommit {
		close(commitCh)
	}

	// Phase 4: delete from src — runs whether earlyCommit or normal path.
	e.logger.Debug("migration phase start", "phase", "4", "task", id)
	if err := e.mover.DeleteShards(ctx, task.SrcNode, task.Bucket, task.Key); err != nil {
		e.logger.Warn("migration: delete src failed",
			"phase", "4", "src", task.SrcNode, "bucket", task.Bucket, "key", task.Key, "err", err)
		metrics.BalancerMigrationsFailedTotal.Inc()
	}
	e.removePending(id)

	metrics.BalancerMigrationsDoneTotal.Inc()
	return nil
}

// cleanupPending removes the pending channel for id (error paths) and closes it
// so any goroutine waiting on it unblocks and returns rather than leaking.
func (e *MigrationExecutor) cleanupPending(id string) {
	e.mu.Lock()
	ch, ok := e.pending[id]
	if ok {
		delete(e.pending, id)
	}
	e.mu.Unlock()
	if ok {
		close(ch)
	}
}

// markDone records id as completed. Must be called with mu held.
// Resets the map when it exceeds maxDoneHistory to bound memory use.
func (e *MigrationExecutor) markDone(id string) {
	if len(e.done) >= maxDoneHistory {
		e.done = make(map[string]struct{}, maxDoneHistory)
	}
	e.done[id] = struct{}{}
}

// markCommitted records an early Raft commit for id. Must be called with mu held.
// Resets the map when it exceeds maxDoneHistory to bound memory use.
func (e *MigrationExecutor) markCommitted(id string) {
	if len(e.committed) >= maxDoneHistory {
		e.committed = make(map[string]struct{}, maxDoneHistory)
	}
	e.committed[id] = struct{}{}
}

func (e *MigrationExecutor) proposeDone(task MigrationTask) error {
	inner, err := proto.Marshal(&clusterpb.MigrationDoneCmd{
		Bucket:    task.Bucket,
		Key:       task.Key,
		VersionId: task.VersionID,
		SrcNode:   task.SrcNode,
		DstNode:   task.DstNode,
	})
	if err != nil {
		return fmt.Errorf("migration: marshal MigrationDoneCmd: %w", err)
	}
	outer, err := proto.Marshal(&clusterpb.Command{
		Type: uint32(CmdMigrationDone),
		Data: inner,
	})
	if err != nil {
		return fmt.Errorf("migration: marshal Command: %w", err)
	}
	return e.node.Propose(outer)
}
