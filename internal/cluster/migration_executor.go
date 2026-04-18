package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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

// MigrationExecutor copies all shards from src→dst, proposes CmdMigrationDone,
// waits for FSM to confirm commit, then deletes from src.
type MigrationExecutor struct {
	mover     ShardMover
	node      MigrationRaft
	numShards int

	mu        sync.Mutex
	done      map[string]struct{}     // idempotency: taskID → done
	committed map[string]struct{}     // early commit arrivals: Raft committed before Execute()
	pending   map[string]chan struct{} // commit channels: taskID → chan

	logger *slog.Logger
}

// NewMigrationExecutor creates an executor with the given shard count.
func NewMigrationExecutor(mover ShardMover, node MigrationRaft, numShards int) *MigrationExecutor {
	return &MigrationExecutor{
		mover:     mover,
		node:      node,
		numShards: numShards,
		done:      make(map[string]struct{}),
		committed: make(map[string]struct{}),
		pending:   make(map[string]chan struct{}),
		logger:    slog.Default().With("component", "migration"),
	}
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
	var commitCh chan struct{}
	if !earlyCommit {
		commitCh = make(chan struct{})
		e.pending[id] = commitCh
	}
	e.mu.Unlock()

	if !earlyCommit {
		// Phase 1: copy all shards src → dst
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
		if err := e.proposeDone(task); err != nil {
			e.cleanupPending(id)
			return err
		}

		// Phase 3: wait for FSM to apply CmdMigrationDone (= Raft commit confirmed)
		select {
		case <-commitCh:
		case <-ctx.Done():
			e.cleanupPending(id)
			return fmt.Errorf("migration: commit wait cancelled for %s/%s", task.Bucket, task.Key)
		}
	}

	// Mark done before Phase 4: concurrent callers waiting on the commit channel
	// would otherwise race to re-enter Execute() and start Phase 1 again before
	// DeleteShards completes (Phase 4 is best-effort and logged on failure).
	e.mu.Lock()
	e.markDone(id)
	e.mu.Unlock()

	// Phase 4: delete from src — runs whether earlyCommit or normal path.
	if err := e.mover.DeleteShards(ctx, task.SrcNode, task.Bucket, task.Key); err != nil {
		e.logger.Warn("migration: delete src failed",
			"src", task.SrcNode, "bucket", task.Bucket, "key", task.Key, "err", err)
	}

	metrics.BalancerMigrationsDoneTotal.Inc()
	return nil
}

// cleanupPending removes the pending channel for id, used on error paths.
func (e *MigrationExecutor) cleanupPending(id string) {
	e.mu.Lock()
	delete(e.pending, id)
	e.mu.Unlock()
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
