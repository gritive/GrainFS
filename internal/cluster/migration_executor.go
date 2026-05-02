package cluster

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

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

const migrationCoordChanBuf = 4096

// ttlEntry tracks a pending migration for TTL-based cancellation. Entries are
// owned by the coordination actor.
type ttlEntry struct {
	cancel     func()
	deadline   time.Time    // when this entry expires; updated on extension
	proposedAt atomic.Int64 // unix nano; non-zero after Phase 2 (Raft proposal submitted)
	extended   atomic.Bool  // true after the single deadline extension (Option A)
}

type pendingMigration struct {
	ch     chan struct{}
	closed bool
}

type migrationCoordKind uint8

const (
	migrationCoordBegin migrationCoordKind = iota
	migrationCoordNotifyCommit
	migrationCoordFinish
	migrationCoordCleanup
	migrationCoordMarkDone
	migrationCoordRegisterTTL
	migrationCoordRemoveTTL
	migrationCoordMarkProposed
	migrationCoordHasPendingTTL
	migrationCoordSweepExpired
	migrationCoordDoneCount
	migrationCoordDoneContains
	migrationCoordCommittedCount
	migrationCoordPendingContains
	migrationCoordPendingIDs
)

type migrationCoordMsg struct {
	kind   migrationCoordKind
	id     string
	cancel func()
	reply  any
}

type migrationBeginResp struct {
	alreadyDone bool
	waitCh      chan struct{}
	earlyCommit bool
	commitCh    chan struct{}
}

type migrationCoordState struct {
	done       map[string]struct{}          // idempotency: taskID → done
	committed  map[string]struct{}          // early commit arrivals: Raft committed before Execute()
	pending    map[string]*pendingMigration // commit waiters: taskID → chan
	ttlPending map[string]*ttlEntry         // TTL-tracked entries: taskID → entry
}

// MigrationExecutor copies all shards from src→dst, proposes CmdMigrationDone,
// waits for FSM to confirm commit, then deletes from src.
type MigrationExecutor struct {
	mover     ShardMover
	node      MigrationRaft
	numShards int

	// shardCountFor returns the number of shards for a given versioned object.
	// nil means use numShards for every object (legacy behaviour).
	shardCountFor func(bucket, key, versionID string) int

	// maxWriteRetries is the maximum number of WriteShard attempts per shard (0 = no retry).
	maxWriteRetries int
	retryBaseDelay  time.Duration

	// coordCh is the actor mailbox for all migration coordination state. The
	// actor owns done/committed/pending/ttlPending exclusively; shard I/O stays
	// in Execute goroutines so large payloads are not copied through the actor.
	coordCh    chan migrationCoordMsg
	pendingTTL time.Duration // 0 = TTL sweep disabled

	// quit is closed by Stop() to terminate the sweep goroutine.
	// stopOnce ensures double-close panic cannot occur.
	quit     chan struct{}
	stopOnce atomic.Bool

	logger zerolog.Logger
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
	e := &MigrationExecutor{
		mover:          mover,
		node:           node,
		numShards:      numShards,
		retryBaseDelay: 500 * time.Millisecond,
		coordCh:        make(chan migrationCoordMsg, migrationCoordChanBuf),
		pendingTTL:     ttl,
		quit:           make(chan struct{}),
		logger:         log.With().Str("component", "migration").Logger(),
	}
	go e.runCoordActor()
	return e
}

// SetShardCounter installs a per-object shard-count callback. fn receives (bucket,
// key, versionID) and returns the number of shards that exist for that object.
// A return value of 0 falls back to numShards (not "zero shards"). Call before Start.
func (e *MigrationExecutor) SetShardCounter(fn func(bucket, key, versionID string) int) {
	e.shardCountFor = fn
}

// SetMaxWriteRetries configures the maximum number of WriteShard attempts per shard.
// 0 (default) means no retry. Call before Start.
func (e *MigrationExecutor) SetMaxWriteRetries(n int) {
	e.maxWriteRetries = n
}

// Start launches the background TTL sweep loop. No-op if pendingTTL <= 0.
func (e *MigrationExecutor) Start(ctx context.Context) {
	if e.pendingTTL <= 0 {
		return
	}
	go e.sweepLoop(ctx)
}

// Stop gracefully shuts down the background sweep loop. Safe to call multiple times.
func (e *MigrationExecutor) Stop() {
	if e.stopOnce.CompareAndSwap(false, true) {
		close(e.quit)
	}
}

// sweepLoop runs sweepExpired every pendingTTL/2, stopping when ctx or quit is done.
func (e *MigrationExecutor) sweepLoop(ctx context.Context) {
	defer e.Stop()
	ticker := time.NewTicker(e.pendingTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.quit:
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
	e.sendCoord(migrationCoordMsg{kind: migrationCoordSweepExpired})
}

// registerPending adds an entry to the TTL sweep map with deadline = now + pendingTTL.
func (e *MigrationExecutor) registerPending(id string, cancel func()) {
	e.sendCoord(migrationCoordMsg{kind: migrationCoordRegisterTTL, id: id, cancel: cancel})
}

// removePending removes an entry from the TTL sweep map (called after successful commit).
func (e *MigrationExecutor) removePending(id string) {
	e.sendCoord(migrationCoordMsg{kind: migrationCoordRemoveTTL, id: id})
}

// markProposed sets proposedAt for id (called after Phase 2 succeeds).
func (e *MigrationExecutor) markProposed(id string) {
	e.sendCoord(migrationCoordMsg{kind: migrationCoordMarkProposed, id: id})
}

// hasPending reports whether id is in the TTL sweep map.
func (e *MigrationExecutor) hasPending(id string) bool {
	reply := make(chan bool, 1)
	if !e.sendCoord(migrationCoordMsg{kind: migrationCoordHasPendingTTL, id: id, reply: reply}) {
		return false
	}
	return <-reply
}

// NotifyCommit is called by the FSM when CmdMigrationDone is applied.
// It unblocks any Execute() waiting for that task's commit.
// If Execute() has not yet registered its pending channel (early arrival), the task is
// recorded in committed so Execute() skips Phases 1–3 but still runs Phase 4 (delete src).
//
// The pending channel is intentionally NOT removed here. It is removed by Execute() after
// markDone, so that concurrent Execute calls arriving between NotifyCommit and markDone
// see an inFlight entry and wait on the (now closed) channel rather than re-copying shards.
func (e *MigrationExecutor) NotifyCommit(bucket, key, versionID string) {
	id := bucket + "/" + key + "/" + versionID
	e.sendCoord(migrationCoordMsg{kind: migrationCoordNotifyCommit, id: id})
}

// Run processes migration tasks from the channel until ctx is cancelled or ch is closed.
// Each task is executed concurrently.
func (e *MigrationExecutor) Run(ctx context.Context, tasks <-chan MigrationTask) {
	defer e.Stop()
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
					e.logger.Warn().Str("task", t.id()).Err(err).Msg("migration execute failed")
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

	begin, err := e.beginExecute(ctx, id)
	if err != nil {
		return err
	}
	if begin.alreadyDone {
		return nil
	}
	if begin.waitCh != nil {
		select {
		case <-begin.waitCh:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	earlyCommit := begin.earlyCommit
	commitCh := begin.commitCh

	// Wire TTL cancellation: derive a cancellable context so sweepExpired can abort
	// a stuck migration by calling cancel(). defer ensures cleanup on all exit paths.
	if e.pendingTTL > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer cancel()
		e.registerPending(id, cancel)
	}

	if !earlyCommit {
		// Phase 1: copy all shards src → dst
		e.logger.Debug().Str("phase", "1").Str("task", id).Msg("migration phase start")
		copyStart := time.Now()
		nShards := e.numShards
		if e.shardCountFor != nil {
			if n := e.shardCountFor(task.Bucket, task.Key, task.VersionID); n > 0 {
				nShards = n
			}
		}
		for i := range nShards {
			data, err := e.mover.ReadShard(ctx, task.SrcNode, task.Bucket, task.Key, i)
			if err != nil {
				e.cleanupPending(id)
				return fmt.Errorf("migration read shard %d: %w", i, err)
			}
			writeAttempts := e.maxWriteRetries
			if writeAttempts <= 0 {
				writeAttempts = 1
			}
			shardIdx := i // capture for closure
			writeErr := retryWriteShard(ctx, func() error {
				return e.mover.WriteShard(ctx, task.DstNode, task.Bucket, task.Key, shardIdx, data)
			}, writeAttempts, e.retryBaseDelay, task.DstNode, shardIdx)
			if writeErr != nil {
				metrics.BalancerShardWriteErrorsTotal.Inc()
				e.cleanupPending(id)
				return fmt.Errorf("migration write shard %d: %w", i, writeErr)
			}
		}
		metrics.BalancerShardCopyDuration.Observe(time.Since(copyStart).Seconds())

		// Phase 2: propose CmdMigrationDone to Raft
		e.logger.Debug().Str("phase", "2").Str("task", id).Msg("migration phase start")
		if err := e.proposeDone(task); err != nil {
			e.cleanupPending(id)
			return err
		}
		e.markProposed(id)

		// Phase 3: wait for FSM to apply CmdMigrationDone (= Raft commit confirmed)
		e.logger.Debug().Str("phase", "3").Str("task", id).Msg("migration phase start")
		select {
		case <-commitCh:
		case <-ctx.Done():
			e.cleanupPending(id)
			return fmt.Errorf("migration: commit wait cancelled for %s/%s", task.Bucket, task.Key)
		}
	}

	// Mark done and remove the pending channel before Phase 4.
	// Both paths (normal and earlyCommit) delete pending[id] here, so concurrent
	// Execute calls that arrive between NotifyCommit and markDone see an inFlight
	// entry (a closed channel) and return nil rather than re-copying shards.
	e.finishPending(id)

	// Phase 4: delete from src — runs whether earlyCommit or normal path.
	e.logger.Debug().Str("phase", "4").Str("task", id).Msg("migration phase start")
	if err := e.mover.DeleteShards(ctx, task.SrcNode, task.Bucket, task.Key); err != nil {
		e.logger.Warn().Str("phase", "4").Str("src", task.SrcNode).Str("bucket", task.Bucket).Str("key", task.Key).Err(err).Msg("migration: delete src failed")
		metrics.BalancerMigrationsFailedTotal.Inc()
	}
	e.removePending(id)

	metrics.BalancerMigrationsDoneTotal.Inc()
	return nil
}

// cleanupPending removes the pending channel for id (error paths) and closes it
// so any goroutine waiting on it unblocks and returns rather than leaking.
func (e *MigrationExecutor) cleanupPending(id string) {
	e.sendCoord(migrationCoordMsg{kind: migrationCoordCleanup, id: id})
	e.removePending(id)
}

// markDone records id as completed through the coordination actor.
// Resets the map when it exceeds maxDoneHistory to bound memory use.
func (e *MigrationExecutor) markDone(id string) {
	e.sendCoord(migrationCoordMsg{kind: migrationCoordMarkDone, id: id})
}

func (e *MigrationExecutor) proposeDone(task MigrationTask) error {
	outer, err := EncodeCommand(CmdMigrationDone, MigrationDoneFSMCmd{
		Bucket:    task.Bucket,
		Key:       task.Key,
		VersionID: task.VersionID,
		SrcNode:   task.SrcNode,
		DstNode:   task.DstNode,
	})
	if err != nil {
		return fmt.Errorf("migration: marshal MigrationDoneCmd: %w", err)
	}
	return e.node.Propose(outer)
}

func (e *MigrationExecutor) beginExecute(ctx context.Context, id string) (migrationBeginResp, error) {
	reply := make(chan migrationBeginResp, 1)
	msg := migrationCoordMsg{kind: migrationCoordBegin, id: id, reply: reply}
	select {
	case e.coordCh <- msg:
	case <-ctx.Done():
		return migrationBeginResp{}, ctx.Err()
	case <-e.quit:
		return migrationBeginResp{}, context.Canceled
	}
	select {
	case resp := <-reply:
		return resp, nil
	case <-ctx.Done():
		return migrationBeginResp{}, ctx.Err()
	case <-e.quit:
		return migrationBeginResp{}, context.Canceled
	}
}

func (e *MigrationExecutor) finishPending(id string) {
	e.sendCoord(migrationCoordMsg{kind: migrationCoordFinish, id: id})
}

func (e *MigrationExecutor) sendCoord(msg migrationCoordMsg) bool {
	select {
	case e.coordCh <- msg:
		return true
	case <-e.quit:
		return false
	}
}

func (e *MigrationExecutor) runCoordActor() {
	st := migrationCoordState{
		done:       make(map[string]struct{}),
		committed:  make(map[string]struct{}),
		pending:    make(map[string]*pendingMigration),
		ttlPending: make(map[string]*ttlEntry),
	}
	for {
		select {
		case <-e.quit:
			st.closeAllPending()
			return
		case msg := <-e.coordCh:
			e.handleCoordMsg(&st, msg)
		}
	}
}

func (e *MigrationExecutor) handleCoordMsg(st *migrationCoordState, msg migrationCoordMsg) {
	switch msg.kind {
	case migrationCoordBegin:
		reply := msg.reply.(chan migrationBeginResp)
		if _, ok := st.done[msg.id]; ok {
			reply <- migrationBeginResp{alreadyDone: true}
			return
		}
		if pending, ok := st.pending[msg.id]; ok {
			reply <- migrationBeginResp{waitCh: pending.ch}
			return
		}
		_, earlyCommit := st.committed[msg.id]
		if earlyCommit {
			delete(st.committed, msg.id)
		}
		ch := make(chan struct{})
		st.pending[msg.id] = &pendingMigration{ch: ch}
		reply <- migrationBeginResp{earlyCommit: earlyCommit, commitCh: ch}
	case migrationCoordNotifyCommit:
		if pending, ok := st.pending[msg.id]; ok {
			pending.close()
			return
		}
		st.markCommitted(msg.id)
	case migrationCoordFinish:
		st.markDone(msg.id)
		if pending, ok := st.pending[msg.id]; ok {
			pending.close()
			delete(st.pending, msg.id)
		}
	case migrationCoordCleanup:
		if pending, ok := st.pending[msg.id]; ok {
			pending.close()
			delete(st.pending, msg.id)
		}
	case migrationCoordMarkDone:
		st.markDone(msg.id)
	case migrationCoordRegisterTTL:
		st.ttlPending[msg.id] = &ttlEntry{
			cancel:   msg.cancel,
			deadline: time.Now().Add(e.pendingTTL),
		}
	case migrationCoordRemoveTTL:
		delete(st.ttlPending, msg.id)
	case migrationCoordMarkProposed:
		if entry, ok := st.ttlPending[msg.id]; ok {
			entry.proposedAt.Store(time.Now().UnixNano())
		}
	case migrationCoordHasPendingTTL:
		reply := msg.reply.(chan bool)
		_, ok := st.ttlPending[msg.id]
		reply <- ok
	case migrationCoordSweepExpired:
		e.sweepExpiredLocked(st)
	case migrationCoordDoneCount:
		msg.reply.(chan int) <- len(st.done)
	case migrationCoordDoneContains:
		_, ok := st.done[msg.id]
		msg.reply.(chan bool) <- ok
	case migrationCoordCommittedCount:
		msg.reply.(chan int) <- len(st.committed)
	case migrationCoordPendingContains:
		_, ok := st.pending[msg.id]
		msg.reply.(chan bool) <- ok
	case migrationCoordPendingIDs:
		ids := make([]string, 0, len(st.pending))
		for id := range st.pending {
			ids = append(ids, id)
		}
		msg.reply.(chan []string) <- ids
	}
}

func (e *MigrationExecutor) sweepExpiredLocked(st *migrationCoordState) {
	now := time.Now()
	expired := 0
	for id, entry := range st.ttlPending {
		if now.Before(entry.deadline) {
			continue
		}
		if entry.proposedAt.Load() != 0 && !entry.extended.Load() {
			entry.deadline = now.Add(e.pendingTTL)
			entry.extended.Store(true)
			continue
		}
		entry.cancel()
		delete(st.ttlPending, id)
		expired++
	}
	if expired > 0 {
		metrics.BalancerMigrationPendingTTLExpiredTotal.Add(float64(expired))
	}
}

func (st *migrationCoordState) markDone(id string) {
	if len(st.done) >= maxDoneHistory {
		st.done = make(map[string]struct{}, maxDoneHistory)
	}
	st.done[id] = struct{}{}
}

func (st *migrationCoordState) markCommitted(id string) {
	if len(st.committed) >= maxDoneHistory {
		st.committed = make(map[string]struct{}, maxDoneHistory)
	}
	st.committed[id] = struct{}{}
}

func (st *migrationCoordState) closeAllPending() {
	for _, pending := range st.pending {
		pending.close()
	}
}

func (p *pendingMigration) close() {
	if p.closed {
		return
	}
	close(p.ch)
	p.closed = true
}
