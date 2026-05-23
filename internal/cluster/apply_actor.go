package cluster

import (
	"errors"
	"os"
	"strconv"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/raft"
)

// maxApplyBatchEntries caps entries per shared-transaction commit. Mirrors the
// raft propose path's maxProposeAppendBatch.
const maxApplyBatchEntries = 64

// maxApplyBatchBytes caps the summed raw-command size per batch. The ceiling is
// badgerutil.SmallMaxBatchSize (~314 KiB); 64 KiB leaves ~5x margin because the
// command-byte sum underestimates the real write set for append/coalesce. The
// ErrTxnTooBig fallback is the safety net when the estimate is wrong.
const maxApplyBatchBytes = 64 << 10

// commitApplyTxn is the batch-commit seam. Tests override it to inject failures.
var commitApplyTxn = func(txn *badger.Txn) error { return txn.Commit() }

// applyBatchMax parses a GRAINFS_RAFT_APPLY_BATCH_MAX value into an entry cap
// in [1, maxApplyBatchEntries]. Empty or invalid -> maxApplyBatchEntries.
// 0 is treated as 1 (batching disabled).
func applyBatchMax(v string) int {
	if v == "" {
		return maxApplyBatchEntries
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return maxApplyBatchEntries
	}
	if n == 0 {
		return 1
	}
	if n > maxApplyBatchEntries {
		return maxApplyBatchEntries
	}
	return n
}

// applyBatchEntriesCap is resolved once at process start. Tests / bench
// isolation set GRAINFS_RAFT_APPLY_BATCH_MAX=1 to disable batching.
var applyBatchEntriesCap = applyBatchMax(os.Getenv("GRAINFS_RAFT_APPLY_BATCH_MAX"))

// applyActor owns the FSM apply loop. It drains committed Raft entries, applies
// each batch to a single BadgerDB transaction, and emits per-entry results.
// Single-goroutine: batch/results buffers are reused without synchronization.
type applyActor struct {
	db      *badger.DB
	fsm     *FSM
	batch   []raft.LogEntry // reused across iterations (batch[:0]); never re-nil'd
	results []error         // reused across iterations
}

// applyBatch applies a batch of command entries to one transaction and returns
// the per-entry results (results[i] for batch[i]). On mid-batch ErrTxnTooBig or
// commit failure it discards and re-applies each entry in its own transaction.
func (a *applyActor) applyBatch(batch []raft.LogEntry) []error {
	results := a.results[:0]
	txn := a.db.NewTransaction(true)
	for _, e := range batch {
		results = append(results, a.fsm.ApplyTxn(txn, e.Command))
	}

	// Check ErrTxnTooBig BEFORE committing. A partial commit followed by
	// individual replay would double-apply the entries that fit and corrupt
	// the result vector for delete-style handlers (applyCompleteMultipart
	// re-run sees the mpu key already gone -> spurious ErrUploadNotFound).
	needFallback := false
	for _, r := range results {
		if errors.Is(r, badger.ErrTxnTooBig) {
			needFallback = true
			break
		}
	}
	if !needFallback {
		needFallback = commitApplyTxn(txn) != nil
	}

	if needFallback {
		txn.Discard() // uncommitted on the ErrTxnTooBig path
		results = results[:0]
		for _, e := range batch {
			results = append(results, a.fsm.Apply(e.Command))
		}
		metrics.ApplyBatchCommitFallbackTotal.Inc()
	}

	metrics.ApplyBatchSize.Observe(float64(len(batch)))
	a.results = results
	return results
}

// run is the apply actor's main loop. It drains committed entries from the Raft
// node, batches command entries into one transaction, and handles snapshot
// entries / snapshot requests as batch barriers. Returns when stop fires or
// ApplyCh closes.
func (a *applyActor) run(b *DistributedBackend, stop <-chan struct{}) {
	applyCh := b.node.ApplyCh()
	for {
		select {
		case <-stop:
			return
		case req := <-b.snapRequests:
			b.completeRaftSnapshotRequest(req)
		case entry, ok := <-applyCh:
			if !ok {
				b.logger.Debug().Msg("apply loop: ApplyCh closed; exiting")
				return
			}
			if !a.collect(b, entry, applyCh) {
				return
			}
		}
	}
}

// collect handles one entry received from ApplyCh: for a command entry it
// opportunistically drains more command entries (up to the caps) and commits
// the batch; for a snapshot entry it restores standalone. Returns false when
// the loop should exit (ApplyCh closed mid-drain).
func (a *applyActor) collect(b *DistributedBackend, first raft.LogEntry, applyCh <-chan raft.LogEntry) bool {
	if first.Type == raft.LogEntrySnapshot {
		a.restore(b, first)
		return true
	}
	if first.Type != raft.LogEntryCommand {
		return true // LogEntryNoOp / conf-change: nothing for the FSM
	}

	a.batch = a.batch[:0]
	a.batch = append(a.batch, first)
	batchBytes := len(first.Command)

	// Opportunistic non-blocking drain of already-available command entries.
	drained := true
	for drained && len(a.batch) < applyBatchEntriesCap && batchBytes < maxApplyBatchBytes {
		select {
		case e, ok := <-applyCh:
			if !ok {
				a.commitBatch(b)
				return false
			}
			if e.Type != raft.LogEntryCommand {
				a.commitBatch(b)
				a.collectNonCommand(b, e)
				return true
			}
			a.batch = append(a.batch, e)
			batchBytes += len(e.Command)
		default:
			drained = false
		}
	}
	a.commitBatch(b)
	return true
}

// collectNonCommand handles a non-command entry encountered mid-drain (after
// the pending batch was already committed).
func (a *applyActor) collectNonCommand(b *DistributedBackend, e raft.LogEntry) {
	if e.Type == raft.LogEntrySnapshot {
		a.restore(b, e)
	}
}

// commitBatch applies a.batch, records results, runs notifications, and advances
// lastApplied. A zero-length batch is a no-op.
func (a *applyActor) commitBatch(b *DistributedBackend) {
	if len(a.batch) == 0 {
		return
	}
	results := a.applyBatch(a.batch)
	for i, entry := range a.batch {
		b.recordApplyResult(entry.Index, results[i])
		if results[i] != nil {
			b.logger.Error().Uint64("index", entry.Index).Err(results[i]).Msg("fsm apply error")
		}
		b.notifyOnApply(entry.Command)
	}
	last := a.batch[len(a.batch)-1]
	b.lastApplied.Store(last.Index)
	b.lastAppliedTerm.Store(last.Term)
	a.batch = a.batch[:0]
}

// restore applies a snapshot entry standalone (FSM.Restore does DropPrefix +
// rewrite — it must not share a transaction with command entries).
func (a *applyActor) restore(b *DistributedBackend, entry raft.LogEntry) {
	meta := raft.SnapshotMeta{
		Index:         entry.Index,
		Term:          entry.Term,
		Servers:       b.node.Configuration().Servers,
		FormatVersion: raft.FSMSnapshotFormatVersion,
	}
	if err := b.fsm.Restore(meta, entry.Command); err != nil {
		b.logger.Error().Uint64("index", entry.Index).Err(err).Msg("fsm restore snapshot error")
	}
	b.lastApplied.Store(entry.Index)
	b.lastAppliedTerm.Store(entry.Term)
}
