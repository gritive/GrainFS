package cluster

import (
	"errors"

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
