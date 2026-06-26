package cluster

import (
	"os"
	"strconv"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/raft"
)

// maxApplyBatchEntries caps entries per shared-transaction commit. Mirrors the
// raft propose path's maxProposeAppendBatch.
const maxApplyBatchEntries = 64

// maxApplyBatchBytes caps the summed raw-command size per batch so one apply
// loop turn does not monopolize the goroutine on very large retired payloads.
const maxApplyBatchBytes = 64 << 10

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

// applyActor owns the data-group raft drain loop. Data-plane commands are
// retired; committed command entries are opaque cursor markers. Snapshot
// entries still restore the group metadata store for brownfield raft state.
type applyActor struct {
	fsm   *FSM
	batch []raft.LogEntry // reused across iterations (batch[:0]); never re-nil'd
}

// run is the apply actor's main loop. It drains committed entries from the Raft
// node, batches command entries only to advance lastApplied efficiently, and
// handles snapshot entries / snapshot requests as batch barriers. Returns when
// stop fires or ApplyCh closes.
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
// opportunistically drains more command entries (up to the caps) and records
// the batch; for a snapshot entry it restores standalone. Returns false when
// the loop should exit (ApplyCh closed mid-drain).
func (a *applyActor) collect(b *DistributedBackend, first raft.LogEntry, applyCh <-chan raft.LogEntry) bool {
	if first.Type == raft.LogEntrySnapshot {
		a.restore(b, first)
		return true
	}
	if first.Type != raft.LogEntryCommand {
		// LogEntryNoOp / conf-change: nothing for the FSM, but advance the applied
		// cursor to the commit frontier so the linearizable read fence
		// (WaitApplied) is satisfied even when the committed log tail is a
		// non-command entry (e.g. a fresh group's leader-election NoOp). Entries
		// arrive in log order, so storing first.Index keeps lastApplied contiguous.
		b.lastApplied.Store(first.Index)
		b.lastAppliedTerm.Store(first.Term)
		return true
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
// the pending batch was already recorded).
func (a *applyActor) collectNonCommand(b *DistributedBackend, e raft.LogEntry) {
	if e.Type == raft.LogEntrySnapshot {
		a.restore(b, e)
		return
	}
	// NoOp / conf-change after a committed batch: no FSM work, but advance the
	// applied cursor to this entry's index (commitBatch already advanced it to the
	// batch's last command; e.Index is higher) so lastApplied reaches the commit
	// frontier. Keeps the forward read fence (WaitApplied) unblocked.
	b.lastApplied.Store(e.Index)
	b.lastAppliedTerm.Store(e.Term)
}

// commitBatch advances lastApplied over a batch of retired data-group command
// entries. Command bytes are deliberately opaque; all live data-plane writes are
// off-raft and brownfield command payloads replay as no-ops.
func (a *applyActor) commitBatch(b *DistributedBackend) {
	if len(a.batch) == 0 {
		return
	}
	metrics.ApplyBatchSize.Observe(float64(len(a.batch)))
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
	// Policy-cache invalidation on snapshot install was driven by the retired
	// onBucketPolicyApply hook (Task 12). Policy invalidation is now handled by
	// the meta post-commit invalidation worker; no hook needed here.
}
