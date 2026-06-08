package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/raft"
)

// indexGroupForwardLocalApplyTimeout bounds the local-apply wait after a follower
// forwards a command and the caller supplied no deadline. Mirrors the meta
// forwarder (meta_bucket_assigner.go:132).
const indexGroupForwardLocalApplyTimeout = 10 * time.Second

// indexGroupForwardFunc forwards an encoded MetaCmd to the index group's raft
// leader and returns the committed log index. nil ⇒ no peer to forward to
// (single-node / leader-only test), so the proposer proposes locally.
type indexGroupForwardFunc func(ctx context.Context, data []byte) (uint64, error)

// indexGroup is a dormant object-index-only raft replica: a *MetaFSM driven
// object-index-only by an apply loop over a raft.Node's ApplyCh. Later slices add
// the objectIndexLookup/objectIndexProposer/objectIndexListSource methods +
// compile-time assertions so it drops into ObjectIndexShard{Reader, Writer,
// Lister}; Slice 4b boot-wires N of them.
type indexGroup struct {
	node    RaftNode // nil only in the channel-driven apply-loop unit test
	fsm     *MetaFSM
	forward indexGroupForwardFunc

	cancel context.CancelFunc // set by Start; cancels the apply loop
	done   chan struct{}      // closed by runApplyLoop on exit

	// FSM-applied watermark — mirrors MetaRaft (meta_raft.go:88-90, 903-960).
	// node.WaitApplied is NOT sufficient: it tracks the node's commit/delivery,
	// not our consumer's FSM apply, so read-your-write needs this.
	lastApplied   atomic.Uint64
	applyNotifyMu sync.Mutex
	applyNotify   chan struct{}

	applyResultMu sync.Mutex
	applyErrs     map[uint64]error
}

func newIndexGroup(node RaftNode, fsm *MetaFSM, forward indexGroupForwardFunc) *indexGroup {
	return &indexGroup{
		node:        node,
		fsm:         fsm,
		forward:     forward,
		applyNotify: make(chan struct{}),
		done:        make(chan struct{}),
	}
}

// runApplyLoop drains committed entries and drives the FSM. It closes g.done on
// exit (the ONLY closer of done — Close waits on it). Object-index commands only
// (coupling guard); a LogEntrySnapshot whose Restore fails halts the loop without
// advancing lastApplied (mirrors MetaRaft.applySnapshotEntry's no-advance-on-failure).
func (g *indexGroup) runApplyLoop(ctx context.Context, applyCh <-chan raft.LogEntry) {
	defer close(g.done)
	for {
		select {
		case <-ctx.Done():
			return
		case entry, ok := <-applyCh:
			if !ok {
				return
			}
			switch entry.Type {
			case raft.LogEntryCommand:
				if err := g.applyGuarded(entry.Command); err != nil {
					g.recordApplyResult(entry.Index, err)
				}
			case raft.LogEntrySnapshot:
				// Unlike MetaRaft.applySnapshotEntry (meta_raft.go:1041), the index
				// group intentionally skips installSnapshotDEKs() after Restore: an
				// object-index replica never decrypts object data — it only tracks
				// DekGen refcounts in the in-memory dekRefCounts map, which Restore
				// already rebuilds. No keeper material is needed.
				if err := g.fsm.Restore(raft.SnapshotMeta{Index: entry.Index, Term: entry.Term}, entry.Command); err != nil {
					// A failed snapshot Restore is unrecoverable, so the loop halts
					// (returns) without advancing lastApplied. Waiters observe the
					// halt via the closed done channel (recording the error here would
					// be a dead store: applyError is only read after waitApplied
					// succeeds, which it never does for an unadvanced index).
					return
				}
			default:
				// NoOp / membership entries are filtered by the adapter ApplyCh.
			}
			g.advanceApplied(entry.Index)
		}
	}
}

func (g *indexGroup) advanceApplied(index uint64) {
	g.lastApplied.Store(index)
	g.applyNotifyMu.Lock()
	old := g.applyNotify
	g.applyNotify = make(chan struct{})
	g.applyNotifyMu.Unlock()
	close(old)
}

// applyGuarded enforces the coupling guard AND bypasses post-commit hooks by
// calling the FSM leaf methods directly. Panic-safe like MetaFSM.applyCmd:750-768.
func (g *indexGroup) applyGuarded(data []byte) (err error) {
	if len(data) == 0 {
		return fmt.Errorf("index group: empty command")
	}
	var cmd *clusterpb.MetaCmd
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("index group: invalid MetaCmd flatbuffer: %v", r)
			}
		}()
		cmd = clusterpb.GetRootAsMetaCmd(data, 0)
	}()
	if err != nil {
		return err
	}
	switch cmd.Type() {
	case clusterpb.MetaCmdTypePutObjectIndex:
		return g.fsm.applyPutObjectIndex(cmd.DataBytes()) // leaf: no post-commit hooks
	case clusterpb.MetaCmdTypeDeleteObjectIndex:
		return g.fsm.applyDeleteObjectIndex(cmd.DataBytes())
	default:
		return fmt.Errorf("index group: rejected non-object-index command type %d", cmd.Type())
	}
}

func (g *indexGroup) recordApplyResult(index uint64, err error) {
	if err == nil {
		return
	}
	g.applyResultMu.Lock()
	if g.applyErrs == nil {
		g.applyErrs = make(map[uint64]error)
	}
	g.applyErrs[index] = err
	for old := range g.applyErrs {
		if old+1024 < index {
			delete(g.applyErrs, old)
		}
	}
	g.applyResultMu.Unlock()
}

func (g *indexGroup) applyError(index uint64) error {
	g.applyResultMu.Lock()
	err := g.applyErrs[index]
	delete(g.applyErrs, index)
	g.applyResultMu.Unlock()
	return err
}

// waitApplied blocks until the apply loop processed the entry at idx. Snapshot
// the channel BEFORE checking lastApplied (see MetaRaft.waitApplied:903).
func (g *indexGroup) waitApplied(ctx context.Context, idx uint64) error {
	for {
		g.applyNotifyMu.Lock()
		ch := g.applyNotify
		g.applyNotifyMu.Unlock()
		if g.lastApplied.Load() >= idx {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-g.done:
			return fmt.Errorf("index group: apply loop stopped before entry %d was applied", idx)
		}
	}
}

func (g *indexGroup) waitAppliedResult(ctx context.Context, idx uint64) error {
	if err := g.waitApplied(ctx, idx); err != nil {
		return err
	}
	if err := g.applyError(idx); err != nil {
		return fmt.Errorf("index group: FSM apply error at index %d: %w", idx, err)
	}
	return nil
}

// Start restores from any persisted snapshot, then launches the node and the
// apply loop. It mirrors MetaRaft.Start but is scoped to the object index.
// Callers must not call Start more than once.
func (g *indexGroup) Start(ctx context.Context) error {
	if g.node == nil {
		return fmt.Errorf("index group: Start called with nil node")
	}
	if snap, err := g.node.LatestSnapshot(); err != nil {
		return fmt.Errorf("index group: load latest snapshot: %w", err)
	} else if snap != nil && snap.Index > 0 {
		meta := raft.SnapshotMeta{Index: snap.Index, Term: snap.Term}
		if err := g.fsm.Restore(meta, snap.Data); err != nil {
			return fmt.Errorf("index group: restore latest snapshot: %w", err)
		}
		g.lastApplied.Store(snap.Index)
	}
	loopCtx, cancel := context.WithCancel(ctx)
	g.cancel = cancel
	g.node.Start()
	go g.runApplyLoop(loopCtx, g.node.ApplyCh())
	return nil
}

// Close cancels the apply loop, waits for it to drain, then closes the raft
// node. Safe to call even if Start was never called.
func (g *indexGroup) Close() {
	if g.cancel != nil {
		g.cancel()
		<-g.done
	}
	if g.node != nil {
		g.node.Close()
	}
}

// ProposeObjectIndex encodes an object-index put command and proposes it
// through the raft node (or forwards to the leader if not leader).
func (g *indexGroup) ProposeObjectIndex(ctx context.Context, entry ObjectIndexEntry, preserveLatest bool) error {
	payload, err := encodeMetaPutObjectIndexCmd(entry, preserveLatest)
	if err != nil {
		return fmt.Errorf("index group: encode put object index: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypePutObjectIndex, payload)
	if err != nil {
		return fmt.Errorf("index group: encode meta cmd: %w", err)
	}
	return g.proposeOrForward(ctx, data)
}

// ProposeDeleteObjectIndex encodes an object-index delete command and proposes it.
func (g *indexGroup) ProposeDeleteObjectIndex(ctx context.Context, bucket, key, versionID string) error {
	payload, err := encodeMetaDeleteObjectIndexCmd(bucket, key, versionID)
	if err != nil {
		return fmt.Errorf("index group: encode delete object index: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeDeleteObjectIndex, payload)
	if err != nil {
		return fmt.Errorf("index group: encode meta cmd: %w", err)
	}
	return g.proposeOrForward(ctx, data)
}

// proposeOrForward proposes data locally when this node is the leader (or
// no forward func is set), otherwise forwards to the leader.
func (g *indexGroup) proposeOrForward(ctx context.Context, data []byte) error {
	if g.forward == nil || g.node.IsLeader() {
		idx, err := g.node.ProposeWait(ctx, data)
		if err != nil {
			return fmt.Errorf("index group: propose: %w", err)
		}
		return g.waitAppliedResult(ctx, idx)
	}
	idx, err := g.forward(ctx, data)
	if err != nil {
		return fmt.Errorf("index group: forward: %w", err)
	}
	if idx > 0 {
		return g.waitForwardedApplied(ctx, idx)
	}
	// idx==0 means the forwarder did not report a committed index, so there is
	// nothing to wait on. The index-group forward hook always returns the
	// leader's committed index from ProposeWait, so idx==0 only occurs with a
	// degenerate/legacy forwarder we don't use.
	return nil
}

// waitForwardedApplied waits for a forwarded command (whose log index is
// known) to be applied locally. If the caller supplied no deadline, a
// bounded local-apply timeout is applied; a local timeout does NOT surface
// as an error to the caller (the commit already succeeded on the leader).
func (g *indexGroup) waitForwardedApplied(ctx context.Context, idx uint64) error {
	localCtx := ctx
	var localCancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		localCtx, localCancel = context.WithTimeout(ctx, indexGroupForwardLocalApplyTimeout)
		defer localCancel()
	}
	if err := g.waitAppliedResult(localCtx, idx); err != nil {
		// If the caller's context is still alive but the local timeout fired,
		// treat it as a non-error (the commit succeeded; we just didn't observe
		// local apply in time). This mirrors meta_bucket_assigner.go:132.
		if ctx.Err() == nil && localCtx.Err() != nil {
			return nil
		}
		return err
	}
	return nil
}

// snapshot captures the current FSM state into a durable raft snapshot and
// returns the raw snapshot bytes plus the applied index it covers.
//
// The snapshot is stamped at the APPLIED index (captured BEFORE fsm.Snapshot()),
// NOT the committed index: CreateSnapshot physically truncates the log via
// CompactBefore(idx) (internal/raft/snapshot_actor.go:75). fsm.Snapshot() data
// only reflects state up to lastApplied, and lastApplied can structurally trail
// committedIndex (in-flight proposals, or committed non-object-index entries the
// adapter ApplyCh drops). Stamping at committedIndex would truncate entries in
// (applied, committed] that the snapshot data does not contain, losing them on the
// next Start/Restore. The applied index is a safe lower bound — replaying a few
// idempotent puts on restart is harmless; truncating past real state is not.
// (MetaRaft sidesteps this by snapshotting inside the apply loop at the just-
// applied entry.Index — meta_raft.go:1126-1139.)
func (g *indexGroup) snapshot() ([]byte, uint64, error) {
	idx := g.lastApplied.Load()
	if idx == 0 {
		return nil, 0, fmt.Errorf("index group: snapshot before any apply")
	}
	data, err := g.fsm.Snapshot()
	if err != nil {
		return nil, 0, fmt.Errorf("index group: FSM snapshot: %w", err)
	}
	if err := g.node.CreateSnapshot(idx, data); err != nil {
		return nil, 0, fmt.Errorf("index group: create snapshot at %d: %w", idx, err)
	}
	return data, idx, nil
}

// ObjectIndexLatest delegates to the FSM.
func (g *indexGroup) ObjectIndexLatest(bucket, key string) (ObjectIndexEntry, bool) {
	return g.fsm.ObjectIndexLatest(bucket, key)
}

// ObjectIndexVersion delegates to the FSM.
func (g *indexGroup) ObjectIndexVersion(bucket, key, versionID string) (ObjectIndexEntry, bool) {
	return g.fsm.ObjectIndexVersion(bucket, key, versionID)
}

// ObjectIndexLatestEntries delegates to the FSM.
func (g *indexGroup) ObjectIndexLatestEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry {
	return g.fsm.ObjectIndexLatestEntries(bucket, prefix, maxKeys)
}

// ObjectIndexLatestEntriesPage delegates to the FSM.
func (g *indexGroup) ObjectIndexLatestEntriesPage(bucket, prefix, marker string, maxKeys int) ([]ObjectIndexEntry, bool) {
	return g.fsm.ObjectIndexLatestEntriesPage(bucket, prefix, marker, maxKeys)
}

// ObjectIndexVersionEntries delegates to the FSM.
func (g *indexGroup) ObjectIndexVersionEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry {
	return g.fsm.ObjectIndexVersionEntries(bucket, prefix, maxKeys)
}
