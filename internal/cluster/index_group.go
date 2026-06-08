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
