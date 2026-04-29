package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/raft"
)

// MetaTransport abstracts RPC delivery for the meta-Raft group.
type MetaTransport interface {
	SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error)
	SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error)
	SendInstallSnapshot(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error)
}

// MetaRaftConfig configures a MetaRaft instance.
type MetaRaftConfig struct {
	NodeID    string
	Peers     []string // addresses of other meta-Raft peers
	DataDir   string   // directory for BadgerDB; meta store lives at DataDir/meta_raft
	Transport MetaTransport
}

// MetaRaft wraps a raft.Node dedicated to cluster control-plane operations.
type MetaRaft struct {
	node    *raft.Node
	store   *raft.BadgerLogStore
	snapMgr *raft.SnapshotManager
	fsm     *MetaFSM
	cfg     MetaRaftConfig
	cancel  context.CancelFunc
	done    chan struct{}

	// lastApplied is read atomically from waitApplied (hot path).
	// applyNotifyMu protects applyNotify channel swaps only.
	lastApplied   atomic.Uint64
	applyNotifyMu sync.Mutex
	applyNotify   chan struct{} // closed each time an entry is applied; replaced atomically
}

// NewMetaRaft constructs a MetaRaft from config. The node is not started yet;
// call Bootstrap then Start. Transport may be nil here and set later via
// SetTransport (needed when the QUIC transport requires the node handle first).
func NewMetaRaft(cfg MetaRaftConfig) (*MetaRaft, error) {
	storePath := filepath.Join(cfg.DataDir, "meta_raft")
	store, err := raft.NewBadgerLogStore(storePath)
	if err != nil {
		return nil, fmt.Errorf("meta_raft: open store: %w", err)
	}

	fsm := NewMetaFSM()
	snapMgr := raft.NewSnapshotManager(store, fsm, raft.SnapshotConfig{
		Threshold:    1024,
		TrailingLogs: 512,
	})

	nodeCfg := raft.DefaultConfig(cfg.NodeID, cfg.Peers)
	node := raft.NewNode(nodeCfg, store)

	m := &MetaRaft{
		node:        node,
		store:       store,
		snapMgr:     snapMgr,
		fsm:         fsm,
		cfg:         cfg,
		done:        make(chan struct{}),
		applyNotify: make(chan struct{}),
	}

	if cfg.Transport != nil {
		m.wireTransport(cfg.Transport)
	}
	return m, nil
}

// Node returns the underlying raft.Node for external transport wiring.
func (m *MetaRaft) Node() *raft.Node { return m.node }

// IsLeader reports whether this MetaRaft node is the current cluster leader.
func (m *MetaRaft) IsLeader() bool { return m.node.IsLeader() }

// FSM returns the MetaFSM for callback registration and state inspection.
// Callers must set callbacks before Start() to avoid a data race with the apply loop.
func (m *MetaRaft) FSM() *MetaFSM { return m.fsm }

// SetTransport wires a MetaTransport into the Raft node after construction.
// Must be called before Start.
func (m *MetaRaft) SetTransport(t MetaTransport) {
	m.cfg.Transport = t
	m.wireTransport(t)
}

func (m *MetaRaft) wireTransport(t MetaTransport) {
	m.node.SetTransport(t.SendRequestVote, t.SendAppendEntries)
	m.node.SetInstallSnapshotTransport(t.SendInstallSnapshot)
}

// Bootstrap marks the store as bootstrapped. Idempotent.
func (m *MetaRaft) Bootstrap() error {
	err := m.node.Bootstrap()
	if err != nil && !errors.Is(err, raft.ErrAlreadyBootstrapped) {
		return fmt.Errorf("meta_raft: bootstrap: %w", err)
	}
	return nil
}

// Start launches the Raft node and the FSM apply loop.
func (m *MetaRaft) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.node.Start()
	go m.runApplyLoop(ctx)
	return nil
}

// Close shuts down the node and waits for the apply loop to exit.
// Safe to call even if Start was never called.
func (m *MetaRaft) Close() error {
	if m.cancel != nil {
		m.cancel()
		m.node.Close()
		<-m.done
	} else {
		m.node.Close()
	}
	return m.store.Close()
}

// Join adds node with id/addr as a learner, promotes it to voter, and records it
// in the FSM. The three steps are not atomic: if ProposeAddNode fails after
// PromoteToVoter succeeds, the node exists in the Raft configuration but not in
// MetaFSM.Nodes(). Callers must retry or use RemoveVoter to restore consistency.
func (m *MetaRaft) Join(ctx context.Context, id, addr string) error {
	if err := m.node.AddLearner(id, addr); err != nil {
		return fmt.Errorf("meta_raft: AddLearner %s: %w", id, err)
	}
	if err := m.node.PromoteToVoter(id); err != nil {
		return fmt.Errorf("meta_raft: PromoteToVoter %s: %w", id, err)
	}
	return m.ProposeAddNode(ctx, MetaNodeEntry{ID: id, Address: addr, Role: 0})
}

// ProposeAddNode encodes an AddNode command and proposes it to the cluster,
// blocking until the entry is applied to the local FSM.
func (m *MetaRaft) ProposeAddNode(ctx context.Context, entry MetaNodeEntry) error {
	payload, err := encodeMetaAddNodeCmd(entry)
	if err != nil {
		return fmt.Errorf("meta_raft: encode AddNode: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeAddNode, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitApplied(ctx, idx)
}

// ProposeBucketAssignment encodes a PutBucketAssignment command and proposes it to
// the cluster, blocking until the entry is applied to the local FSM.
func (m *MetaRaft) ProposeBucketAssignment(ctx context.Context, bucket, groupID string) error {
	payload, err := encodeMetaPutBucketAssignmentCmd(bucket, groupID)
	if err != nil {
		return fmt.Errorf("meta_raft: encode PutBucketAssignment: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypePutBucketAssignment, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitApplied(ctx, idx)
}

// ProposeShardGroup proposes a PutShardGroup command to the cluster and blocks until
// it is applied to the local FSM.
func (m *MetaRaft) ProposeShardGroup(ctx context.Context, sg ShardGroupEntry) error {
	payload, err := encodeMetaPutShardGroupCmd(sg)
	if err != nil {
		return fmt.Errorf("meta_raft: encode PutShardGroup: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypePutShardGroup, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitApplied(ctx, idx)
}

// ProposeLoadSnapshot encodes a SetLoadSnapshot command and proposes it to the
// cluster, blocking until the entry is applied to the local FSM.
func (m *MetaRaft) ProposeLoadSnapshot(ctx context.Context, entries []LoadStatEntry) error {
	payload, err := encodeMetaSetLoadSnapshotCmd(entries)
	if err != nil {
		return fmt.Errorf("meta_raft: encode SetLoadSnapshot: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeSetLoadSnapshot, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitApplied(ctx, idx)
}

// ProposeRebalancePlan encodes a ProposeRebalancePlan command and proposes it to the
// cluster, blocking until the entry is applied to the local FSM.
func (m *MetaRaft) ProposeRebalancePlan(ctx context.Context, plan RebalancePlan) error {
	payload, err := encodeMetaProposeRebalancePlanCmd(plan)
	if err != nil {
		return fmt.Errorf("meta_raft: encode ProposeRebalancePlan: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeProposeRebalancePlan, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitApplied(ctx, idx)
}

// ProposeAbortPlan encodes an AbortPlan command and proposes it to the cluster,
// blocking until the entry is applied to the local FSM.
func (m *MetaRaft) ProposeAbortPlan(ctx context.Context, planID string) error {
	payload, err := encodeMetaAbortPlanCmd(planID)
	if err != nil {
		return fmt.Errorf("meta_raft: encode AbortPlan: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeAbortPlan, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitApplied(ctx, idx)
}

// waitApplied blocks until the FSM apply loop has processed the entry at idx.
// Uses generation channels to avoid goroutine leaks on context cancellation.
//
// Ordering invariant: snapshot the channel BEFORE checking lastApplied.
// If the applier fires between the snapshot and the check, lastApplied is already
// updated so the check returns immediately. If it fires after the check, the waiter
// holds the channel that will be closed by the applier — no notification is missed.
func (m *MetaRaft) waitApplied(ctx context.Context, idx uint64) error {
	for {
		m.applyNotifyMu.Lock()
		ch := m.applyNotify // snapshot first
		m.applyNotifyMu.Unlock()
		if m.lastApplied.Load() >= idx { // check after snapshot
			return nil
		}
		select {
		case <-ch:
			// a new entry was applied; re-check lastApplied
		case <-ctx.Done():
			return ctx.Err()
		case <-m.done:
			return fmt.Errorf("meta_raft: apply loop stopped before entry %d was applied", idx)
		}
	}
}

// runApplyLoop reads committed entries from the node's apply channel and
// applies them to the FSM. Exits when ctx is cancelled.
func (m *MetaRaft) runApplyLoop(ctx context.Context) {
	defer close(m.done)
	applyCh := m.node.ApplyCh()
	for {
		select {
		case entry, ok := <-applyCh:
			if !ok {
				return
			}
			if entry.Type == raft.LogEntryCommand && len(entry.Command) > 0 {
				if err := m.fsm.applyCmd(entry.Command); err != nil {
					log.Error().Err(err).Uint64("index", entry.Index).Msg("meta_raft: FSM apply error")
				}
				m.snapMgr.MaybeTrigger(entry.Index, entry.Term)
			}
			m.lastApplied.Store(entry.Index)
			m.applyNotifyMu.Lock()
			old := m.applyNotify
			m.applyNotify = make(chan struct{})
			m.applyNotifyMu.Unlock()
			close(old) // wake all waitApplied callers for this generation
		case <-ctx.Done():
			return
		}
	}
}
