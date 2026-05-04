package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/raft"
)

// Meta-raft timing is fixed (not driven by --raft-heartbeat-interval, which
// applies to per-group raft only). Exposed so serve.go can validate that
// shared infra (e.g., the mux flush window) doesn't blow past the meta
// heartbeat budget.
const (
	MetaRaftHeartbeatInterval = 150 * time.Millisecond
	MetaRaftElectionTimeout   = 750 * time.Millisecond
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

	icebergMu      sync.Mutex
	icebergWaiters map[string]chan error
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
	// Meta-Raft shares the QUIC transport and process with data Raft, shard RPC,
	// S3 startup probes, and per-group Raft workers. Give the control plane a
	// wider election window so local/CI CPU contention does not leave bucket
	// assignment without a stable leader during multi-process cold starts.
	nodeCfg.ElectionTimeout = MetaRaftElectionTimeout
	nodeCfg.HeartbeatTimeout = MetaRaftHeartbeatInterval
	node := raft.NewNode(nodeCfg, store)

	// §4.3 joint state persistence wiring (Sub-project 2 PR-J5).
	snapMgr.SetJointStateProvider(node.JointSnapshotState)
	snapMgr.SetJointStateRestorer(node.RestoreJointStateFromSnapshot)

	m := &MetaRaft{
		node:           node,
		store:          store,
		snapMgr:        snapMgr,
		fsm:            fsm,
		cfg:            cfg,
		done:           make(chan struct{}),
		applyNotify:    make(chan struct{}),
		icebergWaiters: make(map[string]chan error),
	}
	fsm.SetOnIcebergApplyResult(m.publishIcebergResult)

	if cfg.Transport != nil {
		m.wireTransport(cfg.Transport)
	}
	return m, nil
}

// Node returns the underlying raft.Node for external transport wiring.
func (m *MetaRaft) Node() *raft.Node { return m.node }

// IsLeader reports whether this MetaRaft node is the current cluster leader.
func (m *MetaRaft) IsLeader() bool { return m.node.IsLeader() }

// LeaderID returns the current meta-Raft leader hint, if known.
func (m *MetaRaft) LeaderID() string { return m.node.LeaderID() }

// Nodes returns the current meta-FSM member snapshot.
func (m *MetaRaft) Nodes() []MetaNodeEntry { return m.fsm.Nodes() }

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
	go m.runRotationAutoProgress(ctx)
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

// Join adds node with id/addr as a learner, waits for catch-up/promotion via the
// raft membership API, and records it in the FSM. If FSM registration fails
// after promotion, best-effort cleanup removes the promoted voter by address.
func (m *MetaRaft) Join(ctx context.Context, id, addr string) error {
	if err := m.node.AddVoterCtx(ctx, id, addr); err != nil {
		_ = m.node.RemoveVoter(addr)
		return fmt.Errorf("meta_raft: AddVoterCtx %s: %w", id, err)
	}
	if err := m.ProposeAddNode(ctx, MetaNodeEntry{ID: id, Address: addr, Role: 0}); err != nil {
		_ = m.node.RemoveVoter(addr)
		return err
	}
	return nil
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
	if err := raft.ValidateGroupID(sg.ID); err != nil {
		return fmt.Errorf("meta_raft: ProposeShardGroup: %w", err)
	}
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
func (m *MetaRaft) ProposeAbortPlan(ctx context.Context, planID string, reason clusterpb.AbortPlanReason) error {
	payload, err := encodeMetaAbortPlanCmd(planID, reason)
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

func (m *MetaRaft) ProposeIcebergCreateNamespace(ctx context.Context, cmd IcebergCreateNamespaceCmd) error {
	payload, err := encodeMetaIcebergCreateNamespaceCmd(cmd)
	if err != nil {
		return fmt.Errorf("meta_raft: encode IcebergCreateNamespace: %w", err)
	}
	return m.proposeIcebergCommand(ctx, MetaCmdTypeIcebergCreateNamespace, payload, cmd.RequestID)
}

func (m *MetaRaft) ProposeIcebergDeleteNamespace(ctx context.Context, cmd IcebergDeleteNamespaceCmd) error {
	payload, err := encodeMetaIcebergDeleteNamespaceCmd(cmd)
	if err != nil {
		return fmt.Errorf("meta_raft: encode IcebergDeleteNamespace: %w", err)
	}
	return m.proposeIcebergCommand(ctx, MetaCmdTypeIcebergDeleteNamespace, payload, cmd.RequestID)
}

func (m *MetaRaft) ProposeIcebergCreateTable(ctx context.Context, cmd IcebergCreateTableCmd) error {
	payload, err := encodeMetaIcebergCreateTableCmd(cmd)
	if err != nil {
		return fmt.Errorf("meta_raft: encode IcebergCreateTable: %w", err)
	}
	return m.proposeIcebergCommand(ctx, MetaCmdTypeIcebergCreateTable, payload, cmd.RequestID)
}

func (m *MetaRaft) ProposeIcebergCommitTable(ctx context.Context, cmd IcebergCommitTableCmd) error {
	payload, err := encodeMetaIcebergCommitTableCmd(cmd)
	if err != nil {
		return fmt.Errorf("meta_raft: encode IcebergCommitTable: %w", err)
	}
	return m.proposeIcebergCommand(ctx, MetaCmdTypeIcebergCommitTable, payload, cmd.RequestID)
}

func (m *MetaRaft) ProposeIcebergDeleteTable(ctx context.Context, cmd IcebergDeleteTableCmd) error {
	payload, err := encodeMetaIcebergDeleteTableCmd(cmd)
	if err != nil {
		return fmt.Errorf("meta_raft: encode IcebergDeleteTable: %w", err)
	}
	return m.proposeIcebergCommand(ctx, MetaCmdTypeIcebergDeleteTable, payload, cmd.RequestID)
}

func (m *MetaRaft) ProposeIcebergMetaCommand(ctx context.Context, data []byte) error {
	cmd := clusterpb.GetRootAsMetaCmd(data, 0)
	requestID, err := icebergRequestID(cmd.Type(), cmd.DataBytes())
	if err != nil {
		return err
	}
	return m.proposeIcebergCommand(ctx, cmd.Type(), cmd.DataBytes(), requestID)
}

// ProposeMetaCommand proposes an already-encoded MetaCmd. Iceberg commands use
// the request waiter path so semantic catalog errors are preserved; other meta
// commands only need to wait until the entry is applied locally.
func (m *MetaRaft) ProposeMetaCommand(ctx context.Context, data []byte) error {
	cmd := clusterpb.GetRootAsMetaCmd(data, 0)
	switch cmd.Type() {
	case MetaCmdTypeIcebergCreateNamespace,
		MetaCmdTypeIcebergDeleteNamespace,
		MetaCmdTypeIcebergCreateTable,
		MetaCmdTypeIcebergCommitTable,
		MetaCmdTypeIcebergDeleteTable:
		return m.ProposeIcebergMetaCommand(ctx, data)
	default:
		idx, err := m.node.ProposeWait(ctx, data)
		if err != nil {
			return fmt.Errorf("meta_raft: ProposeWait: %w", err)
		}
		return m.waitApplied(ctx, idx)
	}
}

func icebergRequestID(typ MetaCmdType, payload []byte) (string, error) {
	switch typ {
	case MetaCmdTypeIcebergCreateNamespace:
		cmd, err := decodeMetaIcebergCreateNamespaceCmd(payload)
		return cmd.RequestID, err
	case MetaCmdTypeIcebergDeleteNamespace:
		cmd, err := decodeMetaIcebergDeleteNamespaceCmd(payload)
		return cmd.RequestID, err
	case MetaCmdTypeIcebergCreateTable:
		cmd, err := decodeMetaIcebergCreateTableCmd(payload)
		return cmd.RequestID, err
	case MetaCmdTypeIcebergCommitTable:
		cmd, err := decodeMetaIcebergCommitTableCmd(payload)
		return cmd.RequestID, err
	case MetaCmdTypeIcebergDeleteTable:
		cmd, err := decodeMetaIcebergDeleteTableCmd(payload)
		return cmd.RequestID, err
	default:
		return "", fmt.Errorf("meta_raft: non-iceberg command type %s", typ)
	}
}

func (m *MetaRaft) proposeIcebergCommand(ctx context.Context, typ MetaCmdType, payload []byte, requestID string) error {
	if requestID == "" {
		return fmt.Errorf("meta_raft: iceberg command missing request ID")
	}
	waiter := m.registerIcebergWaiter(requestID)
	defer m.unregisterIcebergWaiter(requestID)
	data, err := encodeMetaCmd(typ, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	if err := m.waitApplied(ctx, idx); err != nil {
		return err
	}
	select {
	case err := <-waiter:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-m.done:
		return fmt.Errorf("meta_raft: apply loop stopped before iceberg result %q was delivered", requestID)
	}
}

func (m *MetaRaft) registerIcebergWaiter(requestID string) chan error {
	ch := make(chan error, 1)
	m.icebergMu.Lock()
	m.icebergWaiters[requestID] = ch
	m.icebergMu.Unlock()
	return ch
}

func (m *MetaRaft) unregisterIcebergWaiter(requestID string) {
	m.icebergMu.Lock()
	delete(m.icebergWaiters, requestID)
	m.icebergMu.Unlock()
}

func (m *MetaRaft) publishIcebergResult(requestID string, err error) {
	m.icebergMu.Lock()
	ch := m.icebergWaiters[requestID]
	m.icebergMu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- err:
	default:
	}
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
				m.snapMgr.MaybeTrigger(entry.Index, entry.Term, m.node.Configuration().Servers)
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
