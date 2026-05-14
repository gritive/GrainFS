package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// Meta-raft timing is fixed (not driven by --raft-heartbeat-interval, which
// applies to per-group raft only). Exposed so serve.go can validate that
// shared infra (e.g., the mux flush window) doesn't blow past the meta
// heartbeat budget.
const (
	MetaRaftHeartbeatInterval       = 150 * time.Millisecond
	MetaRaftElectionTimeout         = 750 * time.Millisecond
	MetaRaftLivenessFreshnessWindow = 3 * MetaRaftElectionTimeout
	metaForwardLocalApplyTimeout    = 5 * time.Second
)

// MetaTransport abstracts RPC delivery for the meta-Raft group.
type MetaTransport interface {
	SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error)
	SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error)
	SendInstallSnapshot(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error)
	SendTimeoutNow(peer string, args *raft.TimeoutNowArgs) (*raft.TimeoutNowReply, error)
}

// MetaRaftConfig configures a MetaRaft instance.
type MetaRaftConfig struct {
	NodeID    string
	RaftID    string   // raft peer ID; production uses the QUIC address
	Peers     []string // raft peer IDs; production uses peer QUIC addresses
	JoinMode  bool     // suppresses solo self-election until dynamic join installs membership
	DataDir   string   // directory for BadgerDB; meta store lives at DataDir/meta_raft
	Transport MetaTransport
}

// metaProposerNode abstracts the raft.Node methods used by proposeOrForward
// so the forwarding path can be unit-tested with a fake.
type metaProposerNode interface {
	IsLeader() bool
	ProposeWait(ctx context.Context, data []byte) (uint64, error)
}

// MetaRaft wraps a raft.Node dedicated to cluster control-plane operations.
type MetaRaft struct {
	node    RaftNode
	closeDB func() error
	fsm     *MetaFSM
	cfg     MetaRaftConfig
	cancel  context.CancelFunc
	done    chan struct{}

	// forwardFn is called when Propose runs on a non-leader node. It RPCs the
	// encoded MetaCmd to the current leader via the meta-raft forward path.
	// nil = leader-only (Propose on a follower returns an error).
	forwardFn func(ctx context.Context, data []byte) error

	// forwardFnWithIndex is used by callers that must wait on the committed
	// raft index after forwarding through a non-leader.
	forwardFnWithIndex func(ctx context.Context, data []byte) (uint64, error)

	capabilityGate *CapabilityGate

	// lastApplied is read atomically from waitApplied (hot path).
	// applyNotifyMu protects applyNotify channel swaps only.
	lastApplied   atomic.Uint64
	applyNotifyMu sync.Mutex
	applyNotify   chan struct{} // closed each time an entry is applied; replaced atomically
	applyResultMu sync.Mutex
	applyErrs     map[uint64]error

	icebergMu      sync.Mutex
	icebergWaiters map[string]chan error

	lastSnapshotIndex uint64
}

// NewMetaRaft constructs a MetaRaft from config. The node is not started yet;
// call Bootstrap then Start. Transport may be nil here and set later via
// SetTransport (needed when the QUIC transport requires the node handle first).
func NewMetaRaft(cfg MetaRaftConfig) (*MetaRaft, error) {
	if cfg.RaftID == "" && containsNetworkPeerID(cfg.Peers) {
		return nil, fmt.Errorf("meta_raft: RaftID is required when Peers contain network addresses")
	}
	storePath := filepath.Join(cfg.DataDir, "meta_raft")
	raftID := cfg.RaftID
	if raftID == "" {
		raftID = cfg.NodeID
	}
	nodeCfg := raft.DefaultConfig(raftID, cfg.Peers)
	nodeCfg.JoinMode = cfg.JoinMode
	// Meta-Raft shares the QUIC transport and process with data Raft, shard RPC,
	// S3 startup probes, and per-group Raft workers. Give the control plane a
	// wider election window so local/CI CPU contention does not leave bucket
	// assignment without a stable leader during multi-process cold starts.
	nodeCfg.ElectionTimeout = MetaRaftElectionTimeout
	nodeCfg.HeartbeatTimeout = MetaRaftHeartbeatInterval
	node, closeDB, err := newRaftNodeV2(nodeCfg, storePath)
	if err != nil {
		return nil, fmt.Errorf("meta_raft: new raft v2 node: %w", err)
	}

	fsm := NewMetaFSM()

	// §5.4.2: new leader must commit an entry in its own term to allow previous-term
	// entries to be committed (leader completeness). Wire a MetaCmdTypeNoOp so
	// runLeader automatically proposes it on every leadership win, committing any
	// backlogged entries (e.g. lifecycle config from the dead leader's term).
	if noOp, err := encodeMetaCmd(MetaCmdTypeNoOp, nil); err == nil {
		node.SetNoOpCommand(noOp)
	}

	m := &MetaRaft{
		node:           node,
		closeDB:        closeDB,
		fsm:            fsm,
		cfg:            cfg,
		done:           make(chan struct{}),
		applyNotify:    make(chan struct{}),
		applyErrs:      make(map[uint64]error),
		icebergWaiters: make(map[string]chan error),
	}
	fsm.SetOnIcebergApplyResult(m.publishIcebergResult)

	if cfg.Transport != nil {
		m.wireTransport(cfg.Transport)
	}
	return m, nil
}

func containsNetworkPeerID(peers []string) bool {
	for _, peer := range peers {
		if _, _, err := net.SplitHostPort(peer); err == nil {
			return true
		}
	}
	return false
}

// Node returns the underlying raft node for external transport wiring.
func (m *MetaRaft) Node() RaftNode { return m.node }

// IsLeader reports whether this MetaRaft node is the current cluster leader.
func (m *MetaRaft) IsLeader() bool { return m.node.IsLeader() }

// LeaderID returns the current meta-Raft leader hint, if known.
func (m *MetaRaft) LeaderID() string { return m.node.LeaderID() }

// TransferLeadership delegates to the underlying raft node. Returns
// raft.ErrNotLeader when not the leader and raft.ErrNoPeers when there
// are no peers to transfer to. Used by the cluster admin CLI's
// transfer-leader command.
func (m *MetaRaft) TransferLeadership() error { return m.node.TransferLeadership() }

// Nodes returns the current meta-FSM member snapshot.
func (m *MetaRaft) Nodes() []MetaNodeEntry { return m.fsm.Nodes() }

// WaitApplied blocks until the meta FSM has applied at least index.
func (m *MetaRaft) WaitApplied(ctx context.Context, index uint64) error {
	return m.waitApplied(ctx, index)
}

// FSM returns the MetaFSM for callback registration and state inspection.
// Callers must set callbacks before Start() to avoid a data race with the apply loop.
func (m *MetaRaft) FSM() *MetaFSM { return m.fsm }

// SetTransport wires a MetaTransport into the Raft node after construction.
// Must be called before Start.
func (m *MetaRaft) SetTransport(t MetaTransport) {
	m.cfg.Transport = t
	m.wireTransport(t)
}

// SetForwarder injects the follower→leader propose-forwarding closure. Wire
// from boot before raft Start so a Propose that lands on a follower (e.g. an
// S3 PUT ?lifecycle on a non-leader node) forwards instead of failing.
func (m *MetaRaft) SetForwarder(fn func(ctx context.Context, data []byte) error) {
	m.forwardFn = fn
}

// SetForwarderWithIndex injects the follower→leader forwarding closure for
// callers that need the committed raft index from the leader.
func (m *MetaRaft) SetForwarderWithIndex(fn func(ctx context.Context, data []byte) (uint64, error)) {
	m.forwardFnWithIndex = fn
}

func (m *MetaRaft) SetCapabilityGate(gate *CapabilityGate) {
	m.capabilityGate = gate
}

func (m *MetaRaft) wireTransport(t MetaTransport) {
	m.node.SetTransport(t.SendRequestVote, t.SendAppendEntries)
	m.node.SetInstallSnapshotTransport(t.SendInstallSnapshot)
	m.node.SetTimeoutNowTransport(t.SendTimeoutNow)
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
	if snap, err := m.node.LatestSnapshot(); err != nil {
		return fmt.Errorf("meta_raft: load latest snapshot: %w", err)
	} else if snap != nil && snap.Index > 0 {
		meta := raft.SnapshotMeta{Index: snap.Index, Term: snap.Term, Servers: snap.Servers}
		if err := m.fsm.Restore(meta, snap.Data); err != nil {
			return fmt.Errorf("meta_raft: restore latest snapshot: %w", err)
		}
		m.lastApplied.Store(snap.Index)
		m.lastSnapshotIndex = snap.Index
	}
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
	if m.closeDB != nil {
		return m.closeDB()
	}
	return nil
}

// Join adds node with id/addr as a learner, waits for catch-up/promotion via the
// raft membership API, and records it in the FSM. If FSM registration fails
// after promotion, best-effort cleanup removes the promoted voter by address.
func (m *MetaRaft) Join(ctx context.Context, id, addr string) error {
	raftID := addr
	if raftID == "" {
		raftID = id
	}
	if err := m.node.AddLearner(raftID, addr); err != nil {
		_ = m.node.RemoveVoter(raftID)
		return fmt.Errorf("meta_raft: AddLearner %s: %w", id, err)
	}
	if err := m.node.PromoteToVoter(raftID); err != nil {
		_ = m.node.RemoveVoter(raftID)
		return fmt.Errorf("meta_raft: PromoteToVoter %s: %w", id, err)
	}
	if err := m.ProposeAddNode(ctx, MetaNodeEntry{ID: id, Address: addr, Role: 0}); err != nil {
		_ = m.node.RemoveVoter(raftID)
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
	return m.waitAppliedResult(ctx, idx)
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
	return m.waitAppliedResult(ctx, idx)
}

// ProposeObjectIndex encodes a PutObjectIndex command and proposes it to the
// meta-Raft group, blocking until the entry is applied to the local FSM.
func (m *MetaRaft) ProposeObjectIndex(ctx context.Context, entry ObjectIndexEntry, preserveLatest bool) error {
	payload, err := encodeMetaPutObjectIndexCmd(entry, preserveLatest)
	if err != nil {
		return fmt.Errorf("meta_raft: encode PutObjectIndex: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypePutObjectIndex, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitAppliedResult(ctx, idx)
}

// ProposeDeleteObjectIndex removes one object-index version row and updates
// the key's latest pointer in the meta-Raft FSM.
func (m *MetaRaft) ProposeDeleteObjectIndex(ctx context.Context, bucket, key, versionID string) error {
	payload, err := encodeMetaDeleteObjectIndexCmd(bucket, key, versionID)
	if err != nil {
		return fmt.Errorf("meta_raft: encode DeleteObjectIndex: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeDeleteObjectIndex, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitAppliedResult(ctx, idx)
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
	return m.waitAppliedResult(ctx, idx)
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
	return m.waitAppliedResult(ctx, idx)
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
	return m.waitAppliedResult(ctx, idx)
}

// ProposeScrubTrigger encodes a ScrubTrigger command and proposes it to the
// cluster, blocking until the entry is applied to the local FSM. Each node's
// onScrubTrigger callback (wired to Director.ApplyFromFSM) creates a session
// for the same SessionID; the resolver then walks the bucket's group's
// BadgerDB if locally owned, empty channel otherwise.
func (m *MetaRaft) ProposeScrubTrigger(ctx context.Context, entry scrubber.ScrubTriggerEntry) error {
	payload, err := encodeMetaScrubTriggerCmd(entry)
	if err != nil {
		return fmt.Errorf("meta_raft: encode ScrubTrigger: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeScrubTrigger, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitAppliedResult(ctx, idx)
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
	return m.waitAppliedResult(ctx, idx)
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
		_, err := m.ProposeMetaCommandWithIndex(ctx, data)
		return err
	}
}

// ProposeMetaCommandWithIndex proposes an already-encoded non-Iceberg MetaCmd
// and returns the committed raft index. Iceberg commands use semantic request
// waiters and do not expose a proposal index through this path.
func (m *MetaRaft) ProposeMetaCommandWithIndex(ctx context.Context, data []byte) (uint64, error) {
	cmd := clusterpb.GetRootAsMetaCmd(data, 0)
	switch cmd.Type() {
	case MetaCmdTypeIcebergCreateNamespace,
		MetaCmdTypeIcebergDeleteNamespace,
		MetaCmdTypeIcebergCreateTable,
		MetaCmdTypeIcebergCommitTable,
		MetaCmdTypeIcebergDeleteTable:
		return 0, fmt.Errorf("meta_raft: iceberg commands do not expose proposal index through this path")
	default:
		idx, err := m.node.ProposeWait(ctx, data)
		if err != nil {
			return 0, fmt.Errorf("meta_raft: ProposeWait: %w", err)
		}
		return idx, m.waitAppliedResult(ctx, idx)
	}
}

// Propose wraps a typed IAM (or other) payload in a MetaCmd FlatBuffers
// envelope and proposes it to the cluster, blocking until applied to the
// local FSM. Used by external dispatchers (e.g., iam.MetaProposer) that
// build their own payload bytes and need a generic propose path.
//
// When the local node is not the leader and a forwardFn has been set via
// SetForwarder, the encoded command is forwarded to the leader via the
// meta-raft forward path (StreamMetaProposeForward) instead of failing.
func (m *MetaRaft) Propose(ctx context.Context, cmdType MetaCmdType, payload []byte) error {
	_, err := m.ProposeWithIndex(ctx, cmdType, payload)
	return err
}

// ProposeWithIndex wraps a typed payload in a MetaCmd envelope, proposes it to
// the cluster, and returns the committed raft index.
func (m *MetaRaft) ProposeWithIndex(ctx context.Context, cmdType MetaCmdType, payload []byte) (uint64, error) {
	data, err := encodeMetaCmd(cmdType, payload)
	if err != nil {
		return 0, fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	return m.proposeOrForwardWithIndex(ctx, m.node, data)
}

func (m *MetaRaft) ProposeWithGate(ctx context.Context, plan compat.GatePlan, cmdType MetaCmdType, payload []byte) (uint64, error) {
	data, err := encodeMetaCmd(cmdType, payload)
	if err != nil {
		return 0, fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	return m.proposeOrForwardWithGate(ctx, m.node, plan, data)
}

// proposeOrForward submits data to the local raft node when this node is the
// leader, otherwise forwards it via the configured forwardFn. Extracted so it
// can be unit-tested without a real raft node.
func (m *MetaRaft) proposeOrForward(ctx context.Context, node metaProposerNode, data []byte) error {
	_, err := m.proposeOrForwardWithIndex(ctx, node, data)
	return err
}

func (m *MetaRaft) proposeOrForwardWithIndex(ctx context.Context, node metaProposerNode, data []byte) (uint64, error) {
	if node.IsLeader() {
		idx, err := node.ProposeWait(ctx, data)
		if err != nil {
			return 0, fmt.Errorf("meta_raft: ProposeWait: %w", err)
		}
		return idx, m.waitAppliedResult(ctx, idx)
	}
	if m.forwardFnWithIndex != nil {
		idx, err := m.forwardFnWithIndex(ctx, data)
		if err != nil {
			return 0, fmt.Errorf("meta_raft: forward to leader: %w", err)
		}
		if idx == 0 {
			return idx, nil
		}
		return idx, m.waitForwardedAppliedResult(ctx, idx)
	}
	if m.forwardFn == nil {
		return 0, fmt.Errorf("meta_raft: not leader and no forwarder configured")
	}
	if err := m.forwardFn(ctx, data); err != nil {
		return 0, fmt.Errorf("meta_raft: forward to leader: %w", err)
	}
	return 0, nil
}

func (m *MetaRaft) proposeOrForwardWithGate(ctx context.Context, node metaProposerNode, plan compat.GatePlan, data []byte) (uint64, error) {
	if m.capabilityGate == nil {
		return 0, fmt.Errorf("meta_raft: capability gate not configured for %s", plan.Capability)
	}
	if err := m.capabilityGate.ValidatePlanStillCurrent(plan); err != nil {
		return 0, err
	}
	return m.proposeOrForwardWithIndex(ctx, node, data)
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

func (m *MetaRaft) waitAppliedResult(ctx context.Context, idx uint64) error {
	if err := m.waitApplied(ctx, idx); err != nil {
		return err
	}
	if err := m.applyError(idx); err != nil {
		return fmt.Errorf("meta_raft: FSM apply error at index %d: %w", idx, err)
	}
	return nil
}

func (m *MetaRaft) waitForwardedAppliedResult(ctx context.Context, idx uint64) error {
	localCtx, cancel := context.WithTimeout(ctx, metaForwardLocalApplyTimeout)
	defer cancel()
	if err := m.waitAppliedResult(localCtx, idx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			log.Warn().Err(err).Uint64("index", idx).Msg("meta_raft: forwarded command committed but local apply is still pending")
			return nil
		}
		return fmt.Errorf("meta_raft: forwarded local apply: %w", err)
	}
	return nil
}

func (m *MetaRaft) recordApplyResult(index uint64, err error) {
	if err == nil {
		return
	}
	m.applyResultMu.Lock()
	if m.applyErrs == nil {
		m.applyErrs = make(map[uint64]error)
	}
	m.applyErrs[index] = err
	for oldIndex := range m.applyErrs {
		if oldIndex+1024 < index {
			delete(m.applyErrs, oldIndex)
		}
	}
	m.applyResultMu.Unlock()
}

func (m *MetaRaft) applyError(index uint64) error {
	m.applyResultMu.Lock()
	err := m.applyErrs[index]
	delete(m.applyErrs, index)
	m.applyResultMu.Unlock()
	return err
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
			switch entry.Type {
			case raft.LogEntryCommand:
				if len(entry.Command) == 0 {
					break
				}
				if err := m.fsm.applyCmd(entry.Command); err != nil {
					log.Error().Err(err).Uint64("index", entry.Index).Msg("meta_raft: FSM apply error")
					m.recordApplyResult(entry.Index, err)
				}
				m.maybeCreateSnapshot(entry.Index)
			case raft.LogEntrySnapshot:
				meta := raft.SnapshotMeta{
					Index:   entry.Index,
					Term:    entry.Term,
					Servers: m.node.Configuration().Servers,
				}
				if err := m.fsm.Restore(meta, entry.Command); err != nil {
					log.Error().Err(err).Uint64("index", entry.Index).Msg("meta_raft: FSM snapshot restore error")
				} else {
					m.lastSnapshotIndex = entry.Index
				}
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

func (m *MetaRaft) maybeCreateSnapshot(index uint64) {
	const threshold uint64 = 1024
	if !m.node.IsLeader() || index == 0 || index-m.lastSnapshotIndex < threshold {
		return
	}
	data, err := m.fsm.Snapshot()
	if err != nil {
		log.Error().Err(err).Uint64("index", index).Msg("meta_raft: snapshot encode error")
		return
	}
	if err := m.node.CreateSnapshot(index, data); err != nil {
		log.Error().Err(err).Uint64("index", index).Msg("meta_raft: create snapshot error")
		return
	}
	m.lastSnapshotIndex = index
}
