package cluster

import (
	"context"

	"github.com/gritive/GrainFS/internal/raft"
)

// RaftNode is the interface that internal/cluster uses to drive a Raft
// consensus node. It is satisfied by *raft.Node (v1) in production and by
// *raftV2Node (the v2 adapter) when GRAINFS_RAFT_V2=cluster is set.
//
// Method set is derived from a usage survey of internal/cluster/*.go (non-test)
// as of the M4 integration milestone. Only methods actually called through this
// interface are listed; v1-internal methods (JointSnapshotState, CompactLog,
// SetInstallSnapshotTransport, …) remain on *raft.Node and are accessed via
// GroupBackend.RaftNode() type assertion in v1-only code paths.
//
// v1 ↔ v2 mismatches are resolved in raftv2adapter.go; callers see v1 names.
type RaftNode interface {
	// Lifecycle.
	Start()
	Close()

	// Identity.
	ID() string

	// State reads (hot path — lock-free in both v1 and v2).
	State() raft.NodeState
	Term() uint64
	IsLeader() bool
	LeaderID() string
	CommittedIndex() uint64

	// Cluster membership (read-only view).
	Configuration() raft.Configuration

	// Peers returns the addresses of peer nodes (excludes self).
	// v2 derives this from Configuration(); the adapter filters out the local ID.
	Peers() []string

	// PeerMatchIndex returns the last known replicated index for the given
	// peerKey (address or nodeID). Used by DataGroupPlanExecutor to wait for
	// catch-up before leadership transfer. v2 adapter returns (0, false) because
	// v2 does not expose per-peer replication state; callers must tolerate this.
	PeerMatchIndex(peerKey string) (uint64, bool)

	// Bootstrapping.
	Bootstrap() error

	// Write path.
	Propose(command []byte) error
	ProposeWait(ctx context.Context, command []byte) (uint64, error)

	// Read path.
	ReadIndex(ctx context.Context) (uint64, error)
	WaitApplied(ctx context.Context, index uint64) error

	// Apply channel — consumer drains this to drive the FSM.
	ApplyCh() <-chan raft.LogEntry

	// Transport wiring — v1-style callbacks; adapter synthesises a v2.Transport.
	// Must be called before Start().
	SetTransport(
		sendRequestVote func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error),
		sendAppendEntries func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error),
	)

	// SetNoOpCommand configures the FSM no-op payload proposed on leader election.
	// v2 handles no-op entries internally; the adapter is a no-op.
	SetNoOpCommand(cmd []byte)

	// Observer pattern. v2 has no observer pattern; the adapter stubs are no-ops
	// that log a warning once. Staging soak will exercise without observers.
	RegisterObserver(ch chan<- raft.Event)
	DeregisterObserver(ch chan<- raft.Event)

	// Membership mutation — added in M4 follow-up to close the v2 nil-skip gap.
	// v2 delegates to raftv2adapter.go; methods that v2 has not yet implemented
	// surface raftv2.ErrNotImplemented so operators see a clear error.

	// AddVoter proposes adding a new full voting member to the cluster.
	// Performs learner-first (v1) or joint-consensus (v2): learner → voter.
	AddVoter(id, addr string) error

	// AddVoterCtx is AddVoter with an explicit context for cancellation/timeout.
	AddVoterCtx(ctx context.Context, id, addr string) error

	// RemoveVoter proposes removing a voting member from the cluster.
	RemoveVoter(id string) error

	// AddLearner proposes adding a non-voting observer to the cluster.
	// v2 returns ErrNotImplemented (M2 scope).
	AddLearner(id, addr string) error

	// PromoteToVoter promotes a learner to a full voting member.
	// v2 returns ErrNotImplemented (M2 scope).
	PromoteToVoter(id string) error

	// TransferLeadership initiates a leadership transfer to another voter.
	// v2 returns ErrNotImplemented (M2 scope).
	TransferLeadership() error

	// ChangeMembership atomically transitions the cluster membership.
	// v1: uses §4.3 joint consensus. v2: sequences AddVoterCtx + RemoveVoter
	// calls — not atomic (partial failure leaves intermediate state; see
	// raftv2adapter.go for the WARN: caveat).
	ChangeMembership(ctx context.Context, adds []raft.ServerEntry, removes []string) error

	// Inbound Raft RPC handlers — invoked by the QUIC RPC server when a peer
	// delivers a Raft message. Argument and reply types are v1's (raft.*) so
	// the QUIC wire codec is shared across v1 and v2. v2's adapter translates
	// at the boundary (see raftv2adapter.go::Handle*).
	//
	// Added in M5 PR 27 so the v2 QUIC RPC bridge can dispatch into either
	// raft.Node (direct) or raftv2.Node (via translation).
	HandleRequestVote(args *raft.RequestVoteArgs) *raft.RequestVoteReply
	HandleAppendEntries(args *raft.AppendEntriesArgs) *raft.AppendEntriesReply
	HandleInstallSnapshot(args *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply
	// HandleTimeoutNow accepts an empty args struct (v1 wire format carries no
	// payload). The v2 adapter synthesises args.Term = receiver currentTerm so
	// v2's stale-term check (Raft §3.10) accepts the call; PR 30 will rework
	// the wire format if v2 needs to propagate the leader's term.
	HandleTimeoutNow()
}

// RaftV2Snapshotter is an optional interface implemented by the v2 RaftNode
// adapter. The v1 path uses the long-standing *raft.SnapshotManager
// (DistributedBackend.SetSnapshotManager); v2 owns snapshot lifecycle
// internally, so the admin TriggerRaftSnapshot path discovers v2 via this
// type-assertion and forwards through CreateSnapshot / SnapshotStatus.
//
// PR 30 (v1 deletion) folds this into RaftNode.
type RaftV2Snapshotter interface {
	// CreateSnapshot persists an FSM snapshot at lastIncludedIndex and compacts
	// the log up to that index inside v2's actor goroutine.
	CreateSnapshot(lastIncludedIndex uint64, data []byte) error
	// SnapshotStatus reports the latest persisted v2 snapshot using v1's
	// raft.SnapshotStatus shape so callers can use a single type.
	SnapshotStatus() (raft.SnapshotStatus, error)
}

// compile-time check: *raft.Node must satisfy RaftNode.
var _ RaftNode = (*raft.Node)(nil)
