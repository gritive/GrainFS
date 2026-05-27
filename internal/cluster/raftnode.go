package cluster

import (
	"context"

	"github.com/gritive/GrainFS/internal/raft"
)

// RaftNode is the interface that internal/cluster uses to drive a Raft
// consensus node. Production nodes are constructed through raftNodeAdapter.
type RaftNode interface {
	// Lifecycle.
	Start()
	Close()

	// Identity.
	ID() string

	// State reads.
	State() raft.NodeState
	Term() uint64
	IsLeader() bool
	LeaderID() string
	CommittedIndex() uint64

	// LastLogIndex returns the durable last-log index (log tail), independent
	// of commitIndex. Used at boot to drain the apply loop up to the persisted
	// log tail before reading replay-dependent FSM state (e.g. the greenfield
	// DEK boundary flag set during type-48 replay).
	LastLogIndex() uint64

	// Cluster membership (read-only view).
	Configuration() raft.Configuration

	// Peers returns the addresses of peer nodes (excludes self).
	Peers() []string

	// PeerMatchIndex returns the last known replicated index for the given
	// peerKey (address or nodeID). Used by DataGroupPlanExecutor to wait for
	// catch-up before leadership transfer.
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

	// Transport wiring. The adapter synthesises a raft.Transport.
	// Must be called before Start().
	SetTransport(
		sendRequestVote func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error),
		sendAppendEntries func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error),
	)

	// SetInstallSnapshotTransport wires the outbound InstallSnapshot send
	// callback. Must be called before Start().
	SetInstallSnapshotTransport(send func(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error))

	// SetTimeoutNowTransport wires the outbound TimeoutNow send callback.
	// Must be called before Start(). Enables immediate leadership transfer
	// on shutdown (Raft §3.10) instead of waiting for election timeout.
	SetTimeoutNowTransport(send func(peer string, args *raft.TimeoutNowArgs) (*raft.TimeoutNowReply, error))

	// SetNoOpCommand configures the FSM no-op payload proposed on leader election.
	// The canonical actor emits no-op entries internally; the adapter is a no-op.
	SetNoOpCommand(cmd []byte)

	// Observer pattern. The adapter stubs are no-ops that log a warning once.
	RegisterObserver(ch chan<- raft.Event)
	DeregisterObserver(ch chan<- raft.Event)

	// AddVoter proposes adding a new full voting member to the cluster.
	AddVoter(id, addr string) error

	// AddVoterCtx is AddVoter with an explicit context for cancellation/timeout.
	AddVoterCtx(ctx context.Context, id, addr string) error

	// RemoveVoter proposes removing a voting member from the cluster.
	RemoveVoter(id string) error

	// AddLearner proposes adding a non-voting observer to the cluster.
	// The new learner immediately starts receiving replicated entries, but its
	// acks never contribute to commit advance.
	AddLearner(id, addr string) error

	// PromoteToVoter triggers the two-entry Path B promotion sequence
	// (drop-from-learners → joint AddVoter). Returns ErrLearnerNotCaughtUp
	// when the learner's matchIndex lags more than
	// cfg.LearnerCatchupThreshold entries behind commit. Surface this
	// error to callers so they can retry once the learner drains.
	PromoteToVoter(id string) error

	// RemoveLearner proposes dropping a non-voting observer from the cluster
	// (inverse of AddLearner). Returns ErrNotALearner when id is not a
	// registered learner. Used to roll back an un-promoted learner.
	RemoveLearner(id string) error

	// TransferLeadership initiates a leadership transfer to another voter.
	TransferLeadership() error

	// ChangeMembership transitions the cluster membership.
	ChangeMembership(ctx context.Context, adds []raft.ServerEntry, removes []string) error

	// Inbound Raft RPC handlers — invoked by the QUIC RPC server when a peer
	// delivers a Raft message.
	HandleRequestVote(args *raft.RequestVoteArgs) *raft.RequestVoteReply
	HandleAppendEntries(args *raft.AppendEntriesArgs) *raft.AppendEntriesReply
	HandleInstallSnapshot(args *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply
	// HandleTimeoutNow accepts an empty args struct for the current wire format.
	HandleTimeoutNow()

	// Snapshot surface.
	CreateSnapshot(lastIncludedIndex uint64, data []byte) error
	SnapshotStatus() (raft.SnapshotStatus, error)
	LatestSnapshot() (*raft.Snapshot, error)
}

var _ RaftNode = (*raftNodeAdapter)(nil)
