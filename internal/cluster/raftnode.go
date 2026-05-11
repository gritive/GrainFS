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
}

// compile-time check: *raft.Node must satisfy RaftNode.
var _ RaftNode = (*raft.Node)(nil)
