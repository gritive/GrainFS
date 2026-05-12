// Package raftv2 is the actor-pattern reimplementation of internal/raft.
//
// Status: PR 1 skeleton — single-node Propose round-trip only. See
// docs/superpowers/plans/2026-05-08-raft-actor-redesign.md for scope and the
// roadmap to feature parity (M1-M5).
//
// Design: a single goroutine ("actor") owns the mutable Raft state. Read-mostly
// hot-path methods serve from an immutable readState snapshot published via
// atomic.Pointer; mutating methods enqueue commands on a buffered channel for
// the actor to apply serially. This eliminates the v1 mu mutex without paying
// a channel round-trip on every State()/Term()/IsLeader() call.
package raft

import (
	"errors"
	"fmt"
	"time"
)

// Sentinel errors mirrored from internal/raft so caller code can treat v1 and
// v2 interchangeably during the M5 phased import flip.
var (
	ErrNotLeader           = errors.New("not the leader")
	ErrProposalFailed      = errors.New("proposal failed: node stepped down")
	ErrNodeStopped         = errors.New("raft: node stopped")
	ErrAlreadyBootstrapped = errors.New("raft: cluster already bootstrapped")
	ErrNotImplemented      = errors.New("raft/v2: not implemented (M2 scope)")
	// ErrNoPeers is returned by TransferLeadership when the cluster has no
	// peer voters to transfer leadership to.
	ErrNoPeers = errors.New("raft: no peers to transfer leadership to")
	// ErrConfChangeInFlight is returned by AddVoter / RemoveVoter when a
	// previous Raft §4.3 joint-consensus membership change has not yet
	// committed both phases (joint entry + final ConfChange entry). One
	// in-flight membership change at a time mirrors hashicorp/raft's
	// pragmatic rule and avoids pipelining edge cases.
	ErrConfChangeInFlight = errors.New("raft: configuration change already in flight")
	// ErrLearnerNotCaughtUp is returned by PromoteToVoter when the
	// learner's matchIndex lags more than cfg.LearnerCatchupThreshold
	// entries behind the leader's commit index. Callers should wait for
	// the learner to drain and retry.
	ErrLearnerNotCaughtUp = errors.New("raft: learner not caught up to leader commit")
	// ErrNotALearner is returned by PromoteToVoter when the target id is
	// not registered as a learner in the live configuration.
	ErrNotALearner = errors.New("raft: target is not a learner")
	// ErrAlreadyLearner is returned by AddLearner when id is already
	// either a voter or a learner.
	ErrAlreadyLearner = errors.New("raft: id is already a voter or learner")
)

// ConfChangeOp tags a single-phase LogEntryConfChange entry so the actor
// can dispatch on the membership change kind without consulting external
// state. Added in M6.0 (Path B). The joint encoder (LogEntryJointConfChange)
// is voter-only and does NOT carry an Op tag — joint entries are always
// "carry the voter set into a Cold ∪ Cnew transition".
//
// Op values must be stable across upgrades — they are persisted in
// LogStore entries. New ops should be appended; values must not be
// reordered.
type ConfChangeOp uint8

const (
	// ConfChangeAddVoter tags the single-phase final entry of a joint
	// AddVoter/RemoveVoter transition (Cnew settles). Also the wire op
	// used by legacy-style v2 voter-set joint exits.
	ConfChangeAddVoter ConfChangeOp = 0
	// ConfChangeAddLearner is a single-phase append that registers a new
	// non-voting observer. Quorum unchanged; voter slices unchanged.
	ConfChangeAddLearner ConfChangeOp = 1
	// ConfChangePromoteStage1 is the first of two log entries that
	// implement PromoteToVoter (Path B). Drops the target from the
	// learners map. The follow-up ConfChangeAddVoter joint transition
	// then adds the target as a Voter.
	ConfChangePromoteStage1 ConfChangeOp = 2
	// ConfChangeRemoveLearner is a single-phase entry that drops a
	// learner without joint consensus (quorum unchanged).
	ConfChangeRemoveLearner ConfChangeOp = 3
)

func (o ConfChangeOp) String() string {
	switch o {
	case ConfChangeAddVoter:
		return "AddVoter"
	case ConfChangeAddLearner:
		return "AddLearner"
	case ConfChangePromoteStage1:
		return "PromoteStage1"
	case ConfChangeRemoveLearner:
		return "RemoveLearner"
	default:
		return fmt.Sprintf("Unknown(%d)", uint8(o))
	}
}

// ServerSuffrage is mirrored from v1 internal/raft/raft.go for M5 swap-time
// API parity. Voter participates in elections and quorum; NonVoter (learner)
// receives log entries but does not vote.
type ServerSuffrage int

const (
	Voter ServerSuffrage = iota
	NonVoter
)

// Server identifies a single cluster member with its voting role.
type Server struct {
	ID       string
	Suffrage ServerSuffrage
}

type ServerEntry struct {
	ID      string
	Address string
}

// Configuration is a point-in-time view of the cluster's voter set.
type Configuration struct {
	Servers []Server
}

type Event struct{}

// NodeState represents the current role of a Raft node. Values match v1.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("Unknown(%d)", int(s))
	}
}

// LogEntryType distinguishes normal FSM commands from Raft protocol entries.
// Mirrored from v1 (internal/raft/membership.go); values must stay in lockstep
// with the FlatBuffers LogEntryType enum.
type LogEntryType int8

const (
	LogEntryCommand         LogEntryType = 0
	LogEntryConfChange      LogEntryType = 1
	LogEntryJointConfChange LogEntryType = 2 // reserved; not implemented
	// LogEntryNoOp is a leader-blank entry appended immediately on election
	// (Raft §5.4.2) so the new term's commit index can advance without waiting
	// for a client request. FSM consumers MUST ignore entries of this type;
	// the Command field is always nil for no-op entries.
	LogEntryNoOp LogEntryType = 3
	// LogEntrySnapshot is a synthetic entry delivered on applyCh when a
	// follower installs a snapshot via InstallSnapshot RPC (Raft §7 / §6.3).
	// It is NOT stored in the log. FSM consumers MUST recognise this Type
	// and reset their state, then load from Command (which carries the
	// snapshot's opaque Data bytes). Index/Term are the snapshot's
	// LastIncludedIndex / LastIncludedTerm. After delivering this entry, the
	// follower resumes normal AE replication starting at LastIncludedIndex+1,
	// so subsequent applyCh entries continue in FIFO order.
	LogEntrySnapshot LogEntryType = 4
)

// LogEntry represents a single entry in the Raft log. Identical to v1's
// LogEntry so the M5 import flip preserves on-the-wire and on-disk shape.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
	Type    LogEntryType
}

// RPC types below mirror v1 verbatim from internal/raft/raft.go (RequestVote*
// at 222-235, AppendEntries* at 237-253) so the M5 swap-time API parity is
// preserved. PR 4 only consumes RequestVote* in HandleRequestVote and exposes
// AppendEntries* via the stub HandleAppendEntries; full semantics for
// AppendEntries land in PR 5+. Pre-vote and leader-transfer fields are
// accepted but ignored until PR 5+.

// RequestVoteArgs is sent by candidates to gather votes.
type RequestVoteArgs struct {
	Term           uint64
	CandidateID    string
	LastLogIndex   uint64
	LastLogTerm    uint64
	PreVote        bool // true = pre-vote round; receiver must not update state/term
	LeaderTransfer bool // true = leadership transfer; receiver must bypass stickiness
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader to replicate log entries.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictTerm  uint64 // term of conflicting entry; 0 = not set or old peer
	ConflictIndex uint64 // first index of ConflictTerm; 0 = not set
}

// InstallSnapshotArgs is sent by the leader to a follower whose nextIndex has
// fallen below the leader's FirstIndex (i.e., the leader has compacted past
// the entries the follower needs). PR 15 sends the entire snapshot in a
// single RPC; chunked transmission is out of scope (acceptable for the
// snapshot sizes seen in tests; future PR will chunk).
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Servers           []Server
	Configuration     []string
	Learners          map[string]string
	Data              []byte
}

// InstallSnapshotReply is the response to an InstallSnapshot RPC. The
// follower reports its currentTerm so a stale leader can step down.
type InstallSnapshotReply struct {
	Term uint64
}

// TimeoutNowArgs is sent by the leader to its chosen transfer target to trigger
// an immediate election (Raft §3.10). The target starts an election in a new
// term without waiting for its election timer to expire.
type TimeoutNowArgs struct {
	Term   uint64 // leader's currentTerm
	Leader string // leader's ID (for logging only)
}

// TimeoutNowReply is the response to a TimeoutNow RPC. The target reports its
// currentTerm so a stale leader can step down. Success is false when the target
// ignores the request (e.g., it is already a candidate/leader).
type TimeoutNowReply struct {
	Term    uint64 // target's currentTerm
	Success bool   // true if target accepted the transfer
}

// Config holds Raft node configuration. Field set is mirrored verbatim from
// v1 so caller code compiles unchanged at swap time. PR 1 only consumes ID
// and Peers; remaining fields are accepted but ignored until later PRs wire
// them through to the actor.
type Config struct {
	ID                            string
	Peers                         []string      // addresses of other nodes (excludes self)
	ElectionTimeout               time.Duration // base election timeout
	HeartbeatTimeout              time.Duration
	ManagedMode                   bool
	LogGCInterval                 time.Duration
	MaxEntriesPerAE               uint64
	MaxAppendEntriesInflight      int
	MaxAppendEntriesInflightBytes int
	TrailingLogs                  uint64
	LearnerCatchupThreshold       uint64
	JointAbortTimeout             time.Duration
	ElectionPriorityKey           string

	// JoinMode disables the solo-voter auto-promote shortcut. When true and
	// the effective config reduces to {selfID} only, the node stays in
	// Follower indefinitely instead of becoming Leader at term 1. Used by
	// dynamic-join paths where the joiner must not self-elect — its node is
	// added as a learner by the cluster leader (via AddLearner) and then
	// promoted (PromoteToVoter), at which point the leader's AppendEntries
	// installs a multi-voter config and the node participates normally.
	//
	// Default false preserves v1-equivalent single-voter bootstrap behaviour
	// for callers that rely on it (tests, non-join cluster init).
	JoinMode bool

	// LogStore, if non-nil, is used as the durable log backing. Defaults to an
	// in-memory implementation if nil. To enable crash recovery, supply a
	// persistent impl such as badgerLogStore.
	LogStore LogStore

	// StableStore, if non-nil, is used to persist HardState (currentTerm,
	// votedFor). Defaults to in-memory if nil. To enable crash recovery,
	// supply a persistent impl such as badgerStableStore. Pairing a
	// persistent LogStore with an in-memory StableStore violates Raft
	// §5.4.1 safety on restart — the caller is responsible for supplying
	// both or neither.
	StableStore StableStore

	// SnapshotStore, if non-nil, is used to persist Raft snapshots (§7).
	// Defaults to in-memory if nil. Pairing a persistent LogStore with an
	// in-memory SnapshotStore is unsafe in the same way that pairing a
	// persistent LogStore with an in-memory StableStore is unsafe — on
	// restart the log's compaction boundary (FirstIndex) survives but the
	// snapshot data needed to seed the FSM beyond that boundary is gone.
	// The caller is responsible for supplying durable LogStore +
	// StableStore + SnapshotStore together (or none of them).
	SnapshotStore SnapshotStore
}
