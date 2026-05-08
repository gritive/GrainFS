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
package raftv2

import (
	"errors"
	"fmt"
	"time"
)

// Sentinel errors mirrored from internal/raft so caller code can treat v1 and
// v2 interchangeably during the M5 phased import flip.
var (
	ErrNotLeader      = errors.New("not the leader")
	ErrProposalFailed = errors.New("proposal failed: node stepped down")
	ErrNodeStopped    = errors.New("raft: node stopped")
)

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
)

// LogEntry represents a single entry in the Raft log. Identical to v1's
// LogEntry so the M5 import flip preserves on-the-wire and on-disk shape.
type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
	Type    LogEntryType
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
}
