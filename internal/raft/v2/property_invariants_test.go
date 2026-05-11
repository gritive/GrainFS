package raftv2

import (
	"fmt"
)

// invObserver is a shared observation store used by invariant checkers.
// It accumulates (term, leaderID) pairs observed across all nodes, and per-node
// applied-entry sequences for the Leader Append-Only invariant.
//
// All mutation happens from a single goroutine: rapid.StateMachine actions and
// Check fire sequentially per sequence (rapid v1.3.0 contract). The drain
// goroutines in propertyCluster forward applied entries to ObsCh; Check
// drains ObsCh before invoking invariant assertions — no concurrent writes
// to the slices/maps/counter.
type invObserver struct {
	// leaderObs records every distinct (term, leaderID) pair seen.
	// Invariant 1 (Election Safety): for each term, at most one leaderID.
	leaderObs []termLeader

	// nodeApplied records, per nodeID, the sequence of applied entries.
	// Invariant 2 (Leader Append-Only): for each node, the index sequence
	// must be strictly monotone (no rewrites of already-applied indices).
	nodeApplied map[string][]LogEntry

	// proposeCounter is incremented by Propose actions to generate unique
	// commands. Single-goroutine access per the struct contract above; plain
	// int64 (not atomic) — atomic would imply concurrent writes that do not
	// occur under rapid's sequential action dispatch.
	proposeCounter int64
}

type termLeader struct {
	term     uint64
	leaderID string
}

func newInvObserver() *invObserver {
	return &invObserver{
		nodeApplied: make(map[string][]LogEntry),
	}
}

// recordLeader records a (term, leaderID) observation. Idempotent for the same pair.
func (o *invObserver) recordLeader(term uint64, leaderID string) {
	if leaderID == "" {
		return
	}
	for _, tl := range o.leaderObs {
		if tl.term == term && tl.leaderID == leaderID {
			return // already recorded
		}
	}
	o.leaderObs = append(o.leaderObs, termLeader{term: term, leaderID: leaderID})
}

// drainObsCh pulls all pending nodeEntry values from ch into o.nodeApplied.
// Must be called before checkInvariants to ensure observations are current.
// Non-blocking: returns immediately when ch has no pending items.
func (o *invObserver) drainObsCh(ch chan nodeEntry) {
	for {
		select {
		case ne, ok := <-ch:
			if !ok {
				return
			}
			o.nodeApplied[ne.nodeID] = append(o.nodeApplied[ne.nodeID], ne.entry)
		default:
			return
		}
	}
}

// checkElectionSafety asserts Invariant 1: for every observed term, at most one
// distinct leaderID must exist. A violation means two nodes believed themselves
// leader in the same term, which breaks Raft's core safety guarantee.
func checkElectionSafety(obs []termLeader) error {
	byTerm := make(map[uint64]string)
	for _, tl := range obs {
		if prev, seen := byTerm[tl.term]; seen {
			if prev != tl.leaderID {
				return fmt.Errorf(
					"election safety violated: term %d has two leaders: %q and %q",
					tl.term, prev, tl.leaderID,
				)
			}
		} else {
			byTerm[tl.term] = tl.leaderID
		}
	}
	return nil
}

// checkLeaderAppendOnly asserts Invariant 2: for each node, the sequence of
// applied entries must be strictly monotone in index (no rewrites). A violation
// means an already-committed index was delivered again with a different value,
// which breaks Raft's state machine safety property.
//
// Note: entries of type LogEntryNoOp have nil Command; we include them because
// they still carry a unique index and must obey the monotone ordering rule.
func checkLeaderAppendOnly(nodeApplied map[string][]LogEntry) error {
	for nodeID, entries := range nodeApplied {
		var lastIdx uint64
		for i, e := range entries {
			if e.Index <= lastIdx {
				return fmt.Errorf(
					"leader append-only violated on node %s: entry[%d].Index=%d not greater than previous index=%d (entries not monotone)",
					nodeID, i, e.Index, lastIdx,
				)
			}
			lastIdx = e.Index
		}
	}
	return nil
}
