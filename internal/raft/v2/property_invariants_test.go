package raftv2

import (
	"bytes"
	"fmt"
	"testing"
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

// checkLogMatching asserts Invariant 3 (Log Matching): if any two nodes have
// both applied an entry at index i with the same Term, then all entries at
// j < i in both streams must have identical (Term, Command) pairs.
//
// The applied stream is indexed by position, not by LogEntry.Index directly,
// so we build a lookup map[index]LogEntry for each node and then compare.
// This catches diverged histories that could arise if Raft's log matching
// property were violated during AppendEntries conflict resolution.
func checkLogMatching(nodeApplied map[string][]LogEntry) error {
	// Build per-node index → entry maps.
	type nodeMap struct {
		id  string
		idx map[uint64]LogEntry
	}
	nodes := make([]nodeMap, 0, len(nodeApplied))
	for id, entries := range nodeApplied {
		m := make(map[uint64]LogEntry, len(entries))
		for _, e := range entries {
			m[e.Index] = e
		}
		nodes = append(nodes, nodeMap{id: id, idx: m})
	}

	// Check every pair.
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			a, b := nodes[i], nodes[j]
			// Find matching (index, term) positions.
			for idx, ea := range a.idx {
				eb, ok := b.idx[idx]
				if !ok {
					continue
				}
				if ea.Term != eb.Term {
					// Different terms at same index — not a log matching violation per se
					// (could be a no-op vs command distinction), but not possible in a
					// correct Raft: terms at the same committed index must match.
					return fmt.Errorf(
						"log matching violated: nodes %s and %s disagree on term at index %d (%d vs %d)",
						a.id, b.id, idx, ea.Term, eb.Term,
					)
				}
				// Same (index, term): verify all prior entries match.
				for prior := uint64(1); prior < idx; prior++ {
					pa, aHas := a.idx[prior]
					pb, bHas := b.idx[prior]
					if !aHas || !bHas {
						continue // one node hasn't applied this yet; not a violation
					}
					if pa.Term != pb.Term || !bytes.Equal(pa.Command, pb.Command) {
						return fmt.Errorf(
							"log matching violated: nodes %s and %s agree at (index=%d, term=%d) but differ at prior index %d: terms %d vs %d",
							a.id, b.id, idx, ea.Term, prior, pa.Term, pb.Term,
						)
					}
				}
			}
		}
	}
	return nil
}

func TestCheckLogMatching_Violation(t *testing.T) {
	// Nodes a and b agree at (index=2, term=1) but differ at prior index=1.
	nodeApplied := map[string][]LogEntry{
		"a": {
			{Index: 1, Term: 1, Command: []byte("cmd-a")},
			{Index: 2, Term: 1, Command: []byte("cmd-2")},
		},
		"b": {
			{Index: 1, Term: 1, Command: []byte("cmd-b")}, // different!
			{Index: 2, Term: 1, Command: []byte("cmd-2")},
		},
	}
	if err := checkLogMatching(nodeApplied); err == nil {
		t.Fatal("expected log matching violation, got nil")
	}
}

func TestCheckLogMatching_Valid(t *testing.T) {
	nodeApplied := map[string][]LogEntry{
		"a": {
			{Index: 1, Term: 1, Command: []byte("cmd-1")},
			{Index: 2, Term: 1, Command: []byte("cmd-2")},
		},
		"b": {
			{Index: 1, Term: 1, Command: []byte("cmd-1")},
			{Index: 2, Term: 1, Command: []byte("cmd-2")},
		},
	}
	if err := checkLogMatching(nodeApplied); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
