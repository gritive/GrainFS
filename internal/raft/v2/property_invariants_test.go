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

// checkLeaderCompleteness asserts Invariant 4 (Leader Completeness): if an
// entry was committed in term T, every leader of any term T' > T has that
// entry in its log.
//
// Operationally: for each observed leader at term T', look up every entry
// that was applied (= committed) in some term T ≤ T' by any node. If the
// leader's applied stream has reached that entry's index (i.e., the leader
// has applied entries beyond that index), then the leader's entry at that
// index must match (same Term and Command). If the leader's stream has not
// yet reached that index, the check is skipped — the leader may still be
// catching up (sampling lag), and absence of the entry in the observation
// window is not a safety violation.
//
// Limitation: leaderObs captures leaders at action-boundary samples. A
// leader that served briefly between two observations is not captured; the
// invariant therefore only asserts on sampled leaders. This is documented
// and accepted.
func checkLeaderCompleteness(leaderObs []termLeader, nodeApplied map[string][]LogEntry) error {
	// Build per-node index → entry maps.
	nodeIdx := make(map[string]map[uint64]LogEntry, len(nodeApplied))
	for id, entries := range nodeApplied {
		m := make(map[uint64]LogEntry, len(entries))
		for _, e := range entries {
			m[e.Index] = e
		}
		nodeIdx[id] = m
	}

	// For each sampled leader, verify completeness for all earlier-committed entries.
	for _, tl := range leaderObs {
		leaderEntries, ok := nodeIdx[tl.leaderID]
		if !ok {
			continue // leader never appeared in applied stream; nothing to check
		}
		// Find max index in the leader's applied stream.
		var leaderMaxIdx uint64
		for idx := range leaderEntries {
			if idx > leaderMaxIdx {
				leaderMaxIdx = idx
			}
		}

		// Check every other node's committed entries against this leader.
		for nodeID, entries := range nodeIdx {
			if nodeID == tl.leaderID {
				continue
			}
			for idx, e := range entries {
				// Only check entries committed in term ≤ this leader's term.
				if e.Term > tl.term {
					continue
				}
				// Skip if the leader hasn't applied this far yet (sampling lag).
				if idx > leaderMaxIdx {
					continue
				}
				le, leaderHas := leaderEntries[idx]
				if !leaderHas {
					return fmt.Errorf(
						"leader completeness violated: leader %s (term %d) is missing committed entry at index %d (committed in term %d by node %s)",
						tl.leaderID, tl.term, idx, e.Term, nodeID,
					)
				}
				if le.Term != e.Term || !bytes.Equal(le.Command, e.Command) {
					return fmt.Errorf(
						"leader completeness violated: leader %s (term %d) has different entry at index %d: got term=%d, want term=%d",
						tl.leaderID, tl.term, idx, le.Term, e.Term,
					)
				}
			}
		}
	}
	return nil
}

func TestCheckLeaderCompleteness_Violation(t *testing.T) {
	// Leader "a" at term 3 is missing entry at index=1 that was committed in term 1.
	leaderObs := []termLeader{{term: 3, leaderID: "a"}}
	nodeApplied := map[string][]LogEntry{
		"a": {
			// index 1 is absent — leader skipped it
			{Index: 2, Term: 2, Command: []byte("cmd-2")},
			{Index: 3, Term: 3, Command: []byte("cmd-3")},
		},
		"b": {
			{Index: 1, Term: 1, Command: []byte("cmd-1")},
			{Index: 2, Term: 2, Command: []byte("cmd-2")},
			{Index: 3, Term: 3, Command: []byte("cmd-3")},
		},
	}
	if err := checkLeaderCompleteness(leaderObs, nodeApplied); err == nil {
		t.Fatal("expected leader completeness violation, got nil")
	}
}

func TestCheckLeaderCompleteness_Valid(t *testing.T) {
	leaderObs := []termLeader{{term: 3, leaderID: "a"}}
	nodeApplied := map[string][]LogEntry{
		"a": {
			{Index: 1, Term: 1, Command: []byte("cmd-1")},
			{Index: 2, Term: 2, Command: []byte("cmd-2")},
			{Index: 3, Term: 3, Command: []byte("cmd-3")},
		},
		"b": {
			{Index: 1, Term: 1, Command: []byte("cmd-1")},
			{Index: 2, Term: 2, Command: []byte("cmd-2")},
		},
	}
	if err := checkLeaderCompleteness(leaderObs, nodeApplied); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCheckLeaderCompleteness_LagSkip(t *testing.T) {
	// Leader "a" at term 3 has only applied index=1; node "b" has index=5.
	// The check must skip index=5 (sampling lag), not fail.
	leaderObs := []termLeader{{term: 3, leaderID: "a"}}
	nodeApplied := map[string][]LogEntry{
		"a": {
			{Index: 1, Term: 1, Command: []byte("cmd-1")},
		},
		"b": {
			{Index: 1, Term: 1, Command: []byte("cmd-1")},
			{Index: 5, Term: 2, Command: []byte("cmd-5")},
		},
	}
	if err := checkLeaderCompleteness(leaderObs, nodeApplied); err != nil {
		t.Fatalf("expected lag to be skipped, got: %v", err)
	}
}

// checkStateMachineSafety asserts Invariant 5 (State Machine Safety): no two
// nodes apply different commands at the same log index. This is the core
// FSM correctness guarantee: all replicas must be identical state machines.
//
// We check (nodeA, nodeB, index): if both nodes have an entry at the same
// index, their Command bytes must be identical. Term mismatches at the same
// index are caught earlier by checkLogMatching (Invariant 3); this checker
// only asserts the command-equivalence half of state machine safety.
func checkStateMachineSafety(nodeApplied map[string][]LogEntry) error {
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

	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			a, b := nodes[i], nodes[j]
			for idx, ea := range a.idx {
				eb, ok := b.idx[idx]
				if !ok {
					continue
				}
				if !bytes.Equal(ea.Command, eb.Command) {
					return fmt.Errorf(
						"state machine safety violated: nodes %s and %s applied different commands at index %d: %q vs %q",
						a.id, b.id, idx, ea.Command, eb.Command,
					)
				}
			}
		}
	}
	return nil
}

func TestCheckStateMachineSafety_Violation(t *testing.T) {
	nodeApplied := map[string][]LogEntry{
		"a": {{Index: 3, Term: 1, Command: []byte("cmd-x")}},
		"b": {{Index: 3, Term: 1, Command: []byte("cmd-y")}}, // different!
	}
	if err := checkStateMachineSafety(nodeApplied); err == nil {
		t.Fatal("expected state machine safety violation, got nil")
	}
}

func TestCheckStateMachineSafety_Valid(t *testing.T) {
	nodeApplied := map[string][]LogEntry{
		"a": {{Index: 3, Term: 1, Command: []byte("cmd-x")}},
		"b": {{Index: 3, Term: 1, Command: []byte("cmd-x")}},
		"c": {{Index: 3, Term: 1, Command: []byte("cmd-x")}},
	}
	if err := checkStateMachineSafety(nodeApplied); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// proposedEntry records the result of a single successful Propose action.
// index is the log index returned by ProposeWait.
type proposedEntry struct {
	index uint64
}

// actionKind categorises rapid actions for liveness-suffix analysis.
type actionKind int

const (
	actionPropose actionKind = iota
	actionStepDownLeader
	actionPartition
	actionHeal
)

// actionRecord captures what happened during one rapid action invocation.
type actionRecord struct {
	kind          actionKind
	leaderExisted bool           // true if a leader was present when this action started
	proposed      *proposedEntry // non-nil if a Propose succeeded (returned non-error)
}

// checkEventualCommit asserts Invariant 6 (Eventual Commit Under Stable
// Leadership): every proposal that succeeded in the final K-action stable
// suffix must have committed by the end of the sequence.
//
// "Stable suffix" means: the final K actions contain no Partition or
// StepDownLeader action, and a leader existed at the start of that suffix.
// If the suffix is too short, or contains destabilising actions, or no
// leader existed at suffix start, the check is skipped (returns nil) — this
// is a conditional invariant and most random sequences will not satisfy the
// stability condition.
//
// maxCommitted is max(n.CommittedIndex()) sampled across all cluster nodes
// just before this call. It must NOT be derived from nodeApplied (applied
// entries lag behind commit — ProposeWait returns when the leader commits,
// not when followers apply).
//
// K = 50 is the recommended stable-window size (as per PR 18 spec).
func checkEventualCommit(history []actionRecord, maxCommitted uint64) error {
	const K = 50
	if len(history) < K {
		return nil // insufficient history
	}

	suffix := history[len(history)-K:]

	// Check that the suffix contains no destabilising actions.
	for _, ar := range suffix {
		if ar.kind == actionPartition || ar.kind == actionStepDownLeader {
			return nil // not stable; skip
		}
	}

	// Check that a leader existed at the start of the suffix.
	if !suffix[0].leaderExisted {
		return nil // no leader at suffix start; skip
	}

	// Every successful propose in the suffix must have committed.
	for i, ar := range suffix {
		if ar.proposed == nil {
			continue
		}
		if ar.proposed.index > maxCommitted {
			return fmt.Errorf(
				"liveness violated: proposed entry at index %d (suffix action %d) not committed; max committed index across all nodes is %d",
				ar.proposed.index, i, maxCommitted,
			)
		}
	}
	return nil
}

func TestCheckEventualCommit_Violation(t *testing.T) {
	// Build a stable suffix of K=50 propose-only actions with leader present.
	// One propose claims index=999 which exceeds the max committed (10).
	const K = 50
	history := make([]actionRecord, K)
	for i := range history {
		history[i] = actionRecord{kind: actionPropose, leaderExisted: true}
	}
	// Inject a successful propose at index 999 in the last action.
	history[K-1].proposed = &proposedEntry{index: 999}

	if err := checkEventualCommit(history, 10); err == nil {
		t.Fatal("expected liveness violation, got nil")
	}
}

func TestCheckEventualCommit_SkipOnPartition(t *testing.T) {
	// If the suffix contains a Partition action, check must be skipped.
	const K = 50
	history := make([]actionRecord, K)
	for i := range history {
		history[i] = actionRecord{kind: actionPropose, leaderExisted: true}
	}
	history[K-5] = actionRecord{kind: actionPartition, leaderExisted: true}
	history[K-1].proposed = &proposedEntry{index: 999}

	if err := checkEventualCommit(history, 10); err != nil {
		t.Fatalf("expected skip on partition, got: %v", err)
	}
}

func TestCheckEventualCommit_SkipOnNoLeader(t *testing.T) {
	// If no leader at suffix start, check must be skipped.
	const K = 50
	history := make([]actionRecord, K)
	for i := range history {
		history[i] = actionRecord{kind: actionPropose, leaderExisted: false}
	}
	history[K-1].proposed = &proposedEntry{index: 999}

	if err := checkEventualCommit(history, 10); err != nil {
		t.Fatalf("expected skip on no-leader, got: %v", err)
	}
}

func TestCheckEventualCommit_SkipOnShortHistory(t *testing.T) {
	history := []actionRecord{
		{kind: actionPropose, leaderExisted: true, proposed: &proposedEntry{index: 999}},
	}
	if err := checkEventualCommit(history, 10); err != nil {
		t.Fatalf("expected skip on short history, got: %v", err)
	}
}

func TestCheckEventualCommit_Valid(t *testing.T) {
	const K = 50
	history := make([]actionRecord, K)
	for i := range history {
		history[i] = actionRecord{kind: actionPropose, leaderExisted: true}
	}
	// A committed propose at index 5 — well within max committed.
	history[K-1].proposed = &proposedEntry{index: 5}

	if err := checkEventualCommit(history, 5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
