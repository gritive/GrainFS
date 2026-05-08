package raftv2

// readState is the immutable snapshot served to read-mostly hot-path callers
// (State, Term, IsLeader, LeaderID, CommittedIndex). The actor goroutine
// constructs a new value after every state transition and stores it via
// atomic.Pointer.Store; readers Load and read fields without locking.
//
// Multi-field reads on the same snapshot are coherent. Multi-field reads
// across two Load calls are NOT — that TOCTOU is a known design tradeoff
// flagged in the plan (Decision T3). Callers needing cross-field atomicity
// must Load once and reuse the pointer.
type readState struct {
	state       NodeState
	term        uint64
	leaderID    string
	commitIndex uint64
	isLeader    bool
}

// actorState is the mutable Raft state owned exclusively by the actor
// goroutine. No locking — single-writer by construction.
type actorState struct {
	id          string
	currentTerm uint64
	state       NodeState
	leaderID    string

	// In-memory log. PR 1 has no persistence; the LogStore adapter lands in
	// PR 6+. Indices are 1-based per Raft convention; log[0] corresponds to
	// log index 1.
	log         []LogEntry
	commitIndex uint64
}

// snapshot builds a readState reflecting the current actor-owned state.
func (s *actorState) snapshot() *readState {
	return &readState{
		state:       s.state,
		term:        s.currentTerm,
		leaderID:    s.leaderID,
		commitIndex: s.commitIndex,
		isLeader:    s.state == Leader && s.leaderID == s.id && s.leaderID != "",
	}
}
