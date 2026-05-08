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
	votedFor    string // candidate this node voted for in the current term ("" = none)
}

// actorState is the mutable Raft state owned exclusively by the actor
// goroutine. No locking — single-writer by construction.
type actorState struct {
	id          string
	currentTerm uint64
	state       NodeState
	leaderID    string
	votedFor    string // candidate this node voted for in the current term ("" = none)

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
		votedFor:    s.votedFor,
	}
}

// lastLogIndex returns the highest log index in the actor's in-memory log,
// or 0 if the log is empty. 1-based indexing per Raft convention.
func (s *actorState) lastLogIndex() uint64 {
	if len(s.log) == 0 {
		return 0
	}
	return s.log[len(s.log)-1].Index
}

// lastLogTerm returns the term of the last log entry, or 0 if the log is empty.
func (s *actorState) lastLogTerm() uint64 {
	if len(s.log) == 0 {
		return 0
	}
	return s.log[len(s.log)-1].Term
}

// isLogUpToDate reports whether a candidate's log (lastIdx, lastTerm) is at
// least as up-to-date as ours per Raft §5.4.1: higher last-term wins, and
// within the same term the longer log wins.
func (s *actorState) isLogUpToDate(lastIdx, lastTerm uint64) bool {
	myIdx, myTerm := s.lastLogIndex(), s.lastLogTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIdx >= myIdx
}
