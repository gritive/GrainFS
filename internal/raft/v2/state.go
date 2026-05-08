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

	// votesGranted counts ballots received in the current Candidate term,
	// including the self-vote. Reset to 1 on becomeCandidate; only meaningful
	// while state == Candidate.
	votesGranted int

	// log holds the Raft log entries via the LogStore interface. The concrete
	// implementation is memLogStore (in-memory); BadgerDB backing lands in PR 10.
	// All access is from the actor goroutine only — no locking required.
	log         LogStore
	commitIndex uint64

	// Leader-only replication tracking. Allocated in becomeLeader, cleared in
	// becomeFollower. matchIndex[peer] is the highest log index known to be
	// replicated on peer; nextIndex[peer] is the next log index to send.
	matchIndex map[string]uint64
	nextIndex  map[string]uint64

	// proposeWaiters maps log index → reply channel for ProposeWait callers
	// whose entries are still in flight (multi-voter path). Drained on
	// commitIndex advance; replies ErrProposalFailed on Leader→Follower
	// step-down. Single-voter path replies inline and never populates this.
	proposeWaiters map[uint64]chan proposalResult
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

// lastLogIndex returns the highest log index in the log, or 0 if empty.
// 1-based indexing per Raft convention.
func (s *actorState) lastLogIndex() uint64 {
	return s.log.LastIndex()
}

// lastLogTerm returns the term of the last log entry, or 0 if the log is empty.
func (s *actorState) lastLogTerm() uint64 {
	last := s.log.LastIndex()
	if last == 0 {
		return 0
	}
	t, err := s.log.TermAt(last)
	if err != nil {
		panic("raftv2: lastLogTerm: " + err.Error())
	}
	return t
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
