package raft

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
	// config is a copy of the live effective configuration. Read by
	// Configuration() from any goroutine. Updated whenever the actor
	// publishes after a config-bearing log mutation.
	config effectiveConfig
}

// actorState is the mutable Raft state owned exclusively by the actor
// goroutine. No locking — single-writer by construction.
type actorState struct {
	id          string
	currentTerm uint64
	state       NodeState
	leaderID    string
	votedFor    string // candidate this node voted for in the current term ("" = none)

	// votesGranted is the set of voter IDs that granted a vote in the current
	// Candidate term, including self. Reset on becomeCandidate; only
	// meaningful while state == Candidate. Using a set (not a count) lets
	// joint-state elections check majority of Cold AND majority of Cnew
	// independently per Raft §4.3 — a single counter loses the per-voter
	// information needed for the joint check.
	votesGranted map[string]bool

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

	// peerInFlight tracks whether an AppendEntries goroutine is currently
	// in flight for each peer. Set to true when dispatchAppendEntries is
	// spawned, cleared when the reply (or error) returns via cmdHeartbeatReply.
	// Single-flight per peer prevents goroutine accumulation when a
	// partitioned/hung transport delays replies — without it, every heartbeat
	// tick spawns a new goroutine while old ones block on the dead transport.
	// Leader-only state — cleared in stepDownToFollower.
	peerInFlight map[string]bool

	// leaderRound increments on every broadcastHeartbeat dispatched by this
	// node while Leader. Used as the round identifier for ReadIndex
	// linearizability confirmation (Raft §6.4): a ReadIndex queued at round
	// R is satisfied when a majority of peers reply with hbRoundID >= R for
	// the current term. Reset to 0 in becomeLeader; irrelevant on step-down.
	leaderRound uint64

	// peerLastRound[peer] is the highest leaderRound the leader has confirmed
	// the peer received in the current term, advanced on successful AE reply
	// (gated by term match). Reset to a fresh map in becomeLeader, nil'd in
	// stepDownToFollower so a regained leadership cannot pre-satisfy a fresh
	// ReadIndex with stale evidence from a prior term.
	peerLastRound map[string]uint64

	// readIndexQueue holds pending ReadIndex requests awaiting heartbeat-round
	// confirmation from a majority of peers. FIFO ordered by submission;
	// drained with ErrProposalFailed on step-down. Leader-only.
	readIndexQueue []readIndexReq

	// currentConfig is the cluster's effective voter set per Raft §4.3 — the
	// configuration in the most recent log entry this server has appended,
	// even if not yet committed. Quorum, election, replication, and ReadIndex
	// all consult this rather than cfg.Peers (which is now seed-only at boot).
	currentConfig effectiveConfig

	// configHistory records (logIndex, prev) pairs in append order. On every
	// LogEntryConfChange / LogEntryJointConfChange append, the pre-transition
	// config is pushed; on a Rule 5a truncation past idx, entries with
	// logIndex > idx are popped and currentConfig reverts to the most recent
	// surviving prev (Raft §4.3 truncation revert).
	configHistory []configHistoryEntry

	// appendedConfigIndex is the index of the most recent config entry this
	// node has appended, or 0 when no config entry exists in the live log.
	// The leader uses this to refuse a fresh AddVoter / RemoveVoter while a
	// previous change is still in flight (mirrors hashicorp/raft pragmatic
	// behaviour: one in-flight membership change at a time).
	appendedConfigIndex uint64

	// leaderPeersScratch caches the peer set (every voter ID except self) for
	// hot-path leaders. Recomputed lazily by peerSet() when nil; invalidated
	// (set to nil) at every site that mutates currentConfig. broadcastHeartbeat
	// fires this every 50ms tick and handlePropose fires it per write — the
	// pre-cache implementation allocated a fresh []string each time, which
	// dominated steady-state allocations on a healthy leader. The slice is
	// read-only — callers MUST NOT mutate it.
	leaderPeersScratch []string

	// leaderReplicasScratch caches the replication-send set (voters ∪
	// learners − self) for the leader's broadcastHeartbeat. Mirrors
	// leaderPeersScratch but includes learners — used ONLY by send paths,
	// never by quorum math. Invalidated alongside leaderPeersScratch.
	leaderReplicasScratch []string

	// pendingConfChange holds the per-change state the leader needs to drive
	// a joint-consensus membership change to completion. When non-nil, a
	// joint entry is in flight and applyCommitted, on observing commit
	// past the joint index, must append the final LogEntryConfChange
	// entry. Cleared once the final entry commits and the caller's reply
	// has been delivered. Leader-only — cleared on stepDownToFollower.
	pendingConfChange *pendingConfChange

	// pendingSingleConf tracks a single-phase ConfChange (M6.0 Path B:
	// AddLearner, PromoteStage1, RemoveLearner). When the entry at idx
	// commits, deliver to reply and clear. Mutually exclusive with
	// pendingConfChange via the in-flight gate in handleAddLearner /
	// handlePromote. Leader-only — drained on stepDownToFollower.
	pendingSingleConf *pendingSingleConf

	// pendingPromote chains the two ordered entries of PromoteToVoter
	// (Path B). After stage 1 (drop-from-learners) commits, the actor
	// proposes the joint AddVoter for the same target ID. Stage 1 lives
	// in pendingSingleConf with a throwaway internal reply; the user's
	// reply lives here and is delivered when the subsequent joint
	// transition completes via pendingConfChange. Cleared on completion
	// or step-down.
	pendingPromote *pendingPromote
}

// pendingConfChange tracks an in-flight Raft §4.3 joint-consensus
// membership change. The leader sequences the change in two phases:
//
//	Phase 1: append LogEntryJointConfChange (Cold ∪ Cnew); wait for commit.
//	Phase 2: append LogEntryConfChange (Cnew alone); wait for commit.
//
// The actor allocates this on AddVoter / RemoveVoter, advances the phase
// when applyCommitted sees commitIndex pass jointIndex (then finalIndex
// is set), and replies to the caller (via reply) once the final entry
// commits.
type pendingConfChange struct {
	jointIndex uint64                // log index of the joint entry (always set)
	finalIndex uint64                // log index of the final ConfChange entry (set after phase 2 dispatched)
	newVoters  []string              // Cnew — used to build the final entry's payload
	reply      chan confChangeResult // single-reply channel; cap-1 buffered
}

// pendingSingleConf tracks a single-phase ConfChange entry (M6.0 Path B:
// AddLearner / PromoteStage1 / RemoveLearner). idx is the entry's log
// index; reply is the caller's channel (or a throwaway internal channel
// for the stage-1 leg of a PromoteToVoter chain).
type pendingSingleConf struct {
	idx   uint64
	reply chan confChangeResult
}

// pendingPromote chains the two-entry PromoteToVoter sequence (Path B).
// After the stage-1 entry (drop-from-learners) commits, the actor must
// propose a joint AddVoter for the same target. targetID + reply travel
// here until the joint flow takes over and delivers via pendingConfChange.
type pendingPromote struct {
	targetID string
	reply    chan confChangeResult
}

// confChangeResult is delivered to AddVoter / RemoveVoter callers once the
// membership change has finished committing both phases (or failed via
// step-down). Index is the final ConfChange entry's log index on success.
type confChangeResult struct {
	index uint64
	err   error
}

// configHistoryEntry records the config that was effective immediately
// BEFORE a config log entry at logIndex was appended. Popped on truncation
// so the post-truncate effective config equals whatever was active just
// before the dropped entry was appended.
type configHistoryEntry struct {
	logIndex uint64
	prev     effectiveConfig
}

// snapshot builds a readState reflecting the current actor-owned state.
func (s *actorState) snapshot() *readState {
	// Defensive-copy the effectiveConfig slices so the published readState
	// is fully detached from the actor-owned state. Reads from
	// Configuration() can then traverse the slices without locking.
	cfgCopy := effectiveConfig{joint: s.currentConfig.joint}
	if len(s.currentConfig.voters) > 0 {
		cfgCopy.voters = make([]string, len(s.currentConfig.voters))
		copy(cfgCopy.voters, s.currentConfig.voters)
	}
	if len(s.currentConfig.oldVoters) > 0 {
		cfgCopy.oldVoters = make([]string, len(s.currentConfig.oldVoters))
		copy(cfgCopy.oldVoters, s.currentConfig.oldVoters)
	}
	cfgCopy.learners = s.currentConfig.cloneLearners()
	return &readState{
		state:       s.state,
		term:        s.currentTerm,
		leaderID:    s.leaderID,
		commitIndex: s.commitIndex,
		isLeader:    s.state == Leader && s.leaderID == s.id && s.leaderID != "",
		votedFor:    s.votedFor,
		config:      cfgCopy,
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

// peerSet returns the live peer set (every voter ID except self) per the
// effective configuration. In joint state this is Cold ∪ Cnew minus self;
// in single state it is voters minus self.
//
// The returned slice is cached on actorState and reused across calls until
// the next currentConfig mutation invalidates it (invalidatePeerSet). It
// is READ-ONLY — callers MUST NOT mutate the slice or retain it past a
// config-changing call. The cache trades a per-config allocation for
// alloc-free hot paths (broadcastHeartbeat, handlePropose) which fire
// every heartbeat tick and every write respectively.
func (s *actorState) peerSet() []string {
	if s.leaderPeersScratch == nil {
		s.leaderPeersScratch = s.currentConfig.peersExcluding(s.id)
	}
	return s.leaderPeersScratch
}

// invalidatePeerSet clears the cached peer + replica slices so the next
// peerSet() / replicaSet() call rebuilds them from currentConfig. Must be
// called by every site that mutates currentConfig OR learners.
func (s *actorState) invalidatePeerSet() {
	s.leaderPeersScratch = nil
	s.leaderReplicasScratch = nil
}

// replicaSet returns the leader's replication-send set: voters (Cold ∪
// Cnew when joint) plus learners, minus self. Used by broadcastHeartbeat
// and dispatchAppendEntries — NEVER by quorum math (commitOK, quorumOK,
// quorumOKByRound iterate only over voter slices). Cached and invalidated
// alongside leaderPeersScratch. Read-only: callers must NOT mutate.
func (s *actorState) replicaSet() []string {
	if s.leaderReplicasScratch == nil {
		s.leaderReplicasScratch = s.currentConfig.replicasExcluding(s.id)
	}
	return s.leaderReplicasScratch
}

// isSoloVoter reports whether the cluster reduces to {self} only — used by
// the single-voter shortcut paths (auto-promote on bootstrap, inline commit
// on propose, skip heartbeat ticker, inline ReadIndex). After membership
// changes this can flip true → false (1→2 voter add) or false → true
// (final voter step-down), so callers MUST consult it via this helper
// rather than caching.
func (s *actorState) isSoloVoter() bool {
	if s.currentConfig.joint {
		return false
	}
	v := s.currentConfig.voters
	return len(v) == 1 && v[0] == s.id
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
