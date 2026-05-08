package raftv2

import (
	"sort"
	"time"
)

// defaultMaxEntriesPerAE is the fallback cap for AppendEntries payload size when
// cfg.MaxEntriesPerAE is zero. Mirrors v1's DefaultConfig value
// (internal/raft/raft.go:212).
const defaultMaxEntriesPerAE = 512

// command is the unit of work passed from public methods (and outbound RPC
// reply goroutines) to the actor goroutine over cmdCh. The actor handles
// each command serially; reply (if any) is delivered via the embedded reply
// channel inside cmd.
type command struct {
	kind cmdKind

	// proposeCmd payload (kind == cmdPropose).
	proposeCommand []byte
	proposeType    LogEntryType
	proposeReply   chan proposalResult

	// RequestVote payload (kind == cmdRequestVote).
	rvArgs  *RequestVoteArgs
	rvReply chan *RequestVoteReply

	// AppendEntries payload (kind == cmdAppendEntries).
	aeArgs  *AppendEntriesArgs
	aeReply chan *AppendEntriesReply

	// cmdVoteReply payload — outbound RequestVote reply collected by a
	// per-call sender goroutine.
	vrPeer    string
	vrTerm    uint64
	vrGranted bool
	vrErr     error

	// cmdHeartbeatReply payload — outbound AppendEntries reply (the same kind
	// covers heartbeats and entry-bearing AEs since heartbeats are just AEs
	// with empty Entries). hbMatchAfter = PrevLogIndex + len(Entries) sent;
	// the leader uses it to advance matchIndex[peer] on success.
	// hbConflictTerm/hbConflictIndex carry the follower's conflict hint on
	// failure (PR 6b); zero when unset.
	hbPeer          string
	hbTerm          uint64
	hbSuccess       bool
	hbMatchAfter    uint64
	hbConflictTerm  uint64
	hbConflictIndex uint64
	hbErr           error
}

type cmdKind int

const (
	cmdPropose cmdKind = iota
	cmdRequestVote
	cmdAppendEntries
	cmdVoteReply
	cmdHeartbeatReply
)

// proposalResult is delivered to ProposeWait callers when their entry commits
// (or fails). PR 1 single-node always succeeds synchronously.
type proposalResult struct {
	index uint64
	err   error
}

// run is the actor goroutine. It is the sole writer of actorState and the
// sole publisher of readState. All state transitions flow through this loop.
func (n *Node) run() {
	defer close(n.doneCh)
	defer close(n.applyCh) // safe: actor is the sole sender; closing signals consumers.
	defer func() {
		// Tear down timers from the actor goroutine to avoid racing concurrent
		// Resets. Drain non-blockingly; the timer may already be drained.
		if !n.electionTimer.Stop() {
			select {
			case <-n.electionTimer.C:
			default:
			}
		}
		if n.heartbeatTicker != nil {
			n.heartbeatTicker.Stop()
			n.heartbeatTicker = nil
		}
	}()

	// Bootstrap: a single-voter cluster (Peers empty == only self) auto-
	// promotes to Leader at term 1 without running an election. Multi-voter
	// Nodes start as Follower and rely on the election timer firing.
	// Convention follows v1's "Peers excludes self" (internal/raft/raft.go:184).
	if len(n.cfg.Peers) == 0 {
		n.st.currentTerm = 1
		n.st.state = Leader
		n.st.leaderID = n.cfg.ID
		n.publish()
		// Park the election timer. Single-voter Leader sends no heartbeats —
		// PR 5a does not enable replication for the single-voter path; that
		// stays the synchronous self-quorum path inherited from PR 1.
		n.electionTimer.Reset(idleElectionTimeout)
	} else {
		// Multi-voter Follower: arm the election timer so a missing leader
		// triggers a Candidate transition.
		n.resetElectionTimer()
	}

	for {
		var heartbeatCh <-chan time.Time
		if n.heartbeatTicker != nil {
			heartbeatCh = n.heartbeatTicker.C
		}
		select {
		case <-n.stopCh:
			return
		case cmd := <-n.cmdCh:
			n.handle(cmd)
		case <-n.electionTimer.C:
			n.onElectionTimeout()
		case <-heartbeatCh:
			n.onHeartbeatTick()
		}
	}
}

// handle dispatches a command to its handler. Adding new command kinds means
// adding a case here and a public method that enqueues it.
func (n *Node) handle(cmd command) {
	switch cmd.kind {
	case cmdPropose:
		n.handlePropose(cmd)
	case cmdRequestVote:
		n.handleRequestVote(cmd)
	case cmdAppendEntries:
		n.handleAppendEntries(cmd)
	case cmdVoteReply:
		n.handleVoteReply(cmd)
	case cmdHeartbeatReply:
		n.handleHeartbeatReply(cmd)
	}
}

// handleRequestVote implements the receiver side of Raft §5.4 vote granting.
// Runs inside the actor goroutine; it is the sole writer to currentTerm,
// votedFor, state, and leaderID.
//
// PR 4 simplification: pre-vote (args.PreVote), leader-transfer
// (args.LeaderTransfer), leader stickiness (lastLeaderContact), and learner
// handling are deferred to PR 5+. We mirror only the core §5.4 path:
//  1. Stale term → deny.
//  2. Higher term → step down (currentTerm = args.Term, state = Follower,
//     votedFor cleared, leaderID cleared) before evaluating the vote.
//  3. Grant if (votedFor == "" || votedFor == args.CandidateID) AND the
//     candidate's log is at least as up-to-date as ours.
func (n *Node) handleRequestVote(cmd command) {
	args := cmd.rvArgs
	reply := &RequestVoteReply{Term: n.st.currentTerm}

	// Rule 1: deny stale term.
	if args.Term < n.st.currentTerm {
		cmd.rvReply <- reply
		return
	}

	// Rule 2: step down on higher term. Clears any vote from the prior term.
	// If we are Leader, route through becomeFollower so leader-only state
	// (matchIndex, nextIndex, proposeWaiters, heartbeat ticker) is torn down
	// — leaving outstanding ProposeWait callers hanging is a correctness bug.
	if args.Term > n.st.currentTerm {
		if n.st.state == Leader {
			n.becomeFollower(args.Term)
		} else {
			n.st.currentTerm = args.Term
			n.st.state = Follower
			n.st.votedFor = ""
			n.st.leaderID = ""
			n.publish()
		}
		reply.Term = n.st.currentTerm
	}

	// Rule 3: grant if we haven't voted (or already voted for this candidate)
	// and the candidate's log is at least as up-to-date as ours.
	if (n.st.votedFor == "" || n.st.votedFor == args.CandidateID) &&
		n.st.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		n.st.votedFor = args.CandidateID
		n.publish()
		reply.VoteGranted = true
		// Mirror v1's signalReset (raft.go:1807): granting a vote counts as
		// "current leader contact" for election timer purposes. Without this
		// reset, a slow candidate could lose its own election to our timeout.
		n.resetElectionTimer()
	}

	cmd.rvReply <- reply
}

// handleAppendEntries handles incoming AppendEntries RPCs. Implements the
// §5.3 path including conflict resolution:
//
//  1. Stale term (args.Term < currentTerm) → reject without state change.
//  2. Higher term → step down, adopt the new term.
//  3. Recognise the leader (Candidate→Follower if needed), reset election timer.
//  4. Validate PrevLogIndex/PrevLogTerm; on mismatch, populate
//     ConflictTerm/ConflictIndex hints so the leader can back off by an entire
//     conflicting term in a single round-trip (Raft §5.3 optimisation).
//  5. Append Entries, truncating any conflicting suffix on the way; advance
//     commitIndex = min(LeaderCommit, lastLogIndex); deliver newly-committed
//     entries on ApplyCh.
//
// PR 6b note on truncation policy: we only truncate on accept (Rule 5a),
// matching the plan. v1 (raft.go:1860) also truncates eagerly on Rule 4
// rejection, which converges one round faster in some scenarios. Both are
// correct; the accept-only path is simpler and the leader's nextIndex will
// drive the next AE to a matching prefix anyway.
func (n *Node) handleAppendEntries(cmd command) {
	args := cmd.aeArgs

	// Rule 1: stale leader → reject. Reply with our higher term so the sender
	// learns it is no longer leader.
	if args.Term < n.st.currentTerm {
		cmd.aeReply <- &AppendEntriesReply{
			Term:    n.st.currentTerm,
			Success: false,
		}
		return
	}

	// Rule 2 (term advance): adopt the new term. If we are Leader, route
	// through becomeFollower so leader-only state (matchIndex, nextIndex,
	// proposeWaiters, heartbeat ticker) is torn down — otherwise outstanding
	// ProposeWait callers would hang until ctx timeout. Note we do NOT
	// publish in the inline path; Rule 3 below publishes once leaderID is
	// also set, emitting a single coherent snapshot.
	if args.Term > n.st.currentTerm {
		if n.st.state == Leader {
			n.becomeFollower(args.Term)
			// becomeFollower already published with leaderID="" and rearmed
			// the election timer; Rule 3's leaderID set + publish overrides
			// that with the real leader.
		} else {
			n.st.currentTerm = args.Term
			n.st.votedFor = ""
		}
	}

	// Rule 3 (same/advanced term): recognise the leader. Candidate steps down,
	// Leader cannot occur here (impossible — same-term-different-leader would
	// violate Raft's election safety; if it does, treat as Follower).
	n.st.state = Follower
	n.st.leaderID = args.LeaderID
	n.resetElectionTimer()

	// Rule 4: log consistency check. On mismatch, populate the conflict hint
	// so the leader can skip the entire offending term in one round-trip
	// (Raft §5.3 optimisation; mirrors v1 raft.go:1843-1862).
	if args.PrevLogIndex > n.st.lastLogIndex() {
		// Case 1: follower's log too short. Hint ConflictTerm=0,
		// ConflictIndex=lastLogIndex+1 so the leader jumps directly to where
		// our log ends.
		n.publish()
		cmd.aeReply <- &AppendEntriesReply{
			Term:          n.st.currentTerm,
			Success:       false,
			ConflictTerm:  0,
			ConflictIndex: n.st.lastLogIndex() + 1,
		}
		return
	}
	if args.PrevLogIndex > 0 && n.st.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		// Case 2: term mismatch at PrevLogIndex. Walk back to the first index
		// of the conflicting term so the leader can decide to skip the whole
		// term (if it has it) or jump to that boundary (if it doesn't).
		conflictTerm := n.st.log[args.PrevLogIndex-1].Term
		conflictIdx := args.PrevLogIndex
		// Logical index conflictIdx maps to slice index conflictIdx-1.
		// We're walking back while the *previous* logical entry (conflictIdx-1)
		// also has conflictTerm; that's slice index conflictIdx-2.
		for conflictIdx > 1 && n.st.log[conflictIdx-2].Term == conflictTerm {
			conflictIdx--
		}
		n.publish()
		cmd.aeReply <- &AppendEntriesReply{
			Term:          n.st.currentTerm,
			Success:       false,
			ConflictTerm:  conflictTerm,
			ConflictIndex: conflictIdx,
		}
		return
	}

	// Rule 5a: append entries with conflict-aware truncation. Walk the entries;
	// for each, if the follower already has an entry at that logical index AND
	// the term differs, truncate from that index onward and append the rest.
	// If the term matches, the entry is already correct (skip — duplicate
	// retry). If we run past lastLogIndex, append.
	//
	// Log Matching guard (§5.3 / M2 correctness blocker): each entry must carry
	// the expected logical index. A mismatch indicates a buggy or byzantine
	// leader; reject immediately and force resync via ConflictIndex.
	for i, e := range args.Entries {
		target := args.PrevLogIndex + uint64(i) + 1
		if e.Index != target {
			// Entry index disagrees with the expected logical position.
			// Reject without mutating the log; leader will retry.
			n.publish()
			cmd.aeReply <- &AppendEntriesReply{
				Term:          n.st.currentTerm,
				Success:       false,
				ConflictIndex: target,
				ConflictTerm:  0, // force leader to resync from target
			}
			return
		}
		if target <= n.st.lastLogIndex() {
			if n.st.log[target-1].Term != e.Term {
				// Conflict at logical index target. Truncate and break — the
				// remaining loop iterations would walk indices we just dropped.
				n.st.log = n.st.log[:target-1]
				n.st.log = append(n.st.log, args.Entries[i:]...)
				break
			}
			// Same term/index → same entry by Raft Log Matching. Skip.
			continue
		}
		// target > lastLogIndex: pure extension. Append remaining entries in
		// one shot and break out of the loop.
		n.st.log = append(n.st.log, args.Entries[i:]...)
		break
	}

	// Rule 5b: advance commitIndex and deliver newly-committed entries.
	if args.LeaderCommit > n.st.commitIndex {
		newCommit := args.LeaderCommit
		if last := n.st.lastLogIndex(); newCommit > last {
			newCommit = last
		}
		if newCommit > n.st.commitIndex {
			n.applyCommitted(n.st.commitIndex, newCommit)
		}
	}

	n.publish()
	cmd.aeReply <- &AppendEntriesReply{
		Term:    n.st.currentTerm,
		Success: true,
	}
}

// applyCommitted advances commitIndex from oldCommit to newCommit, sending
// each newly-committed entry on applyCh and resolving any matching
// proposeWaiters. Caller is the actor goroutine. Both leader and follower
// reuse this path; the only difference is that leaders also have waiters to
// drain, while followers (proposeWaiters always nil for non-Leader) skip.
func (n *Node) applyCommitted(oldCommit, newCommit uint64) {
	for i := oldCommit + 1; i <= newCommit; i++ {
		entry := n.st.log[i-1]
		select {
		case n.applyCh <- entry:
		case <-n.stopCh:
			// Stop won the race — leave commitIndex at the highest value we
			// successfully delivered. Mirrors the single-voter path's behaviour
			// at handlePropose.
			n.st.commitIndex = i - 1
			return
		}
		if w, ok := n.st.proposeWaiters[i]; ok {
			delete(n.st.proposeWaiters, i)
			// Buffered cap-1 reply chan (propose.go:39) — non-blocking.
			select {
			case w <- proposalResult{index: i}:
			default:
			}
		}
	}
	n.st.commitIndex = newCommit
}

// handlePropose appends the proposed command to the in-memory log and either
// (single-voter) commits + applies inline, or (multi-voter) registers a
// waiter and dispatches AppendEntries to every peer.
//
// Single-voter shortcut: with no peers, self-quorum is met immediately, so we
// keep the PR 1 inline-commit path. Multi-voter callers wait for a majority of
// matchIndex to reach the new entry's index before applyCommitted resolves
// their proposeWaiter.
func (n *Node) handlePropose(cmd command) {
	if n.st.state != Leader {
		if cmd.proposeReply != nil {
			cmd.proposeReply <- proposalResult{err: ErrNotLeader}
		}
		return
	}

	idx := uint64(len(n.st.log)) + 1
	entry := LogEntry{
		Term:    n.st.currentTerm,
		Index:   idx,
		Command: cmd.proposeCommand,
		Type:    cmd.proposeType,
	}
	n.st.log = append(n.st.log, entry)

	// Single-voter shortcut: self-quorum is met by appending.
	if len(n.cfg.Peers) == 0 {
		n.st.commitIndex = idx
		n.publish()
		// Deliver to FSM. Select on stopCh so Stop() never deadlocks against a
		// slow ApplyCh reader (mirrors v1 raft.go:707-711).
		select {
		case n.applyCh <- entry:
		case <-n.stopCh:
			// If Stop wins, the entry is committed in-memory but undelivered.
			// Acceptable pre-persistence; future LogStore (PR 6+) will re-
			// deliver on restart by replaying log[applied+1..commitIndex].
			if cmd.proposeReply != nil {
				cmd.proposeReply <- proposalResult{err: ErrNodeStopped}
			}
			return
		}
		if cmd.proposeReply != nil {
			cmd.proposeReply <- proposalResult{index: idx}
		}
		return
	}

	// Multi-voter: register the waiter BEFORE dispatching senders so a fast
	// follower's reply (synchronous through memTransport) cannot race ahead of
	// the waiter map.
	if cmd.proposeReply != nil {
		if n.st.proposeWaiters == nil {
			n.st.proposeWaiters = make(map[uint64]chan proposalResult)
		}
		n.st.proposeWaiters[idx] = cmd.proposeReply
	}
	n.publish()

	for _, peer := range n.cfg.Peers {
		args := n.buildAppendEntriesArgs(peer)
		go n.dispatchAppendEntries(peer, args)
	}
}

// publish stores a fresh readState snapshot for hot-path readers. Called by
// the actor after every state mutation.
func (n *Node) publish() {
	n.rs.Store(n.st.snapshot())
}

// onElectionTimeout fires when the election timer elapses while we are
// Follower or Candidate. Triggers a fresh Candidate transition (which itself
// resets the timer for the next election attempt).
func (n *Node) onElectionTimeout() {
	if n.st.state == Leader {
		// Should not happen — Leader parks the election timer. Defensive.
		n.resetElectionTimer()
		return
	}
	n.becomeCandidate()
}

// onHeartbeatTick fires while we are Leader. Sends a heartbeat AppendEntries
// to every peer. Tick channel is nil while not Leader (ticker not allocated),
// so this is unreachable outside the Leader state.
func (n *Node) onHeartbeatTick() {
	if n.st.state != Leader {
		return
	}
	n.broadcastHeartbeat()
}

// becomeCandidate runs the §5.2 transition: bump term, vote for self, switch
// state, publish, reset timer, dispatch RequestVote to every peer. Order
// matters — the timer reset must precede goroutine dispatch so a
// fast-arriving reply cannot race ahead of the timer arm.
func (n *Node) becomeCandidate() {
	n.st.currentTerm++
	n.st.state = Candidate
	n.st.votedFor = n.st.id
	n.st.leaderID = ""
	n.st.votesGranted = 1 // self-vote
	n.publish()
	n.resetElectionTimer()

	// Snapshot what we send so each goroutine has its own consistent args.
	args := &RequestVoteArgs{
		Term:         n.st.currentTerm,
		CandidateID:  n.st.id,
		LastLogIndex: n.st.lastLogIndex(),
		LastLogTerm:  n.st.lastLogTerm(),
	}

	// Single-voter Candidate is impossible under the bootstrap path (single-
	// voter starts at Leader). Multi-voter dispatch:
	for _, peer := range n.cfg.Peers {
		go n.sendRequestVote(peer, args)
	}

	// Edge case: if we have already met quorum (only happens when the cluster
	// is misconfigured to a single-voter via Peers=[]; the bootstrap path
	// above already covers that, so this branch is theoretical). Keep it
	// explicit for safety.
	if n.hasMajority(n.st.votesGranted) {
		n.becomeLeader()
	}
}

// becomeLeader runs the §5.2 leader transition: state, leaderID, publish,
// stop the election timer (Leader has none), start the heartbeat ticker, and
// fire an immediate heartbeat so followers don't time out waiting for the
// first tick.
func (n *Node) becomeLeader() {
	n.st.state = Leader
	n.st.leaderID = n.st.id

	// Raft §5.4.2 no-op: append a blank entry at the new term immediately on
	// election so that maybeAdvanceCommitIndex can satisfy the term gate
	// (log[N].Term == currentTerm) without waiting for a client request.
	// This also causes prior-term uncommitted entries to be committed as a
	// side effect once the no-op's commit advances the commit index. FSM
	// consumers MUST ignore LogEntryNoOp entries (Command is always nil).
	noOpIdx := n.st.lastLogIndex() + 1
	n.st.log = append(n.st.log, LogEntry{
		Term:  n.st.currentTerm,
		Index: noOpIdx,
		Type:  LogEntryNoOp,
	})

	// Initialise per-peer replication state per Raft §5.3: nextIndex starts
	// at lastLogIndex+1 (optimistic — assume peer is in sync), matchIndex
	// starts at 0 (we've confirmed nothing yet). Set after no-op append so
	// nextIndex[peer] accounts for the no-op.
	last := n.st.lastLogIndex()
	n.st.matchIndex = make(map[string]uint64, len(n.cfg.Peers))
	n.st.nextIndex = make(map[string]uint64, len(n.cfg.Peers))
	for _, peer := range n.cfg.Peers {
		n.st.matchIndex[peer] = 0
		n.st.nextIndex[peer] = last + 1
	}

	n.publish()

	// Park the election timer — Leader does not hold one.
	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.electionTimer.Reset(idleElectionTimeout)

	// Start heartbeat ticker.
	interval := n.cfg.HeartbeatTimeout
	if interval <= 0 {
		interval = defaultHeartbeatTimeout
	}
	n.heartbeatTicker = time.NewTicker(interval)

	// Immediate heartbeat — followers learn of the new leader without waiting
	// a full interval, and our election timer reset propagates to them.
	n.broadcastHeartbeat()
}

// becomeFollower steps down to Follower at the given term. votedFor and
// leaderID are cleared because the term advanced; if the caller already
// knows the new leader they should set leaderID after this call. Heartbeat
// ticker (if any) is stopped and the election timer is rearmed.
//
// Leader-only state (matchIndex, nextIndex, proposeWaiters) is cleared.
// Outstanding waiters receive ErrProposalFailed since their entries' commit
// fates are no longer this node's responsibility.
func (n *Node) becomeFollower(term uint64) {
	n.st.currentTerm = term
	n.st.state = Follower
	n.st.votedFor = ""
	n.st.leaderID = ""
	n.st.votesGranted = 0
	if n.heartbeatTicker != nil {
		n.heartbeatTicker.Stop()
		n.heartbeatTicker = nil
	}
	// Clear leader-only replication state. Reply ErrProposalFailed to any
	// waiter still attached so callers don't block forever. Sends are non-
	// blocking (cap-1 buffered chan; one send guaranteed to succeed since the
	// chan was created fresh per ProposeWait at propose.go:39).
	for idx, w := range n.st.proposeWaiters {
		select {
		case w <- proposalResult{err: ErrProposalFailed}:
		default:
		}
		delete(n.st.proposeWaiters, idx)
	}
	n.st.matchIndex = nil
	n.st.nextIndex = nil
	n.st.proposeWaiters = nil
	n.publish()
	n.resetElectionTimer()
}

// broadcastHeartbeat dispatches an AppendEntries to every peer. PR 6a unifies
// the heartbeat and replication paths: each per-peer message carries entries
// log[nextIndex[peer]..lastLogIndex] (possibly empty when the peer is fully
// caught up — that's the heartbeat case). Caller must already be Leader.
func (n *Node) broadcastHeartbeat() {
	for _, peer := range n.cfg.Peers {
		args := n.buildAppendEntriesArgs(peer)
		go n.dispatchAppendEntries(peer, args)
	}
}

// buildAppendEntriesArgs constructs the AppendEntriesArgs for peer based on
// current actor state. Caller must be inside the actor goroutine. PR 6b copies
// the entries slice rather than aliasing n.st.log — the follower-side
// truncation path may now mutate the leader's own log when the leader steps
// down, and an in-flight dispatch goroutine reading the aliased slice would
// observe a torn view. The copy cost is bounded by MaxEntriesPerAE.
func (n *Node) buildAppendEntriesArgs(peer string) *AppendEntriesArgs {
	next := n.st.nextIndex[peer]
	if next < 1 {
		next = 1 // defensive: nextIndex is 1-based
	}
	prevIdx := next - 1
	prevTerm := n.st.lastLogTermAt(prevIdx)
	var entries []LogEntry
	if last := n.st.lastLogIndex(); last >= next {
		// Cap the batch to cfg.MaxEntriesPerAE so we never attempt to ship the
		// entire log in a single RPC (e.g. fresh follower with a 1 GB log).
		// Mirrors v1 (internal/raft/raft.go:1511-1512). Default: 512.
		maxAE := n.cfg.MaxEntriesPerAE
		if maxAE == 0 {
			maxAE = defaultMaxEntriesPerAE
		}
		// Copy to defend against concurrent log truncation (see PR 6b note above).
		src := n.st.log[next-1:]
		if uint64(len(src)) > maxAE {
			src = src[:maxAE]
		}
		entries = make([]LogEntry, len(src))
		copy(entries, src)
	}
	return &AppendEntriesArgs{
		Term:         n.st.currentTerm,
		LeaderID:     n.st.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.st.commitIndex,
	}
}

// sendRequestVote runs in a per-call goroutine; it dispatches the outbound
// RPC and feeds the reply (or error) back through cmdCh as a cmdVoteReply.
// The select-on-stopCh guard prevents a leak when the actor exits while a
// transport call is in flight.
func (n *Node) sendRequestVote(peer string, args *RequestVoteArgs) {
	if n.transport == nil {
		return
	}
	reply, err := n.transport.SendRequestVote(peer, args)
	c := command{kind: cmdVoteReply, vrPeer: peer, vrErr: err}
	if err == nil && reply != nil {
		c.vrTerm = reply.Term
		c.vrGranted = reply.VoteGranted
	}
	select {
	case n.cmdCh <- c:
	case <-n.stopCh:
	}
}

// dispatchAppendEntries runs in a per-call goroutine; it dispatches the
// outbound AppendEntries RPC and ships the reply back through cmdCh. The
// matchAfter value (= PrevLogIndex + len(Entries)) is computed locally from
// args so the actor's reply handler can advance matchIndex without rebuilding
// the per-peer view. Select-on-stopCh prevents goroutine leaks when the
// actor exits while a transport call is in flight.
func (n *Node) dispatchAppendEntries(peer string, args *AppendEntriesArgs) {
	if n.transport == nil {
		return
	}
	matchAfter := args.PrevLogIndex + uint64(len(args.Entries))
	reply, err := n.transport.SendAppendEntries(peer, args)
	c := command{
		kind:         cmdHeartbeatReply,
		hbPeer:       peer,
		hbErr:        err,
		hbMatchAfter: matchAfter,
	}
	if err == nil && reply != nil {
		c.hbTerm = reply.Term
		c.hbSuccess = reply.Success
		c.hbConflictTerm = reply.ConflictTerm
		c.hbConflictIndex = reply.ConflictIndex
	}
	select {
	case n.cmdCh <- c:
	case <-n.stopCh:
	}
}

// handleVoteReply tallies a vote reply for the current Candidate term. Replies
// from prior terms or arriving after step-down are dropped (stale-reply guard).
// A reply whose Term exceeds ours forces a step-down.
func (n *Node) handleVoteReply(cmd command) {
	if cmd.vrErr != nil {
		return // transport failure; election timer will retry on timeout
	}
	if cmd.vrTerm > n.st.currentTerm {
		n.becomeFollower(cmd.vrTerm)
		return
	}
	// Stale-reply guard: only count if we're still the Candidate at this term.
	if n.st.state != Candidate || cmd.vrTerm != n.st.currentTerm {
		return
	}
	if !cmd.vrGranted {
		return
	}
	n.st.votesGranted++
	if n.hasMajority(n.st.votesGranted) {
		n.becomeLeader()
	}
}

// handleHeartbeatReply processes the reply from an outbound AppendEntries
// (heartbeat or entry-bearing). Leader logic:
//   - Higher reply.Term → step down.
//   - Stale reply (we're no longer Leader, or reply for a prior term) → drop.
//   - Success → advance matchIndex/nextIndex for peer; recompute commitIndex
//     and apply newly-committed entries.
//   - Failure with conflict hint → roll nextIndex back to skip the entire
//     conflicting term in one round-trip (Raft §5.3 optimisation; mirrors v1
//     applyConflictHint at raft.go:1561-1594).
//   - Failure without hint → conservative decrement-by-one (legacy peers).
func (n *Node) handleHeartbeatReply(cmd command) {
	if cmd.hbErr != nil {
		return
	}
	if cmd.hbTerm > n.st.currentTerm {
		n.becomeFollower(cmd.hbTerm)
		return
	}
	// Stale-reply guard: only act if we're still Leader at the term the reply
	// was generated for.
	if n.st.state != Leader || cmd.hbTerm != n.st.currentTerm {
		return
	}

	if cmd.hbSuccess {
		if cmd.hbMatchAfter > n.st.matchIndex[cmd.hbPeer] {
			n.st.matchIndex[cmd.hbPeer] = cmd.hbMatchAfter
		}
		n.st.nextIndex[cmd.hbPeer] = n.st.matchIndex[cmd.hbPeer] + 1
		n.maybeAdvanceCommitIndex()
		return
	}

	// Failure path: apply conflict hint if present.
	n.applyConflictHint(cmd)
}

// applyConflictHint rolls nextIndex[peer] back according to the follower's
// ConflictTerm/ConflictIndex hint. Caller must be Leader, and the reply must
// already be confirmed same-term (handleHeartbeatReply gates on this).
//
// Three cases (mirrors v1 raft.go:1561-1594):
//
//  1. ConflictIndex == 0  → legacy peer with no hint. Decrement nextIndex by
//     one entry, clamped to 1.
//  2. ConflictTerm == 0   → follower's log is too short. Jump nextIndex
//     directly to ConflictIndex (= follower's lastLogIndex+1).
//  3. ConflictTerm  > 0   → follower's log has a different term at
//     PrevLogIndex. Scan our log backwards: if we have ConflictTerm, set
//     nextIndex past our last entry of that term (skip our slice of the bad
//     term in one shot). If we don't, fall back to ConflictIndex.
func (n *Node) applyConflictHint(cmd command) {
	if cmd.hbConflictIndex == 0 {
		// Legacy peer: no hint. Conservative decrement-by-one.
		if n.st.nextIndex[cmd.hbPeer] > 1 {
			n.st.nextIndex[cmd.hbPeer]--
		}
		return
	}
	if cmd.hbConflictTerm == 0 {
		// Follower too short: jump directly.
		n.st.nextIndex[cmd.hbPeer] = cmd.hbConflictIndex
		if n.st.nextIndex[cmd.hbPeer] < 1 {
			n.st.nextIndex[cmd.hbPeer] = 1
		}
		return
	}
	// Term-mismatch case: binary-search our log for the last entry at
	// ConflictTerm. Raft terms are monotonically non-decreasing in the leader's
	// log (leader-driven append + truncate-on-conflict preserves this invariant),
	// so binary search is correct.
	//
	// Strategy: find the first index where term > ConflictTerm; the entry just
	// before it (if its term == ConflictTerm) is the rightmost match.
	// This replaces the O(N) backward scan with O(log N) — critical for large
	// logs where a stale follower's reject would otherwise block the actor.
	newNext := cmd.hbConflictIndex
	log := n.st.log
	if len(log) > 0 {
		// firstGT = first slice index where term > ConflictTerm.
		firstGT := sort.Search(len(log), func(i int) bool {
			return log[i].Term > cmd.hbConflictTerm
		})
		// firstGT-1 is the last index with term <= ConflictTerm.
		if firstGT > 0 && log[firstGT-1].Term == cmd.hbConflictTerm {
			// Leader has ConflictTerm; advance past its last entry.
			newNext = uint64(firstGT) + 1 // logical index = firstGT+1, nextIndex = logical+1
		}
		// else: either firstGT==0 (all entries > ConflictTerm, leader never had
		// it) or firstGT-1 has a term < ConflictTerm (same conclusion). Fall
		// back to hbConflictIndex (already set above).
	}
	if newNext < 1 {
		newNext = 1
	}
	n.st.nextIndex[cmd.hbPeer] = newNext
}

// maybeAdvanceCommitIndex finds the highest log index N such that a majority
// of voters (self + peers) have matchIndex >= N AND log[N].Term ==
// currentTerm. The term gate is Raft §5.4.2 — leaders may not commit entries
// from prior terms by counting replicas alone. Caller must be Leader.
func (n *Node) maybeAdvanceCommitIndex() {
	last := n.st.lastLogIndex()
	if last <= n.st.commitIndex {
		return
	}
	// Walk from highest candidate down; first one that meets quorum + term
	// gate becomes the new commitIndex.
	newCommit := n.st.commitIndex
	for N := last; N > n.st.commitIndex; N-- {
		// §5.4.2 term gate: only commit entries from the current term directly.
		if n.st.log[N-1].Term != n.st.currentTerm {
			continue
		}
		// Self counts as matched at lastLogIndex (we definitionally have N
		// since N <= last). Quorum threshold: floor(total/2)+1.
		count := 1
		for _, peer := range n.cfg.Peers {
			if n.st.matchIndex[peer] >= N {
				count++
			}
		}
		if n.hasMajority(count) {
			newCommit = N
			break
		}
	}
	if newCommit > n.st.commitIndex {
		n.applyCommitted(n.st.commitIndex, newCommit)
		n.publish()
	}
}

// resetElectionTimer reschedules the election timer with a fresh randomized
// timeout in [base, 2*base). Caller must be inside the actor goroutine.
func (n *Node) resetElectionTimer() {
	base := n.cfg.ElectionTimeout
	if base <= 0 {
		base = defaultElectionTimeout
	}
	jitter := time.Duration(n.rng.Int63n(int64(base)))
	timeout := base + jitter

	if !n.electionTimer.Stop() {
		select {
		case <-n.electionTimer.C:
		default:
		}
	}
	n.electionTimer.Reset(timeout)
}

// hasMajority reports whether n votes constitutes a majority of the
// configured cluster. Total voters = len(Peers)+1 (Peers excludes self per
// v1 convention). Majority = floor(total/2)+1.
func (n *Node) hasMajority(votes int) bool {
	total := len(n.cfg.Peers) + 1
	return votes >= total/2+1
}
