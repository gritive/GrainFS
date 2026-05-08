package raftv2

import "time"

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

	// cmdHeartbeatReply payload — outbound AppendEntries (heartbeat) reply.
	hbPeer    string
	hbTerm    uint64
	hbSuccess bool
	hbErr     error
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
	if args.Term > n.st.currentTerm {
		n.st.currentTerm = args.Term
		n.st.state = Follower
		n.st.votedFor = ""
		n.st.leaderID = ""
		n.publish()
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

// handleAppendEntries handles incoming AppendEntries RPCs. PR 5a accepts
// heartbeats (empty Entries with PrevLogIndex=0) so leaders can hold their
// term across the cluster; full log replication lands in PR 6. The handler
// covers three branches:
//
//  1. Stale term (args.Term < currentTerm) → reject without state change.
//  2. Higher term → step down, accept as the new leader, reset election timer.
//  3. Same term → recognise the leader, drop Candidate→Follower if needed,
//     reset election timer, accept the heartbeat.
//
// Real PrevLogIndex/PrevLogTerm matching is deferred to PR 6; PR 5a assumes
// PrevLogIndex=0 and Entries=[] so the log is trivially consistent.
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

	// Rule 2 (term advance): adopt the new term. Note we do NOT publish here —
	// the same-term path below will publish once leaderID is also set, so we
	// emit a single coherent snapshot rather than two intermediate ones.
	if args.Term > n.st.currentTerm {
		n.st.currentTerm = args.Term
		n.st.votedFor = ""
	}

	// Rule 3 (same/advanced term): recognise the leader. Candidate steps down,
	// Leader cannot occur here (impossible — same-term-different-leader would
	// violate Raft's election safety; if it does, treat as Follower).
	n.st.state = Follower
	n.st.leaderID = args.LeaderID
	n.publish()
	n.resetElectionTimer()

	// PR 5a: assume Entries=[] and PrevLogIndex=0 → log is trivially consistent.
	// Accept LeaderCommit (clamped to our log length, which is just our existing
	// commitIndex when no entries are appended). Real replication in PR 6 will
	// reuse this handler with proper PrevLog matching.
	cmd.aeReply <- &AppendEntriesReply{
		Term:    n.st.currentTerm,
		Success: true,
	}
}

// handlePropose appends the proposed command to the in-memory log, commits it
// (single-voter == self-quorum), publishes the new readState, sends the entry
// to ApplyCh, and replies to the waiter.
//
// PR 1 simplification: commit happens synchronously inside the actor. Once
// replication lands (PR 4-5), commit will lag the propose and a waiter map
// will gate the reply on the commit watermark.
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
	n.st.commitIndex = idx
	n.publish()

	// Deliver to FSM. Select on stopCh so Stop() never deadlocks against a
	// slow ApplyCh reader (mirrors v1 raft.go:707-711).
	select {
	case n.applyCh <- entry:
	case <-n.stopCh:
		// NOTE: if Stop wins this select, the entry is committed in-memory but
		// undelivered. Acceptable pre-persistence; future LogStore (PR 6+) will
		// re-deliver on restart by replaying log[applied+1..commitIndex].
		if cmd.proposeReply != nil {
			cmd.proposeReply <- proposalResult{err: ErrNodeStopped}
		}
		return
	}

	if cmd.proposeReply != nil {
		cmd.proposeReply <- proposalResult{index: idx}
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
	n.publish()
	n.resetElectionTimer()
}

// broadcastHeartbeat dispatches a heartbeat AppendEntries (Entries=[],
// PrevLogIndex=0) to every peer. Caller must already be Leader.
func (n *Node) broadcastHeartbeat() {
	args := &AppendEntriesArgs{
		Term:         n.st.currentTerm,
		LeaderID:     n.st.id,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: n.st.commitIndex,
	}
	for _, peer := range n.cfg.Peers {
		go n.sendHeartbeat(peer, args)
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

// sendHeartbeat is sendRequestVote's heartbeat counterpart.
func (n *Node) sendHeartbeat(peer string, args *AppendEntriesArgs) {
	if n.transport == nil {
		return
	}
	reply, err := n.transport.SendAppendEntries(peer, args)
	c := command{kind: cmdHeartbeatReply, hbPeer: peer, hbErr: err}
	if err == nil && reply != nil {
		c.hbTerm = reply.Term
		c.hbSuccess = reply.Success
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

// handleHeartbeatReply processes the reply from an outbound heartbeat. The
// only state-changing reaction is term advance → step down; PR 6 will use
// Success=false to drive log catch-up.
func (n *Node) handleHeartbeatReply(cmd command) {
	if cmd.hbErr != nil {
		return
	}
	if cmd.hbTerm > n.st.currentTerm {
		n.becomeFollower(cmd.hbTerm)
		return
	}
	// PR 5a: heartbeat-only path; nothing else to do on reply.
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
