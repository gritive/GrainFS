package raftv2

// command is the unit of work passed from public methods to the actor
// goroutine over cmdCh. The actor handles each command serially; reply (if
// any) is delivered via the embedded reply channel inside cmd.
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
}

type cmdKind int

const (
	cmdPropose cmdKind = iota
	cmdRequestVote
	cmdAppendEntries
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

	// Bootstrap: a single-voter cluster (Peers empty == only self) auto-
	// promotes to Leader at term 1. This is a PR 1 shortcut; real elections
	// arrive in PR 4-5. The condition follows v1's "Peers excludes self"
	// convention (internal/raft/raft.go:184).
	if len(n.cfg.Peers) == 0 {
		n.st.currentTerm = 1
		n.st.state = Leader
		n.st.leaderID = n.cfg.ID
		n.publish()
	}

	for {
		select {
		case <-n.stopCh:
			return
		case cmd := <-n.cmdCh:
			n.handle(cmd)
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
	}

	cmd.rvReply <- reply
}

// handleAppendEntries is the PR 4 stub. It replies Success=false for every
// request because log-replication semantics aren't implemented yet (PR 5+
// heartbeat, PR 6+ entry append). The stub still observes the term invariant:
// a higher-term AppendEntries forces a step-down so the actor's currentTerm
// and votedFor stay consistent with the rest of the cluster.
func (n *Node) handleAppendEntries(cmd command) {
	args := cmd.aeArgs

	if args.Term > n.st.currentTerm {
		n.st.currentTerm = args.Term
		n.st.state = Follower
		n.st.votedFor = ""
		n.st.leaderID = ""
		n.publish()
	}

	cmd.aeReply <- &AppendEntriesReply{
		Term:    n.st.currentTerm,
		Success: false,
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
