package raftv2

import (
	"fmt"
	"sort"
	"time"
)

// defaultMaxEntriesPerAE is the fallback cap for AppendEntries payload size when
// cfg.MaxEntriesPerAE is zero. Mirrors v1's DefaultConfig value
// (internal/raft/raft.go:212).
const defaultMaxEntriesPerAE = 512

// mustEntry retrieves the log entry at 1-based logical index idx from the
// actor's log. Panics if the index is out of range — log indexing bugs are
// programmer errors, not recoverable runtime conditions.
func (n *Node) mustEntry(idx uint64) LogEntry {
	e, err := n.st.log.Entry(idx)
	if err != nil {
		panic("raftv2: mustEntry: " + err.Error())
	}
	return e
}

// mustTermAt retrieves the term of the entry at 1-based logical index idx.
// Returns 0 for idx==0 (Raft sentinel). Panics on out-of-range access.
func (n *Node) mustTermAt(idx uint64) uint64 {
	t, err := n.st.log.TermAt(idx)
	if err != nil {
		panic("raftv2: mustTermAt: " + err.Error())
	}
	return t
}

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
	// hbDispatchTerm captures args.Term at dispatch time so handleHeartbeatReply
	// can detect stale replies from a previous leader term (leader churn scenario:
	// step-down → re-election → old goroutine returns with stale term → must not
	// clear the fresh peerInFlight entry allocated by the new leader term).
	// hbRoundID captures n.st.leaderRound at dispatch time so the leader can
	// advance peerLastRound[peer] for ReadIndex linearizability confirmation
	// (Raft §6.4). Gated on (state==Leader && same term) — same as peerInFlight —
	// so a stale goroutine from a previous leader term cannot satisfy a fresh
	// ReadIndex with old evidence.
	hbPeer          string
	hbTerm          uint64
	hbDispatchTerm  uint64
	hbRoundID       uint64
	hbSuccess       bool
	hbMatchAfter    uint64
	hbConflictTerm  uint64
	hbConflictIndex uint64
	hbErr           error

	// ReadIndex payload (kind == cmdReadIndex). riReply receives the barrier
	// index after a heartbeat round confirms leadership at the current term.
	riReply chan readIndexResult

	// CreateSnapshot payload (kind == cmdCreateSnapshot). csIndex is the
	// snapshot's LastIncludedIndex; csData is the opaque FSM state bytes.
	// csReply delivers nil on success or an error on validation failure.
	csIndex uint64
	csData  []byte
	csReply chan error

	// InstallSnapshot inbound payload (kind == cmdInstallSnapshot).
	isArgs  *InstallSnapshotArgs
	isReply chan *InstallSnapshotReply

	// InstallSnapshot outbound reply payload (kind == cmdInstallSnapshotReply).
	// Carried back via cmdCh by dispatchInstallSnapshot. We use a dedicated
	// kind (rather than reusing cmdHeartbeatReply) because the success-side
	// semantics differ: matchIndex jumps to LastIncludedIndex regardless of
	// any matchAfter math, and the dispatch-term/state gating is identical
	// to AE but the action on success is not. Keeping them separate avoids
	// a forest of conditionals inside handleHeartbeatReply.
	isrPeer         string
	isrTerm         uint64
	isrDispatchTerm uint64
	isrLastIndex    uint64 // LastIncludedIndex shipped — matchIndex target on success
	isrErr          error

	// ConfChange payload (kind == cmdConfChange). ccID is the voter being
	// added or removed; ccAdd discriminates add vs remove. Reply delivered
	// via ccReply once the final phase-2 entry commits.
	ccID    string
	ccAdd   bool
	ccReply chan confChangeResult
}

type cmdKind int

const (
	cmdPropose cmdKind = iota
	cmdRequestVote
	cmdAppendEntries
	cmdVoteReply
	cmdHeartbeatReply
	cmdReadIndex
	cmdCreateSnapshot
	cmdInstallSnapshot
	cmdInstallSnapshotReply
	cmdConfChange
)

// proposalResult is delivered to ProposeWait callers when their entry commits
// (or fails). PR 1 single-node always succeeds synchronously.
type proposalResult struct {
	index uint64
	err   error
}

// readIndexResult is delivered to ReadIndex callers once the heartbeat round
// confirms leadership (or step-down/cancel rejects the request).
type readIndexResult struct {
	index uint64
	err   error
}

// readIndexReq is a queued ReadIndex request awaiting majority confirmation
// of the heartbeat round identified by minPeerRound. Tracked under actorState
// (single-goroutine access). FIFO order preserved by tryFlushReadIndex.
type readIndexReq struct {
	barrier      uint64 // commitIndex captured at request time
	term         uint64 // currentTerm at request time
	minPeerRound uint64 // peers must have peerLastRound >= this for confirmation
	reply        chan readIndexResult
}

// persistHardState saves the current currentTerm and votedFor to stable storage.
// It is a no-op if the HardState is unchanged from the last save (avoids
// redundant fsyncs on the happy path — e.g. becomeFollower followed by
// handleRequestVote in the same tick at the same term/vote). Storage failures
// are unrecoverable — a node that cannot persist its vote/term cannot safely
// participate in Raft.
func (n *Node) persistHardState() {
	hs := HardState{
		CurrentTerm: n.st.currentTerm,
		VotedFor:    n.st.votedFor,
	}
	if hs == n.lastPersistedHS {
		return
	}
	if err := n.stable.SaveHardState(hs); err != nil {
		panic("raftv2: SaveHardState: " + err.Error())
	}
	n.lastPersistedHS = hs
}

// run is the actor goroutine. It is the sole writer of actorState and the
// sole publisher of readState. All state transitions flow through this loop.
func (n *Node) run() {
	defer close(n.doneCh)
	// Close applyInCh on exit so applyLoop knows to drain its buffer and shut
	// down. applyLoop owns close(applyCh) — see Node.Stop lifecycle docs.
	defer close(n.applyInCh)
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

	// Bootstrap: a single-voter cluster ({self} only in the effective config)
	// auto-promotes to Leader at term 1 without running an election. Multi-
	// voter Nodes start as Follower and rely on the election timer firing.
	// The effective config was reconstructed in NewNode from snapshot + log
	// replay; cfg.Peers is the seed-only fallback when no snapshot exists.
	if n.st.isSoloVoter() {
		// Only advance currentTerm to 1 if we haven't already been here
		// (recovery case: persisted currentTerm >= 1 means we were Leader before).
		if n.st.currentTerm < 1 {
			n.st.currentTerm = 1
			n.persistHardState()
		}
		// Route through becomeLeader so the §5.4.2 no-op append fires (FSM
		// consumers see the same "no-op on becomeLeader" contract that the
		// multi-voter path provides). becomeLeader skips the heartbeat ticker
		// when Peers is empty, so this stays cheap.
		n.becomeLeader()
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
	case cmdReadIndex:
		n.handleReadIndex(cmd)
	case cmdCreateSnapshot:
		n.handleCreateSnapshot(cmd)
	case cmdInstallSnapshot:
		n.handleInstallSnapshot(cmd)
	case cmdInstallSnapshotReply:
		n.handleInstallSnapshotReply(cmd)
	case cmdConfChange:
		n.handleConfChange(cmd)
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

	// Rule 1: deny stale term — no state change → no publish.
	if args.Term < n.st.currentTerm {
		cmd.rvReply <- reply
		return
	}

	// Rule 2 mutates state if term advances; Rule 3 mutates votedFor on grant.
	// Either way, publish exactly once at exit.
	defer n.publish()

	// Rule 2: step down on higher term. stepDownToFollower handles leader-only
	// state cleanup, persists HardState, and does NOT publish.
	if args.Term > n.st.currentTerm {
		n.stepDownToFollower(args.Term)
		reply.Term = n.st.currentTerm
	}

	// Rule 3: grant if we haven't voted (or already voted for this candidate)
	// and the candidate's log is at least as up-to-date as ours.
	if (n.st.votedFor == "" || n.st.votedFor == args.CandidateID) &&
		n.st.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		n.st.votedFor = args.CandidateID
		// Persist before sending the reply — the vote must be durable before the
		// candidate learns it was granted. §5.4.1 safety depends on this order.
		n.persistHardState()
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
	// learns it is no longer leader. No state change → no publish needed.
	if args.Term < n.st.currentTerm {
		cmd.aeReply <- &AppendEntriesReply{
			Term:    n.st.currentTerm,
			Success: false,
		}
		return
	}

	// From here on every reject/success path mutates state; publish exactly
	// once at function exit instead of per-branch (hot path: per peer per
	// heartbeat).
	defer n.publish()

	// Rule 2 (term advance): adopt the new term. stepDownToFollower handles
	// leader-only state cleanup if applicable, persists HardState, and does
	// NOT publish — Rule 3 sets leaderID before our deferred publish fires.
	if args.Term > n.st.currentTerm {
		n.stepDownToFollower(args.Term)
	}

	// Rule 3 (same/advanced term): recognise the leader. Candidate steps down,
	// Leader cannot occur here (impossible — same-term-different-leader would
	// violate Raft's election safety; if it does, treat as Follower).
	n.st.state = Follower
	n.st.leaderID = args.LeaderID
	n.resetElectionTimer()

	// Rule 4 prelude (compaction): if the leader's PrevLogIndex falls below
	// our snapshot boundary (FirstIndex-1), we cannot validate it against
	// our log — those entries are gone. Reply with a hint that pushes the
	// leader's nextIndex up to FirstIndex so the next dispatch picks
	// InstallSnapshot. The follower-side compaction floor is FirstIndex-1
	// itself (TermAt returns prevTerm at that index), but anything strictly
	// below it is unreachable.
	if first := n.st.log.FirstIndex(); args.PrevLogIndex > 0 && args.PrevLogIndex < first-1 {
		cmd.aeReply <- &AppendEntriesReply{
			Term:          n.st.currentTerm,
			Success:       false,
			ConflictTerm:  0,
			ConflictIndex: first,
		}
		return
	}

	// Rule 4: log consistency check. On mismatch, populate the conflict hint
	// so the leader can skip the entire offending term in one round-trip
	// (Raft §5.3 optimisation; mirrors v1 raft.go:1843-1862).
	if args.PrevLogIndex > n.st.lastLogIndex() {
		// Case 1: follower's log too short. Hint ConflictTerm=0,
		// ConflictIndex=lastLogIndex+1 so the leader jumps directly to where
		// our log ends.
		cmd.aeReply <- &AppendEntriesReply{
			Term:          n.st.currentTerm,
			Success:       false,
			ConflictTerm:  0,
			ConflictIndex: n.st.lastLogIndex() + 1,
		}
		return
	}
	if args.PrevLogIndex > 0 && n.mustTermAt(args.PrevLogIndex) != args.PrevLogTerm {
		// Case 2: term mismatch at PrevLogIndex. Walk back to the first index
		// of the conflicting term so the leader can decide to skip the whole
		// term (if it has it) or jump to that boundary (if it doesn't).
		conflictTerm := n.mustTermAt(args.PrevLogIndex)
		conflictIdx := args.PrevLogIndex
		// Walk back while the previous logical entry (conflictIdx-1) also has
		// conflictTerm. Floor at FirstIndex: indices below FirstIndex-1 are
		// compacted into the snapshot and TermAt would panic. Stopping at
		// floor=FirstIndex (so conflictIdx-1 stays ≥ FirstIndex-1, where TermAt
		// returns the snapshot's prevTerm) keeps the hint correct — if the
		// leader's view of ConflictTerm extends into the snapshot, it falls
		// back to ConflictIndex and dispatchOne picks InstallSnapshot.
		floor := n.st.log.FirstIndex()
		for conflictIdx > floor && n.mustTermAt(conflictIdx-1) == conflictTerm {
			conflictIdx--
		}
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
			cmd.aeReply <- &AppendEntriesReply{
				Term:          n.st.currentTerm,
				Success:       false,
				ConflictIndex: target,
				ConflictTerm:  0, // force leader to resync from target
			}
			return
		}
		if target <= n.st.lastLogIndex() {
			if n.mustTermAt(target) != e.Term {
				// Conflict at logical index target. Truncate and break — the
				// remaining loop iterations would walk indices we just dropped.
				n.truncateAndRevertConfig(target - 1)
				n.appendAndTrackConfig(args.Entries[i:])
				break
			}
			// Same term/index → same entry by Raft Log Matching. Skip.
			continue
		}
		// target > lastLogIndex: pure extension. Append remaining entries in
		// one shot and break out of the loop.
		n.appendAndTrackConfig(args.Entries[i:])
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

	cmd.aeReply <- &AppendEntriesReply{
		Term:    n.st.currentTerm,
		Success: true,
	}
}

// applyCommitted advances commitIndex from oldCommit to newCommit, enqueueing
// each newly-committed entry on applyInCh (forwarded by applyLoop to applyCh)
// and resolving any matching proposeWaiters. Caller is the actor goroutine.
// Both leader and follower reuse this path; the only difference is that
// leaders also have waiters to drain, while followers (proposeWaiters always
// nil for non-Leader) skip.
//
// applyInCh is buffered (applyInChBuffer) and drained aggressively by applyLoop
// into an unbounded in-goroutine buffer. The actor's send is therefore
// effectively non-blocking under normal operation: a slow FSM consumer wedges
// only the applyLoop goroutine, never the actor — so RPC handling and election
// timers remain responsive even when applyCh back-pressures.
//
// commitIndex is advanced per-entry inside the loop so that a Stop mid-iteration
// leaves commitIndex at the last fully-enqueued entry (not rolled back). Entries
// that didn't get through will be re-delivered on restart via the next leader's
// AE (LeaderCommit > local commitIndex triggers replay).
func (n *Node) applyCommitted(oldCommit, newCommit uint64) {
	for i := oldCommit + 1; i <= newCommit; i++ {
		entry := n.mustEntry(i)
		select {
		case n.applyInCh <- entry:
		case <-n.stopCh:
			// Stop won the race. commitIndex stays at the last fully-enqueued
			// entry's index (set by the previous iteration via the per-entry
			// advance below). Entries that didn't get through will be
			// re-delivered after restart via the next leader's AE — leader's
			// LeaderCommit > our commitIndex triggers the replay path.
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
		// Advance commitIndex after each successful enqueue so Stop mid-iteration
		// leaves commitIndex at the last fully-enqueued entry.
		n.st.commitIndex = i
	}
	// Joint-consensus phase progression hook: if commit just advanced past
	// our pendingConfChange's joint or final index, drive the next phase
	// (or resolve the caller). Idempotent — safe to call after any commit.
	n.advanceConfChangePhase()
	// Self-removed-leader hook (Raft §4.3): if we are no longer in the
	// committed Cnew, step down so a Cnew member can take over.
	n.maybeStepDownAfterRemoval()
}

// applyLoop is the FSM-feeder goroutine. It receives committed entries from
// applyInCh into an unbounded in-goroutine buffer, then forwards them to
// applyCh in FIFO order. When the FSM consumer is slow, the buffer grows
// (memory cost) but the actor is unaffected — applyInCh's receive branch is
// always selectable.
//
// Shutdown: actor closes applyInCh on exit; applyLoop sees the channel closed,
// best-effort drains its buffer to applyCh, then closes applyCh and signals
// applyDoneCh. A second close-of-stopCh during that drain (rare; Stop is
// idempotent) escapes the inner send so the goroutine can't deadlock if the
// FSM consumer is gone.
func (n *Node) applyLoop() {
	defer close(n.applyDoneCh)
	defer close(n.applyCh)

	var buf []LogEntry
	for {
		var outCh chan<- LogEntry
		var first LogEntry
		if len(buf) > 0 {
			outCh = n.applyCh
			first = buf[0]
		}
		select {
		case e, ok := <-n.applyInCh:
			if !ok {
				// Actor exited; flush remaining buffer.
				for _, ent := range buf {
					select {
					case n.applyCh <- ent:
					case <-n.stopCh:
						return
					}
				}
				return
			}
			buf = append(buf, e)
		case outCh <- first:
			// Slide window down. Zero the popped slot so the LogEntry's
			// Command []byte can be GC'd before buf empties (otherwise the
			// underlying array retains the slice header). When buf empties,
			// set to nil so the GC can also reclaim the array.
			var zero LogEntry
			buf[0] = zero
			buf = buf[1:]
			if len(buf) == 0 {
				buf = nil
			}
		}
	}
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

	idx := n.st.log.LastIndex() + 1
	entry := LogEntry{
		Term:    n.st.currentTerm,
		Index:   idx,
		Command: cmd.proposeCommand,
		Type:    cmd.proposeType,
	}
	if err := n.st.log.Append([]LogEntry{entry}); err != nil {
		panic("raftv2: Append: " + err.Error())
	}

	// Single-voter shortcut: self-quorum is met by appending. Route through
	// applyCommitted so the becomeLeader no-op (idx 1, present after PR 12's
	// uniform single/multi-voter Leader transition) is delivered to applyCh
	// before this entry. The waiter is registered in proposeWaiters so
	// applyCommitted resolves it.
	if n.st.isSoloVoter() {
		if cmd.proposeReply != nil {
			if n.st.proposeWaiters == nil {
				n.st.proposeWaiters = make(map[uint64]chan proposalResult)
			}
			n.st.proposeWaiters[idx] = cmd.proposeReply
		}
		n.applyCommitted(n.st.commitIndex, idx)
		n.publish()
		// On Stop mid-delivery, applyCommitted leaves commitIndex at the last
		// fully-delivered entry. If the proposeWaiter never fires, ProposeWait
		// handles that via its own ctx/stopCh select (propose.go:48-58).
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

	for _, peer := range n.st.peers() {
		// dispatchOne picks AE vs InstallSnapshot based on nextIndex[peer]
		// vs FirstIndex(); single-flight gate honoured inside.
		n.dispatchOne(peer)
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
	// Non-voter guard (Raft §4.3): if self is not a voter in EITHER side
	// of the current effective configuration (Cold ∪ Cnew during joint),
	// we are a removed/learner node that must not run for office. During
	// a joint state, a server in Cold-only is still a legitimate voter
	// — "any server from either configuration may serve as leader" —
	// and may be the only node able to drive the joint forward, so we
	// must NOT short-circuit it. Re-arm the timer so we can try again
	// later in case our config catches up via AE replay. In single-state
	// with self absent, this loops indefinitely until either (a) we're
	// added back via a future ConfChange or (b) the operator stops the
	// node — both are the correct behaviour for a node that has been
	// ejected.
	if !n.st.currentConfig.containsAnyVoter(n.st.id) {
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
	n.st.votesGranted = map[string]bool{n.st.id: true} // self-vote
	// Persist before broadcasting: peers must see a durable self-vote before
	// we send RequestVote RPCs. A crash after dispatch but before persist
	// would let us re-enter the same term without the self-vote, violating §5.4.1.
	n.persistHardState()
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
	for _, peer := range n.st.peers() {
		go n.sendRequestVote(peer, args)
	}

	// Edge case: if we have already met quorum (only happens when the cluster
	// is misconfigured to a single-voter via Peers=[]; the bootstrap path
	// above already covers that, so this branch is theoretical). Keep it
	// explicit for safety.
	if n.st.currentConfig.quorumOK(n.st.votesGranted) {
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

	// Raft §5.4.2 no-op: multi-voter only. Append a blank entry at the new
	// term immediately on election so maybeAdvanceCommitIndex can satisfy
	// the term gate (log[N].Term == currentTerm) without waiting for a
	// client request, committing any prior-term uncommitted entries as a
	// side effect. Single-voter clusters have self-quorum so entries commit
	// at append time — there can never be a prior-term uncommitted entry
	// at single-voter election, making the no-op unnecessary noise on the
	// applyCh. FSM consumers MUST ignore LogEntryNoOp entries (Command nil).
	peers := n.st.peers()
	if len(peers) > 0 {
		noOpIdx := n.st.lastLogIndex() + 1
		if err := n.st.log.Append([]LogEntry{{
			Term:  n.st.currentTerm,
			Index: noOpIdx,
			Type:  LogEntryNoOp,
		}}); err != nil {
			panic("raftv2: Append no-op: " + err.Error())
		}
	}

	// Initialise per-peer replication state per Raft §5.3: nextIndex starts
	// at lastLogIndex+1 (optimistic — assume peer is in sync), matchIndex
	// starts at 0 (we've confirmed nothing yet). Set after no-op append so
	// nextIndex[peer] accounts for the no-op.
	last := n.st.lastLogIndex()
	n.st.matchIndex = make(map[string]uint64, len(peers))
	n.st.nextIndex = make(map[string]uint64, len(peers))
	n.st.peerInFlight = make(map[string]bool, len(peers))
	// Reset ReadIndex tracking for this term — peerLastRound from any prior
	// term must NOT pre-satisfy a fresh ReadIndex (load-bearing for §6.4
	// linearizability). leaderRound restarts at 0, so a queued req from a
	// prior term that still references its old minPeerRound (>0) cannot match
	// even if peerLastRound were stale; we nil both anyway as defense in depth.
	n.st.leaderRound = 0
	n.st.peerLastRound = make(map[string]uint64, len(peers))
	n.st.readIndexQueue = nil
	for _, peer := range peers {
		n.st.matchIndex[peer] = 0
		n.st.nextIndex[peer] = last + 1
		n.st.peerLastRound[peer] = 0
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

	// Multi-voter only: start heartbeat ticker and fire an immediate AE so
	// followers learn of the new leader without waiting a full interval.
	// Single-voter clusters have no peers; skipping the ticker avoids waking
	// the actor every 50ms for an empty broadcastHeartbeat.
	if len(peers) > 0 {
		interval := n.cfg.HeartbeatTimeout
		if interval <= 0 {
			interval = defaultHeartbeatTimeout
		}
		n.heartbeatTicker = time.NewTicker(interval)
		n.broadcastHeartbeat()
	}

	// Leader-churn mid-joint recovery (Raft §4.3): if the prior leader died
	// after appending the joint entry but before appending the final
	// LogEntryConfChange, the live config we just inherited is joint and
	// no caller is waiting on a pendingConfChange. Without intervention the
	// joint state never settles — every future AddVoter / RemoveVoter
	// returns ErrConfChangeInFlight forever. Synthesize a pendingConfChange
	// so applyCommitted's hook fires phase 2 once the joint entry's commit
	// is re-confirmed under the new term. The reply chan is a cap-1
	// throwaway since no caller is waiting.
	n.recoverInFlightJoint()
}

// stepDownToFollower applies the state transition to Follower (term, vote,
// leader, leader-only state cleanup) and persists HardState, but does NOT
// publish() or resetElectionTimer(). Callers that will publish/reset
// themselves at function exit (e.g. handleAppendEntries, handleRequestVote)
// use this to avoid redundant readState allocations on the AE/RV hot path.
//
// Outstanding ProposeWait callers receive ErrProposalFailed because the
// entries' commit fate is no longer this node's responsibility.
func (n *Node) stepDownToFollower(term uint64) {
	if n.st.state == Leader {
		if n.heartbeatTicker != nil {
			n.heartbeatTicker.Stop()
			n.heartbeatTicker = nil
		}
		// Clear leader-only replication state. Sends are non-blocking
		// (cap-1 buffered chan per ProposeWait at propose.go:39).
		for idx, w := range n.st.proposeWaiters {
			select {
			case w <- proposalResult{err: ErrProposalFailed}:
			default:
			}
			delete(n.st.proposeWaiters, idx)
		}
		// Drain any queued ReadIndex requests with ErrProposalFailed — they
		// queued under our (now-defunct) Leader term and the new leader will
		// have its own commit history, so the barrier we captured no longer
		// reflects a state we can serve linearizably. reply is cap-1 buffered
		// and at-most-one send per req, so blocking is safe and matches the
		// inline-reply paths in handleReadIndex / tryFlushReadIndex.
		for _, req := range n.st.readIndexQueue {
			req.reply <- readIndexResult{err: ErrProposalFailed}
		}
		n.st.readIndexQueue = nil
		n.st.peerLastRound = nil
		n.st.leaderRound = 0
		n.st.matchIndex = nil
		n.st.nextIndex = nil
		n.st.proposeWaiters = nil
		n.st.peerInFlight = nil
		// Drain any in-flight membership change with ErrProposalFailed —
		// the new leader will start its own (or the operator retries).
		// Reply chan is cap-1 buffered.
		if n.st.pendingConfChange != nil {
			select {
			case n.st.pendingConfChange.reply <- confChangeResult{err: ErrProposalFailed}:
			default:
			}
			n.st.pendingConfChange = nil
		}
	}
	n.st.currentTerm = term
	n.st.state = Follower
	n.st.votedFor = ""
	n.st.leaderID = ""
	n.st.votesGranted = nil
	// Persist before any caller-visible side effect (reply.Term, publish).
	n.persistHardState()
}

// becomeFollower steps down to Follower at the given term and immediately
// publishes + rearms the election timer. Use stepDownToFollower instead from
// hot paths (handleAppendEntries, handleRequestVote) that batch publish at
// function exit.
func (n *Node) becomeFollower(term uint64) {
	n.stepDownToFollower(term)
	n.publish()
	n.resetElectionTimer()
}

// broadcastHeartbeat dispatches an outbound RPC (AE or InstallSnapshot) to
// every peer. dispatchOne picks the right RPC kind for each peer based on
// whether nextIndex[peer] has fallen below FirstIndex() (snapshot) or not
// (AE). Caller must already be Leader.
//
// Per-peer single-flight gate (in dispatchOne): if a goroutine is already in
// flight for a peer (peerInFlight[peer] == true), the dispatch is skipped.
// This prevents goroutine accumulation when the transport is slow or
// partitioned.
//
// Each AE dispatch carries the current leaderRound as hbRoundID so
// handleHeartbeatReply can advance peerLastRound for ReadIndex
// linearizability confirmation. InstallSnapshot dispatches do NOT carry a
// round; ReadIndex requests in flight during a snapshot install simply wait
// for the next heartbeat round.
func (n *Node) broadcastHeartbeat() {
	for _, peer := range n.st.peers() {
		n.dispatchOne(peer)
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
	prevTerm := n.mustTermAt(prevIdx)
	var entries []LogEntry
	if last := n.st.lastLogIndex(); last >= next {
		// Cap the batch to cfg.MaxEntriesPerAE so we never attempt to ship the
		// entire log in a single RPC (e.g. fresh follower with a 1 GB log).
		// Mirrors v1 (internal/raft/raft.go:1511-1512). Default: 512.
		maxAE := n.cfg.MaxEntriesPerAE
		if maxAE == 0 {
			maxAE = defaultMaxEntriesPerAE
		}
		// EntriesFrom returns a copy, defending against concurrent log truncation
		// (see PR 6b note above). The memLogStore copy cost is bounded by maxAE.
		var err error
		entries, err = n.st.log.EntriesFrom(next, int(maxAE))
		if err != nil {
			panic("raftv2: EntriesFrom: " + err.Error())
		}
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
	t := n.loadTransport()
	if t == nil {
		return
	}
	reply, err := t.SendRequestVote(peer, args)
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
//
// roundID is the leaderRound at dispatch time, reported back via cmd.hbRoundID
// for ReadIndex linearizability confirmation. The actor's stale-reply gate
// (term match) ensures rounds from a previous leader term cannot satisfy a
// fresh request.
func (n *Node) dispatchAppendEntries(peer string, args *AppendEntriesArgs, roundID uint64) {
	matchAfter := args.PrevLogIndex + uint64(len(args.Entries))
	c := command{
		kind:           cmdHeartbeatReply,
		hbPeer:         peer,
		hbMatchAfter:   matchAfter,
		hbDispatchTerm: args.Term,
		hbRoundID:      roundID,
	}
	t := n.loadTransport()
	if t == nil {
		// No transport configured (test setup, or pre-SetTransport call). Send
		// a synthetic error reply so handleHeartbeatReply clears peerInFlight
		// — otherwise the gate stays true forever and the leader silently
		// stops dispatching to this peer on every subsequent heartbeat.
		c.hbErr = fmt.Errorf("raftv2: no transport configured")
	} else {
		reply, err := t.SendAppendEntries(peer, args)
		c.hbErr = err
		if err == nil && reply != nil {
			c.hbTerm = reply.Term
			c.hbSuccess = reply.Success
			c.hbConflictTerm = reply.ConflictTerm
			c.hbConflictIndex = reply.ConflictIndex
		}
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
	if n.st.votesGranted == nil {
		// Defensive: should be allocated in becomeCandidate. A nil map here
		// indicates a logic bug; treat as fresh and continue.
		n.st.votesGranted = make(map[string]bool)
	}
	n.st.votesGranted[cmd.vrPeer] = true
	if n.st.currentConfig.quorumOK(n.st.votesGranted) {
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
	// Clear the per-peer in-flight gate so the next heartbeat tick can retry.
	// Gate on (state == Leader && same dispatch term): a reply from a goroutine
	// dispatched in a previous leader term (leader churn: step-down → re-election
	// → stale goroutine finally returns) must NOT clear the fresh entry set by
	// the new term's broadcastHeartbeat, or it would open a slot for a duplicate
	// in-flight goroutine. Transport errors also clear the gate so the next tick
	// can retry a partitioned peer.
	if n.st.state == Leader && n.st.peerInFlight != nil && cmd.hbDispatchTerm == n.st.currentTerm {
		n.st.peerInFlight[cmd.hbPeer] = false
	}
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
		// Advance peerLastRound for ReadIndex confirmation (Raft §6.4). Same
		// term gate as above ensures stale-term replies cannot pre-satisfy a
		// fresh ReadIndex.
		if n.st.peerLastRound != nil && cmd.hbRoundID > n.st.peerLastRound[cmd.hbPeer] {
			n.st.peerLastRound[cmd.hbPeer] = cmd.hbRoundID
		}
		n.maybeAdvanceCommitIndex()
		n.tryFlushReadIndex()
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
	// Strategy: find the first logical index where term > ConflictTerm; the
	// entry just before it (if its term == ConflictTerm) is the rightmost
	// match. This replaces the O(N) backward scan with O(log N).
	//
	// Snapshot floor: search range is [FirstIndex, LastIndex] only. Indices
	// below FirstIndex are compacted into the snapshot — mustTermAt would panic.
	// If ConflictTerm lives entirely in the compacted prefix, the predicate
	// finds no match and we fall back to hbConflictIndex; if that index also
	// falls below FirstIndex, dispatchOne picks InstallSnapshot on the next
	// dispatch and replication recovers via the snapshot path.
	newNext := cmd.hbConflictIndex
	last := n.st.lastLogIndex()
	first := n.st.log.FirstIndex()
	if last >= first {
		// Search over [first, last+1): probe maps to logical index first+i.
		count := int(last - first + 1)
		firstGT := uint64(sort.Search(count, func(i int) bool {
			return n.mustTermAt(first+uint64(i)) > cmd.hbConflictTerm
		}))
		firstGTLogical := first + firstGT
		// firstGTLogical-1 is the last logical index ≤ ConflictTerm. If its
		// term matches, advance nextIndex past it. Guard firstGTLogical > first
		// so we never look below FirstIndex (where prevTerm-equality would
		// mean ConflictTerm extends into the snapshot — fall through to
		// hbConflictIndex in that case).
		if firstGTLogical > first && n.mustTermAt(firstGTLogical-1) == cmd.hbConflictTerm {
			newNext = firstGTLogical
		}
	}
	if newNext < 1 {
		newNext = 1
	}
	n.st.nextIndex[cmd.hbPeer] = newNext
}

// maybeAdvanceCommitIndex finds the highest log index N such that a majority
// of voters (per effective config) have matchIndex >= N AND log[N].Term ==
// currentTerm. The term gate is Raft §5.4.2 — leaders may not commit entries
// from prior terms by counting replicas alone. Caller must be Leader.
//
// In joint state (Raft §4.3), the quorum check requires a majority from
// BOTH Cold and Cnew independently. effectiveConfig.commitOK encodes that.
func (n *Node) maybeAdvanceCommitIndex() {
	last := n.st.lastLogIndex()
	if last <= n.st.commitIndex {
		return
	}
	// Build the per-voter match view: self matches at last (we definitionally
	// have every entry up to last), peers contribute their tracked matchIndex.
	match := make(map[string]uint64, len(n.st.matchIndex)+1)
	for k, v := range n.st.matchIndex {
		match[k] = v
	}
	match[n.st.id] = last

	// Walk from highest candidate down; first one that meets quorum + term
	// gate becomes the new commitIndex.
	newCommit := n.st.commitIndex
	for N := last; N > n.st.commitIndex; N-- {
		// §5.4.2 term gate: only commit entries from the current term directly.
		if n.mustTermAt(N) != n.st.currentTerm {
			continue
		}
		if n.st.currentConfig.commitOK(N, match) {
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

// handleReadIndex processes a queued ReadIndex request (Raft §6.4). The
// linearizability protocol:
//
//  1. Capture barrier = commitIndex (the highest definitely-applied state).
//  2. Bump leaderRound and dispatch a fresh heartbeat round; majority confirmation
//     proves we are still leader for the request's term (no later term has
//     deposed us yet from the perspective of those peers).
//  3. Queue the request; tryFlushReadIndex reaps it once peerLastRound is
//     advanced for a majority of peers.
//
// Single-voter shortcut: with no peers, leadership is implicit at every commit
// (self-quorum), so we reply inline with the current commitIndex. Same caller
// contract as the multi-voter path: caller awaits FSM lastApplied >= barrier
// before serving the read.
func (n *Node) handleReadIndex(cmd command) {
	if n.st.state != Leader {
		cmd.riReply <- readIndexResult{err: ErrNotLeader}
		return
	}
	if n.st.isSoloVoter() {
		cmd.riReply <- readIndexResult{index: n.st.commitIndex}
		return
	}
	n.st.leaderRound++
	n.st.readIndexQueue = append(n.st.readIndexQueue, readIndexReq{
		barrier:      n.st.commitIndex,
		term:         n.st.currentTerm,
		minPeerRound: n.st.leaderRound,
		reply:        cmd.riReply,
	})
	// Force an immediate heartbeat broadcast so the new round reaches idle
	// peers without waiting for the next 50ms tick. Peers currently in flight
	// (peerInFlight[peer] == true) are skipped — their next AE will carry
	// the bumped round once the in-flight reply returns and the gate clears.
	// SLA: idle-peer-majority case is bounded by RTT; in-flight-peer-majority
	// case is bounded by heartbeatTimeout + RTT.
	n.broadcastHeartbeat()
}

// tryFlushReadIndex reaps queued ReadIndex requests whose minPeerRound has
// been confirmed by a majority of voters (self counts as confirmed at any
// round). Called from handleHeartbeatReply on a successful AE reply that
// advanced peerLastRound. Stale-term entries are reaped with ErrProposalFailed
// — though stepDownToFollower already drains on transition, this guards
// against logical errors and is cheap.
func (n *Node) tryFlushReadIndex() {
	if len(n.st.readIndexQueue) == 0 {
		return
	}
	out := n.st.readIndexQueue[:0]
	for _, req := range n.st.readIndexQueue {
		if req.term != n.st.currentTerm {
			req.reply <- readIndexResult{err: ErrProposalFailed}
			continue
		}
		// Build a confirmed-set view: self confirmed at any round; peers
		// confirmed if their tracked peerLastRound is at or beyond the
		// request's minPeerRound. Joint-aware quorum check handles both
		// Cold and Cnew majorities.
		confirmed := make(map[string]bool, len(n.st.peerLastRound)+1)
		confirmed[n.st.id] = true
		for peer, r := range n.st.peerLastRound {
			if r >= req.minPeerRound {
				confirmed[peer] = true
			}
		}
		if n.st.currentConfig.quorumOK(confirmed) {
			req.reply <- readIndexResult{index: req.barrier}
			continue
		}
		out = append(out, req)
	}
	n.st.readIndexQueue = out
}
