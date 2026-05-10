package raftv2

import "fmt"

// handleCreateSnapshot processes a Node.CreateSnapshot request. Validates
// that the requested boundary is within [FirstIndex, commitIndex], captures
// the term at the boundary, persists the snapshot, then compacts the log.
// Storage failures panic the actor — durability errors are unrecoverable.
//
// Validation order matters: commitIndex check first (caller may be racing a
// becomeFollower-induced commitIndex stay-but-log-grow scenario; we want a
// clean error rather than a silent write of uncommitted state). Then the
// "above the snapshot floor" check.
func (n *Node) handleCreateSnapshot(cmd command) {
	if cmd.csIndex == 0 {
		cmd.csReply <- fmt.Errorf("raftv2: CreateSnapshot: index must be > 0")
		return
	}
	if cmd.csIndex > n.st.commitIndex {
		cmd.csReply <- fmt.Errorf("raftv2: CreateSnapshot: index %d > commitIndex %d", cmd.csIndex, n.st.commitIndex)
		return
	}
	first := n.st.log.FirstIndex()
	if cmd.csIndex < first {
		cmd.csReply <- fmt.Errorf("raftv2: CreateSnapshot: index %d < FirstIndex %d (already snapshotted)", cmd.csIndex, first)
		return
	}

	// Capture the term at the boundary BEFORE compacting it away.
	lastIncludedTerm, err := n.st.log.TermAt(cmd.csIndex)
	if err != nil {
		cmd.csReply <- fmt.Errorf("raftv2: CreateSnapshot: TermAt(%d): %w", cmd.csIndex, err)
		return
	}

	// Capture the configuration from the live effective config (Raft §4.3).
	// Snapshots flatten to a single voter list, so we refuse to snapshot
	// while the cluster is in the joint state — joint windows are short
	// (one or two AE round-trips) and the operator should retry once the
	// final ConfChange entry commits. Without this guard, a snapshot taken
	// mid-joint would silently drop the Cold/Cnew distinction and a
	// follower restoring from it would resume in the wrong (non-joint)
	// state, breaking quorum invariants until a new config entry arrives.
	if n.st.currentConfig.joint {
		cmd.csReply <- fmt.Errorf("raftv2: CreateSnapshot: cannot snapshot during joint configuration; retry after final ConfChange commits")
		return
	}
	cfgVoters := make([]string, len(n.st.currentConfig.voters))
	copy(cfgVoters, n.st.currentConfig.voters)

	snap := &Snapshot{
		LastIncludedIndex: cmd.csIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Configuration:     cfgVoters,
		Data:              cmd.csData,
	}
	if err := n.snaps.Save(snap); err != nil {
		// Mirror StableStore: a node that cannot persist its snapshot must
		// not silently advance its in-memory state, because a subsequent
		// crash would leave commitIndex/lastApplied beyond what the durable
		// log+snapshot can reconstruct.
		panic("raftv2: CreateSnapshot: SnapshotStore.Save: " + err.Error())
	}
	if err := n.st.log.CompactBefore(cmd.csIndex); err != nil {
		panic("raftv2: CreateSnapshot: CompactBefore: " + err.Error())
	}

	cmd.csReply <- nil
}

// handleInstallSnapshot processes an inbound InstallSnapshot RPC (Raft §6.3
// + §7). Behaviour:
//
//  1. Stale term → reject without state change.
//  2. Higher term → step down (term advance + persist HardState).
//  3. Same/advanced term → become Follower, reset election timer, recognise leader.
//  4. If our log already has an entry matching (LastIncludedIndex,
//     LastIncludedTerm), the snapshot is redundant — skip the side effects
//     (do NOT truncate; the entry is already correct via normal AE
//     replication). Reply success.
//  5. Otherwise: save the snapshot, truncate the entire log, compact past the
//     boundary, set commitIndex = LastIncludedIndex, deliver a
//     LogEntrySnapshot signal on applyCh so the FSM resets.
//
// The applyCh delivery uses the same applyInCh pipeline as committed entries
// so FIFO ordering is preserved: a subsequent AE that brings entries past
// LastIncludedIndex will deliver them in order after the snapshot signal.
func (n *Node) handleInstallSnapshot(cmd command) {
	args := cmd.isArgs

	// Rule 1: stale term — reject. No state change → no publish needed.
	if args.Term < n.st.currentTerm {
		cmd.isReply <- &InstallSnapshotReply{Term: n.st.currentTerm}
		return
	}

	// From here on, every path mutates state; publish exactly once on exit.
	defer n.publish()

	// Rule 2: term advance.
	if args.Term > n.st.currentTerm {
		n.stepDownToFollower(args.Term)
	}

	// Rule 3: same/advanced term — recognise the leader. Candidate steps down
	// implicitly via state assignment.
	n.st.state = Follower
	n.st.leaderID = args.LeaderID
	n.resetElectionTimer()

	// Rule 4a: stale-snapshot guard. If the leader's snapshot is OLDER than
	// our compaction floor (LastIncludedIndex < FirstIndex), our state already
	// covers it — accept the RPC but skip the install. This protects against
	// a buggy/stale leader regressing our snapshot boundary.
	if args.LastIncludedIndex < n.st.log.FirstIndex() {
		cmd.isReply <- &InstallSnapshotReply{Term: n.st.currentTerm}
		return
	}

	// Rule 4b: redundancy check. If our log already has an entry at
	// LastIncludedIndex with the same term, the snapshot is no-op territory —
	// normal AE replication has already covered this state. We still reply
	// success (the leader needs a positive ack to advance its matchIndex).
	// Per Figure 13 step 6, we also nudge commitIndex up to LastIncludedIndex
	// if the leader knows of a higher commit than we have observed so far.
	if args.LastIncludedIndex <= n.st.lastLogIndex() {
		myTerm, err := n.st.log.TermAt(args.LastIncludedIndex)
		if err == nil && myTerm == args.LastIncludedTerm {
			if args.LastIncludedIndex > n.st.commitIndex {
				n.st.commitIndex = args.LastIncludedIndex
			}
			cmd.isReply <- &InstallSnapshotReply{Term: n.st.currentTerm}
			return
		}
	}

	// Rule 5: install. Save first so a crash between Save and log mutation
	// leaves us with a usable snapshot (next start re-validates).
	snap := &Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Configuration:     args.Configuration,
		Data:              args.Data,
	}
	if err := n.snaps.Save(snap); err != nil {
		panic("raftv2: InstallSnapshot: SnapshotStore.Save: " + err.Error())
	}

	// Truncate the entire log: anything we had before is either covered by
	// the snapshot (already applied to the leader's FSM at this index) or
	// from a divergent term we no longer want. We truncate down to the
	// existing FirstIndex-1 (the prior snapshot boundary, or 0 if none) —
	// TruncateAfter rejects idx < FirstIndex-1 to protect that boundary,
	// so we cannot pass 0 unconditionally on a follower that already had
	// a snapshot. After this, LastIndex == FirstIndex-1 (log empty above
	// the prior boundary). InstallSnapshotBoundary then advances FirstIndex
	// past the new LastIncludedIndex.
	priorBoundary := n.st.log.FirstIndex() - 1
	if err := n.st.log.TruncateAfter(priorBoundary); err != nil {
		panic("raftv2: InstallSnapshot: TruncateAfter: " + err.Error())
	}
	if err := n.st.log.(snapshotInstaller).InstallSnapshotBoundary(args.LastIncludedIndex, args.LastIncludedTerm); err != nil {
		panic("raftv2: InstallSnapshot: InstallSnapshotBoundary: " + err.Error())
	}
	// commitIndex advances BEFORE the applyInCh send (the inverse of
	// applyCommitted's per-entry "advance after enqueue" rule). This is safe
	// because the Snapshot is already durable in SnapshotStore: if Stop wins
	// the send race below, the next start re-discovers via LatestSnapshot and
	// re-delivers the signal. The runtime FSM consumer therefore cannot see
	// commitIndex advance past entries it never received without the
	// snapshot replay covering them.
	n.st.commitIndex = args.LastIncludedIndex

	// Deliver the snapshot signal to the FSM. Use the same applyInCh pipeline
	// as committed entries so FIFO ordering is preserved across the
	// install + subsequent AE replay path. The Command field carries the
	// raw snapshot bytes; FSM consumers MUST recognise LogEntrySnapshot,
	// reset their state, and load from Command.
	signal := LogEntry{
		Term:    args.LastIncludedTerm,
		Index:   args.LastIncludedIndex,
		Command: args.Data,
		Type:    LogEntrySnapshot,
	}
	select {
	case n.applyInCh <- signal:
	case <-n.stopCh:
		// Stop won the race. The snapshot is durable (we Save'd above) and
		// the next start will re-deliver via LatestSnapshot. Drop the reply
		// gracefully (the caller will see ErrNodeStopped via its own
		// stopCh select on Handle*).
	}

	cmd.isReply <- &InstallSnapshotReply{Term: n.st.currentTerm}
}

// snapshotInstaller is the optional LogStore extension used by InstallSnapshot
// to advance FirstIndex past a remote-supplied boundary when the local log is
// empty (or otherwise lacks any content covering the boundary). It is NOT in
// the public LogStore interface — only the two in-tree impls satisfy it,
// and this handler is the sole caller.
type snapshotInstaller interface {
	// InstallSnapshotBoundary sets FirstIndex == lastIncludedIndex+1 and
	// prevTerm == lastIncludedTerm. The log must be empty (LastIndex ==
	// FirstIndex-1) at call time. Persists durably.
	InstallSnapshotBoundary(lastIncludedIndex, lastIncludedTerm uint64) error
}

// handleInstallSnapshotReply processes the leader-side reply to an outbound
// InstallSnapshot. Mirrors handleHeartbeatReply's stale-reply / step-down
// gate, but on success advances matchIndex/nextIndex to LastIncludedIndex+1
// (the snapshot is committed by definition).
func (n *Node) handleInstallSnapshotReply(cmd command) {
	// Clear single-flight gate — gate on (Leader && same dispatch term) so
	// a stale-term reply cannot reopen a slot owned by the new term's
	// dispatcher (mirror handleHeartbeatReply).
	if n.st.state == Leader && n.st.peerInFlight != nil && cmd.isrDispatchTerm == n.st.currentTerm {
		n.st.peerInFlight[cmd.isrPeer] = false
	}
	if cmd.isrErr != nil {
		return
	}
	if cmd.isrTerm > n.st.currentTerm {
		n.becomeFollower(cmd.isrTerm)
		return
	}
	// Stale-reply guard.
	if n.st.state != Leader || cmd.isrTerm != n.st.currentTerm {
		return
	}
	// Success: snapshot is committed by definition. Advance the per-peer
	// indices to past the snapshot.
	if cmd.isrLastIndex > n.st.matchIndex[cmd.isrPeer] {
		n.st.matchIndex[cmd.isrPeer] = cmd.isrLastIndex
	}
	n.st.nextIndex[cmd.isrPeer] = n.st.matchIndex[cmd.isrPeer] + 1
	// A successful InstallSnapshot doesn't carry a leaderRound (it's a
	// distinct dispatch path), so we do NOT advance peerLastRound here —
	// the next regular heartbeat will do that. ReadIndex requests in flight
	// during a snapshot install will simply wait one extra heartbeat round.
	n.maybeAdvanceCommitIndex()
}

// dispatchInstallSnapshot is the outbound counterpart of dispatchAppendEntries
// for the snapshot path. Runs in a per-call goroutine. The single-flight gate
// (peerInFlight) is set by the caller (dispatchOne) before this goroutine runs.
func (n *Node) dispatchInstallSnapshot(peer string, args *InstallSnapshotArgs) {
	c := command{
		kind:            cmdInstallSnapshotReply,
		isrPeer:         peer,
		isrDispatchTerm: args.Term,
		isrLastIndex:    args.LastIncludedIndex,
	}
	t := n.loadTransport()
	if t == nil {
		c.isrErr = fmt.Errorf("raftv2: no transport configured")
	} else {
		reply, err := t.SendInstallSnapshot(peer, args)
		c.isrErr = err
		if err == nil && reply != nil {
			c.isrTerm = reply.Term
		}
	}
	select {
	case n.cmdCh <- c:
	case <-n.stopCh:
	}
}

// dispatchOne picks the right outbound RPC for a peer based on whether
// nextIndex[peer] has fallen below FirstIndex (snapshot path) or not (AE
// path). Caller MUST be inside the actor and MUST be Leader. Caller is
// responsible for the peerInFlight gate (set true here, cleared on reply).
func (n *Node) dispatchOne(peer string) {
	if n.st.peerInFlight[peer] {
		return
	}
	first := n.st.log.FirstIndex()
	if n.st.nextIndex[peer] < first {
		// Follower is behind the leader's compaction floor — must install
		// a snapshot. By construction, FirstIndex > 1 implies a snapshot
		// was persisted (CompactBefore is only called from CreateSnapshot
		// + InstallSnapshot, both of which Save the snapshot first). A
		// missing snapshot here is a corrupt-state bug; panic loudly rather
		// than fall back to AE, which would call mustTermAt(prev) and
		// panic with a less-informative "TermAt out of range" message.
		snap, err := n.snaps.Latest()
		if err != nil {
			panic("raftv2: dispatchOne: SnapshotStore.Latest: " + err.Error())
		}
		if snap == nil {
			panic("raftv2: dispatchOne: log compacted (FirstIndex>1) but SnapshotStore is empty — corrupt state")
		}
		args := &InstallSnapshotArgs{
			Term:              n.st.currentTerm,
			LeaderID:          n.st.id,
			LastIncludedIndex: snap.LastIncludedIndex,
			LastIncludedTerm:  snap.LastIncludedTerm,
			Configuration:     snap.Configuration,
			Data:              snap.Data,
		}
		n.st.peerInFlight[peer] = true
		go n.dispatchInstallSnapshot(peer, args)
		return
	}
	args := n.buildAppendEntriesArgs(peer)
	n.st.peerInFlight[peer] = true
	go n.dispatchAppendEntries(peer, args, n.st.leaderRound)
}
