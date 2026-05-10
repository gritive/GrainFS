package raftv2

import (
	"fmt"
	"time"
)

// handleConfChange processes an AddVoter / RemoveVoter request submitted via
// cmdCh. Implements Raft §4.3 phase 1: validate, build Cnew, append a
// LogEntryJointConfChange (Cold ∪ Cnew), update the in-memory effective
// config to joint, and dispatch replication. The caller's reply channel is
// stored in n.st.pendingConfChange and resolved when phase 2 commits.
//
// Validation:
//   - Must be Leader (ErrNotLeader).
//   - At most one in-flight membership change. If pendingConfChange is set,
//     or if the latest config entry is not yet committed, reject with
//     ErrConfChangeInFlight (mirrors hashicorp/raft's pragmatic rule).
//   - AddVoter: id must NOT already be a voter. RemoveVoter: id MUST already
//     be a voter (otherwise the change is a no-op and the operator probably
//     made a typo — surface the error rather than silently succeed).
func (n *Node) handleConfChange(cmd command) {
	if n.st.state != Leader {
		cmd.ccReply <- confChangeResult{err: ErrNotLeader}
		return
	}
	if n.st.pendingConfChange != nil {
		cmd.ccReply <- confChangeResult{err: ErrConfChangeInFlight}
		return
	}
	// Refuse if the previous membership change has not yet committed —
	// guards against pipelining edge cases where a second change races
	// the first's final ConfChange entry to commit.
	if n.st.appendedConfigIndex > n.st.commitIndex {
		cmd.ccReply <- confChangeResult{err: ErrConfChangeInFlight}
		return
	}
	// Defensive: cannot have a sane joint config and pendingConfChange == nil
	// after a leader takeover (becomeLeader does not currently rebuild
	// pendingConfChange from log replay). If we observe a joint state on
	// becoming leader, the new leader cannot continue someone else's
	// half-finished change — refuse to start a fresh one until the joint
	// settles via the existing log entry being re-replicated. The simplest
	// safe answer: refuse.
	if n.st.currentConfig.joint {
		cmd.ccReply <- confChangeResult{err: ErrConfChangeInFlight}
		return
	}

	old := append([]string(nil), n.st.currentConfig.voters...)
	newV, err := buildNextVoters(old, cmd.ccID, cmd.ccAdd)
	if err != nil {
		cmd.ccReply <- confChangeResult{err: err}
		return
	}

	jointIdx := n.st.lastLogIndex() + 1
	jointEntry := LogEntry{
		Term:    n.st.currentTerm,
		Index:   jointIdx,
		Type:    LogEntryJointConfChange,
		Command: encodeJointConfChange(old, newV),
	}
	if err := n.st.log.Append([]LogEntry{jointEntry}); err != nil {
		panic("raftv2: handleConfChange: Append joint: " + err.Error())
	}

	// Push the prior config onto the history stack BEFORE swapping in the
	// joint config so a later truncation can revert exactly. Then swap
	// currentConfig to the joint state — per Raft §4.3, a server uses the
	// configuration in the most recent log entry it has appended, even
	// while uncommitted.
	prev := n.st.currentConfig
	n.st.configHistory = append(n.st.configHistory, configHistoryEntry{
		logIndex: jointIdx,
		prev:     prev,
	})
	n.st.currentConfig = newJointConfig(old, newV)
	n.st.appendedConfigIndex = jointIdx

	// On entering the joint state, peers may have grown (Cnew adds). Add
	// any newcomers to the leader's per-peer replication maps so the next
	// dispatch can address them. nextIndex starts at lastLogIndex+1 which
	// is the joint entry's index + 1; matchIndex stays at 0 until we get a
	// successful AE reply. Existing peers keep their tracked indices.
	last := n.st.lastLogIndex()
	for _, p := range n.st.currentConfig.peersExcluding(n.st.id) {
		if _, ok := n.st.nextIndex[p]; !ok {
			n.st.nextIndex[p] = last + 1
			n.st.matchIndex[p] = 0
			n.st.peerInFlight[p] = false
			n.st.peerLastRound[p] = 0
		}
	}

	n.st.pendingConfChange = &pendingConfChange{
		jointIndex: jointIdx,
		newVoters:  newV,
		reply:      cmd.ccReply,
	}

	n.publish()

	// Solo-voter → multi-voter transition: the previous becomeLeader skipped
	// the heartbeat ticker for an empty peer set, so start one now.
	if n.heartbeatTicker == nil && len(n.st.currentConfig.peersExcluding(n.st.id)) > 0 {
		interval := n.cfg.HeartbeatTimeout
		if interval <= 0 {
			interval = defaultHeartbeatTimeout
		}
		n.heartbeatTicker = time.NewTicker(interval)
	}
	n.broadcastHeartbeat()

	// Solo-leader edge case: if Cold = {self} (e.g. AddVoter on a 1-node
	// cluster) the joint quorum reduces to "self approves" because Cold
	// majority is just self. The shared maybeAdvanceCommitIndex handles
	// that correctly via commitOK. Cnew majority still requires self plus
	// any added voter to ack — so this only commits the joint entry inline
	// when adding to a degenerate cluster (rare). Normal case: peers
	// catch up on the broadcast above and commit advances on heartbeat
	// reply.
	n.maybeAdvanceCommitIndex()
}

// advanceConfChangePhase drives the in-flight membership change forward.
// Called from applyCommitted after every commitIndex advance. Idempotent.
//
// Phase 1 → 2: when commitIndex >= jointIndex and the final entry hasn't
// been appended yet, append LogEntryConfChange (Cnew alone) and replicate.
// The final entry's index is recorded so the next commit can resolve.
//
// Phase 2 → done: when commitIndex >= finalIndex, deliver confChangeResult
// to the caller, clear pendingConfChange, and let the
// maybeStepDownAfterRemoval hook handle the self-removed-leader case.
func (n *Node) advanceConfChangePhase() {
	pcc := n.st.pendingConfChange
	if pcc == nil {
		return
	}
	// Only the leader drives phase progression — followers shadow whatever
	// the leader appends and have no caller to reply to. Followers
	// reconstruct their own pendingConfChange from log replay only via
	// reconstructConfig (no waiter); they will not enter this branch.
	if n.st.state != Leader {
		return
	}
	// Phase 2 has already committed → resolve.
	if pcc.finalIndex != 0 && n.st.commitIndex >= pcc.finalIndex {
		pcc.reply <- confChangeResult{index: pcc.finalIndex}
		n.st.pendingConfChange = nil
		return
	}
	// Phase 1 committed → append the final entry.
	if pcc.finalIndex == 0 && n.st.commitIndex >= pcc.jointIndex {
		finalIdx := n.st.lastLogIndex() + 1
		finalEntry := LogEntry{
			Term:    n.st.currentTerm,
			Index:   finalIdx,
			Type:    LogEntryConfChange,
			Command: encodeConfChange(pcc.newVoters),
		}
		if err := n.st.log.Append([]LogEntry{finalEntry}); err != nil {
			panic("raftv2: advanceConfChangePhase: Append final: " + err.Error())
		}
		// Push history before swapping, mirroring handleConfChange so
		// truncation reverts apply uniformly.
		prev := n.st.currentConfig
		n.st.configHistory = append(n.st.configHistory, configHistoryEntry{
			logIndex: finalIdx,
			prev:     prev,
		})
		// Leave joint state, settle on Cnew.
		n.st.currentConfig = newSingleConfig(pcc.newVoters)
		n.st.appendedConfigIndex = finalIdx
		pcc.finalIndex = finalIdx

		// Stale replication-map entries for now-non-voters are benign:
		// commitOK and quorumOK both consult currentConfig (not the maps)
		// for membership, so an entry for a dropped voter has no effect
		// on commit decisions. They age out at the next becomeLeader.

		n.publish()
		n.broadcastHeartbeat()

		// If quorum is already satisfied at append (e.g. solo voter or
		// degenerate Cnew = {self}), drive commit advancement inline.
		n.maybeAdvanceCommitIndex()
		return
	}
}

// recoverInFlightJoint resurrects a pendingConfChange when a new leader
// inherits a joint state with no following final entry. Reads the joint
// entry's encoded Cnew so phase 2 can append the right payload once
// commit is re-confirmed under the new term.
//
// Pre-conditions (caller's responsibility): we just became Leader, no
// pendingConfChange is set, and currentConfig.joint is true.
//
// If the live log lacks the originating joint entry (because the
// snapshot ate it but a follower's currentConfig is stale — should not
// happen since handleInstallSnapshot resets currentConfig, but defensive
// nonetheless), the recovery is a no-op. The ErrConfChangeInFlight gate
// would still apply, requiring operator intervention; that is preferable
// to dispatching a fabricated entry.
func (n *Node) recoverInFlightJoint() {
	if n.st.pendingConfChange != nil {
		return
	}
	if !n.st.currentConfig.joint {
		return
	}
	if n.st.appendedConfigIndex == 0 {
		return
	}
	idx := n.st.appendedConfigIndex
	first := n.st.log.FirstIndex()
	if idx < first || idx > n.st.lastLogIndex() {
		return
	}
	e, err := n.st.log.Entry(idx)
	if err != nil {
		return
	}
	if e.Type != LogEntryJointConfChange {
		// The latest config entry isn't a joint entry — nothing to
		// recover. Should not happen given currentConfig.joint == true,
		// but guard against state corruption.
		return
	}
	p := configEntryPayload(e)
	// Reply is a buffered throwaway: no caller is waiting because the
	// original AddVoter/RemoveVoter call died with the previous leader.
	n.st.pendingConfChange = &pendingConfChange{
		jointIndex: idx,
		newVoters:  p.NewVoters,
		reply:      make(chan confChangeResult, 1),
	}
	// applyCommitted will fire advanceConfChangePhase the next time
	// commitIndex advances; if the joint entry was already committed
	// before we became leader, drive it inline now.
	if n.st.commitIndex >= idx {
		n.advanceConfChangePhase()
	}
}

// maybeStepDownAfterRemoval implements the Raft §4.3 self-removed-leader
// rule: a leader that is in Cold but not in Cnew stays at its post until
// the final ConfChange entry commits, then steps down. Called after every
// applyCommitted advance.
//
// Step-down condition: live config is non-joint, the most recent appended
// config entry (appendedConfigIndex) is committed, and self is not a
// voter in the resulting Cnew. Checking commit (rather than just append)
// is load-bearing — advanceConfChangePhase appends and immediately swaps
// currentConfig to Cnew before the new entry commits, so a step-down
// gated on append alone would fire one tick too early and drain the
// caller's pendingConfChange with ErrProposalFailed.
func (n *Node) maybeStepDownAfterRemoval() {
	if n.st.state != Leader {
		return
	}
	if n.st.currentConfig.joint {
		return
	}
	if n.st.currentConfig.containsVoter(n.st.id) {
		return
	}
	if n.st.appendedConfigIndex == 0 || n.st.appendedConfigIndex > n.st.commitIndex {
		// The Cnew entry that excludes us has not yet committed (or there
		// is no config entry at all — degenerate case where cfg.Peers
		// already excluded us at boot, which is operator misconfiguration).
		// Stay at our post.
		return
	}
	// Cnew is committed and excludes us. Step down so a Cnew voter can
	// take over. stepDownToFollower drains leader-only state including
	// any residual pendingConfChange (which advanceConfChangePhase has
	// already resolved by this point on the happy path).
	n.becomeFollower(n.st.currentTerm)
}

// appendAndTrackConfig appends entries to the actor's log and updates
// currentConfig + configHistory + appendedConfigIndex for any
// LogEntryConfChange / LogEntryJointConfChange entries among them.
// Used by handleAppendEntries on the follower path so a follower's
// effective config tracks the leader's per Raft §4.3.
//
// Single-call append per the existing log invariants — entries must be
// contiguous in index. We update config state in-order so configHistory
// remains sorted by logIndex.
func (n *Node) appendAndTrackConfig(entries []LogEntry) {
	if len(entries) == 0 {
		return
	}
	if err := n.st.log.Append(entries); err != nil {
		panic("raftv2: appendAndTrackConfig: Append: " + err.Error())
	}
	for _, e := range entries {
		if e.Type != LogEntryConfChange && e.Type != LogEntryJointConfChange {
			continue
		}
		prev := n.st.currentConfig
		n.st.configHistory = append(n.st.configHistory, configHistoryEntry{
			logIndex: e.Index,
			prev:     prev,
		})
		n.st.currentConfig = applyConfigEntry(prev, e)
		n.st.appendedConfigIndex = e.Index
	}
}

// truncateAndRevertConfig truncates the log past idx and rolls back any
// config-history entries with logIndex > idx. currentConfig reverts to the
// most recent surviving prev (or, if all were popped, to the original
// effective config that pre-dates the configHistory).
//
// "Pre-history" config: when configHistory is empty after pop, we cannot
// reconstruct the original boot config without re-reading the snapshot +
// log replay. The simpler invariant: the FIRST configHistory entry's prev
// IS the pre-history config, since we always push prev BEFORE swapping in
// the new config. So if we pop all entries, restore the popped-stack's
// last (deepest) prev. If history was already empty before this call,
// currentConfig is unchanged (no config entries to revert).
func (n *Node) truncateAndRevertConfig(idx uint64) {
	if err := n.st.log.TruncateAfter(idx); err != nil {
		panic("raftv2: truncateAndRevertConfig: TruncateAfter: " + err.Error())
	}
	// Walk configHistory from the back, popping entries strictly above idx.
	// The popped entry's prev becomes the candidate currentConfig — the
	// LAST popped (deepest) prev is the correct revert target because pop
	// order is from newest to oldest.
	for len(n.st.configHistory) > 0 {
		top := n.st.configHistory[len(n.st.configHistory)-1]
		if top.logIndex <= idx {
			break
		}
		n.st.currentConfig = top.prev
		n.st.configHistory = n.st.configHistory[:len(n.st.configHistory)-1]
	}
	// Recompute appendedConfigIndex: the highest surviving config-entry
	// index, or 0 if none.
	if len(n.st.configHistory) > 0 {
		n.st.appendedConfigIndex = n.st.configHistory[len(n.st.configHistory)-1].logIndex
	} else {
		n.st.appendedConfigIndex = 0
	}
}

// buildNextVoters returns Cnew given Cold, the target voter ID, and whether
// the operation is add or remove. For add: appends id if not present, errors
// if already present. For remove: filters id out, errors if id is not
// present. Order of remaining voters is preserved.
func buildNextVoters(old []string, id string, add bool) ([]string, error) {
	if id == "" {
		return nil, fmt.Errorf("raftv2: confchange: empty voter id")
	}
	if add {
		for _, v := range old {
			if v == id {
				return nil, fmt.Errorf("raftv2: AddVoter: %q is already a voter", id)
			}
		}
		out := make([]string, len(old)+1)
		copy(out, old)
		out[len(old)] = id
		return out, nil
	}
	out := make([]string, 0, len(old))
	found := false
	for _, v := range old {
		if v == id {
			found = true
			continue
		}
		out = append(out, v)
	}
	if !found {
		return nil, fmt.Errorf("raftv2: RemoveVoter: %q is not a voter", id)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("raftv2: RemoveVoter: cannot remove the last voter %q", id)
	}
	return out, nil
}
