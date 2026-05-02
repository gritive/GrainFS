package raft

import (
	"context"
	"errors"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
)

// jointPhase tracks the §4.3 joint consensus state machine.
//
// Sub-project 2 implements two phases:
//
//	JointNone     — single configuration (C_old or C_new); normal operation.
//	JointEntering — C_old+new committed; both quorums required for any decision.
//
// JointLeave commit transitions Entering → None on every node via the apply path.
type jointPhase int8

const (
	JointNone     jointPhase = 0
	JointEntering jointPhase = 1
)

// JointOp mirrors raftpb.JointOp for in-memory use.
type JointOp int8

const (
	JointOpEnter JointOp = 0 // C_old → C_old+new
	JointOpLeave JointOp = 1 // C_old+new → C_new
	JointOpAbort JointOp = 2 // C_old+new → C_old (revert; commits under C_old quorum only)
)

// ServerEntry is the in-memory mirror of raftpb.ServerEntry. Joint entries
// carry full address+suffrage on the wire because standalone Raft groups have
// no external address registry (Decision 1).
type ServerEntry struct {
	ID       string
	Address  string
	Suffrage ServerSuffrage
}

// JointConfChange is the decoded payload of a LogEntryJointConfChange entry.
type JointConfChange struct {
	Op         JointOp
	NewServers []ServerEntry
	OldServers []ServerEntry
}

// encodeJointConfChange serializes a JointConfChange to LogEntry.Command bytes.
func encodeJointConfChange(jc JointConfChange) []byte {
	b := flatbuffers.NewBuilder(256)

	serverVec := func(servers []ServerEntry, startVec func(*flatbuffers.Builder, int) flatbuffers.UOffsetT) flatbuffers.UOffsetT {
		offsets := make([]flatbuffers.UOffsetT, len(servers))
		for i, s := range servers {
			id := b.CreateString(s.ID)
			addr := b.CreateString(s.Address)
			pb.ServerEntryStart(b)
			pb.ServerEntryAddId(b, id)
			pb.ServerEntryAddAddress(b, addr)
			pb.ServerEntryAddSuffrage(b, int8(s.Suffrage))
			offsets[i] = pb.ServerEntryEnd(b)
		}
		startVec(b, len(servers))
		for i := len(offsets) - 1; i >= 0; i-- {
			b.PrependUOffsetT(offsets[i])
		}
		return b.EndVector(len(servers))
	}

	newOff := serverVec(jc.NewServers, pb.JointConfChangeEntryStartNewServersVector)
	oldOff := serverVec(jc.OldServers, pb.JointConfChangeEntryStartOldServersVector)

	pb.JointConfChangeEntryStart(b)
	pb.JointConfChangeEntryAddOp(b, pb.JointOp(jc.Op))
	pb.JointConfChangeEntryAddNewServers(b, newOff)
	pb.JointConfChangeEntryAddOldServers(b, oldOff)
	root := pb.JointConfChangeEntryEnd(b)
	pb.FinishJointConfChangeEntryBuffer(b, root)
	return b.FinishedBytes()
}

// decodeJointConfChange deserializes a JointConfChange from LogEntry.Command bytes.
func decodeJointConfChange(data []byte) JointConfChange {
	e := pb.GetRootAsJointConfChangeEntry(data, 0)
	out := JointConfChange{Op: JointOp(e.Op())}

	read := func(length int, get func(*pb.ServerEntry, int) bool) []ServerEntry {
		if length == 0 {
			return nil
		}
		entries := make([]ServerEntry, 0, length)
		for i := 0; i < length; i++ {
			var s pb.ServerEntry
			if !get(&s, i) {
				continue
			}
			entries = append(entries, ServerEntry{
				ID:       string(s.Id()),
				Address:  string(s.Address()),
				Suffrage: ServerSuffrage(s.Suffrage()),
			})
		}
		return entries
	}

	out.NewServers = read(e.NewServersLength(), e.NewServers)
	out.OldServers = read(e.OldServersLength(), e.OldServers)
	return out
}

// serverPeerKeys returns the peer-key list for a ServerEntry slice.
// peerKey matches the convention from membership.go: prefer Address (data-Raft
// QUIC address), fall back to ID (meta-Raft nodeID). The result is what gets
// stored in config.Peers / jointOldVoters / jointNewVoters.
func serverPeerKeys(servers []ServerEntry) []string {
	out := make([]string, len(servers))
	for i, s := range servers {
		if s.Address != "" {
			out[i] = s.Address
		} else {
			out[i] = s.ID
		}
	}
	return out
}

func containsPeer(set []string, item string) bool {
	for _, s := range set {
		if s == item {
			return true
		}
	}
	return false
}

// initLeaderStateForNewVoters bootstraps replication state (nextIndex / matchIndex)
// only for voters introduced by a JointEnter — i.e. members in newServers but not
// in oldServers. Existing voters retain their replication progress so a partial
// joint commit doesn't stall already-replicating peers.
//
// Caller MUST hold n.mu and be the Leader.
func (n *Node) initLeaderStateForNewVoters(oldServers, newServers []ServerEntry) {
	oldKeys := make(map[string]struct{}, len(oldServers))
	for _, s := range oldServers {
		key := s.Address
		if key == "" {
			key = s.ID
		}
		oldKeys[key] = struct{}{}
	}
	nextIdx := n.lastLogIdx() + 1
	for _, s := range newServers {
		key := s.Address
		if key == "" {
			key = s.ID
		}
		if _, existed := oldKeys[key]; existed {
			continue
		}
		if key == n.id {
			continue
		}
		n.nextIndex[key] = nextIdx
		n.matchIndex[key] = 0
	}
}

// hasJointLeaveAfter scans the in-memory log for a JointLeave entry at index
// > startIndex that is "in flight": either from the current term (the active
// leader's own proposal) or already committed (the apply loop will finalise it).
//
// Old-term entries written by a prior leader before it crashed are NOT counted.
// Such entries cannot commit on their own — they need a current-term entry to
// piggyback on. The current leader must re-propose JointLeave so the joint
// phase can advance.
//
// Caller MUST hold n.mu.
func (n *Node) hasJointLeaveAfter(startIndex uint64) bool {
	for _, entry := range n.log {
		if entry.Index <= startIndex {
			continue
		}
		if entry.Type != LogEntryJointConfChange {
			continue
		}
		jc := decodeJointConfChange(entry.Command)
		if jc.Op == JointOpLeave {
			if entry.Term == n.currentTerm || entry.Index <= n.commitIndex {
				return true
			}
		}
	}
	return false
}

// hasCommittedJointLeaveAfter reports whether a JointLeave after startIndex has
// already committed and only needs the apply loop to finalise the transition.
//
// Caller MUST hold n.mu.
func (n *Node) hasCommittedJointLeaveAfter(startIndex uint64) bool {
	for _, entry := range n.log {
		if entry.Index <= startIndex || entry.Index > n.commitIndex {
			continue
		}
		if entry.Type != LogEntryJointConfChange {
			continue
		}
		jc := decodeJointConfChange(entry.Command)
		if jc.Op == JointOpLeave {
			return true
		}
	}
	return false
}

// hasJointAbortAfter reports whether a later JointAbort exists in the local log.
// A leave immediately followed by abort is stale once the abort is present; the
// abort must be allowed to restore C_old instead of becoming a no-op behind the
// leave's phase reset.
//
// Caller MUST hold n.mu.
func (n *Node) hasJointAbortAfter(startIndex uint64) bool {
	for _, entry := range n.log {
		if entry.Index <= startIndex {
			continue
		}
		if entry.Type != LogEntryJointConfChange {
			continue
		}
		jc := decodeJointConfChange(entry.Command)
		if jc.Op == JointOpAbort {
			return true
		}
	}
	return false
}

// proposeJointEnter sends a JointEnter entry through the proposal pipeline.
// MUST NOT be called with n.mu held (proposalCh consumer takes the lock).
func (n *Node) proposeJointEnter(ctx context.Context, oldServers, newServers []ServerEntry) error {
	cmd := encodeJointConfChange(JointConfChange{
		Op:         JointOpEnter,
		NewServers: newServers,
		OldServers: oldServers,
	})
	return n.proposeJointEntry(ctx, cmd)
}

// proposeJointLeave sends a JointLeave entry through the proposal pipeline.
// MUST NOT be called with n.mu held.
func (n *Node) proposeJointLeave(ctx context.Context, newServers []ServerEntry) error {
	cmd := encodeJointConfChange(JointConfChange{
		Op:         JointOpLeave,
		NewServers: newServers,
		OldServers: nil,
	})
	return n.proposeJointEntry(ctx, cmd)
}

// proposeJointEntry submits a JointConfChange command via the batcher pipeline
// and waits for commit (or context cancellation / node stop).
func (n *Node) proposeJointEntry(ctx context.Context, cmd []byte) error {
	doneCh := make(chan proposalResult, 1)
	p := proposal{command: cmd, entryType: LogEntryJointConfChange, doneCh: doneCh, ctx: ctx}
	select {
	case n.proposalCh <- p:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return ErrProposalFailed
	}
	select {
	case result := <-doneCh:
		return result.err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return ErrProposalFailed
	}
}

// checkJointAdvance is called inline on the leader's heartbeat tick. When the
// JointEnter entry has committed and no JointLeave is yet in the log, the
// leader auto-proposes JointLeave to drive the joint phase to completion.
// If JointAbortTimeout is set and elapsed, triggers abort instead.
//
// Idempotency layers:
//  1. state != Leader → no-op (only leader drives advance).
//  2. jointPhase != JointEntering → already done or never started.
//  3. commitIndex < jointEnterIndex → C_old+new not yet committed.
//  4. log already contains a JointLeave at index > jointEnterIndex → in flight.
//  5. jointLeaveProposed flag → propose goroutine already in progress.
//  6. jointAbortProposed flag → abort goroutine already in progress (mutually exclusive with leave).
//
// Caller MUST hold n.mu.
func (n *Node) checkJointAdvance() {
	if n.state != Leader {
		return
	}
	if n.jointPhase != JointEntering {
		return
	}
	if n.jointEnterIndex == 0 || n.commitIndex < n.jointEnterIndex {
		return
	}

	// Auto-abort if timeout is configured and elapsed. An in-flight but
	// uncommitted JointLeave must not suppress abort; it may be unable to form
	// C_new quorum. A committed JointLeave is different: the apply loop will
	// finalise it, so do not append an abort behind it.
	if n.config.JointAbortTimeout > 0 &&
		!n.jointAbortProposed &&
		!n.jointEnterTime.IsZero() &&
		time.Since(n.jointEnterTime) > n.config.JointAbortTimeout &&
		!n.hasCommittedJointLeaveAfter(n.jointEnterIndex) {
		n.triggerAbortAsync()
		return
	}

	if n.hasJointLeaveAfter(n.jointEnterIndex) {
		return
	}

	if n.jointLeaveProposed || n.jointAbortProposed {
		return
	}
	n.jointLeaveProposed = true

	// Snapshot voter list and addresses under lock; propose off-lock.
	newVoters := make([]string, len(n.jointNewVoters))
	copy(newVoters, n.jointNewVoters)
	addrs := n.peerAddressSnapshotLocked()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*n.config.HeartbeatTimeout)
		defer cancel()
		entries := serverEntriesFromIDs(newVoters, addrs)
		if err := n.proposeJointLeave(ctx, entries); err != nil {
			n.mu.Lock()
			n.jointLeaveProposed = false
			n.mu.Unlock()
		}
	}()
}

// proposeJointAbort sends a JointOpAbort entry through the proposal pipeline.
// The entry carries old_servers (C_old) so it is self-contained after snapshot truncation.
// MUST NOT be called with n.mu held.
func (n *Node) proposeJointAbort(ctx context.Context) error {
	n.mu.Lock()
	oldVoters := make([]ServerEntry, 0, len(n.jointOldVoters))
	addrs := n.peerAddressSnapshotLocked()
	for _, id := range n.jointOldVoters {
		oldVoters = append(oldVoters, ServerEntry{ID: id, Address: addrs[id], Suffrage: Voter})
	}
	n.mu.Unlock()

	cmd := encodeJointConfChange(JointConfChange{
		Op:         JointOpAbort,
		NewServers: nil,
		OldServers: oldVoters,
	})
	return n.proposeJointEntry(ctx, cmd)
}

// triggerAbortAsync launches a goroutine to propose JointOpAbort.
// Caller MUST hold n.mu.
func (n *Node) triggerAbortAsync() {
	if n.jointAbortProposed {
		return
	}
	n.jointAbortProposed = true
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*n.config.HeartbeatTimeout)
		defer cancel()
		select {
		case <-n.stopCh:
			return
		default:
		}
		if err := n.proposeJointAbort(ctx); err != nil {
			n.mu.Lock()
			n.jointAbortProposed = false
			n.mu.Unlock()
		}
	}()
}

// ForceAbortJoint proposes a JointOpAbort entry, reverting the cluster to C_old.
// Only valid when this node is leader and in JointEntering phase.
//
// Returns:
//   - nil: abort committed, config reverted to C_old.
//   - ErrNotLeader: not the current leader.
//   - ErrNotInJointPhase: not in JointEntering; safe no-op.
//   - ErrProposalFailed: proposal pipeline error.
//   - ctx.Err(): context cancelled before commit.
func (n *Node) ForceAbortJoint(ctx context.Context) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if n.jointPhase != JointEntering {
		n.mu.Unlock()
		return ErrNotInJointPhase
	}
	// Set flag inside the lock so concurrent checkJointAdvance heartbeat ticks
	// cannot propose a JointLeave between this unlock and the abort proposal.
	n.jointAbortProposed = true
	n.mu.Unlock()

	if err := n.proposeJointAbort(ctx); err != nil {
		n.mu.Lock()
		n.jointAbortProposed = false
		n.mu.Unlock()
		return err
	}
	// proposeJointEntry waits for commit; abort is applied by membership.go.
	return nil
}

// peerAddressSnapshotLocked returns a peerKey → address map for joint payloads.
// In data-Raft mode peerKey already equals address (so the map is identity); in
// meta-Raft mode peerKey is the nodeID and address resolution lives elsewhere
// — for this codebase the joint payload Address may be left equal to ID since
// the in-memory peerKey lookups already resolve through the same convention.
//
// Caller MUST hold n.mu.
func (n *Node) peerAddressSnapshotLocked() map[string]string {
	addrs := make(map[string]string, len(n.config.Peers)+1)
	addrs[n.id] = n.id
	for _, p := range n.config.Peers {
		addrs[p] = p
	}
	for _, p := range n.jointOldVoters {
		if _, ok := addrs[p]; !ok {
			addrs[p] = p
		}
	}
	for _, p := range n.jointNewVoters {
		if _, ok := addrs[p]; !ok {
			addrs[p] = p
		}
	}
	return addrs
}

// serverEntriesFromIDs builds a ServerEntry slice from id list and address map.
// Suffrage defaults to Voter — joint membership entries describe voter sets only.
func serverEntriesFromIDs(ids []string, addrs map[string]string) []ServerEntry {
	out := make([]ServerEntry, 0, len(ids))
	for _, id := range ids {
		out = append(out, ServerEntry{
			ID:       id,
			Address:  addrs[id],
			Suffrage: Voter,
		})
	}
	return out
}

// proposeJointConfChangeWait performs an atomic multi-server membership change.
//
// Inputs:
//
//	adds:    voters to add (full ServerEntry — id+address required for standalone groups).
//	removes: voter ids to remove from the current configuration.
//
// Behavior:
//
//	C_old = current voters (config.Peers + self).
//	C_new = (C_old \ removes) ∪ adds.
//	Propose JointEnter(C_old, C_new). The leader's heartbeat watcher auto-proposes
//	JointLeave once C_old+new commits. Caller wakes up when JointLeave commits on
//	this node's apply loop (commit-time close, truncation-safe).
//
// Edge cases:
//
//   - C_old == C_new: no-op, returns nil immediately without proposing.
//   - ctx canceled: returns ctx.Err(); joint state is preserved (a new leader's
//     heartbeat watcher continues auto-progression — no rollback).
//   - jointPhase != JointNone or pendingConfChangeIndex != 0: ErrConfChangeInProgress.
//   - Not leader: ErrNotLeader.
//   - mixedVersion: ErrMixedVersionNoMembershipChange.
//
// MUST NOT be called with n.mu held.
func (n *Node) proposeJointConfChangeWait(ctx context.Context, adds []ServerEntry, removes []string) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if n.mixedVersion {
		n.mu.Unlock()
		return ErrMixedVersionNoMembershipChange
	}
	if n.jointPhase != JointNone || n.pendingConfChangeIndex != 0 {
		n.mu.Unlock()
		return ErrConfChangeInProgress
	}

	// C_old = self + config.Peers as ServerEntry. Address falls back to id when
	// a peer-key registry is not maintained (data-Raft uses peerKey == address).
	oldVoters := make([]ServerEntry, 0, len(n.config.Peers)+1)
	oldVoters = append(oldVoters, ServerEntry{ID: n.id, Address: n.id, Suffrage: Voter})
	for _, peer := range n.config.Peers {
		if peer == n.id {
			continue
		}
		oldVoters = append(oldVoters, ServerEntry{ID: peer, Address: peer, Suffrage: Voter})
	}

	// C_new = (C_old \ removes) ∪ adds.
	removeSet := make(map[string]struct{}, len(removes))
	for _, r := range removes {
		removeSet[r] = struct{}{}
	}
	newVoters := make([]ServerEntry, 0, len(oldVoters))
	for _, v := range oldVoters {
		if _, removed := removeSet[v.ID]; removed {
			continue
		}
		newVoters = append(newVoters, v)
	}
	for _, a := range adds {
		dup := false
		for _, v := range newVoters {
			if v.ID == a.ID {
				dup = true
				break
			}
		}
		if !dup {
			entry := a
			if entry.Suffrage == 0 {
				entry.Suffrage = Voter
			}
			newVoters = append(newVoters, entry)
		}
	}

	if equalServerSets(oldVoters, newVoters) {
		n.mu.Unlock()
		return nil
	}

	ch := make(chan error, 1)
	n.jointResultCh = ch
	n.mu.Unlock()

	if err := n.proposeJointEnter(ctx, oldVoters, newVoters); err != nil {
		// Propose failed before the entry hit the log; release the wait channel
		// so a retry can install a fresh one. JointEnter that failed never moved
		// the state machine, so jointPhase remains JointNone.
		n.mu.Lock()
		if n.jointResultCh == ch {
			n.jointResultCh = nil
		}
		n.mu.Unlock()
		return err
	}

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		// Joint state is left intact. The cluster's heartbeat watcher (this node
		// or the next leader) will drive C_old+new → C_new on its own. A new
		// caller cannot reattach in this sub-project; that is Sub-project 3's
		// observation API.
		return ctx.Err()
	case <-n.stopCh:
		return ErrProposalFailed
	}
}

// equalServerSets reports whether two ServerEntry slices represent the same
// voter set (id-keyed, order-independent).
func equalServerSets(a, b []ServerEntry) bool {
	if len(a) != len(b) {
		return false
	}
	setA := make(map[string]struct{}, len(a))
	for _, v := range a {
		setA[v.ID] = struct{}{}
	}
	for _, v := range b {
		if _, ok := setA[v.ID]; !ok {
			return false
		}
	}
	return true
}

// ChangeMembership atomically transitions the cluster from C_old to
// C_new = (C_old \ removes) ∪ adds via §4.3 joint consensus, with §4.4
// learner-first catch-up for added voters.
//
// See docs/superpowers/specs/2026-04-30-raft-changemembership-design.md.
//
// MUST NOT be called with n.mu held.
func (n *Node) ChangeMembership(ctx context.Context, adds []ServerEntry, removes []string) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if n.mixedVersion {
		n.mu.Unlock()
		return ErrMixedVersionNoMembershipChange
	}
	if n.jointPhase != JointNone || n.pendingConfChangeIndex != 0 {
		n.mu.Unlock()
		return ErrConfChangeInProgress
	}

	// Empty diff: caller wants no change.
	if len(adds) == 0 && len(removes) == 0 {
		n.mu.Unlock()
		return nil
	}
	n.mu.Unlock()

	// Adds-empty fast path: skip learner phase, go straight to joint.
	if len(adds) == 0 {
		return n.proposeJointConfChangeWait(ctx, nil, removes)
	}

	// Adds path: learner-first hybrid. Register adds as joint-managed before
	// proposing AddLearner so checkLearnerCatchup Guard 2 sees the entry at
	// the moment AddLearner commits.
	n.mu.Lock()
	opts := n.effectiveChangeMembershipOpts()
	for _, a := range adds {
		n.jointManagedLearners[a.ID] = struct{}{}
	}
	n.mu.Unlock()

	// addedIDs tracks learners successfully proposed; defer cleanup attempts
	// RemoveVoter on each on any error path.
	addedIDs := make([]string, 0, len(adds))
	cleanupOnError := func() {
		for _, id := range addedIDs {
			_ = n.RemoveVoter(id)
		}
		n.mu.Lock()
		for _, a := range adds {
			delete(n.jointManagedLearners, a.ID)
		}
		n.mu.Unlock()
	}

	if !opts.SkipLearnerPhase {
		// 1. AddLearner each.
		for _, a := range adds {
			if err := n.proposeConfChangeWait(ctx, ConfChangeAddLearner, a.ID, a.Address, true); err != nil {
				cleanupOnError()
				return err
			}
			addedIDs = append(addedIDs, a.ID)
		}

		// 2. Wait for each learner to catch up (heartbeat-tick polling).
		if err := n.waitLearnersCaughtUp(ctx, adds, opts.CatchUpTimeout); err != nil {
			cleanupOnError()
			return err
		}
	}

	// 3. Joint atomic promote+remove.
	promoteAdds := make([]ServerEntry, len(adds))
	for i, a := range adds {
		entry := a
		entry.Suffrage = Voter
		promoteAdds[i] = entry
	}
	if err := n.proposeJointConfChangeWait(ctx, promoteAdds, removes); err != nil {
		cleanupOnError()
		return err
	}

	// 4. Joint applied — learnerIDs and jointManagedLearners cleared in apply path.
	// Defensive clear here in case apply path races or was skipped.
	n.mu.Lock()
	for _, a := range adds {
		delete(n.jointManagedLearners, a.ID)
	}
	n.mu.Unlock()
	return nil
}

// ErrLearnerCatchUpTimeout is returned by ChangeMembership when an added
// learner failed to catch up within the configured timeout.
var ErrLearnerCatchUpTimeout = errors.New("raft: learner catch-up timeout in ChangeMembership")

// waitLearnersCaughtUp polls each added learner's matchIndex against
// commitIndex at heartbeat-tick frequency, returning when all catch up
// (mi+threshold ≥ commitIndex) or the deadline expires.
func (n *Node) waitLearnersCaughtUp(ctx context.Context, adds []ServerEntry, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.Now().Add(timeout)

	tickInterval := 50 * time.Millisecond
	if n.config.HeartbeatTimeout > 0 {
		tickInterval = n.config.HeartbeatTimeout
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		threshold := n.config.LearnerCatchupThreshold
		if threshold == 0 {
			threshold = 100
		}
		commit := n.commitIndex
		caught := 0
		for _, a := range adds {
			peerKey := a.Address
			if peerKey == "" {
				peerKey = a.ID
			}
			mi := n.matchIndex[peerKey]
			if mi+threshold >= commit {
				caught++
			}
		}
		n.mu.Unlock()
		if caught == len(adds) {
			return nil
		}

		select {
		case <-ticker.C:
			if time.Now().After(deadline) {
				return ErrLearnerCatchUpTimeout
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopCh:
			return ErrProposalFailed
		}
	}
}

// ChangeMembershipOpts configures default behavior for ChangeMembership.
type ChangeMembershipOpts struct {
	// CatchUpTimeout bounds the per-learner catch-up wait. Default 30s.
	CatchUpTimeout time.Duration
	// SkipLearnerPhase forces direct joint propose (no learner-first catch-up).
	// Use only when callers have other catch-up guarantees.
	SkipLearnerPhase bool
}

// SetChangeMembershipDefaults configures default ChangeMembership behavior.
// Concurrent updates are safe under n.mu. Zero CatchUpTimeout falls back to 30s.
func (n *Node) SetChangeMembershipDefaults(opts ChangeMembershipOpts) {
	n.mu.Lock()
	n.changeMembershipDefaults = opts
	n.mu.Unlock()
}

// effectiveChangeMembershipOpts returns the merged options (caller's defaults
// vs internal fallback). Called with n.mu held.
func (n *Node) effectiveChangeMembershipOpts() ChangeMembershipOpts {
	opts := n.changeMembershipDefaults
	if opts.CatchUpTimeout <= 0 {
		opts.CatchUpTimeout = 30 * time.Second
	}
	return opts
}
