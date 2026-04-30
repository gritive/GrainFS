package raft

import (
	"context"

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

// quorumSets returns the voter sets used for quorum decisions, with self
// always included for uniform majority arithmetic.
//
// Single mode (jointPhase == JointNone): current = config.Peers + self, old = nil.
//
//	(config.Peers in this codebase lists peer IDs excluding self; we splice self
//	in here so dualMajority/hasMajorityInSet can use a single uniform formula.)
//
// Joint mode (jointPhase == JointEntering): current = jointNewVoters,
//
//	old = jointOldVoters. Joint voter sets already carry full membership lists
//	including self (or excluding it if self is being removed).
//
// Caller MUST hold n.mu.
func (n *Node) quorumSets() (current []string, old []string) {
	if n.jointPhase == JointEntering {
		return n.jointNewVoters, n.jointOldVoters
	}
	cur := make([]string, 0, len(n.config.Peers)+1)
	cur = append(cur, n.id)
	cur = append(cur, n.config.Peers...)
	return cur, nil
}

// hasMajorityInSet reports whether matched ids form a strict majority of set.
// Self counts as matched if and only if self is a voter in this configuration —
// nodes that are not part of the configuration cannot contribute to its quorum.
//
// Caller MUST hold n.mu.
func (n *Node) hasMajorityInSet(matched map[string]bool, set []string) bool {
	if len(set) == 0 {
		return false
	}
	count := 0
	for _, id := range set {
		if id == n.id || matched[id] {
			count++
		}
	}
	return count > len(set)/2
}

// dualMajority returns true if matched satisfies majority in BOTH the current
// voter set AND (when in joint mode) the old voter set.
//
// In single mode, only the current set is checked. In joint mode (JointEntering),
// quorum requires both — this is the §4.3 safety guarantee.
//
// Caller MUST hold n.mu.
func (n *Node) dualMajority(matched map[string]bool) bool {
	current, old := n.quorumSets()
	if !n.hasMajorityInSet(matched, current) {
		return false
	}
	if old != nil && !n.hasMajorityInSet(matched, old) {
		return false
	}
	return true
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
// > startIndex. The check is committed-agnostic — uncommitted tail entries are
// considered too. Used by checkJointAdvance for log-state idempotency: a leader
// re-elected mid-joint must not append a duplicate JointLeave when one already
// exists in the log it inherited.
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
//
// Idempotency layers:
//  1. state != Leader → no-op (only leader drives advance).
//  2. jointPhase != JointEntering → already done or never started.
//  3. commitIndex < jointEnterIndex → C_old+new not yet committed.
//  4. log already contains a JointLeave at index > jointEnterIndex → in flight.
//  5. jointLeaveProposed flag → propose goroutine already in progress.
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
	if n.hasJointLeaveAfter(n.jointEnterIndex) {
		return
	}
	if n.jointLeaveProposed {
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

	ch := make(chan struct{})
	n.jointPromoteCh = ch
	n.mu.Unlock()

	if err := n.proposeJointEnter(ctx, oldVoters, newVoters); err != nil {
		// Propose failed before the entry hit the log; release the wait channel
		// so a retry can install a fresh one. JointEnter that failed never moved
		// the state machine, so jointPhase remains JointNone.
		n.mu.Lock()
		if n.jointPromoteCh == ch {
			n.jointPromoteCh = nil
		}
		n.mu.Unlock()
		return err
	}

	select {
	case <-ch:
		return nil
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
