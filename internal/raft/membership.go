package raft

import (
	"context"
	"errors"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
)

var (
	ErrConfChangeInProgress           = errors.New("raft: another config change is already in progress")
	ErrMixedVersionNoMembershipChange = errors.New("raft: membership change rejected during mixed-version operation")
)

// LogEntryType distinguishes normal FSM commands from Raft protocol entries.
// Values match the FlatBuffers LogEntryType enum (default 0 = Command, backward-compatible).
type LogEntryType int8

const (
	LogEntryCommand         LogEntryType = 0
	LogEntryConfChange      LogEntryType = 1
	LogEntryJointConfChange LogEntryType = 2 // reserved; not implemented
)

// ConfChangeOp is the membership change operation.
// Values match the FlatBuffers ConfChangeOp enum.
type ConfChangeOp int8

const (
	ConfChangeAddVoter    ConfChangeOp = 0
	ConfChangeRemoveVoter ConfChangeOp = 1
	ConfChangeAddLearner  ConfChangeOp = 2
	ConfChangePromote     ConfChangeOp = 3
)

// ConfChangePayload is the decoded in-memory representation of a membership change.
type ConfChangePayload struct {
	Op             ConfChangeOp
	ID             string
	Address        string
	ManagedByJoint bool // PR-K3: true if ChangeMembership added this learner
}

// encodeConfChange serializes a membership change for use as LogEntry.Command.
// managedByJoint is true for learners added by ChangeMembership (PR-K3).
func encodeConfChange(p ConfChangePayload) []byte {
	b := flatbuffers.NewBuilder(128)
	idOff := b.CreateString(p.ID)
	addrOff := b.CreateString(p.Address)
	pb.ConfChangeEntryStart(b)
	pb.ConfChangeEntryAddOp(b, pb.ConfChangeOp(p.Op))
	pb.ConfChangeEntryAddServerId(b, idOff)
	pb.ConfChangeEntryAddServerAddress(b, addrOff)
	pb.ConfChangeEntryAddManagedByJoint(b, p.ManagedByJoint)
	root := pb.ConfChangeEntryEnd(b)
	pb.FinishConfChangeEntryBuffer(b, root)
	return b.FinishedBytes()
}

// decodeConfChange deserializes a ConfChangePayload from LogEntry.Command bytes.
func decodeConfChange(data []byte) ConfChangePayload {
	e := pb.GetRootAsConfChangeEntry(data, 0)
	return ConfChangePayload{
		Op:             ConfChangeOp(e.Op()),
		ID:             string(e.ServerId()),
		Address:        string(e.ServerAddress()),
		ManagedByJoint: e.ManagedByJoint(),
	}
}

// AddVoter proposes adding a new full voting member to the cluster.
// Performs learner-first: AddLearner → wait catch-up → PromoteToVoter automatically.
// Idempotent if the node is already a voter (returns nil immediately).
func (n *Node) AddVoter(id, addr string) error {
	return n.AddVoterCtx(context.Background(), id, addr)
}

// AddVoterCtx is AddVoter with an explicit context for cancellation/timeout.
//
// Sequence:
//  1. Pre-check: if id (or addr) is already in config.Peers, return nil (idempotent).
//  2. Propose AddLearner ConfChange and wait for commit.
//  3. Register a per-id promote channel and wait until apply loop closes it
//     (PromoteToVoter committed by the leader watcher).
//
// Failure modes: ErrNotLeader (caller on follower), ErrMixedVersionNoMembershipChange,
// ErrConfChangeInProgress (concurrent), ctx.Err() (timeout — learner remains).
func (n *Node) AddVoterCtx(ctx context.Context, id, addr string) error {
	// 0. Already-voter pre-check (idempotent, prevents voter→learner regression)
	n.mu.Lock()
	for _, p := range n.config.Peers {
		if p == id || p == addr {
			n.mu.Unlock()
			return nil
		}
	}
	n.mu.Unlock()

	// 1. AddLearner (proposeConfChangeWait checks ErrNotLeader / MixedVersion / ConfChangeInProgress)
	if err := n.proposeConfChangeWait(ctx, ConfChangeAddLearner, id, addr, false); err != nil {
		return err
	}

	// 2. Register promote notification channel
	promoteCh := make(chan struct{})
	n.mu.Lock()
	n.learnerPromoteCh[id] = promoteCh
	_, isStillLearner := n.learnerIDs[id]
	n.mu.Unlock()

	if !isStillLearner {
		// Promoted between AddLearner commit and registration — cleanup and return.
		n.mu.Lock()
		delete(n.learnerPromoteCh, id)
		n.mu.Unlock()
		return nil
	}

	// 3. Wait for promote commit (apply loop closes promoteCh on commit-time)
	defer func() {
		n.mu.Lock()
		delete(n.learnerPromoteCh, id)
		n.mu.Unlock()
	}()

	select {
	case <-promoteCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return ErrProposalFailed
	}
}

// RemoveVoter proposes removing a voting member from the cluster.
func (n *Node) RemoveVoter(id string) error {
	return n.proposeConfChangeWait(context.Background(), ConfChangeRemoveVoter, id, "", false)
}

// AddLearner proposes adding a non-voting observer to the cluster.
// Learners replicate the log but do not count toward quorum.
func (n *Node) AddLearner(id, addr string) error {
	return n.proposeConfChangeWait(context.Background(), ConfChangeAddLearner, id, addr, false)
}

// PromoteToVoter promotes a learner to a full voting member.
func (n *Node) PromoteToVoter(id string) error {
	return n.proposeConfChangeWait(context.Background(), ConfChangePromote, id, "", false)
}

// SetMixedVersion marks the cluster as mixed-version, blocking all membership
// changes until cleared. Call with true when a rolling upgrade is in progress;
// call with false once all nodes are confirmed to be on the same version.
func (n *Node) SetMixedVersion(v bool) {
	n.mu.Lock()
	n.mixedVersion = v
	n.mu.Unlock()
}

// proposeConfChangeWait enforces the single-pending-change invariant and waits
// for the ConfChange entry to be committed (or context to cancel).
func (n *Node) proposeConfChangeWait(ctx context.Context, op ConfChangeOp, id, addr string, managedByJoint bool) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if n.mixedVersion {
		n.mu.Unlock()
		return ErrMixedVersionNoMembershipChange
	}
	if n.pendingConfChangeIndex != 0 {
		n.mu.Unlock()
		return ErrConfChangeInProgress
	}
	n.mu.Unlock()

	cmd := encodeConfChange(ConfChangePayload{Op: op, ID: id, Address: addr, ManagedByJoint: managedByJoint})
	doneCh := make(chan proposalResult, 1)
	p := proposal{command: cmd, entryType: LogEntryConfChange, doneCh: doneCh, ctx: ctx}
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

// applyConfigChangeLocked applies a ConfChange or JointConfChange entry to
// config.Peers immediately when the entry is appended (§4.4: "regardless of
// whether committed"). For JointConfChange this also maintains the §4.3 phase
// state on every node, including caller wakeup on JointLeave commit.
// MUST be called with n.mu held.
func (n *Node) applyConfigChangeLocked(entry LogEntry) {
	switch entry.Type {
	case LogEntryConfChange:
		n.applyConfChangeLocked(entry)
	case LogEntryJointConfChange:
		n.applyJointConfChangeLocked(entry)
	}
}

func (n *Node) applyConfChangeLocked(entry LogEntry) {
	cc := decodeConfChange(entry.Command)
	// peerKey: data-Raft uses QUIC address; meta-Raft uses nodeID.
	peerKey := cc.ID
	if cc.Address != "" {
		peerKey = cc.Address
	}

	switch cc.Op {
	case ConfChangeAddVoter:
		alreadyPresent := false
		for _, p := range n.config.Peers {
			if p == peerKey {
				alreadyPresent = true
				break
			}
		}
		if !alreadyPresent {
			n.config.Peers = append(n.config.Peers, peerKey)
			if n.state == Leader {
				n.nextIndex[peerKey] = n.lastLogIdx() + 1
				n.matchIndex[peerKey] = 0
			}
		}

	case ConfChangeRemoveVoter:
		peers := make([]string, 0, len(n.config.Peers))
		for _, p := range n.config.Peers {
			if p != peerKey {
				peers = append(peers, p)
			}
		}
		n.config.Peers = peers
		delete(n.nextIndex, peerKey)
		delete(n.matchIndex, peerKey)

	case ConfChangeAddLearner:
		if _, exists := n.learnerIDs[cc.ID]; !exists {
			n.learnerIDs[cc.ID] = peerKey
			if n.state == Leader {
				n.nextIndex[peerKey] = n.lastLogIdx() + 1
				n.matchIndex[peerKey] = 0
			}
		}
		if cc.ManagedByJoint {
			n.jointManagedLearners[cc.ID] = struct{}{}
		}

	case ConfChangePromote:
		pk, ok := n.learnerIDs[cc.ID]
		if !ok {
			break // idempotent: already promoted or not tracked
		}
		delete(n.learnerIDs, cc.ID)
		alreadyVoter := false
		for _, p := range n.config.Peers {
			if p == pk {
				alreadyVoter = true
				break
			}
		}
		if !alreadyVoter {
			n.config.Peers = append(n.config.Peers, pk)
		}
	}

	n.pendingConfChangeIndex = entry.Index
}

// applyJointConfChangeLocked drives the §4.3 state machine.
//
//	JointEnter:
//	  jointPhase = JointEntering, voter sets recorded, jointEnterIndex = entry.Index.
//	  Leader bootstraps replication state for newly added voters.
//	JointLeave:
//	  jointPhase = JointNone, config.Peers = newServers (single mode).
//	  jointResultCh signaled on every node so proposeJointConfChangeWait callers
//	  wake up regardless of which node currently holds leadership.
//	  Leader self-removal triggers step-down at the end of apply (Decision 8 +
//	  cross-model Codex #7 / Gemini #8 — apply-time, not append-time).
//	JointAbort:
//	  jointPhase = JointNone, config.Peers restored to C_old from entry payload.
//	  jointResultCh signaled with ErrJointAborted. No-op if not in JointEntering.
func (n *Node) applyJointConfChangeLocked(entry LogEntry) {
	jc := decodeJointConfChange(entry.Command)
	switch jc.Op {
	case JointOpEnter:
		n.jointPhase = JointEntering
		n.jointOldVoters = serverPeerKeys(jc.OldServers)
		n.jointNewVoters = serverPeerKeys(jc.NewServers)
		n.jointEnterIndex = entry.Index
		n.jointEnterTime = time.Now()
		n.jointLeaveProposed = false
		if n.state == Leader {
			n.initLeaderStateForNewVoters(jc.OldServers, jc.NewServers)
		}

	case JointOpLeave:
		if n.jointPhase != JointEntering {
			return // idempotency guard: JointAbort already resolved this transition
		}
		// Append-time: §4.4 invariant — config.Peers updates immediately so
		// quorum decisions on subsequent entries use C_new. config.Peers in
		// this codebase excludes self (peer IDs are the OTHER nodes); voter
		// sets in joint entries include self because §4.3 reasons about full
		// voter membership.
		n.config.Peers = peersExcludingSelf(jc.NewServers, n.id)
		// learnerIDs cleanup: any ID promoted to voter via this JointLeave
		// must be removed from learnerIDs to keep the state-machine invariant
		// (a server is either voter or learner, never both).
		for _, s := range jc.NewServers {
			delete(n.learnerIDs, s.ID)
			delete(n.jointManagedLearners, s.ID)
		}
		n.jointPhase = JointNone
		n.jointOldVoters = nil
		n.jointNewVoters = nil
		n.jointEnterIndex = 0
		n.jointLeaveProposed = false
		n.jointAbortProposed = false

	case JointOpAbort:
		if n.jointPhase != JointEntering {
			return // idempotency guard: already resolved
		}
		// Restore C_old from entry payload — self-contained, safe after snapshot truncation.
		n.config.Peers = peersExcludingSelf(jc.OldServers, n.id)
		n.jointPhase = JointNone
		n.jointOldVoters = nil
		n.jointNewVoters = nil
		n.jointEnterIndex = 0
		n.jointLeaveProposed = false
		n.jointAbortProposed = false
		// Remove managed learners from learnerIDs before clearing the map.
		// Without this, checkLearnerCatchup would see them as ordinary learners
		// and attempt auto-promotion after the abort.
		for id := range n.jointManagedLearners {
			delete(n.learnerIDs, id)
		}
		n.jointManagedLearners = nil
	}
}

// peersExcludingSelf returns peerKeys for every entry whose id is not selfID.
// This bridges the §4.3 voter-set convention (full membership) and the existing
// config.Peers convention (peers excluding self).
func peersExcludingSelf(servers []ServerEntry, selfID string) []string {
	out := make([]string, 0, len(servers))
	for _, s := range servers {
		key := s.Address
		if key == "" {
			key = s.ID
		}
		if key == selfID || s.ID == selfID {
			continue
		}
		out = append(out, key)
	}
	return out
}

// restoreConfigFromServers splits a snapshot servers list into peers (Voter,
// excluding self) and a learnerIDs map (NonVoter, nodeID → nodeID).
// selfID is the local node's ID; it is excluded from both outputs.
func restoreConfigFromServers(servers []Server, selfID string) (peers []string, learners map[string]string) {
	learners = make(map[string]string)
	for _, sv := range servers {
		if sv.ID == selfID {
			continue
		}
		if sv.Suffrage == Voter {
			peers = append(peers, sv.ID)
		} else {
			learners[sv.ID] = sv.ID
		}
	}
	return peers, learners
}

// rebuildConfigFromLog reconstructs config.Peers from basePeers and all
// ConfChange entries in the current in-memory log at index ≥ startIndex.
// Pass startIndex=0 and basePeers=n.initialPeers for normal bootstrap.
// Pass the snapshot index and servers-derived peers when restoring from snapshot.
// MUST be called with n.mu held.
func (n *Node) rebuildConfigFromLog(startIndex uint64, basePeers []string, baseLearners map[string]string) {
	peers := make([]string, len(basePeers))
	copy(peers, basePeers)
	learnerAddrs := make(map[string]string, len(baseLearners))
	for k, v := range baseLearners {
		learnerAddrs[k] = v
	}

	// Joint state replays alongside ConfChange entries so a truncated JointLeave
	// reverts to JointEntering state (Decision 4-bis). Without this, a node that
	// applied JointLeave but had it truncated by a new leader would silently
	// keep config.Peers = C_new while the cluster expects C_old+new dual quorum.
	var jPhase jointPhase
	var jOldVoters, jNewVoters []string
	var jEnterIndex uint64
	// removedFromCluster mirrors the apply-path commit-time hook (raft.go:586):
	// orphan election guard must survive process restart from log replay.
	// Without this, a restarted self-removed node has flag=false and rejoins
	// elections, splitting votes against C_new.
	var removed bool
	// managedLearners mirrors n.jointManagedLearners for the replay path.
	managedLearners := make(map[string]struct{})
	// jAborted tracks whether the current joint cycle was resolved by a
	// JointOpAbort so that a subsequent JointOpLeave (for the same cycle)
	// is skipped rather than re-applied.
	var jAborted bool

	for _, entry := range n.log {
		if entry.Index < startIndex {
			continue
		}
		switch entry.Type {
		case LogEntryConfChange:
			cc := decodeConfChange(entry.Command)
			peerKey := cc.ID
			if cc.Address != "" {
				peerKey = cc.Address
			}
			switch cc.Op {
			case ConfChangeAddVoter:
				found := false
				for _, p := range peers {
					if p == peerKey {
						found = true
						break
					}
				}
				if !found {
					peers = append(peers, peerKey)
				}
			case ConfChangeRemoveVoter:
				out := peers[:0]
				for _, p := range peers {
					if p != peerKey {
						out = append(out, p)
					}
				}
				peers = out
			case ConfChangeAddLearner:
				learnerAddrs[cc.ID] = peerKey
				if cc.ManagedByJoint {
					managedLearners[cc.ID] = struct{}{}
				}
			case ConfChangePromote:
				if pk, ok := learnerAddrs[cc.ID]; ok {
					delete(learnerAddrs, cc.ID)
					found := false
					for _, p := range peers {
						if p == pk {
							found = true
							break
						}
					}
					if !found {
						peers = append(peers, pk)
					}
				}
			}

		case LogEntryJointConfChange:
			jc := decodeJointConfChange(entry.Command)
			switch jc.Op {
			case JointOpEnter:
				jPhase = JointEntering
				jOldVoters = serverPeerKeys(jc.OldServers)
				jNewVoters = serverPeerKeys(jc.NewServers)
				jEnterIndex = entry.Index
				jAborted = false // new joint cycle resets prior abort flag
				// JointEnter that brings self into C_new clears any prior
				// removal flag (rejoin scenario).
				if containsPeer(jNewVoters, n.id) {
					removed = false
				}
			case JointOpLeave:
				if jAborted {
					// JointOpAbort already resolved this joint cycle; skip the
					// stale JointOpLeave that was appended before the abort committed.
					jAborted = false
					continue
				}
				peers = peersExcludingSelf(jc.NewServers, n.id)
				// Mirror apply path: clear promoted learners from the replay map.
				for _, s := range jc.NewServers {
					delete(learnerAddrs, s.ID)
					delete(managedLearners, s.ID)
				}
				// Mirror commit-time hook (raft.go:586): orphan election guard.
				removed = !containsPeer(serverPeerKeys(jc.NewServers), n.id)
				jPhase = JointNone
				jOldVoters = nil
				jNewVoters = nil
				jEnterIndex = 0

			case JointOpAbort:
				// Restore C_old from entry payload — self-contained, safe after snapshot truncation.
				peers = peersExcludingSelf(jc.OldServers, n.id)
				managedLearners = make(map[string]struct{})
				jPhase = JointNone
				jOldVoters = nil
				jNewVoters = nil
				jEnterIndex = 0
				jAborted = true // signal to skip any stale JointOpLeave in this cycle
			}
		}
	}
	n.config.Peers = peers
	n.learnerIDs = learnerAddrs
	n.jointPhase = jPhase
	n.jointOldVoters = jOldVoters
	n.jointNewVoters = jNewVoters
	n.jointEnterIndex = jEnterIndex
	n.removedFromCluster = removed
	n.jointManagedLearners = managedLearners
	n.jointLeaveProposed = false // truncation 후 leader watcher가 재평가
}
