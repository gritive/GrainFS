package raft

import (
	"context"
	"errors"

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
	Op      ConfChangeOp
	ID      string
	Address string
}

// encodeConfChange serializes a membership change for use as LogEntry.Command.
func encodeConfChange(op ConfChangeOp, id, addr string) []byte {
	b := flatbuffers.NewBuilder(128)
	idOff := b.CreateString(id)
	addrOff := b.CreateString(addr)
	pb.ConfChangeEntryStart(b)
	pb.ConfChangeEntryAddOp(b, pb.ConfChangeOp(op))
	pb.ConfChangeEntryAddServerId(b, idOff)
	pb.ConfChangeEntryAddServerAddress(b, addrOff)
	root := pb.ConfChangeEntryEnd(b)
	pb.FinishConfChangeEntryBuffer(b, root)
	return b.FinishedBytes()
}

// decodeConfChange deserializes a ConfChangePayload from LogEntry.Command bytes.
func decodeConfChange(data []byte) ConfChangePayload {
	e := pb.GetRootAsConfChangeEntry(data, 0)
	return ConfChangePayload{
		Op:      ConfChangeOp(e.Op()),
		ID:      string(e.ServerId()),
		Address: string(e.ServerAddress()),
	}
}

// AddVoter proposes adding a new full voting member to the cluster.
func (n *Node) AddVoter(id, addr string) error {
	return n.proposeConfChangeWait(context.Background(), ConfChangeAddVoter, id, addr)
}

// RemoveVoter proposes removing a voting member from the cluster.
func (n *Node) RemoveVoter(id string) error {
	return n.proposeConfChangeWait(context.Background(), ConfChangeRemoveVoter, id, "")
}

// AddLearner proposes adding a non-voting observer to the cluster.
// Learners replicate the log but do not count toward quorum.
func (n *Node) AddLearner(id, addr string) error {
	return n.proposeConfChangeWait(context.Background(), ConfChangeAddLearner, id, addr)
}

// PromoteToVoter promotes a learner to a full voting member.
func (n *Node) PromoteToVoter(id string) error {
	return n.proposeConfChangeWait(context.Background(), ConfChangePromote, id, "")
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
func (n *Node) proposeConfChangeWait(ctx context.Context, op ConfChangeOp, id, addr string) error {
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

	cmd := encodeConfChange(op, id, addr)
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

// applyConfigChangeLocked applies a ConfChange entry to config.Peers immediately
// when the entry is appended (§4.4: "regardless of whether committed").
// MUST be called with n.mu held.
func (n *Node) applyConfigChangeLocked(entry LogEntry) {
	if entry.Type != LogEntryConfChange {
		return
	}
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

// rebuildConfigFromLog reconstructs config.Peers from initialPeers and all
// ConfChange entries in the current in-memory log (after log truncation).
// MUST be called with n.mu held.
func (n *Node) rebuildConfigFromLog() {
	// Start from bootstrap peers
	peers := make([]string, len(n.initialPeers))
	copy(peers, n.initialPeers)
	learnerAddrs := make(map[string]string) // nodeID → peerKey for replay

	// Replay all ConfChange entries still in the log
	for _, entry := range n.log {
		if entry.Type != LogEntryConfChange {
			continue
		}
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
	}
	n.config.Peers = peers
	n.learnerIDs = learnerAddrs // persist surviving learners after replay
}
