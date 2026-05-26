package cluster

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"time"
)

// IsSPKIDenylisted reports whether an SPKI is revoked (invite admission gate).
func (m *MetaRaft) IsSPKIDenylisted(spki [32]byte) bool { return m.fsm.peers.isDenylisted(spki) }

// SPKIOwner returns the node-id that owns an SPKI, if any (invite gate).
func (m *MetaRaft) SPKIOwner(spki [32]byte) (string, bool) { return m.fsm.peers.spkiOwner(spki) }

// LookupInvite returns an invite public key if present, unused, and unexpired.
func (m *MetaRaft) LookupInvite(id string, now time.Time) (ed25519.PublicKey, bool) {
	return m.fsm.invites.lookup(id, now)
}

// AcceptSPKIBytes returns the cluster transport accept-set as raw SPKI bytes.
func (m *MetaRaft) AcceptSPKIBytes() [][]byte { return m.fsm.peers.acceptSPKIBytes() }

// JoinViaInvite admits a brand-new node that presented a valid invite +
// per-node signature (zero-CA Phase 2, §4.2). It stages membership the same
// way Join does, but additionally registers the joiner's pinned SPKI and
// single-use-consumes the invite, all in commit1, then promotes in commit2.
//
// Rollback policy: a transient AddLearner / InviteConsume / promotion failure
// is NOT the joiner's fault, so we roll back with m.fsm.peers.remove (drop the
// pending entry) instead of ProposeRevokePeer (which denylists the SPKI and
// would permanently burn a legitimate identity). Denylisting is reserved for
// genuine revocation (Phase 3).
func (m *MetaRaft) JoinViaInvite(ctx context.Context, nodeID, addr string, spki [32]byte, inviteID string) error {
	if _, ok := m.fsm.invites.lookup(inviteID, time.Now()); !ok {
		return errInviteInvalid
	}
	raftID := addr
	if raftID == "" {
		raftID = nodeID
	}
	// commit1: register pending-learner SPKI + AddLearner + consume invite.
	if err := m.ProposeRegisterPendingLearner(ctx, nodeID, spki, addr); err != nil {
		return err
	}
	if err := m.node.AddLearner(raftID, addr); err != nil {
		_, _ = m.fsm.peers.remove(nodeID)
		return fmt.Errorf("meta_raft: AddLearner %s: %w", nodeID, err)
	}
	if err := m.ProposeInviteConsume(ctx, inviteID); err != nil {
		_ = m.node.RemoveVoter(raftID)
		_, _ = m.fsm.peers.remove(nodeID)
		return err
	}
	// commit2: promote.
	if err := m.node.PromoteToVoter(raftID); err != nil {
		_ = m.node.RemoveVoter(raftID)
		_, _ = m.fsm.peers.remove(nodeID)
		return fmt.Errorf("meta_raft: PromoteToVoter %s: %w", nodeID, err)
	}
	if err := m.ProposePromoteMember(ctx, nodeID); err != nil {
		return err
	}
	return m.ProposeAddNode(ctx, MetaNodeEntry{ID: nodeID, Address: addr, Role: 0})
}
