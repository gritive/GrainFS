package cluster

import (
	"context"
	"errors"
	"fmt"
)

// ErrRevokeNodeNotFound is returned when a Zero-CA revocation target is neither
// a registered peer nor a Phase-1 pending invite redeemer.
var ErrRevokeNodeNotFound = errors.New("zero-ca revoke: node not found")

// RevokeNode revokes a Zero-CA node identity. Deterministic security state
// lives in the RevokePeer raft command; local side effects remain here.
func (m *MetaRaft) RevokeNode(ctx context.Context, nodeID string, closePeer func(string)) error {
	if nodeID == "" {
		return ErrRevokeNodeNotFound
	}

	entry, hasMember := m.fsm.peers.revokeLookupByNodeID(nodeID)
	metaNode, hasMetaNode := m.fsm.NodeByID(nodeID)
	hasPending := len(m.fsm.invites.pendingSPKIsForNode(nodeID)) > 0
	if !hasMember && !hasPending && !hasMetaNode {
		return ErrRevokeNodeNotFound
	}

	m.membershipMu.Lock()
	defer m.membershipMu.Unlock()

	if err := m.ProposeRevokePeer(ctx, nodeID); err != nil {
		return err
	}
	if !hasMember && !hasMetaNode {
		return nil
	}

	raftID := metaNode.Address
	if hasMember && entry.Address != "" {
		raftID = entry.Address
	}
	if raftID == "" {
		raftID = nodeID
	}
	if err := m.node.RemoveVoter(raftID); err != nil {
		return fmt.Errorf("zero-ca revoke: remove voter %s: %w", nodeID, err)
	}
	if closePeer != nil && raftID != "" {
		closePeer(raftID)
	}
	return nil
}

func (r *peerRegistry) revokeLookupByNodeID(nodeID string) (peerEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.byNodeID[nodeID]
	return e, ok
}
