package cluster

import (
	"context"
	"fmt"
)

// ProposeRegisterPendingLearner proposes registration of a pending-learner peer
// (node-id, SPKI, dial address). The peer is transport-accepted but non-voting
// until promoted. Caller must be leader.
func (m *MetaRaft) ProposeRegisterPendingLearner(ctx context.Context, nodeID string, spki [32]byte, addr string) error {
	payload, err := encodeRegisterPendingLearnerCmd(nodeID, spki, addr)
	if err != nil {
		return fmt.Errorf("meta_raft: encode RegisterPendingLearner: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeRegisterPendingLearner, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitAppliedResult(ctx, idx)
}

// ProposeRegisterMember proposes non-demoting boot-time self-registration of a
// peer as member (D-rev3 step 2). Every node self-registers and most are
// followers, so this uses the generic forwarding Propose path (which forwards
// to the leader when not leader and blocks until applied) — NOT the leader-only
// m.node.ProposeWait path.
func (m *MetaRaft) ProposeRegisterMember(ctx context.Context, nodeID string, spki [32]byte, addr string, presentsPerNode bool, nodeKeyKEKGen uint32) error {
	payload, err := encodeRegisterMemberCmd(nodeID, spki, addr, presentsPerNode, nodeKeyKEKGen)
	if err != nil {
		return fmt.Errorf("meta_raft: encode RegisterMember: %w", err)
	}
	return m.Propose(ctx, MetaCmdTypeRegisterMember, payload)
}

// ProposePromoteMember proposes promotion of a pending-learner to voting member.
// Caller must be leader.
func (m *MetaRaft) ProposePromoteMember(ctx context.Context, nodeID string) error {
	payload, err := encodePromoteMemberCmd(nodeID)
	if err != nil {
		return fmt.Errorf("meta_raft: encode PromoteMember: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypePromoteMember, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitAppliedResult(ctx, idx)
}

// ProposeRevokePeer proposes removal of a peer and denylisting of its SPKI.
// Caller must be leader.
func (m *MetaRaft) ProposeRevokePeer(ctx context.Context, nodeID string) error {
	payload, err := encodeRevokePeerCmd(nodeID)
	if err != nil {
		return fmt.Errorf("meta_raft: encode RevokePeer: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeRevokePeer, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitAppliedResult(ctx, idx)
}
