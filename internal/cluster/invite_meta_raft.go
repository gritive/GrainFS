package cluster

import (
	"context"
	"crypto/ed25519"
	"fmt"
)

// ProposeInviteMint proposes an InviteMint command to the cluster. The invite
// token (identified by id) is recorded with its public key and TTL; the
// corresponding private key is kept only by the invitee. Caller must be leader.
func (m *MetaRaft) ProposeInviteMint(ctx context.Context, id string, pub ed25519.PublicKey, expiryNanos int64) error {
	payload, err := encodeInviteMintCmd(id, pub, expiryNanos)
	if err != nil {
		return fmt.Errorf("meta_raft: encode InviteMint: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeInviteMint, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitApplied(ctx, idx)
}

// ProposeInviteConsume proposes an InviteConsume command to the cluster,
// marking the invite single-use slot as spent. Caller must be leader.
func (m *MetaRaft) ProposeInviteConsume(ctx context.Context, id string) error {
	payload, err := encodeInviteConsumeCmd(id)
	if err != nil {
		return fmt.Errorf("meta_raft: encode InviteConsume: %w", err)
	}
	data, err := encodeMetaCmd(MetaCmdTypeInviteConsume, payload)
	if err != nil {
		return fmt.Errorf("meta_raft: encode MetaCmd: %w", err)
	}
	idx, err := m.node.ProposeWait(ctx, data)
	if err != nil {
		return fmt.Errorf("meta_raft: ProposeWait: %w", err)
	}
	return m.waitApplied(ctx, idx)
}
