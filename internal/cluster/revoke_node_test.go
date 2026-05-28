package cluster

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

func TestMetaRaftRevokeNode_PendingOnlyBurnsAndDenylists(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pub := ed25519.PublicKey(make([]byte, ed25519.PublicKeySize))
	var s [32]byte
	s[0] = 0xB2
	require.NoError(t, m.ProposeInviteMint(ctx, "inv-pending", pub, time.Now().Add(time.Hour).UnixNano()))
	require.NoError(t, m.ProposeInvitePending(ctx, "inv-pending", "node-pending", s, "127.0.0.1:7022"))

	require.NoError(t, m.RevokeNode(ctx, "node-pending", func(string) {
		t.Fatal("pending-only revoke must not close a member connection")
	}))

	require.True(t, m.IsSPKIDenylisted(s))
	_, _, _, ok := m.LookupPending("inv-pending")
	require.False(t, ok, "revoke must burn stale Phase-1 pending redemption")
}

func TestMetaRaftRevokeNode_UnknownNodeReturnsNotFound(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	require.ErrorIs(t, m.RevokeNode(context.Background(), "missing", nil), ErrRevokeNodeNotFound)
}
