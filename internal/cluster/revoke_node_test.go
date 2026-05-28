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

func TestMetaRaftRevokeNode_RegisteredMemberRemovesVoterAndClosesPeer(t *testing.T) {
	fsm := NewMetaFSM()
	memberSPKI := spki(0xB3)
	require.NoError(t, fsm.peers.registerMember("node-member", memberSPKI, "127.0.0.1:7023", true))

	m := &MetaRaft{
		fsm:         fsm,
		done:        make(chan struct{}),
		applyNotify: make(chan struct{}),
	}
	node := &revokeNodeFakeRaftNode{
		applyChCloseStubNode: applyChCloseStubNode{ch: make(chan raft.LogEntry)},
		m:                    m,
	}
	m.node = node

	var closed string
	require.NoError(t, m.RevokeNode(context.Background(), "node-member", func(addr string) {
		closed = addr
	}))

	require.Equal(t, "127.0.0.1:7023", node.removed)
	require.Equal(t, "127.0.0.1:7023", closed)
	require.True(t, fsm.peers.isDenylisted(memberSPKI))
	_, ok := fsm.peers.revokeLookupByNodeID("node-member")
	require.False(t, ok, "registered member must be removed from the peer registry")
}

func TestMetaRaftRevokeNode_UnknownNodeReturnsNotFound(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	require.ErrorIs(t, m.RevokeNode(context.Background(), "missing", nil), ErrRevokeNodeNotFound)
}

type revokeNodeFakeRaftNode struct {
	applyChCloseStubNode
	m       *MetaRaft
	nextIdx uint64
	removed string
}

func (n *revokeNodeFakeRaftNode) ProposeWait(_ context.Context, data []byte) (uint64, error) {
	n.nextIdx++
	idx := n.nextIdx
	err := n.m.fsm.applyCmdAtIndex(data, idx)
	n.m.recordApplyResult(idx, err)
	n.m.lastApplied.Store(idx)

	n.m.applyNotifyMu.Lock()
	ch := n.m.applyNotify
	n.m.applyNotify = make(chan struct{})
	close(ch)
	n.m.applyNotifyMu.Unlock()
	return idx, nil
}

func (n *revokeNodeFakeRaftNode) RemoveVoter(id string) error {
	n.removed = id
	return nil
}
