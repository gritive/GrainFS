package admin

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockPeerHealth struct {
	peers []ClusterPeerInfo
}

func (m *mockPeerHealth) Snapshot() []ClusterPeerInfo { return m.peers }

func TestListClusterPeers_NilPeerHealth_EmptyList(t *testing.T) {
	// Single-node deploy: no peers wired. Endpoint must return 0-list, not error.
	resp, err := ListClusterPeers(context.Background(), &Deps{})
	require.NoError(t, err)
	require.NotNil(t, resp.Peers)
	require.Empty(t, resp.Peers)
}

func TestListClusterPeers_AllHealthy(t *testing.T) {
	mock := &mockPeerHealth{peers: []ClusterPeerInfo{
		{ID: "127.0.0.1:7001", Healthy: true},
		{ID: "127.0.0.1:7002", Healthy: true},
	}}
	resp, err := ListClusterPeers(context.Background(), &Deps{PeerHealth: mock})
	require.NoError(t, err)
	require.Len(t, resp.Peers, 2)
	require.True(t, resp.Peers[0].Healthy)
	require.True(t, resp.Peers[1].Healthy)
}

func TestListClusterPeers_SomeUnhealthy(t *testing.T) {
	mock := &mockPeerHealth{peers: []ClusterPeerInfo{
		{ID: "127.0.0.1:7001", Healthy: true},
		{ID: "127.0.0.1:7002", Healthy: false, LastFailure: "2026-05-05T03:55:00Z", CooldownRemainingMs: 8500},
	}}
	resp, err := ListClusterPeers(context.Background(), &Deps{PeerHealth: mock})
	require.NoError(t, err)
	require.Len(t, resp.Peers, 2)
	require.False(t, resp.Peers[1].Healthy)
	require.Equal(t, int64(8500), resp.Peers[1].CooldownRemainingMs)
	require.Equal(t, "2026-05-05T03:55:00Z", resp.Peers[1].LastFailure)
}
