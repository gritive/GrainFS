package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardGroupPeerSet_MatchLocalPrefersNodeID(t *testing.T) {
	peers := NewShardGroupPeerSet(ShardGroupEntry{
		ID:      "group-1",
		PeerIDs: []string{"127.0.0.1:7001", "node-a", "127.0.0.1:7002"},
	})

	got, ok := peers.MatchLocal("node-a", "127.0.0.1:7001")
	require.True(t, ok)
	require.Equal(t, "node-a", got)
}

func TestShardGroupPeerSet_MatchLocalAllowsLegacyAlias(t *testing.T) {
	peers := NewShardGroupPeerSet(ShardGroupEntry{
		ID:      "group-1",
		PeerIDs: []string{"127.0.0.1:7001", "127.0.0.1:7002"},
	})

	got, ok := peers.MatchLocal("node-a", "127.0.0.1:7001")
	require.True(t, ok)
	require.Equal(t, "127.0.0.1:7001", got)
}

func TestShardGroupPeerSet_ForwardOrderMovesLocalAliasesLast(t *testing.T) {
	peers := NewShardGroupPeerSet(ShardGroupEntry{
		ID:      "group-1",
		PeerIDs: []string{"127.0.0.1:7001", "node-b", "node-c"},
	})

	got := peers.ForwardOrder("node-a", "127.0.0.1:7001")
	require.Equal(t, []string{"node-b", "node-c", "127.0.0.1:7001"}, got)
}
