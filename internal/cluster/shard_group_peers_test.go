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

func TestShardGroupPeerSet_OwnerPeerDeterministic(t *testing.T) {
	peers := NewShardGroupPeerSet(ShardGroupEntry{
		ID:      "group-7",
		PeerIDs: []string{"node-c", "node-a", "node-b"},
	})

	a, okA := peers.OwnerPeer("group-7")
	b, okB := peers.OwnerPeer("group-7")
	require.True(t, okA)
	require.True(t, okB)
	require.Equal(t, a, b)
	require.Contains(t, []string{"node-a", "node-b", "node-c"}, a)
}

func TestShardGroupPeerSet_OwnerPeerSingleNodeDuplicate(t *testing.T) {
	peers := NewShardGroupPeerSet(ShardGroupEntry{
		ID:      "group-0",
		PeerIDs: []string{"node-a", "node-a", "node-a"},
	})

	got, ok := peers.OwnerPeer("group-0")
	require.True(t, ok)
	require.Equal(t, "node-a", got)
}

func TestShardGroupPeerSet_OwnerMatchesLocalAlias(t *testing.T) {
	peers := NewShardGroupPeerSet(ShardGroupEntry{
		ID:      "group-legacy",
		PeerIDs: []string{"127.0.0.1:7001"},
	})

	require.True(t, peers.OwnerMatchesLocal("group-legacy", "node-a", "127.0.0.1:7001"))
}

func TestResolveShardGroupPeers_PreservesLegacyAndUnresolvedState(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddNodeCmd(t, "node-a", "127.0.0.1:7001", 0)))

	got := ResolveShardGroupPeers(f, ShardGroupEntry{
		ID:      "group-1",
		PeerIDs: []string{"node-a", "127.0.0.1:7001", "127.0.0.1:7999"},
	})

	require.Len(t, got, 3)
	require.Equal(t, ResolvedShardGroupPeer{
		Input:    "node-a",
		NodeID:   "node-a",
		RaftAddr: "127.0.0.1:7001",
	}, got[0])
	require.Equal(t, ResolvedShardGroupPeer{
		Input:    "127.0.0.1:7001",
		NodeID:   "node-a",
		RaftAddr: "127.0.0.1:7001",
		Legacy:   true,
	}, got[1])
	require.Equal(t, ResolvedShardGroupPeer{
		Input:      "127.0.0.1:7999",
		RaftAddr:   "127.0.0.1:7999",
		Legacy:     true,
		Unresolved: true,
	}, got[2])
}
