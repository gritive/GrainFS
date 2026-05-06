package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/serveruntime"
)

func TestRaftClusterInfo_NormalizesPeerAddressesToNodeIDs(t *testing.T) {
	node := raft.NewNode(raft.DefaultConfig("node-0", []string{"10.0.0.1:7001"}))
	info := serveruntime.NewRaftClusterInfo(node, nil, nil, fakeClusterInfoAddressBook{nodes: []cluster.MetaNodeEntry{
		{ID: "node-1", Address: "10.0.0.1:7001"},
	}})

	require.Equal(t, []string{"node-1"}, info.Peers())
	require.Equal(t, []string{"node-0", "node-1"}, info.LivePeers())
}

func TestRaftClusterInfo_SurfacesUnresolvedLegacyPeerState(t *testing.T) {
	node := raft.NewNode(raft.DefaultConfig("node-0", []string{"10.0.0.9:7001"}))
	info := serveruntime.NewRaftClusterInfo(node, nil, nil, fakeClusterInfoAddressBook{})

	require.Equal(t, []string{"10.0.0.9:7001"}, info.Peers())
	require.Equal(t, map[string]string{"10.0.0.9:7001": "unresolved_legacy"}, info.PeerStates())
}

func TestRaftClusterInfo_BuildsPeerSnapshot(t *testing.T) {
	node := raft.NewNode(raft.DefaultConfig("node-0", []string{"10.0.0.1:7001", "10.0.0.9:7001"}))
	info := serveruntime.NewRaftClusterInfo(node, nil, nil, fakeClusterInfoAddressBook{nodes: []cluster.MetaNodeEntry{
		{ID: "node-1", Address: "10.0.0.1:7001"},
	}})

	require.Equal(t, []cluster.PeerLivenessRow{
		{PeerID: "node-0", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive, Reason: "self"},
		{PeerID: "node-1", RaftAddr: "10.0.0.1:7001", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessConfigured, Reason: "configured"},
		{PeerID: "10.0.0.9:7001", RaftAddr: "10.0.0.9:7001", IdentityState: cluster.PeerIdentityUnresolvedLegacy, LivenessState: cluster.PeerLivenessConfigured, Reason: "identity_unresolved"},
	}, info.PeerSnapshot())
}

type fakeClusterInfoAddressBook struct {
	nodes []cluster.MetaNodeEntry
}

func (f fakeClusterInfoAddressBook) Nodes() []cluster.MetaNodeEntry {
	return f.nodes
}
