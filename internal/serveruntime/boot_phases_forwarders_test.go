package serveruntime

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/stretchr/testify/require"
)

func TestAddJoinedNodeToLegacyDataRaftAddsAddressBackedVoter(t *testing.T) {
	node := &fakeLegacyDataRaftMembership{id: "node-0", peers: []string{"addr-0"}}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "addr-0"},
		{ID: "addr-1", Address: "addr-1"},
	}

	require.NoError(t, addJoinedNodeToLegacyDataRaft(context.Background(), node, nodes, "addr-1"))

	require.Equal(t, []string{"addr-1"}, node.addedIDs)
	require.Equal(t, []string{"addr-1"}, node.addedAddrs)
}

func TestAddJoinedNodeToLegacyDataRaftSkipsStableNodeIDUntilTransportCanResolveIt(t *testing.T) {
	node := &fakeLegacyDataRaftMembership{id: "node-0", peers: []string{"addr-0"}}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-1", Address: "addr-1"},
	}

	require.NoError(t, addJoinedNodeToLegacyDataRaft(context.Background(), node, nodes, "node-1"))

	require.Empty(t, node.addedIDs)
	require.Empty(t, node.addedAddrs)
}

func TestAddJoinedNodeToLegacyDataRaftSkipsExistingVoter(t *testing.T) {
	node := &fakeLegacyDataRaftMembership{id: "node-0", peers: []string{"addr-1"}}
	nodes := []cluster.MetaNodeEntry{{ID: "node-1", Address: "addr-1"}}

	require.NoError(t, addJoinedNodeToLegacyDataRaft(context.Background(), node, nodes, "node-1"))

	require.Empty(t, node.addedIDs)
	require.Empty(t, node.addedAddrs)
}

type fakeLegacyDataRaftMembership struct {
	id         string
	peers      []string
	addedIDs   []string
	addedAddrs []string
}

func (f *fakeLegacyDataRaftMembership) ID() string { return f.id }

func (f *fakeLegacyDataRaftMembership) Peers() []string {
	return append([]string(nil), f.peers...)
}

func (f *fakeLegacyDataRaftMembership) AddVoterCtx(_ context.Context, id, addr string) error {
	f.addedIDs = append(f.addedIDs, id)
	f.addedAddrs = append(f.addedAddrs, addr)
	return nil
}
