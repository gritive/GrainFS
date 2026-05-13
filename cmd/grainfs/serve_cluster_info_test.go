package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/serveruntime"
)

func TestRaftClusterInfo_NormalizesPeerAddressesToNodeIDs(t *testing.T) {
	node := &clusterInfoRaftNode{id: "node-0", peers: []string{"10.0.0.1:7001"}}
	info := serveruntime.NewRaftClusterInfo(node, nil, nil, fakeClusterInfoAddressBook{nodes: []cluster.MetaNodeEntry{
		{ID: "node-1", Address: "10.0.0.1:7001"},
	}})

	require.Equal(t, []string{"node-1"}, info.Peers())
	require.Equal(t, []string{"node-0", "node-1"}, info.LivePeers())
}

func TestRaftClusterInfo_SurfacesUnresolvedLegacyPeerState(t *testing.T) {
	node := &clusterInfoRaftNode{id: "node-0", peers: []string{"10.0.0.9:7001"}}
	info := serveruntime.NewRaftClusterInfo(node, nil, nil, fakeClusterInfoAddressBook{})

	require.Equal(t, []string{"10.0.0.9:7001"}, info.Peers())
	require.Equal(t, map[string]string{"10.0.0.9:7001": "unresolved_legacy"}, info.PeerStates())
}

func TestRaftClusterInfo_BuildsPeerSnapshot(t *testing.T) {
	node := &clusterInfoRaftNode{id: "node-0", peers: []string{"10.0.0.1:7001", "10.0.0.9:7001"}}
	info := serveruntime.NewRaftClusterInfo(node, nil, nil, fakeClusterInfoAddressBook{nodes: []cluster.MetaNodeEntry{
		{ID: "node-1", Address: "10.0.0.1:7001"},
	}})

	require.Equal(t, []cluster.PeerLivenessRow{
		{PeerID: "node-0", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive, Reason: "self"},
		{PeerID: "node-1", RaftAddr: "10.0.0.1:7001", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive, Reason: "probe_live"},
		{PeerID: "10.0.0.9:7001", RaftAddr: "10.0.0.9:7001", IdentityState: cluster.PeerIdentityUnresolvedLegacy, LivenessState: cluster.PeerLivenessConfigured, Reason: "identity_unresolved"},
	}, info.PeerSnapshot())
}

type fakeClusterInfoAddressBook struct {
	nodes []cluster.MetaNodeEntry
}

func (f fakeClusterInfoAddressBook) Nodes() []cluster.MetaNodeEntry {
	return f.nodes
}

type clusterInfoRaftNode struct {
	id    string
	peers []string
}

func (n *clusterInfoRaftNode) Start()                 {}
func (n *clusterInfoRaftNode) Close()                 {}
func (n *clusterInfoRaftNode) ID() string             { return n.id }
func (n *clusterInfoRaftNode) State() raft.NodeState  { return raft.Leader }
func (n *clusterInfoRaftNode) Term() uint64           { return 1 }
func (n *clusterInfoRaftNode) IsLeader() bool         { return true }
func (n *clusterInfoRaftNode) LeaderID() string       { return n.id }
func (n *clusterInfoRaftNode) CommittedIndex() uint64 { return 0 }
func (n *clusterInfoRaftNode) Configuration() raft.Configuration {
	return raft.Configuration{}
}
func (n *clusterInfoRaftNode) Peers() []string                      { return n.peers }
func (n *clusterInfoRaftNode) PeerMatchIndex(string) (uint64, bool) { return 0, false }
func (n *clusterInfoRaftNode) Bootstrap() error                     { return nil }
func (n *clusterInfoRaftNode) Propose([]byte) error                 { return nil }
func (n *clusterInfoRaftNode) ProposeWait(context.Context, []byte) (uint64, error) {
	return 0, nil
}
func (n *clusterInfoRaftNode) ReadIndex(context.Context) (uint64, error) { return 0, nil }
func (n *clusterInfoRaftNode) WaitApplied(context.Context, uint64) error { return nil }
func (n *clusterInfoRaftNode) ApplyCh() <-chan raft.LogEntry             { return nil }
func (n *clusterInfoRaftNode) SetTransport(
	func(string, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error),
	func(string, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error),
) {
}
func (n *clusterInfoRaftNode) SetInstallSnapshotTransport(
	func(string, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error),
) {
}
func (n *clusterInfoRaftNode) SetNoOpCommand([]byte)                {}
func (n *clusterInfoRaftNode) RegisterObserver(chan<- raft.Event)   {}
func (n *clusterInfoRaftNode) DeregisterObserver(chan<- raft.Event) {}
func (n *clusterInfoRaftNode) AddVoter(string, string) error        { return nil }
func (n *clusterInfoRaftNode) AddVoterCtx(context.Context, string, string) error {
	return nil
}
func (n *clusterInfoRaftNode) RemoveVoter(string) error        { return nil }
func (n *clusterInfoRaftNode) AddLearner(string, string) error { return nil }
func (n *clusterInfoRaftNode) PromoteToVoter(string) error     { return nil }
func (n *clusterInfoRaftNode) TransferLeadership() error       { return nil }
func (n *clusterInfoRaftNode) ChangeMembership(context.Context, []raft.ServerEntry, []string) error {
	return nil
}
func (n *clusterInfoRaftNode) HandleRequestVote(*raft.RequestVoteArgs) *raft.RequestVoteReply {
	return &raft.RequestVoteReply{}
}
func (n *clusterInfoRaftNode) HandleAppendEntries(*raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	return &raft.AppendEntriesReply{}
}
func (n *clusterInfoRaftNode) HandleInstallSnapshot(*raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	return &raft.InstallSnapshotReply{}
}
func (n *clusterInfoRaftNode) HandleTimeoutNow()                   {}
func (n *clusterInfoRaftNode) CreateSnapshot(uint64, []byte) error { return nil }
func (n *clusterInfoRaftNode) SnapshotStatus() (raft.SnapshotStatus, error) {
	return raft.SnapshotStatus{}, nil
}
func (n *clusterInfoRaftNode) LatestSnapshot() (*raft.Snapshot, error) { return nil, nil }
