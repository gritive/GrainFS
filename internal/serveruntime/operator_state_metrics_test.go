package serveruntime

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/stretchr/testify/require"
)

func TestOperatorClusterStateSource_UsesVoterMajority(t *testing.T) {
	source := operatorClusterStateSource{
		nodeID: "node-a",
		metaRaft: fakeMetaRaftOperatorSource{node: &fakeOperatorRaftNode{
			id: "node-a",
			cfg: raft.Configuration{Servers: []raft.Server{
				{ID: "node-a", Suffrage: raft.Voter},
				{ID: "node-b", Suffrage: raft.Voter},
				{ID: "node-c", Suffrage: raft.Voter},
				{ID: "node-d", Suffrage: raft.NonVoter},
			}},
		}},
		peers: fakeOperatorPeerHealth{peers: []admin.ClusterPeerInfo{
			{ID: "node-b", Healthy: true},
			{ID: "node-c", Healthy: false},
			{ID: "node-d", Healthy: true},
			{ID: "stale-node", Healthy: true},
		}},
	}

	got, err := source.ClusterStateSnapshot()
	require.NoError(t, err)
	require.True(t, got.QuorumAvailable)
	require.Equal(t, map[string]int{
		"self_voter":           1,
		"healthy_voter_peer":   1,
		"unhealthy_voter_peer": 1,
		"healthy_learner_peer": 1,
		"unknown_peer":         1,
	}, got.MembersByState)
}

func TestOperatorClusterStateSource_UsesJointConsensusMajorities(t *testing.T) {
	source := operatorClusterStateSource{
		nodeID: "a",
		metaRaft: fakeMetaRaftOperatorSource{node: &fakeOperatorRaftNode{
			id: "a",
			cfg: raft.Configuration{
				Servers: []raft.Server{
					{ID: "a", Suffrage: raft.Voter},
					{ID: "b", Suffrage: raft.Voter},
					{ID: "c", Suffrage: raft.Voter},
					{ID: "d", Suffrage: raft.Voter},
				},
				Joint:     true,
				OldVoters: []string{"a", "b", "c"},
				NewVoters: []string{"b", "c", "d"},
			},
		}},
		peers: fakeOperatorPeerHealth{peers: []admin.ClusterPeerInfo{
			{ID: "b", Healthy: true},
			{ID: "c", Healthy: false},
			{ID: "d", Healthy: false},
		}},
	}

	got, err := source.ClusterStateSnapshot()
	require.NoError(t, err)
	require.False(t, got.QuorumAvailable, "flat majority a+b is not enough for new voter set b+c+d")
}

func TestOperatorRaftStateSource_EmitsOnlyMetaAndPrimaryData(t *testing.T) {
	source := operatorRaftStateSource{
		nodeID: "node-a",
		metaRaft: fakeMetaRaftOperatorSource{
			lastApplied: 8,
			node:        &fakeOperatorRaftNode{id: "node-a", state: raft.Leader, term: 3, committed: 10},
		},
		dataNode: &fakeOperatorRaftNode{id: "node-a", state: raft.Follower, term: 4, committed: 12},
	}

	got, err := source.RaftStateSnapshot()
	require.NoError(t, err)
	require.Equal(t, []metrics.OperatorRaftState{
		{NodeID: "node-a", Group: "meta", Role: "leader", Term: 3, CommitIndex: 10, AppliedIndex: 8, HasAppliedIndex: true},
		{NodeID: "node-a", Group: "data", Role: "follower", Term: 4, CommitIndex: 12},
	}, got)
}

type fakeMetaRaftOperatorSource struct {
	node        *fakeOperatorRaftNode
	lastApplied uint64
}

func (f fakeMetaRaftOperatorSource) Node() dataRaftOperatorSource { return f.node }
func (f fakeMetaRaftOperatorSource) LastApplied() uint64          { return f.lastApplied }

type fakeOperatorPeerHealth struct {
	peers []admin.ClusterPeerInfo
}

func (f fakeOperatorPeerHealth) Snapshot() []admin.ClusterPeerInfo { return f.peers }

type fakeOperatorRaftNode struct {
	id        string
	state     raft.NodeState
	term      uint64
	committed uint64
	cfg       raft.Configuration
}

func (f *fakeOperatorRaftNode) Start()                                              {}
func (f *fakeOperatorRaftNode) Close()                                              {}
func (f *fakeOperatorRaftNode) ID() string                                          { return f.id }
func (f *fakeOperatorRaftNode) State() raft.NodeState                               { return f.state }
func (f *fakeOperatorRaftNode) Term() uint64                                        { return f.term }
func (f *fakeOperatorRaftNode) IsLeader() bool                                      { return f.state == raft.Leader }
func (f *fakeOperatorRaftNode) LeaderID() string                                    { return "" }
func (f *fakeOperatorRaftNode) CommittedIndex() uint64                              { return f.committed }
func (f *fakeOperatorRaftNode) LastLogIndex() uint64                                { return 0 }
func (f *fakeOperatorRaftNode) Configuration() raft.Configuration                   { return f.cfg }
func (f *fakeOperatorRaftNode) Peers() []string                                     { return nil }
func (f *fakeOperatorRaftNode) PeerMatchIndex(string) (uint64, bool)                { return 0, false }
func (f *fakeOperatorRaftNode) Bootstrap() error                                    { return nil }
func (f *fakeOperatorRaftNode) Propose([]byte) error                                { return nil }
func (f *fakeOperatorRaftNode) ProposeWait(context.Context, []byte) (uint64, error) { return 0, nil }
func (f *fakeOperatorRaftNode) ReadIndex(context.Context) (uint64, error)           { return 0, nil }
func (f *fakeOperatorRaftNode) WaitApplied(context.Context, uint64) error           { return nil }
func (f *fakeOperatorRaftNode) ApplyCh() <-chan raft.LogEntry                       { return nil }
func (f *fakeOperatorRaftNode) SetNoOpCommand([]byte)                               {}
func (f *fakeOperatorRaftNode) RegisterObserver(chan<- raft.Event)                  {}
func (f *fakeOperatorRaftNode) DeregisterObserver(chan<- raft.Event)                {}
func (f *fakeOperatorRaftNode) AddVoter(string, string) error                       { return nil }
func (f *fakeOperatorRaftNode) AddVoterCtx(context.Context, string, string) error   { return nil }
func (f *fakeOperatorRaftNode) AddLearner(string, string) error                     { return nil }
func (f *fakeOperatorRaftNode) PromoteToVoter(string) error                         { return nil }
func (f *fakeOperatorRaftNode) RemoveLearner(string) error                          { return nil }
func (f *fakeOperatorRaftNode) RemoveVoter(string) error                            { return nil }
func (f *fakeOperatorRaftNode) TransferLeadership() error                           { return nil }
func (f *fakeOperatorRaftNode) ChangeMembership(context.Context, []raft.ServerEntry, []string) error {
	return nil
}
func (f *fakeOperatorRaftNode) HandleRequestVote(*raft.RequestVoteArgs) *raft.RequestVoteReply {
	return nil
}
func (f *fakeOperatorRaftNode) HandleAppendEntries(*raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	return nil
}
func (f *fakeOperatorRaftNode) HandleInstallSnapshot(*raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	return nil
}
func (f *fakeOperatorRaftNode) HandleTimeoutNow()                   {}
func (f *fakeOperatorRaftNode) CreateSnapshot(uint64, []byte) error { return nil }
func (f *fakeOperatorRaftNode) SnapshotStatus() (raft.SnapshotStatus, error) {
	return raft.SnapshotStatus{}, nil
}
func (f *fakeOperatorRaftNode) LatestSnapshot() (*raft.Snapshot, error) { return nil, nil }
func (f *fakeOperatorRaftNode) SetTransport(
	func(string, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error),
	func(string, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error),
) {
}
func (f *fakeOperatorRaftNode) SetInstallSnapshotTransport(func(string, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error)) {
}
func (f *fakeOperatorRaftNode) SetTimeoutNowTransport(func(string, *raft.TimeoutNowArgs) (*raft.TimeoutNowReply, error)) {
}
