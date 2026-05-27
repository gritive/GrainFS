package serveruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server/execution"
)

func TestScrubExecutionBackendPreservesDedupResult(t *testing.T) {
	director := scrubber.NewDirector(scrubber.DirectorOpts{QueueSize: 1, NodeID: "n1"})
	director.Start(context.Background())
	defer director.Stop()
	req := scrubber.TriggerReq{
		Bucket:    "ec1",
		KeyPrefix: "prefix/",
		DryRun:    true,
	}
	existingSessionID, created := director.Trigger(req)
	require.NotEmpty(t, existingSessionID)
	require.True(t, created)

	proposer := NewScrubProposerAdapter(nil, director, "n1")
	backend := NewScrubExecutionBackend(proposer)

	got, err := backend.TriggerScrub(context.Background(), execution.Operation{
		ID:   "operation-id-not-session-id",
		Kind: execution.OperationScrub,
		Scrub: execution.ScrubOperation{
			Bucket:    req.Bucket,
			KeyPrefix: req.KeyPrefix,
			DryRun:    req.DryRun,
		},
	})

	require.NoError(t, err)
	require.Equal(t, execution.ScrubResult{SessionID: existingSessionID, Created: false}, got)
	require.NotEqual(t, "operation-id-not-session-id", got.SessionID)
}

func TestScrubExecutionBackendRejectsMissingProposer(t *testing.T) {
	backend := NewScrubExecutionBackend(nil)

	_, err := backend.TriggerScrub(context.Background(), execution.Operation{
		Kind:  execution.OperationScrub,
		Scrub: execution.ScrubOperation{Bucket: "ec1"},
	})

	require.ErrorIs(t, err, execution.ErrExecutionUnsupported)
	require.Equal(t, execution.CodeUnsupported, execution.CodeOf(err))
}

func TestScrubExecutionBackendRejectsUnsupportedOperationKind(t *testing.T) {
	director := scrubber.NewDirector(scrubber.DirectorOpts{})
	director.Start(context.Background())
	defer director.Stop()
	backend := NewScrubExecutionBackend(NewScrubProposerAdapter(nil, director, "n1"))

	_, err := backend.TriggerScrub(context.Background(), execution.Operation{})

	require.Error(t, err)
	require.True(t, errors.Is(err, execution.ErrExecutionUnsupported))
	require.Equal(t, execution.CodeUnsupported, execution.CodeOf(err))
}

func TestFreshReplicationProbeResultsKeepsOnlyFreshSuccessEvidence(t *testing.T) {
	now := time.Date(2026, 5, 7, 10, 0, 0, 0, time.UTC)

	got := freshReplicationProbeResults([]raft.PeerReplicationEvidence{
		{PeerID: "n2", LastAppendSuccess: now.Add(-time.Second)},
		{PeerID: "n3", LastAppendSuccess: now.Add(-3 * time.Second)},
	}, nil, now, 2*time.Second)

	require.Equal(t, []cluster.PeerProbeResult{
		{
			PeerID:     "n2",
			Live:       true,
			ObservedAt: now.Add(-time.Second),
			Reason:     "raft_append_success",
		},
	}, got)
}

func TestFreshReplicationProbeResultsNormalizesRaftAddressEvidenceToNodeID(t *testing.T) {
	now := time.Date(2026, 5, 7, 10, 0, 0, 0, time.UTC)

	got := freshReplicationProbeResults([]raft.PeerReplicationEvidence{
		{PeerID: "10.0.0.2:7001", LastAppendSuccess: now.Add(-time.Second)},
	}, fakeAddressBook{nodes: []cluster.MetaNodeEntry{
		{ID: "n2", Address: "10.0.0.2:7001"},
	}}, now, 2*time.Second)

	require.Equal(t, []cluster.PeerProbeResult{
		{
			PeerID:     "n2",
			Live:       true,
			ObservedAt: now.Add(-time.Second),
			Reason:     "raft_append_success",
		},
	}, got)
}

func TestRaftMembershipRemoveVoterResolvesRemoteNodeIDToRaftAddress(t *testing.T) {
	node := &fakeMembershipNode{id: "n1"}
	membership := &RaftMembership{
		node: node,
		addrBook: fakeAddressBook{nodes: []cluster.MetaNodeEntry{
			{ID: "n2", Address: "10.0.0.2:7001"},
		}},
	}

	require.NoError(t, membership.RemoveVoter(context.Background(), "n2"))

	require.Equal(t, [][]string{{"10.0.0.2:7001"}}, node.removes)
}

func TestRaftMembershipRemoveVoterKeepsSelfNodeID(t *testing.T) {
	node := &fakeMembershipNode{id: "n1"}
	membership := &RaftMembership{
		node: node,
		addrBook: fakeAddressBook{nodes: []cluster.MetaNodeEntry{
			{ID: "n1", Address: "10.0.0.1:7001"},
		}},
	}

	require.NoError(t, membership.RemoveVoter(context.Background(), "n1"))

	require.Equal(t, [][]string{{"n1"}}, node.removes)
}

func TestRaftClusterInfoLeaderIDNormalizesRaftAddress(t *testing.T) {
	info := NewRaftClusterInfo(&fakeRaftNode{
		id:       "n1",
		leaderID: "10.0.0.2:7001",
	}, nil, nil, fakeAddressBook{nodes: []cluster.MetaNodeEntry{
		{ID: "n2", Address: "10.0.0.2:7001"},
	}})

	require.Equal(t, "n2", info.LeaderID())
}

type fakeAddressBook struct {
	nodes []cluster.MetaNodeEntry
}

func (f fakeAddressBook) Nodes() []cluster.MetaNodeEntry {
	return f.nodes
}

type fakeMembershipNode struct {
	id      string
	removes [][]string
}

func (f *fakeMembershipNode) ID() string { return f.id }

func (f *fakeMembershipNode) ChangeMembership(_ context.Context, _ []raft.ServerEntry, removes []string) error {
	f.removes = append(f.removes, append([]string(nil), removes...))
	return nil
}

// transferLeaderConformance mirrors the unexported server.clusterTransferLeader
// interface — the transfer-leader handler type-asserts s.cluster against it, so
// RaftClusterInfo must satisfy this method set or the endpoint 503s.
type transferLeaderConformance interface {
	TransferLeadership() error
	IsLeader() bool
}

func TestRaftClusterInfo_SatisfiesTransferLeader(t *testing.T) {
	var _ transferLeaderConformance = (*RaftClusterInfo)(nil)
}

func TestRaftClusterInfo_PeerSnapshotMarksLivePeersLiveWithoutReplicationEvidence(t *testing.T) {
	info := NewRaftClusterInfo(&fakeRaftNode{
		id:    "n1",
		peers: []string{"10.0.0.2:7001", "10.0.0.3:7001"},
	}, nil, nil, fakeAddressBook{nodes: []cluster.MetaNodeEntry{
		{ID: "n2", Address: "10.0.0.2:7001"},
		{ID: "n3", Address: "10.0.0.3:7001"},
	}})

	got := info.PeerSnapshot()

	require.Equal(t, []cluster.PeerLivenessRow{
		{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive, Reason: "self"},
		{PeerID: "n2", RaftAddr: "10.0.0.2:7001", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive, Reason: "probe_live"},
		{PeerID: "n3", RaftAddr: "10.0.0.3:7001", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive, Reason: "probe_live"},
	}, got)
}

type fakeRaftNode struct {
	id       string
	leaderID string
	peers    []string
}

func (f *fakeRaftNode) Start()                {}
func (f *fakeRaftNode) Close()                {}
func (f *fakeRaftNode) ID() string            { return f.id }
func (f *fakeRaftNode) State() raft.NodeState { return raft.Leader }
func (f *fakeRaftNode) Term() uint64          { return 1 }
func (f *fakeRaftNode) IsLeader() bool        { return true }
func (f *fakeRaftNode) LeaderID() string {
	if f.leaderID != "" {
		return f.leaderID
	}
	return f.id
}
func (f *fakeRaftNode) CommittedIndex() uint64                              { return 0 }
func (f *fakeRaftNode) LastLogIndex() uint64                                { return 0 }
func (f *fakeRaftNode) Configuration() raft.Configuration                   { return raft.Configuration{} }
func (f *fakeRaftNode) Peers() []string                                     { return append([]string(nil), f.peers...) }
func (f *fakeRaftNode) PeerMatchIndex(string) (uint64, bool)                { return 0, false }
func (f *fakeRaftNode) Bootstrap() error                                    { return nil }
func (f *fakeRaftNode) Propose([]byte) error                                { return nil }
func (f *fakeRaftNode) ProposeWait(context.Context, []byte) (uint64, error) { return 0, nil }
func (f *fakeRaftNode) ReadIndex(context.Context) (uint64, error)           { return 0, nil }
func (f *fakeRaftNode) WaitApplied(context.Context, uint64) error           { return nil }
func (f *fakeRaftNode) ApplyCh() <-chan raft.LogEntry                       { return nil }
func (f *fakeRaftNode) SetTransport(func(string, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error), func(string, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error)) {
}
func (f *fakeRaftNode) SetInstallSnapshotTransport(func(string, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error)) {
}
func (f *fakeRaftNode) SetTimeoutNowTransport(func(string, *raft.TimeoutNowArgs) (*raft.TimeoutNowReply, error)) {
}
func (f *fakeRaftNode) SetNoOpCommand([]byte)                             {}
func (f *fakeRaftNode) RegisterObserver(chan<- raft.Event)                {}
func (f *fakeRaftNode) DeregisterObserver(chan<- raft.Event)              {}
func (f *fakeRaftNode) AddVoter(string, string) error                     { return nil }
func (f *fakeRaftNode) AddVoterCtx(context.Context, string, string) error { return nil }
func (f *fakeRaftNode) RemoveVoter(string) error                          { return nil }
func (f *fakeRaftNode) AddLearner(string, string) error                   { return nil }
func (f *fakeRaftNode) PromoteToVoter(string) error                       { return nil }
func (f *fakeRaftNode) RemoveLearner(string) error                        { return nil }
func (f *fakeRaftNode) TransferLeadership() error                         { return nil }
func (f *fakeRaftNode) ChangeMembership(context.Context, []raft.ServerEntry, []string) error {
	return nil
}
func (f *fakeRaftNode) HandleRequestVote(*raft.RequestVoteArgs) *raft.RequestVoteReply { return nil }
func (f *fakeRaftNode) HandleAppendEntries(*raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	return nil
}
func (f *fakeRaftNode) HandleInstallSnapshot(*raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	return nil
}
func (f *fakeRaftNode) HandleTimeoutNow()                   {}
func (f *fakeRaftNode) CreateSnapshot(uint64, []byte) error { return nil }
func (f *fakeRaftNode) SnapshotStatus() (raft.SnapshotStatus, error) {
	return raft.SnapshotStatus{}, nil
}
func (f *fakeRaftNode) LatestSnapshot() (*raft.Snapshot, error) { return nil, nil }
