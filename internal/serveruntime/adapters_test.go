package serveruntime

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/raft"
)

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
