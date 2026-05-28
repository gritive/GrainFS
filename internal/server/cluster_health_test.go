package server

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/cluster"
)

// TestMapPeerHealthRows_KnownAndUnknownStates verifies that mapPeerHealthRows
// renders known states with stable wire labels and falls through unknown
// states to their raw internal name (instead of silently masking them as
// "configured"). This guards against future PeerLivenessState additions
// being invisible in the dashboard / CLI.
func TestMapPeerHealthRows_KnownAndUnknownStates(t *testing.T) {
	rows := []cluster.PeerLivenessRow{
		{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive},
		{PeerID: "n2", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive},
		{PeerID: "n3", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessHealthCooldown},
		{PeerID: "n4", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessProbeFailed},
		{PeerID: "n5", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessConfigured},
		{PeerID: "n6", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessState("future_state")},
	}
	got := mapPeerHealthRows(rows)
	require.Len(t, got, 6)
	require.Equal(t, "self", got[0].State)
	require.Equal(t, "live", got[1].State)
	require.Equal(t, "cooldown", got[2].State)
	require.Equal(t, "down", got[3].State)
	require.Equal(t, "configured", got[4].State)
	// Unknown future state passes through verbatim — visible to operators.
	require.Equal(t, "future_state", got[5].State)
}

func TestBuildClusterHealth_IncludesDataGroupRaftHealth(t *testing.T) {
	info := &fakeClusterInfo{
		nodeID:   "n1",
		state:    "Leader",
		term:     7,
		leaderID: "n1",
		peers:    []string{"n2", "n3"},
		snapshot: []cluster.PeerLivenessRow{
			{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "n2", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "n3", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive},
		},
		status: cluster.ClusterStatus{
			DataGroupRaftHealth: []cluster.DataGroupRaftHealth{
				{
					GroupID:        "group-1",
					PeerIDs:        []string{"n1", "n2", "n3"},
					LocalState:     "Follower",
					LeaderID:       "n2",
					Term:           8,
					CommitIndex:    1204,
					LastLogIndex:   1204,
					PeerMatchIndex: map[string]uint64{"n1": 1204, "n2": 1204, "n3": 1200},
					MaxPeerLag:     4,
					Issues:         []string{"peer_lag"},
				},
			},
		},
	}

	h := buildClusterHealth(info, false)
	require.NotNil(t, h.DataGroups)
	require.Equal(t, 1, h.DataGroups.Total)
	require.Equal(t, 0, h.DataGroups.Healthy)
	require.Equal(t, 1, h.DataGroups.Lagging)
	require.Len(t, h.DataGroups.Groups, 1)
	require.Equal(t, "group-1", h.DataGroups.Groups[0].GroupID)
	require.Equal(t, uint64(4), h.DataGroups.Groups[0].MaxPeerLag)
}

func TestDeriveIssues_LocalMode_WithConfiguredPeers(t *testing.T) {
	h := Health{Mode: "local"}
	issues := deriveIssues(h, true)
	require.Contains(t, issues, "single-node mode")
}

func TestDeriveIssues_LocalMode_NoPeers(t *testing.T) {
	h := Health{Mode: "local"}
	issues := deriveIssues(h, false)
	require.Empty(t, issues, "pure local mode (no configured peers) should be silent")
}

func TestDeriveIssues_HealthyQuorum_NoIssues(t *testing.T) {
	h := Health{
		Mode:   "cluster",
		Quorum: QuorumInfo{VotersTotal: 3, AliveCount: 3, Required: 2, Healthy: true},
		Peers: []PeerHealthRow{
			{PeerID: "n1", State: "self"},
			{PeerID: "n2", State: "live"},
			{PeerID: "n3", State: "live"},
		},
	}
	require.Empty(t, deriveIssues(h, true))
}

func TestDeriveIssues_VoterDown(t *testing.T) {
	h := Health{
		Mode:   "cluster",
		Quorum: QuorumInfo{VotersTotal: 3, AliveCount: 2, Required: 2, Healthy: true},
		Peers: []PeerHealthRow{
			{PeerID: "n1", State: "self"},
			{PeerID: "n2", State: "live"},
			{PeerID: "n3", State: "down"},
		},
	}
	issues := deriveIssues(h, true)
	require.Contains(t, issues, "voter n3 down — investigate")
}

func TestDeriveIssues_VoterCooldown(t *testing.T) {
	h := Health{
		Mode:   "cluster",
		Quorum: QuorumInfo{VotersTotal: 3, AliveCount: 2, Required: 2, Healthy: true},
		Peers: []PeerHealthRow{
			{PeerID: "n1", State: "self"},
			{PeerID: "n2", State: "live"},
			{PeerID: "n3", State: "cooldown"},
		},
	}
	issues := deriveIssues(h, true)
	require.Contains(t, issues, "voter n3 cooldown — investigate")
}

func TestDeriveIssues_QuorumLost(t *testing.T) {
	h := Health{
		Mode:   "cluster",
		Quorum: QuorumInfo{VotersTotal: 3, AliveCount: 1, Required: 2, Healthy: false},
	}
	issues := deriveIssues(h, true)
	require.NotEmpty(t, issues)
	require.Contains(t, issues[0], "QUORUM LOST")
	require.Contains(t, issues[0], "1/3")
}

func TestDeriveIssues_DegradedFlag(t *testing.T) {
	h := Health{
		Mode:     "cluster",
		Degraded: true,
		Quorum:   QuorumInfo{VotersTotal: 3, AliveCount: 3, Required: 2, Healthy: true},
		Peers: []PeerHealthRow{
			{PeerID: "n1", State: "self"},
			{PeerID: "n2", State: "live"},
			{PeerID: "n3", State: "live"},
		},
	}
	issues := deriveIssues(h, true)
	require.Contains(t, issues, "EC degraded mode")
}

func TestDeriveIssues_DataGroupLeaderless(t *testing.T) {
	h := Health{
		Mode:   "cluster",
		Quorum: QuorumInfo{VotersTotal: 3, AliveCount: 3, Required: 2, Healthy: true},
		DataGroups: &adminapi.DataGroupHealthSummary{
			Total:      1,
			Leaderless: 1,
			Groups: []adminapi.DataGroupHealthRow{
				{GroupID: "group-2", Issues: []string{"leaderless"}},
			},
		},
	}
	require.Contains(t, deriveIssues(h, true), "data group group-2 leaderless")
}

func TestDeriveIssues_DataGroupPeerLag(t *testing.T) {
	h := Health{
		Mode:   "cluster",
		Quorum: QuorumInfo{VotersTotal: 3, AliveCount: 3, Required: 2, Healthy: true},
		DataGroups: &adminapi.DataGroupHealthSummary{
			Total:   1,
			Lagging: 1,
			Groups: []adminapi.DataGroupHealthRow{
				{GroupID: "group-3", MaxPeerLag: 421, Issues: []string{"peer_lag"}},
			},
		},
	}
	require.Contains(t, deriveIssues(h, true), "data group group-3 peer replication lag 421")
}
