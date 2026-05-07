package server

import (
	"testing"

	"github.com/stretchr/testify/require"

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
