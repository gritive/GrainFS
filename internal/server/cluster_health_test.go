package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
