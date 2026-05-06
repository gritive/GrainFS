package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEvaluateRemovePeerPreflight_ConfiguredDoesNotCountAsAlive(t *testing.T) {
	result := EvaluateRemovePeerPreflight(RemovePeerPreflightInput{
		TargetID: "n3",
		Voters:   []string{"n2", "n3"},
		Snapshot: []PeerLivenessRow{
			{PeerID: "n1", IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessLive},
			{PeerID: "n2", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessConfigured},
			{PeerID: "n3", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
		},
	})

	require.False(t, result.Allowed)
	require.Equal(t, RemovePeerPreflightQuorumWouldBreak, result.Reason)
	require.Equal(t, 2, result.VotersAfter)
	require.Equal(t, 1, result.AliveAfter)
	require.Equal(t, 2, result.NewQuorum)
}

func TestEvaluateRemovePeerPreflight_LivePeersAndSelfSatisfyQuorum(t *testing.T) {
	result := EvaluateRemovePeerPreflight(RemovePeerPreflightInput{
		TargetID: "n3",
		Voters:   []string{"n2", "n3"},
		Snapshot: []PeerLivenessRow{
			{PeerID: "n1", IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessConfigured},
			{PeerID: "n2", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
			{PeerID: "n3", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
		},
	})

	require.True(t, result.Allowed)
	require.Equal(t, RemovePeerPreflightAllowed, result.Reason)
	require.Equal(t, 2, result.VotersAfter)
	require.Equal(t, 2, result.AliveAfter)
	require.Equal(t, 2, result.NewQuorum)
}

func TestEvaluateRemovePeerPreflight_ExplicitDownDoesNotCountAsAlive(t *testing.T) {
	result := EvaluateRemovePeerPreflight(RemovePeerPreflightInput{
		TargetID: "n3",
		Voters:   []string{"n2", "n3"},
		Snapshot: []PeerLivenessRow{
			{PeerID: "n1", IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessLive},
			{PeerID: "n2", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessProbeFailed},
			{PeerID: "n3", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
		},
	})

	require.False(t, result.Allowed)
	require.Equal(t, RemovePeerPreflightQuorumWouldBreak, result.Reason)
	require.Equal(t, 1, result.AliveAfter)
}

func TestEvaluateRemovePeerPreflight_UnresolvedLegacyBlocksOtherRemoval(t *testing.T) {
	result := EvaluateRemovePeerPreflight(RemovePeerPreflightInput{
		TargetID: "n2",
		Voters:   []string{"n2", "10.0.0.9:7001"},
		Snapshot: []PeerLivenessRow{
			{PeerID: "n1", IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessLive},
			{PeerID: "n2", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
			{PeerID: "10.0.0.9:7001", IdentityState: PeerIdentityUnresolvedLegacy, LivenessState: PeerLivenessConfigured},
		},
	})

	require.False(t, result.Allowed)
	require.Equal(t, RemovePeerPreflightIdentityUnresolved, result.Reason)
	require.Equal(t, []string{"10.0.0.9:7001"}, result.BlockingPeers)
}

func TestEvaluateRemovePeerPreflight_AllowsRemovingUnresolvedLegacyTarget(t *testing.T) {
	result := EvaluateRemovePeerPreflight(RemovePeerPreflightInput{
		TargetID: "10.0.0.9:7001",
		Voters:   []string{"n2", "10.0.0.9:7001"},
		Snapshot: []PeerLivenessRow{
			{PeerID: "n1", IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessLive},
			{PeerID: "n2", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
			{PeerID: "10.0.0.9:7001", IdentityState: PeerIdentityUnresolvedLegacy, LivenessState: PeerLivenessConfigured},
		},
	})

	require.True(t, result.Allowed)
	require.Equal(t, RemovePeerPreflightAllowed, result.Reason)
	require.Empty(t, result.BlockingPeers)
	require.Equal(t, 2, result.AliveAfter)
}

func TestEvaluateRemovePeerPreflight_TargetNotInCluster(t *testing.T) {
	result := EvaluateRemovePeerPreflight(RemovePeerPreflightInput{
		TargetID: "n9",
		Voters:   []string{"n2", "n3"},
		Snapshot: []PeerLivenessRow{
			{PeerID: "n1", IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessLive},
			{PeerID: "n2", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
			{PeerID: "n3", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
		},
	})

	require.False(t, result.Allowed)
	require.Equal(t, RemovePeerPreflightNotInCluster, result.Reason)
}
