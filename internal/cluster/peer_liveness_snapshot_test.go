package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildPeerLivenessSnapshot_ComposesIdentityAndLivenessSignals(t *testing.T) {
	now := time.Unix(1000, 0)
	rows := BuildPeerLivenessSnapshot(PeerLivenessInput{
		SelfID: "n1",
		Voters: []string{
			"n2",
			"10.0.0.3:7001",
			"10.0.0.9:7001",
			"n4",
			"n5",
			"n6",
		},
		AddressBook: fakePeerSnapshotAddressBook{nodes: []MetaNodeEntry{
			{ID: "n2", Address: "10.0.0.2:7001"},
			{ID: "n3", Address: "10.0.0.3:7001"},
			{ID: "n4", Address: "10.0.0.4:7001"},
			{ID: "n5", Address: "10.0.0.5:7001"},
			{ID: "n6", Address: "10.0.0.6:7001"},
		}},
		PeerHealth: []PeerHealthEntry{
			{ID: "n4", Healthy: false, CooldownRemainingMs: 9000},
			{ID: "n5", Healthy: false, CooldownRemainingMs: 9000},
		},
		ProbeResults: []PeerProbeResult{
			{PeerID: "n2", Live: true, ObservedAt: now},
			{PeerID: "n5", Live: true, ObservedAt: now},
			{PeerID: "n6", Live: false, ObservedAt: now, Reason: "dial_failed"},
		},
	})

	require.Equal(t, []PeerLivenessRow{
		{PeerID: "n1", IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessLive, Reason: "self"},
		{PeerID: "n2", RaftAddr: "10.0.0.2:7001", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive, Reason: "probe_live"},
		{PeerID: "n3", RaftAddr: "10.0.0.3:7001", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessConfigured, Reason: "configured"},
		{PeerID: "10.0.0.9:7001", RaftAddr: "10.0.0.9:7001", IdentityState: PeerIdentityUnresolvedLegacy, LivenessState: PeerLivenessConfigured, Reason: "identity_unresolved"},
		{PeerID: "n4", RaftAddr: "10.0.0.4:7001", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessHealthCooldown, Reason: "peer_health_cooldown"},
		{PeerID: "n5", RaftAddr: "10.0.0.5:7001", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive, Reason: "probe_live"},
		{PeerID: "n6", RaftAddr: "10.0.0.6:7001", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessProbeFailed, Reason: "dial_failed"},
	}, rows)
}

func TestPeerLivenessPredicates(t *testing.T) {
	require.True(t, IsExplicitlyDown(PeerLivenessRow{LivenessState: PeerLivenessHealthCooldown}))
	require.True(t, IsExplicitlyDown(PeerLivenessRow{LivenessState: PeerLivenessProbeFailed}))
	require.False(t, IsExplicitlyDown(PeerLivenessRow{LivenessState: PeerLivenessConfigured}))

	require.True(t, IsAliveForMembershipMutation(PeerLivenessRow{IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessConfigured}))
	require.True(t, IsAliveForMembershipMutation(PeerLivenessRow{IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive}))
	require.False(t, IsAliveForMembershipMutation(PeerLivenessRow{IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessConfigured}))

	require.True(t, BlocksMembershipMutation(PeerLivenessRow{IdentityState: PeerIdentityUnresolvedLegacy}))
	require.False(t, BlocksMembershipMutation(PeerLivenessRow{IdentityState: PeerIdentityResolved}))
}

func TestBuildPeerLivenessSnapshot_ProbeSuccessWinsOverFailure(t *testing.T) {
	now := time.Unix(1000, 0)
	rows := BuildPeerLivenessSnapshot(PeerLivenessInput{
		SelfID: "n1",
		Voters: []string{"n2"},
		AddressBook: fakePeerSnapshotAddressBook{nodes: []MetaNodeEntry{
			{ID: "n2", Address: "10.0.0.2:7001"},
		}},
		PeerHealth: []PeerHealthEntry{{ID: "n2", Healthy: false, CooldownRemainingMs: 9000}},
		ProbeResults: []PeerProbeResult{
			{PeerID: "n2", Live: true, ObservedAt: now},
			{PeerID: "n2", Live: false, ObservedAt: now.Add(time.Second), Reason: "dial_failed"},
		},
	})

	require.Equal(t, PeerLivenessLive, rows[1].LivenessState)
	require.Equal(t, "probe_live", rows[1].Reason)
}

type fakePeerSnapshotAddressBook struct {
	nodes []MetaNodeEntry
}

func (f fakePeerSnapshotAddressBook) Nodes() []MetaNodeEntry {
	return f.nodes
}
