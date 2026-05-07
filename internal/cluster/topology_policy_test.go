package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDesiredECConfigForGroup_UsesZeroConfigTable(t *testing.T) {
	cases := []struct {
		name  string
		peers []string
		want  ECConfig
	}{
		{"one", []string{"n1"}, ECConfig{DataShards: 1, ParityShards: 0}},
		{"two", []string{"n1", "n2"}, ECConfig{DataShards: 1, ParityShards: 1}},
		{"three", []string{"n1", "n2", "n3"}, ECConfig{DataShards: 2, ParityShards: 1}},
		{"four", []string{"n1", "n2", "n3", "n4"}, ECConfig{DataShards: 2, ParityShards: 2}},
		{"five", []string{"n1", "n2", "n3", "n4", "n5"}, ECConfig{DataShards: 3, ParityShards: 2}},
		{"six", []string{"n1", "n2", "n3", "n4", "n5", "n6"}, ECConfig{DataShards: 4, ParityShards: 2}},
		{"seven", []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7"}, ECConfig{DataShards: 5, ParityShards: 2}},
		{"eight", []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8"}, ECConfig{DataShards: 6, ParityShards: 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DesiredECConfigForGroup(ShardGroupEntry{ID: "g", PeerIDs: tc.peers})
			require.Equal(t, tc.want, got)
		})
	}
}

func TestCompareECProfile_TableOrder(t *testing.T) {
	require.Equal(t, ProfileLower, CompareECProfile(ECConfig{DataShards: 2, ParityShards: 1}, ECConfig{DataShards: 4, ParityShards: 2}))
	require.Equal(t, ProfileEqual, CompareECProfile(ECConfig{DataShards: 4, ParityShards: 2}, ECConfig{DataShards: 4, ParityShards: 2}))
	require.Equal(t, ProfileHigher, CompareECProfile(ECConfig{DataShards: 6, ParityShards: 2}, ECConfig{DataShards: 4, ParityShards: 2}))
	require.Equal(t, ProfileUnknown, CompareECProfile(ECConfig{DataShards: 9, ParityShards: 3}, ECConfig{DataShards: 4, ParityShards: 2}))
}

func TestClassifyObjectLayout(t *testing.T) {
	group := ShardGroupEntry{ID: "group-1", PeerIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6"}}

	require.Equal(t, LayoutPendingUpgrade, ClassifyObjectLayout(ObjectIndexEntry{PlacementGroupID: "group-1", ECData: 2, ECParity: 1, NodeIDs: []string{"n1", "n2", "n3"}}, group, RepairSignals{}))
	require.Equal(t, LayoutCurrent, ClassifyObjectLayout(ObjectIndexEntry{PlacementGroupID: "group-1", ECData: 4, ECParity: 2, NodeIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6"}}, group, RepairSignals{}))
	require.Equal(t, LayoutDowngradeSkipped, ClassifyObjectLayout(ObjectIndexEntry{PlacementGroupID: "group-1", ECData: 6, ECParity: 2, NodeIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8"}}, group, RepairSignals{}))
	require.Equal(t, LayoutRepairNeeded, ClassifyObjectLayout(ObjectIndexEntry{PlacementGroupID: "group-1", ECData: 4, ECParity: 2}, group, RepairSignals{}))
	require.Equal(t, LayoutRepairNeeded, ClassifyObjectLayout(ObjectIndexEntry{PlacementGroupID: "group-1", ECData: 4, ECParity: 2, NodeIDs: []string{"missing"}}, group, RepairSignals{}))
	require.Equal(t, LayoutUnknown, ClassifyObjectLayout(ObjectIndexEntry{PlacementGroupID: "missing", ECData: 4, ECParity: 2, NodeIDs: []string{"n1"}}, ShardGroupEntry{}, RepairSignals{}))
}

func TestErrInsufficientPlacementTargets(t *testing.T) {
	err := &ErrInsufficientPlacementTargets{
		Operation:     "put_object",
		GroupID:       "group-1",
		Desired:       ECConfig{DataShards: 4, ParityShards: 2},
		Configured:    []string{"n1", "n2", "n3", "n4", "n5", "n6"},
		Unavailable:   []string{"n6"},
		FailureReason: "write target not reachable",
	}
	require.ErrorContains(t, err, "put_object")
	require.ErrorContains(t, err, "group-1")
	require.True(t, errors.Is(err, ErrPlacementTargetsUnavailable))
}
