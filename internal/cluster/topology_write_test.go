package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlacementTargetsFromContext_RequiresFullGroup(t *testing.T) {
	group := ShardGroupEntry{ID: "group-1", PeerIDs: []string{"n1", "n2", "n3"}}
	targets, cfg, err := placementTargetsFromContext(ContextWithPlacementGroupEntry(context.Background(), group), "put_object")
	require.NoError(t, err)
	require.Equal(t, ECConfig{DataShards: 2, ParityShards: 1}, cfg)
	require.Equal(t, []string{"n1", "n2", "n3"}, targets.PeerIDs)
}

func TestPlacementTargetsFromContext_RejectsMissingFullGroup(t *testing.T) {
	_, _, err := placementTargetsFromContext(ContextWithPlacementGroup(context.Background(), "group-1"), "put_object")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPlacementTargetsUnavailable))
}

func TestPlanObjectWritePlacement_UsesFullGroupPlan(t *testing.T) {
	group := ShardGroupEntry{ID: "group-1", PeerIDs: []string{"n1", "n2", "n3"}}
	plan, err := PlanObjectWritePlacement(ObjectWritePlacementInput{
		Operation:        "put_object",
		PlacementGroupID: "group-1",
		PlacementGroup:   &group,
		LiveNodes:        []string{"fallback-1", "fallback-2", "fallback-3"},
		CurrentECConfig:  ECConfig{DataShards: 2, ParityShards: 1},
		ShardKey:         "k/v1",
	})
	require.NoError(t, err)
	require.True(t, plan.TopologyWrite)
	require.Equal(t, "group-1", plan.PlacementGroupID)
	require.Equal(t, ECConfig{DataShards: 2, ParityShards: 1}, plan.Config)
	require.Equal(t, []string{"n1", "n2", "n3"}, plan.NodeIDs)
}

func TestPlanObjectWritePlacement_UsesFallbackPlacementWithoutFullGroup(t *testing.T) {
	liveNodes := []string{"n1", "n2", "n3"}
	plan, err := PlanObjectWritePlacement(ObjectWritePlacementInput{
		Operation:        "put_object",
		PlacementGroupID: "group-1",
		LiveNodes:        liveNodes,
		CurrentECConfig:  ECConfig{DataShards: 2, ParityShards: 1},
		ShardKey:         "k/v1",
	})
	require.NoError(t, err)
	require.False(t, plan.TopologyWrite)
	require.Equal(t, "group-1", plan.PlacementGroupID)
	require.Equal(t, ECConfig{DataShards: 2, ParityShards: 1}, plan.Config)
	require.Equal(t, PlaceShards("k/v1", liveNodes, nil, 3), plan.NodeIDs)
}

func TestPlanObjectWritePlacement_UsesNodeStateSnapshotForWeightedFallback(t *testing.T) {
	liveNodes := []string{"n1", "n2", "n3", "n4"}
	count := make(map[string]int)
	for i := 0; i < 1000; i++ {
		plan, err := PlanObjectWritePlacement(ObjectWritePlacementInput{
			Operation:        "put_object",
			PlacementGroupID: "group-1",
			LiveNodes:        liveNodes,
			CurrentECConfig:  ECConfig{DataShards: 2, ParityShards: 1},
			ShardKey:         fmt.Sprintf("k/%d", i),
			NodeStates: []ObjectWritePlacementNodeState{
				{NodeID: "n1", DiskAvailBytes: 4_000_000_000_000},
				{NodeID: "n2", DiskAvailBytes: 1_000_000_000_000},
				{NodeID: "n3", DiskAvailBytes: 1_000_000_000_000},
				{NodeID: "n4", DiskAvailBytes: 1_000_000_000_000},
			},
			WeightedHRWEnabled: true,
		})
		require.NoError(t, err)
		for _, nodeID := range plan.NodeIDs {
			count[nodeID]++
		}
	}
	require.Greater(t, count["n1"], 900)
	for _, nodeID := range []string{"n2", "n3", "n4"} {
		require.Greater(t, count["n1"], count[nodeID])
	}
}

func TestPlanObjectWritePlacement_RejectsInvalidFullGroupPlan(t *testing.T) {
	group := ShardGroupEntry{ID: "group-1"}
	_, err := PlanObjectWritePlacement(ObjectWritePlacementInput{
		Operation:        "put_object",
		PlacementGroupID: "group-1",
		PlacementGroup:   &group,
		LiveNodes:        []string{"fallback-1", "fallback-2", "fallback-3"},
		CurrentECConfig:  ECConfig{DataShards: 2, ParityShards: 1},
		ShardKey:         "k/v1",
	})
	require.ErrorIs(t, err, ErrPlacementTargetsUnavailable)
	require.ErrorContains(t, err, "placement group has no zero-config EC profile")
}

func TestPlanObjectWritePlacement_RejectsUnhealthyTopologyTarget(t *testing.T) {
	group := ShardGroupEntry{ID: "group-1", PeerIDs: []string{"n1", "n2", "n3"}}
	_, err := PlanObjectWritePlacement(ObjectWritePlacementInput{
		Operation:        "put_object",
		PlacementGroupID: "group-1",
		PlacementGroup:   &group,
		LiveNodes:        []string{"n1", "n2", "n3"},
		CurrentECConfig:  ECConfig{DataShards: 2, ParityShards: 1},
		ShardKey:         "k/v1",
		PeerHealth: []PeerHealthEntry{
			{ID: "n1", Healthy: true},
			{ID: "n2", Healthy: false},
			{ID: "n3", Healthy: true},
		},
		HasPeerHealth: true,
	})
	require.ErrorIs(t, err, ErrPlacementTargetsUnavailable)
	require.ErrorContains(t, err, "known unhealthy placement target")
}
