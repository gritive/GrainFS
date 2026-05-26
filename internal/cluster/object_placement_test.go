package cluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectECPlacement_WeightedFromDiskAvail(t *testing.T) {
	liveNodes := []string{"n1", "n2", "n3", "n4"}
	nodeStates := []ObjectWritePlacementNodeState{
		{NodeID: "n1", DiskAvailBytes: 4_000_000_000_000},
		{NodeID: "n2", DiskAvailBytes: 1_000_000_000_000},
		{NodeID: "n3", DiskAvailBytes: 1_000_000_000_000},
		{NodeID: "n4", DiskAvailBytes: 1_000_000_000_000},
	}
	cfg := ECConfig{DataShards: 2, ParityShards: 1} // 3 shards/object
	count := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("obj-%d", i)
		nodes := selectECPlacementFromNodeStates(cfg, liveNodes, key, nodeStates, true, true)
		for _, n := range nodes {
			count[n]++
		}
	}
	// Each iteration selects 3 of 4 nodes, so max per-node count = 1000 (one slot/iter).
	// With 4:1:1:1 disk-avail weights, WRH selects n1 in ~97% of iterations.
	// Assert n1 wins strongly (>900) and outranks all lighter peers.
	assert.Greater(t, count["n1"], 900, "n1 share too low: %d", count["n1"])
	for _, n := range []string{"n2", "n3", "n4"} {
		assert.Greater(t, count["n1"], count[n], "n1 should outrank %s (n1=%d %s=%d)", n, count["n1"], n, count[n])
	}
}

func TestSelectObjectPlacementGroup_ExcludesGroup0(t *testing.T) {
	groups := []ShardGroupEntry{
		{ID: "group-0", PeerIDs: []string{"n1", "n2", "n3"}},
		{ID: "group-1", PeerIDs: []string{"n1", "n2", "n3"}},
	}
	got, err := SelectObjectPlacementGroup("b", "k", groups, ECConfig{DataShards: 2, ParityShards: 1})
	require.NoError(t, err)
	require.Equal(t, "group-1", got.ID)
}

func TestSelectObjectPlacementGroup_FiltersECIncapableGroups(t *testing.T) {
	groups := []ShardGroupEntry{
		{ID: "group-1"},
		{ID: "group-2", PeerIDs: []string{"n1", "n2", "n3"}},
	}
	got, err := SelectObjectPlacementGroup("b", "k", groups, ECConfig{DataShards: 2, ParityShards: 1})
	require.NoError(t, err)
	require.Equal(t, "group-2", got.ID)
}

func TestSelectObjectPlacementGroup_UsesOnlyWidestTopologyGroups(t *testing.T) {
	groups := []ShardGroupEntry{
		{ID: "group-1", PeerIDs: []string{"n1"}},
		{ID: "group-2", PeerIDs: []string{"n1", "n2", "n3"}},
		{ID: "group-3", PeerIDs: []string{"n1", "n2", "n3", "n4", "n5"}},
		{ID: "group-4", PeerIDs: []string{"n1", "n2", "n3", "n4", "n5"}},
	}

	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		got, err := SelectObjectPlacementGroup("bucket", key, groups, ECConfig{DataShards: 3, ParityShards: 2})
		require.NoError(t, err)
		require.Contains(t, []string{"group-3", "group-4"}, got.ID)
		require.Len(t, got.PeerIDs, 5)
	}
}

func TestSelectObjectPlacementGroup_FallsBackToGroup0WhenNoDataGroupsExist(t *testing.T) {
	got, err := SelectObjectPlacementGroup("b", "k", []ShardGroupEntry{
		{ID: "group-0", PeerIDs: []string{"n1", "n2", "n3"}},
	}, ECConfig{DataShards: 2, ParityShards: 1})
	require.NoError(t, err)
	require.Equal(t, "group-0", got.ID)
}

func TestSelectObjectPlacementGroup_NoCandidate(t *testing.T) {
	_, err := SelectObjectPlacementGroup("b", "k", []ShardGroupEntry{
		{ID: "group-1"},
	}, ECConfig{DataShards: 2, ParityShards: 1})
	require.ErrorContains(t, err, "no EC-capable object placement group")
}

func TestSelectObjectPlacementGroup_Deterministic(t *testing.T) {
	groups := []ShardGroupEntry{
		{ID: "group-1", PeerIDs: []string{"n1"}},
		{ID: "group-2", PeerIDs: []string{"n1"}},
		{ID: "group-3", PeerIDs: []string{"n1"}},
	}
	cfg := ECConfig{DataShards: 1, ParityShards: 0}
	a, err := SelectObjectPlacementGroup("b", "same-key", groups, cfg)
	require.NoError(t, err)
	b, err := SelectObjectPlacementGroup("b", "same-key", groups, cfg)
	require.NoError(t, err)
	require.Equal(t, a.ID, b.ID)
}

func TestSelectSegmentPlacementGroup_FanOutAcrossPGs(t *testing.T) {
	groups := []ShardGroupEntry{
		{ID: "group-1", PeerIDs: []string{"n1", "n2", "n3"}},
		{ID: "group-2", PeerIDs: []string{"n1", "n2", "n3"}},
		{ID: "group-3", PeerIDs: []string{"n1", "n2", "n3"}},
		{ID: "group-4", PeerIDs: []string{"n1", "n2", "n3"}},
	}
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	blobID := uuid.Must(uuid.NewV7()).String()

	seen := make(map[string]struct{})
	for i := 0; i < 100; i++ {
		got, err := SelectSegmentPlacementGroup("bucket", "key", i, blobID, groups, cfg)
		require.NoError(t, err)
		seen[got.ID] = struct{}{}
	}
	require.GreaterOrEqual(t, len(seen), 2, "segments should fan out across at least 2 PGs, saw %v", seen)
}

func TestSelectSegmentPlacementGroup_DifferentBlobIDsDifferentPGs(t *testing.T) {
	groups := []ShardGroupEntry{
		{ID: "group-1", PeerIDs: []string{"n1", "n2", "n3"}},
		{ID: "group-2", PeerIDs: []string{"n1", "n2", "n3"}},
		{ID: "group-3", PeerIDs: []string{"n1", "n2", "n3"}},
		{ID: "group-4", PeerIDs: []string{"n1", "n2", "n3"}},
	}
	cfg := ECConfig{DataShards: 2, ParityShards: 1}

	// Probe many blobID pairs until we find one pair that lands on different
	// PGs — proves blobID participates in the hash. Bounded loop so test
	// stays deterministic and fast.
	found := false
	for i := 0; i < 64 && !found; i++ {
		blobA := uuid.Must(uuid.NewV7()).String()
		blobB := uuid.Must(uuid.NewV7()).String()
		gotA, err := SelectSegmentPlacementGroup("bucket", "key", 0, blobA, groups, cfg)
		require.NoError(t, err)
		gotB, err := SelectSegmentPlacementGroup("bucket", "key", 0, blobB, groups, cfg)
		require.NoError(t, err)
		if gotA.ID != gotB.ID {
			found = true
		}
	}
	require.True(t, found, "blobID should influence placement: never observed different PGs across 64 blobID pairs")
}

func TestSelectSegmentPlacementGroup_FiltersGroup0(t *testing.T) {
	groups := []ShardGroupEntry{
		{ID: "group-0", PeerIDs: []string{"n1", "n2", "n3"}},
		{ID: "group-1", PeerIDs: []string{"n1", "n2", "n3"}},
	}
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	blobID := uuid.Must(uuid.NewV7()).String()
	got, err := SelectSegmentPlacementGroup("b", "k", 0, blobID, groups, cfg)
	require.NoError(t, err)
	require.Equal(t, "group-1", got.ID)
}

func TestSelectSegmentPlacementGroup_FallsBackToGroup0(t *testing.T) {
	groups := []ShardGroupEntry{
		{ID: "group-0", PeerIDs: []string{"n1", "n2", "n3"}},
	}
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	blobID := uuid.Must(uuid.NewV7()).String()
	got, err := SelectSegmentPlacementGroup("b", "k", 0, blobID, groups, cfg)
	require.NoError(t, err)
	require.Equal(t, "group-0", got.ID)
}

func TestSelectSegmentPlacementGroup_NoCandidates(t *testing.T) {
	groups := []ShardGroupEntry{
		{ID: "group-1"},
	}
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	blobID := uuid.Must(uuid.NewV7()).String()
	_, err := SelectSegmentPlacementGroup("b", "k", 0, blobID, groups, cfg)
	require.ErrorContains(t, err, "no EC-capable segment placement group")
}

func TestValidatePlacementGroupIDRejectsEmpty(t *testing.T) {
	require.ErrorContains(t, ValidatePlacementGroupID(""), "empty placement_group_id")
	require.NoError(t, ValidatePlacementGroupID("group-1"))
}

func TestPlacementContextCarriesShardGroup(t *testing.T) {
	group := ShardGroupEntry{ID: "group-1", PeerIDs: []string{"n1", "n2", "n3"}}
	ctx := ContextWithPlacementGroupEntry(context.Background(), group)

	got, ok := PlacementGroupEntryFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, "group-1", got.ID)
	require.Equal(t, []string{"n1", "n2", "n3"}, got.PeerIDs)

	group.PeerIDs[0] = "mutated"
	got, ok = PlacementGroupEntryFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, []string{"n1", "n2", "n3"}, got.PeerIDs)

	groupID, ok := PlacementGroupFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, "group-1", groupID)
}

func TestSelectECPlacement_AllStaleFallback(t *testing.T) {
	// nodeStates is empty — every node is stale.
	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	nodes := selectECPlacementFromNodeStates(cfg, []string{"n1", "n2", "n3", "n4"}, "key", nil, true, false)
	assert.Len(t, nodes, 3, "should fall back to unweighted placement, not empty")
}
