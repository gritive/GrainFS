package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
		{ID: "group-1", PeerIDs: []string{"n1", "n2"}},
		{ID: "group-2", PeerIDs: []string{"n1", "n2", "n3"}},
	}
	got, err := SelectObjectPlacementGroup("b", "k", groups, ECConfig{DataShards: 2, ParityShards: 1})
	require.NoError(t, err)
	require.Equal(t, "group-2", got.ID)
}

func TestSelectObjectPlacementGroup_NoCandidate(t *testing.T) {
	_, err := SelectObjectPlacementGroup("b", "k", []ShardGroupEntry{
		{ID: "group-0", PeerIDs: []string{"n1", "n2", "n3"}},
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

func TestValidatePlacementGroupIDRejectsEmpty(t *testing.T) {
	require.ErrorContains(t, ValidatePlacementGroupID(""), "empty placement_group_id")
	require.NoError(t, ValidatePlacementGroupID("group-1"))
}
