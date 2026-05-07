package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompleteMultipartRestoresFullPlacementGroupContext(t *testing.T) {
	group := ShardGroupEntry{ID: "group-1", PeerIDs: []string{"n1", "n2", "n3"}}
	ctx := ContextWithPlacementGroupEntry(context.Background(), group)
	uploadMeta := clusterMultipartMeta{PlacementGroupID: group.ID}

	restored, err := contextForMultipartComplete(ctx, uploadMeta, func(id string) (ShardGroupEntry, bool) {
		require.Equal(t, "group-1", id)
		return group, true
	})
	require.NoError(t, err)

	got, ok := PlacementGroupEntryFromContext(restored)
	require.True(t, ok)
	require.Equal(t, group.ID, got.ID)
	require.Equal(t, group.PeerIDs, got.PeerIDs)
}

func TestCompleteMultipartRejectsMissingPlacementGroup(t *testing.T) {
	_, err := contextForMultipartComplete(context.Background(), clusterMultipartMeta{PlacementGroupID: "missing"}, func(string) (ShardGroupEntry, bool) {
		return ShardGroupEntry{}, false
	})
	require.ErrorIs(t, err, ErrPlacementTargetsUnavailable)
}
