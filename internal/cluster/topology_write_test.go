package cluster

import (
	"context"
	"errors"
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
