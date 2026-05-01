package main

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementMonitorRegistry_ReplacesBackendForSameGroupID(t *testing.T) {
	parent := t.Context()
	registry := newPlacementMonitorRegistry()
	gb1 := &cluster.GroupBackend{}
	gb2 := &cluster.GroupBackend{}

	var started []context.Context
	start := func(ctx context.Context, _ *cluster.DataGroup) {
		started = append(started, ctx)
	}

	registry.refresh(parent, []*cluster.DataGroup{
		cluster.NewDataGroupWithBackend("g", nil, gb1),
	}, start)
	require.Len(t, started, 1)

	registry.refresh(parent, []*cluster.DataGroup{
		cluster.NewDataGroupWithBackend("g", nil, gb1),
	}, start)
	require.Len(t, started, 1)
	assert.NoError(t, started[0].Err())

	registry.refresh(parent, []*cluster.DataGroup{
		cluster.NewDataGroupWithBackend("g", nil, gb2),
	}, start)
	require.Len(t, started, 2)
	assert.ErrorIs(t, started[0].Err(), context.Canceled)
	assert.NoError(t, started[1].Err())

	registry.refresh(parent, nil, start)
	assert.ErrorIs(t, started[1].Err(), context.Canceled)
}
