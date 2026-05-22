package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBoundedLoads_EmptySnapshot(t *testing.T) {
	store := NewNodeStatsStore(2 * time.Minute)
	bl := NewBoundedLoads(store, BoundedLoadsParams{C: 1.25, CLow: 1.0})

	snap := bl.Snapshot()
	assert.Equal(t, 0.0, snap.AvgRPS, "empty store: AvgRPS should be 0")
	assert.Empty(t, snap.HotSet, "empty store: HotSet should be empty")
}

func TestBoundedLoads_IsHotMissingNode(t *testing.T) {
	store := NewNodeStatsStore(2 * time.Minute)
	bl := NewBoundedLoads(store, BoundedLoadsParams{C: 1.25, CLow: 1.0})
	assert.False(t, bl.IsHot("missing-node"))
}
