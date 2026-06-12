package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/gossip"
)

func TestRequestRateCollector_ComputesDeltaRate(t *testing.T) {
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	store.Set(gossip.NodeStats{NodeID: "n1", DiskUsedPct: 40.0})

	var count float64
	c := NewRequestRateCollector("n1", store, time.Second, func() float64 { return count })

	t0 := time.Now()
	count = 100 // requests already served before the collector seeds
	c.seed(t0)

	// 150 more requests over a 10s window → 15 rps.
	count = 250
	c.collect(t0.Add(10 * time.Second))

	got, ok := store.Get("n1")
	require.True(t, ok)
	assert.InDelta(t, 15.0, got.RequestsPerSec, 1e-9)
	assert.Equal(t, 40.0, got.DiskUsedPct, "disk field must be preserved")
}

func TestRequestRateCollector_NoOpWhenNodeAbsent(t *testing.T) {
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	var count float64
	c := NewRequestRateCollector("ghost", store, time.Second, func() float64 { return count })
	c.seed(time.Now())
	count = 1000
	c.collect(time.Now().Add(time.Second))
	_, ok := store.Get("ghost")
	assert.False(t, ok, "collector must not create a store entry for an unknown node")
}

func TestRequestRateCollector_CounterResetClampsToZero(t *testing.T) {
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	store.Set(gossip.NodeStats{NodeID: "n1"})
	var count float64
	c := NewRequestRateCollector("n1", store, time.Second, func() float64 { return count })

	t0 := time.Now()
	count = 500
	c.seed(t0)
	count = 10 // counter went backwards
	c.collect(t0.Add(5 * time.Second))

	got, _ := store.Get("n1")
	assert.Equal(t, 0.0, got.RequestsPerSec, "counter reset must clamp to 0, not a negative rate")
}
