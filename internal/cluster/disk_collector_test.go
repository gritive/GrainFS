package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiskCollector_CollectUpdatesStore(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second)
	dc.SetStatFunc(func(string) (float64, uint64) { return 80.0, 5000 })

	dc.collect()

	s, ok := store.Get("n1")
	require.True(t, ok)
	assert.Equal(t, 80.0, s.DiskUsedPct)
	assert.Equal(t, uint64(5000), s.DiskAvailBytes)
}

func TestDiskCollector_SetStatFunc_OverridesDefault(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second)

	var called bool
	dc.SetStatFunc(func(string) (float64, uint64) {
		called = true
		return 42.0, 9999
	})
	dc.collect()

	assert.True(t, called)
	s, ok := store.Get("n1")
	require.True(t, ok)
	assert.Equal(t, 42.0, s.DiskUsedPct)
}

func TestDiskCollector_CollectNoopIfNodeNotInStore(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second)
	dc.SetStatFunc(func(string) (float64, uint64) { return 80.0, 5000 })

	assert.NotPanics(t, func() { dc.collect() })
	assert.Equal(t, 0, store.Len())
}

func TestDiskCollector_Collect_SkipsOnZeroStats(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1", DiskUsedPct: 50.0})

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second)
	dc.SetStatFunc(func(string) (float64, uint64) { return 0, 0 })
	dc.collect()

	s, ok := store.Get("n1")
	require.True(t, ok)
	assert.Equal(t, 50.0, s.DiskUsedPct, "store should not be updated when stat returns (0,0)")
}

func TestDiskCollector_Collect_ClampsNegativeUsedPct(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second)
	dc.SetStatFunc(func(string) (float64, uint64) { return -10.0, 1000 })
	dc.collect()

	s, ok := store.Get("n1")
	require.True(t, ok)
	assert.Equal(t, 0.0, s.DiskUsedPct, "negative usedPct should be clamped to 0 in store")
}

func TestDiskCollector_Collect_ClampsOverHundredUsedPct(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second)
	dc.SetStatFunc(func(string) (float64, uint64) { return 150.0, 1000 })
	dc.collect()

	s, ok := store.Get("n1")
	require.True(t, ok)
	assert.Equal(t, 100.0, s.DiskUsedPct, "usedPct > 100 should be clamped to 100 in store")
}

func TestDiskCollector_RunCallsCollectOnInterval(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Millisecond)

	var count atomic.Int64
	dc.SetStatFunc(func(string) (float64, uint64) {
		count.Add(1)
		return 50.0, 1000
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	dc.Run(ctx)

	assert.GreaterOrEqual(t, count.Load(), int64(3))
}
