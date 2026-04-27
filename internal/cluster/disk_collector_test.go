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

// thresholdCall captures one OnThreshold invocation for assertion.
type thresholdCall struct {
	level     DiskThresholdLevel
	pct       float64
	availByts uint64
}

func TestDiskCollector_Threshold_FiresOnceOnEntry(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second)

	var calls []thresholdCall
	dc.SetOnThreshold(func(l DiskThresholdLevel, p float64, a uint64) {
		calls = append(calls, thresholdCall{l, p, a})
	})

	// 1st sample: 85% → enters Warn → fires once.
	dc.SetStatFunc(func(string) (float64, uint64) { return 85.0, 1000 })
	dc.collect()
	require.Len(t, calls, 1)
	assert.Equal(t, DiskLevelWarn, calls[0].level)
	assert.Equal(t, 85.0, calls[0].pct)

	// 2nd sample: still 85% → no transition → no new call.
	dc.collect()
	require.Len(t, calls, 1, "must not re-fire on same level")

	// 3rd sample: 92% → Warn → Critical transition → fires.
	dc.SetStatFunc(func(string) (float64, uint64) { return 92.0, 500 })
	dc.collect()
	require.Len(t, calls, 2)
	assert.Equal(t, DiskLevelCritical, calls[1].level)

	// 4th sample: 70% → recovers to OK → fires (operator visibility).
	dc.SetStatFunc(func(string) (float64, uint64) { return 70.0, 5000 })
	dc.collect()
	require.Len(t, calls, 3)
	assert.Equal(t, DiskLevelOK, calls[2].level)
}

func TestDiskCollector_Threshold_NoCallback_NoOp(t *testing.T) {
	// Unset callback must not panic at any disk percentage.
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})
	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second)
	dc.SetStatFunc(func(string) (float64, uint64) { return 95.0, 100 })
	assert.NotPanics(t, func() { dc.collect() })
}

func TestDiskCollector_SetThresholds_OverridesDefaults(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})
	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second)
	dc.SetThresholds(50, 70) // tighter

	var got DiskThresholdLevel
	dc.SetOnThreshold(func(l DiskThresholdLevel, _ float64, _ uint64) { got = l })
	dc.SetStatFunc(func(string) (float64, uint64) { return 60.0, 1000 })
	dc.collect()
	assert.Equal(t, DiskLevelWarn, got, "60% should be warn under (50,70)")
}

func TestDiskCollector_SetThresholds_RejectsInvalidInput(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})
	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second)

	dc.SetThresholds(90, 80) // warn > critical, invalid
	dc.SetThresholds(0, 50)  // warn=0, invalid
	dc.SetThresholds(50, 0)  // critical=0, invalid
	dc.SetThresholds(-1, 90) // negative, invalid

	var got DiskThresholdLevel
	dc.SetOnThreshold(func(l DiskThresholdLevel, _ float64, _ uint64) { got = l })
	dc.SetStatFunc(func(string) (float64, uint64) { return 85.0, 100 })
	dc.collect()
	// Defaults (80, 90) must still be in effect — 85% should be Warn.
	assert.Equal(t, DiskLevelWarn, got, "invalid SetThresholds calls must be ignored")
}
