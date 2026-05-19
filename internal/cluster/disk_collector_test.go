package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDiskCfg is a hot-reloadable DiskCfgReader for tests. Fractions are
// stored in atomic.Pointer so a Set() from one goroutine is observed by the
// collect loop on its next tick.
type fakeDiskCfg struct {
	warn atomic.Pointer[float64]
	crit atomic.Pointer[float64]
}

func newFakeDiskCfg(warn, crit float64) *fakeDiskCfg {
	c := &fakeDiskCfg{}
	c.Set(warn, crit)
	return c
}

func (c *fakeDiskCfg) DiskWarnFrac() float64     { return *c.warn.Load() }
func (c *fakeDiskCfg) DiskCriticalFrac() float64 { return *c.crit.Load() }
func (c *fakeDiskCfg) Set(warn, crit float64) {
	c.warn.Store(&warn)
	c.crit.Store(&crit)
}

func TestDiskCollector_CollectUpdatesStore(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second, nil)
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

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second, nil)

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

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second, nil)
	dc.SetStatFunc(func(string) (float64, uint64) { return 80.0, 5000 })

	assert.NotPanics(t, func() { dc.collect() })
	assert.Equal(t, 0, store.Len())
}

func TestDiskCollector_Collect_SkipsOnZeroStats(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1", DiskUsedPct: 50.0})

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second, nil)
	dc.SetStatFunc(func(string) (float64, uint64) { return 0, 0 })
	dc.collect()

	s, ok := store.Get("n1")
	require.True(t, ok)
	assert.Equal(t, 50.0, s.DiskUsedPct, "store should not be updated when stat returns (0,0)")
}

func TestDiskCollector_Collect_ClampsNegativeUsedPct(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second, nil)
	dc.SetStatFunc(func(string) (float64, uint64) { return -10.0, 1000 })
	dc.collect()

	s, ok := store.Get("n1")
	require.True(t, ok)
	assert.Equal(t, 0.0, s.DiskUsedPct, "negative usedPct should be clamped to 0 in store")
}

func TestDiskCollector_Collect_ClampsOverHundredUsedPct(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second, nil)
	dc.SetStatFunc(func(string) (float64, uint64) { return 150.0, 1000 })
	dc.collect()

	s, ok := store.Get("n1")
	require.True(t, ok)
	assert.Equal(t, 100.0, s.DiskUsedPct, "usedPct > 100 should be clamped to 100 in store")
}

func TestDiskCollector_RunCallsCollectOnInterval(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})

	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Millisecond, nil)

	var count atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dc.SetStatFunc(func(string) (float64, uint64) {
		if count.Add(1) >= 3 {
			cancel()
		}
		return 50.0, 1000
	})

	dc.Run(ctx)
	assert.Equal(t, int64(3), count.Load())
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

	// Defaults: 0.80 warn, 0.90 critical (matches DefaultClusterDiskWarnFrac/CriticalFrac).
	cfg := newFakeDiskCfg(0.80, 0.90)
	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second, cfg)

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
	cfg := newFakeDiskCfg(0.80, 0.90)
	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second, cfg)
	dc.SetStatFunc(func(string) (float64, uint64) { return 95.0, 100 })
	assert.NotPanics(t, func() { dc.collect() })
}

func TestDiskCollector_Threshold_NilCfg_NoOp(t *testing.T) {
	// nil cfg disables threshold callback entirely (used by standalone balancer
	// collector which only emits stats). Must not panic and must not invoke the
	// callback.
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})
	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second, nil)
	var called bool
	dc.SetOnThreshold(func(DiskThresholdLevel, float64, uint64) { called = true })
	dc.SetStatFunc(func(string) (float64, uint64) { return 95.0, 100 })
	assert.NotPanics(t, func() { dc.collect() })
	assert.False(t, called, "threshold callback must not fire when cfg is nil")
}

func TestDiskCollector_Threshold_HotReload(t *testing.T) {
	// Pin the rotation contract: a cluster-config PATCH that tightens the
	// thresholds is observed on the next collect tick — no restart, no
	// re-construction.
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "n1"})

	cfg := newFakeDiskCfg(0.80, 0.90)
	dc := NewDiskCollector("n1", "/tmp", store, 10*time.Second, cfg)

	var calls []thresholdCall
	dc.SetOnThreshold(func(l DiskThresholdLevel, p float64, a uint64) {
		calls = append(calls, thresholdCall{l, p, a})
	})

	// 60% under (0.80, 0.90) → OK → first transition fires.
	dc.SetStatFunc(func(string) (float64, uint64) { return 60.0, 1000 })
	dc.collect()
	require.Len(t, calls, 1)
	assert.Equal(t, DiskLevelOK, calls[0].level)

	// Operator tightens thresholds via cluster-config PATCH: now (0.50, 0.70).
	// 60% under the new thresholds → Warn. Next tick must observe the change.
	cfg.Set(0.50, 0.70)
	dc.collect()
	require.Len(t, calls, 2, "tightened thresholds must take effect on next tick")
	assert.Equal(t, DiskLevelWarn, calls[1].level)

	// Operator tightens further to (0.30, 0.50). 60% → Critical.
	cfg.Set(0.30, 0.50)
	dc.collect()
	require.Len(t, calls, 3)
	assert.Equal(t, DiskLevelCritical, calls[2].level)

	// Loosen back to defaults. 60% → OK.
	cfg.Set(0.80, 0.90)
	dc.collect()
	require.Len(t, calls, 4)
	assert.Equal(t, DiskLevelOK, calls[3].level)
}
