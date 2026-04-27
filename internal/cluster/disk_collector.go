package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
)

// DiskThresholdLevel labels which threshold a sample crossed.
type DiskThresholdLevel string

const (
	DiskLevelOK       DiskThresholdLevel = ""
	DiskLevelWarn     DiskThresholdLevel = "warn"
	DiskLevelCritical DiskThresholdLevel = "critical"
)

// Default warning thresholds — tuneable via SetThresholds. 80% / 90% match
// the design doc's predictive-warning spec; tuning to lower values gives
// earlier signal at the cost of more frequent transitions.
const (
	defaultDiskWarnPct     = 80.0
	defaultDiskCriticalPct = 90.0
)

// DiskCollector periodically reads local disk stats and updates the NodeStatsStore.
type DiskCollector struct {
	nodeID   string
	dataDir  string
	store    *NodeStatsStore
	interval time.Duration
	mu       sync.RWMutex
	statFunc func(dir string) (usedPct float64, availBytes uint64)

	// Threshold callback. Fires once per transition between OK/Warn/Critical
	// levels — does NOT spam every tick while above threshold. Set via
	// SetOnThreshold before Run().
	onThreshold func(level DiskThresholdLevel, pct float64, availBytes uint64)
	warnPct     float64
	criticalPct float64
	// lastLevel is read+written from the same goroutine (collect loop) so an
	// atomic.Value is overkill — simple field is fine. Kept under mu for the
	// rare external Reset() use cases (tests).
	lastLevel atomic.Pointer[DiskThresholdLevel]
}

// NewDiskCollector creates a collector for nodeID reading disk stats from dataDir.
func NewDiskCollector(nodeID, dataDir string, store *NodeStatsStore, interval time.Duration) *DiskCollector {
	return &DiskCollector{
		nodeID:      nodeID,
		dataDir:     dataDir,
		store:       store,
		interval:    interval,
		statFunc:    sysDiskStat,
		warnPct:     defaultDiskWarnPct,
		criticalPct: defaultDiskCriticalPct,
	}
}

// SetStatFunc replaces the disk stat function (for testing).
func (d *DiskCollector) SetStatFunc(f func(string) (float64, uint64)) {
	d.mu.Lock()
	d.statFunc = f
	d.mu.Unlock()
}

// SetOnThreshold registers a callback that fires once per transition between
// OK / Warn / Critical levels. Must be set before Run() starts. The callback
// runs synchronously inside the collect loop — it must not block (wrap in a
// goroutine if it makes blocking calls like webhook delivery).
func (d *DiskCollector) SetOnThreshold(fn func(level DiskThresholdLevel, pct float64, availBytes uint64)) {
	d.onThreshold = fn
}

// SetThresholds overrides the default warn/critical disk-used percentages.
// Call before Run(). Validates ordering (warn ≤ critical); silently ignores
// invalid input rather than panicking on operator typo.
func (d *DiskCollector) SetThresholds(warnPct, criticalPct float64) {
	if warnPct <= 0 || warnPct > 100 || criticalPct <= 0 || criticalPct > 100 || warnPct > criticalPct {
		return
	}
	d.warnPct = warnPct
	d.criticalPct = criticalPct
}

// Run starts the collection loop. Blocks until ctx is cancelled.
func (d *DiskCollector) Run(ctx context.Context) {
	log.Info().Str("nodeID", d.nodeID).Str("dataDir", d.dataDir).Dur("interval", d.interval).Msg("disk collector started")
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	d.collect()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.collect()
		}
	}
}

func (d *DiskCollector) collect() {
	d.mu.RLock()
	fn := d.statFunc
	d.mu.RUnlock()
	usedPct, availBytes := fn(d.dataDir)
	if usedPct == 0 && availBytes == 0 {
		log.Warn().Str("nodeID", d.nodeID).Str("dataDir", d.dataDir).Msg("disk stat unavailable, skipping update")
		return
	}
	if usedPct < 0 {
		usedPct = 0
	} else if usedPct > 100 {
		usedPct = 100
	}
	log.Debug().Str("nodeID", d.nodeID).Float64("usedPct", usedPct).Uint64("availBytes", availBytes).Msg("disk stat collected")
	if d.store != nil {
		d.store.UpdateDiskStats(d.nodeID, usedPct, availBytes)
	}
	metrics.DiskUsedPct.WithLabelValues(d.nodeID).Set(usedPct)

	d.fireThresholdIfChanged(usedPct, availBytes)
}

// fireThresholdIfChanged fires onThreshold exactly once per OK↔Warn↔Critical
// transition. A sample exactly at criticalPct counts as critical; at warnPct
// counts as warn. Lower-bound transitions (e.g. critical→warn) also fire so
// the operator sees recovery progress.
func (d *DiskCollector) fireThresholdIfChanged(usedPct float64, availBytes uint64) {
	if d.onThreshold == nil {
		return
	}
	var level DiskThresholdLevel
	switch {
	case usedPct >= d.criticalPct:
		level = DiskLevelCritical
	case usedPct >= d.warnPct:
		level = DiskLevelWarn
	default:
		level = DiskLevelOK
	}
	prev := d.lastLevel.Load()
	if prev != nil && *prev == level {
		return // no transition — stay quiet
	}
	d.lastLevel.Store(&level)
	d.onThreshold(level, usedPct, availBytes)
}
