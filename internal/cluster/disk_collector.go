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

// DiskCfgReader is the minimal cluster-config surface the disk collector reads
// at every collect tick so threshold rotations land without a process restart.
// *cluster.ClusterConfig satisfies this contract; the values are fractions in
// (0, 1] (e.g. 0.80 = 80% disk used).
type DiskCfgReader interface {
	DiskWarnFrac() float64
	DiskCriticalFrac() float64
}

// DiskCollector periodically reads local disk stats and updates the NodeStatsStore.
type DiskCollector struct {
	nodeID   string
	dataDirs []string
	store    *NodeStatsStore
	interval time.Duration
	mu       sync.RWMutex
	statFunc func(dir string) (usedPct float64, availBytes uint64)

	// cfg is the live cluster-config view; thresholds are read every tick so
	// PATCH /v1/cluster/config takes effect without restart. nil disables the
	// threshold callback entirely (used by the standalone balancer collector
	// which never sets OnThreshold).
	cfg DiskCfgReader

	// Threshold callback. Fires once per transition between OK/Warn/Critical
	// levels — does NOT spam every tick while above threshold. Set via
	// SetOnThreshold before Run().
	onThreshold func(level DiskThresholdLevel, pct float64, availBytes uint64)

	// lastLevel is read+written from the same goroutine (collect loop) so an
	// atomic.Value is overkill — simple field is fine. Kept under mu for the
	// rare external Reset() use cases (tests).
	lastLevel atomic.Pointer[DiskThresholdLevel]
}

// NewDiskCollector creates a collector for nodeID reading disk stats from dataDir.
// cfg provides live warn/critical thresholds; pass nil to disable threshold
// callbacks (e.g. when the collector is used only for stats gossip).
func NewDiskCollector(nodeID, dataDir string, store *NodeStatsStore, interval time.Duration, cfg DiskCfgReader) *DiskCollector {
	return NewMultiRootDiskCollector(nodeID, []string{dataDir}, store, interval, cfg)
}

func NewMultiRootDiskCollector(nodeID string, dataDirs []string, store *NodeStatsStore, interval time.Duration, cfg DiskCfgReader) *DiskCollector {
	return &DiskCollector{
		nodeID:   nodeID,
		dataDirs: dataDirs,
		store:    store,
		interval: interval,
		statFunc: sysDiskStat,
		cfg:      cfg,
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

// Run starts the collection loop. Blocks until ctx is cancelled.
func (d *DiskCollector) Run(ctx context.Context) {
	log.Info().Str("nodeID", d.nodeID).Strs("dataDirs", d.dataDirs).Dur("interval", d.interval).Msg("disk collector started")
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

	var maxUsedPct float64 = -1.0
	var minAvailBytes uint64 = ^uint64(0)
	var collectedAny bool

	for _, dir := range d.dataDirs {
		usedPct, availBytes := fn(dir)
		if usedPct == 0 && availBytes == 0 {
			log.Warn().Str("nodeID", d.nodeID).Str("dir", dir).Msg("disk stat unavailable, skipping this directory")
			continue
		}
		if usedPct < 0 {
			usedPct = 0
		} else if usedPct > 100 {
			usedPct = 100
		}
		collectedAny = true
		if usedPct > maxUsedPct {
			maxUsedPct = usedPct
		}
		if availBytes < minAvailBytes {
			minAvailBytes = availBytes
		}
	}

	if !collectedAny {
		log.Warn().Str("nodeID", d.nodeID).Msg("all disk stats unavailable, skipping update")
		return
	}

	log.Debug().Str("nodeID", d.nodeID).Float64("maxUsedPct", maxUsedPct).Uint64("minAvailBytes", minAvailBytes).Msg("disk stat collected")
	if d.store != nil {
		d.store.UpdateDiskStats(d.nodeID, maxUsedPct, minAvailBytes)
	}
	metrics.DiskUsedPct.WithLabelValues(d.nodeID).Set(maxUsedPct)

	d.fireThresholdIfChanged(maxUsedPct, minAvailBytes)
}

// fireThresholdIfChanged fires onThreshold exactly once per OK↔Warn↔Critical
// transition. Thresholds are re-read from cfg each tick so cluster-config
// PATCH lands without restart. A sample exactly at criticalPct counts as
// critical; at warnPct counts as warn. Lower-bound transitions (e.g.
// critical→warn) also fire so the operator sees recovery progress.
func (d *DiskCollector) fireThresholdIfChanged(usedPct float64, availBytes uint64) {
	if d.onThreshold == nil || d.cfg == nil {
		return
	}
	warnPct := d.cfg.DiskWarnFrac() * 100
	criticalPct := d.cfg.DiskCriticalFrac() * 100
	var level DiskThresholdLevel
	switch {
	case usedPct >= criticalPct:
		level = DiskLevelCritical
	case usedPct >= warnPct:
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
