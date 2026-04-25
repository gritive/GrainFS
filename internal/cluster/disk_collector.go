package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
)

// DiskCollector periodically reads local disk stats and updates the NodeStatsStore.
type DiskCollector struct {
	nodeID   string
	dataDir  string
	store    *NodeStatsStore
	interval time.Duration
	mu       sync.RWMutex
	statFunc func(dir string) (usedPct float64, availBytes uint64)
}

// NewDiskCollector creates a collector for nodeID reading disk stats from dataDir.
func NewDiskCollector(nodeID, dataDir string, store *NodeStatsStore, interval time.Duration) *DiskCollector {
	return &DiskCollector{
		nodeID:   nodeID,
		dataDir:  dataDir,
		store:    store,
		interval: interval,
		statFunc: sysDiskStat,
	}
}

// SetStatFunc replaces the disk stat function (for testing).
func (d *DiskCollector) SetStatFunc(f func(string) (float64, uint64)) {
	d.mu.Lock()
	d.statFunc = f
	d.mu.Unlock()
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
}
