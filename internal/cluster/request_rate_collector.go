package cluster

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/gossip"
	"github.com/gritive/GrainFS/internal/metrics"
)

// RequestRateCollector periodically derives this node's request rate (RPS) from a
// monotonic request counter and writes it into the gossip.NodeStatsStore, mirroring how
// DiskCollector owns the disk fields. Gossip then propagates the value cluster-wide
// so BoundedLoads (hot-node read reranking) and the balancer can consume it.
//
// The counter is sampled off the hot path (once per interval), so this adds zero
// per-request cost: countFunc reads an already-maintained, label-sharded counter
// (metrics.ServiceRequestCount in production). RPS is the delta over elapsed wall
// time between successive samples.
type RequestRateCollector struct {
	nodeID    string
	store     *gossip.NodeStatsStore
	interval  time.Duration
	countFunc func() float64

	lastCount float64
	lastTime  time.Time
}

// NewRequestRateCollector creates a collector that samples countFunc every interval.
// countFunc must return a monotonically non-decreasing cumulative request count.
func NewRequestRateCollector(nodeID string, store *gossip.NodeStatsStore, interval time.Duration, countFunc func() float64) *RequestRateCollector {
	return &RequestRateCollector{
		nodeID:    nodeID,
		store:     store,
		interval:  interval,
		countFunc: countFunc,
	}
}

// Run starts the collection loop. Blocks until ctx is cancelled.
func (c *RequestRateCollector) Run(ctx context.Context) {
	log.Info().Str("nodeID", c.nodeID).Dur("interval", c.interval).Msg("request-rate collector started")
	// Seed the baseline so the first computed sample excludes any request count
	// accumulated before the collector started (avoids a spurious startup spike).
	c.seed(time.Now())
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.collect(time.Now())
		}
	}
}

// seed records the current count/time without emitting a sample.
func (c *RequestRateCollector) seed(now time.Time) {
	c.lastCount = c.countFunc()
	c.lastTime = now
}

// collect computes RPS over the window since the last sample and updates the store.
func (c *RequestRateCollector) collect(now time.Time) {
	elapsed := now.Sub(c.lastTime).Seconds()
	if elapsed <= 0 {
		return
	}
	count := c.countFunc()
	delta := count - c.lastCount
	if delta < 0 {
		// Counter went backwards (e.g. registry reset). Treat as no progress
		// rather than emitting a negative rate; re-baseline from here.
		delta = 0
	}
	rps := delta / elapsed
	c.lastCount = count
	c.lastTime = now
	if c.store != nil {
		c.store.UpdateRequestStats(c.nodeID, rps)
	}
	log.Debug().Str("nodeID", c.nodeID).Float64("rps", rps).Msg("request rate collected")
	metrics.NodeRequestsPerSec.WithLabelValues(c.nodeID).Set(rps)
}
