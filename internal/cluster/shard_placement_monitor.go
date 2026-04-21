package cluster

// Phase 18 Cluster EC — Slice 4: ShardPlacementMonitor.
//
// Periodically scans the FSM for shard placement records and verifies that
// each node's locally-assigned shards actually exist on disk. Missing shards
// are counted + logged (+ hooked for repair in Slice 5). Each cluster node
// runs its own monitor and only verifies shards assigned to itself — peers
// do the same for theirs, so a dead node's shards surface indirectly (every
// caller sees GetObject reconstruction fall back to read-k when any node's
// monitor would otherwise have flagged the gap).
//
// Scope intentionally small for Slice 4: detection only, no repair. Slice 5
// adds the repair loop; Slice 6 wires HealReceipt emission.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"
)

// ShardPlacementMonitor watches local shard placements and reports missing
// shards. Zero value is not usable — call NewShardPlacementMonitor.
type ShardPlacementMonitor struct {
	fsm      *FSM
	shardSvc *ShardService
	nodeID   string
	interval time.Duration
	logger   *slog.Logger

	// Observability counters (atomic for lock-free read from metrics handler).
	lastScan         atomic.Int64 // unix nanos
	lastMissingCount atomic.Int64
	totalScans       atomic.Uint64
	onMissing        func(bucket, key string, shardIdx int) // Slice 5 hook
}

// NewShardPlacementMonitor creates a monitor rooted at the given FSM. interval
// is how often Scan runs when Start is used. shardSvc and nodeID identify the
// local node's shard storage.
func NewShardPlacementMonitor(fsm *FSM, shardSvc *ShardService, nodeID string, interval time.Duration) *ShardPlacementMonitor {
	return &ShardPlacementMonitor{
		fsm:      fsm,
		shardSvc: shardSvc,
		nodeID:   nodeID,
		interval: interval,
		logger:   slog.With("component", "shard-placement-monitor", "node", nodeID),
	}
}

// SetOnMissing registers a callback invoked for every locally-missing shard
// during a scan. Used by Slice 5 to schedule re-fetch + restore. Thread-safe
// only if the callback itself is.
func (m *ShardPlacementMonitor) SetOnMissing(fn func(bucket, key string, shardIdx int)) {
	m.onMissing = fn
}

// Scan walks every placement record once, verifies each shard assigned to
// this node exists on disk, and returns the number of missing shards it
// found. Callers can hook repair logic via SetOnMissing.
//
// On-disk paths compose the shardKey as `key/versionID` because
// putObjectEC stores shards under `{bucket}/{key}/{versionID}/shard_{N}`.
// The monitor resolves the latest versionID via the FSM `lat:` pointer;
// pre-Slice-1 legacy EC without a version pointer falls back to the bare
// key layout.
func (m *ShardPlacementMonitor) Scan(ctx context.Context) (int, error) {
	if m.shardSvc == nil {
		return 0, errors.New("shard service not configured")
	}
	var missing int64
	err := m.fsm.IterShardPlacements(func(bucket, key string, nodes []string) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		shardKey := key
		if version, verr := m.fsm.LookupLatestVersion(bucket, key); verr == nil && version != "" {
			shardKey = key + "/" + version
		}
		for shardIdx, holder := range nodes {
			if holder != m.nodeID {
				continue // someone else's shard; their monitor handles it
			}
			if _, rerr := m.shardSvc.ReadLocalShard(bucket, shardKey, shardIdx); rerr != nil {
				if os.IsNotExist(rerr) {
					atomic.AddInt64(&missing, 1)
					m.logger.Warn("missing local shard",
						"bucket", bucket, "key", key, "shard_idx", shardIdx)
					if m.onMissing != nil {
						m.onMissing(bucket, key, shardIdx)
					}
					continue
				}
				// Non-ENOENT read error — log but don't count as missing (could be
				// a transient I/O issue; a future scan will catch persistent corruption).
				m.logger.Warn("shard read error during scan",
					"bucket", bucket, "key", key, "shard_idx", shardIdx, "error", rerr)
			}
		}
		return nil
	})
	m.lastScan.Store(time.Now().UnixNano())
	m.lastMissingCount.Store(missing)
	m.totalScans.Add(1)
	if err != nil {
		return int(missing), fmt.Errorf("scan placements: %w", err)
	}
	return int(missing), nil
}

// Start runs Scan in a loop until ctx is cancelled. Errors are logged and do
// not stop the loop — transient FSM read failures should not kill monitoring.
func (m *ShardPlacementMonitor) Start(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	m.logger.Info("shard placement monitor started", "interval", m.interval)
	for {
		select {
		case <-ctx.Done():
			m.logger.Info("shard placement monitor stopped")
			return
		case <-ticker.C:
			if _, err := m.Scan(ctx); err != nil {
				m.logger.Warn("placement scan failed", "error", err)
			}
		}
	}
}

// Stats returns a point-in-time snapshot for metrics / health endpoints.
type ShardPlacementMonitorStats struct {
	LastScanUnixNano int64
	LastMissingCount int64
	TotalScans       uint64
}

// Stats returns counters. Safe to call concurrently with Scan.
func (m *ShardPlacementMonitor) Stats() ShardPlacementMonitorStats {
	return ShardPlacementMonitorStats{
		LastScanUnixNano: m.lastScan.Load(),
		LastMissingCount: m.lastMissingCount.Load(),
		TotalScans:       m.totalScans.Load(),
	}
}
