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
	"os"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ShardPlacementMonitor watches local shard placements and reports missing
// shards. Zero value is not usable — call NewShardPlacementMonitor.
type ShardPlacementMonitor struct {
	fsm      *FSM
	resolver PlacementResolver
	shardSvc *ShardService
	nodeID   string
	interval time.Duration
	logger   zerolog.Logger

	// Observability counters (atomic for lock-free read from metrics handler).
	lastScan         atomic.Int64 // unix nanos
	lastMissingCount atomic.Int64
	totalScans       atomic.Uint64
	onMissing        func(target ECShardScanTarget, shardIdx int) // Slice 5 hook
	onCorrupt        func(target ECShardScanTarget, shardIdx int, err error)
}

type pendingRepair struct {
	target   ECShardScanTarget
	shardIdx int
}

type pendingCorruptShard struct {
	target   ECShardScanTarget
	shardIdx int
	err      error
}

// NewShardPlacementMonitor creates a monitor rooted at the given FSM. interval
// is how often Scan runs when Start is used. shardSvc and nodeID identify the
// local node's shard storage.
func NewShardPlacementMonitor(fsm *FSM, resolver PlacementResolver, shardSvc *ShardService, nodeID string, interval time.Duration) *ShardPlacementMonitor {
	return &ShardPlacementMonitor{
		fsm:      fsm,
		resolver: resolver,
		shardSvc: shardSvc,
		nodeID:   nodeID,
		interval: interval,
		logger:   log.With().Str("component", "shard-placement-monitor").Str("node", nodeID).Logger(),
	}
}

// SetOnMissing registers a callback invoked for every locally-missing shard
// during a scan. Used by Slice 5 to schedule re-fetch + restore. Thread-safe
// only if the callback itself is.
func (m *ShardPlacementMonitor) SetOnMissing(fn func(target ECShardScanTarget, shardIdx int)) {
	m.onMissing = fn
}

// SetOnCorrupt registers a callback invoked for every locally-corrupt shard
// during a scan. The callback is invoked after the scan transaction closes.
func (m *ShardPlacementMonitor) SetOnCorrupt(fn func(target ECShardScanTarget, shardIdx int, err error)) {
	m.onCorrupt = fn
}

// Scan enumerates every EC shard to verify via IterECShardScanTargets, checks
// each shard assigned to this node exists on disk, and returns the number of
// missing shards it found. Callers can hook repair logic via SetOnMissing.
//
// Object-version targets carry the raw object EC fields and are resolved to a
// shard key + placement via ResolvePlacement (non-EC objects are skipped).
// Segment/coalesced targets already carry a final ShardKey + Placement and are
// scanned directly with no resolver call.
//
// If resolver is nil, object-version targets are skipped individually;
// segment/coalesced targets are unaffected.
//
// onMissing/onCorrupt callbacks are invoked AFTER the BadgerDB iterator closes
// so the read transaction does not stay open during potentially long-running
// network repair calls.
func (m *ShardPlacementMonitor) Scan(ctx context.Context) (int, error) {
	if m.shardSvc == nil {
		return 0, errors.New("shard service not configured")
	}

	var repairs []pendingRepair
	var corrupt []pendingCorruptShard
	var missing int64
	seen := make(map[string]struct{})

	var targets []ECShardScanTarget
	if err := m.fsm.IterECShardScanTargets(func(t ECShardScanTarget) error {
		targets = append(targets, t)
		return nil
	}); err != nil {
		return 0, fmt.Errorf("scan ec shard targets: %w", err)
	}

	for _, t := range targets {
		if ctx.Err() != nil {
			return int(missing), ctx.Err()
		}
		switch t.Kind {
		case ECShardObjectVersion:
			if m.resolver == nil {
				continue
			}
			resolved, rerr := m.resolver.ResolvePlacement(ctx, t.Bucket, t.ObjectKey, PlacementMeta{
				VersionID:        t.VersionID,
				ECData:           t.ECData,
				ECParity:         t.ECParity,
				NodeIDs:          t.NodeIDs,
				PlacementGroupID: t.PlacementGroupID,
			})
			if errors.Is(rerr, ErrNotEC) {
				continue
			}
			if rerr != nil {
				m.logger.Warn().Str("bucket", t.Bucket).Str("key", t.ObjectKey).Err(rerr).Msg("resolve placement during scan failed")
				continue
			}
			if _, ok := seen[t.Bucket+"\x00"+resolved.ShardKey]; ok {
				continue
			}
			seen[t.Bucket+"\x00"+resolved.ShardKey] = struct{}{}
			m.scanRecord(ctx, t, resolved.ShardKey, resolved.Record, &missing, &repairs, &corrupt)
		case ECShardSegment, ECShardCoalesced:
			if _, ok := seen[t.Bucket+"\x00"+t.ShardKey]; ok {
				continue
			}
			seen[t.Bucket+"\x00"+t.ShardKey] = struct{}{}
			m.scanRecord(ctx, t, t.ShardKey, t.Placement, &missing, &repairs, &corrupt)
		}
	}

	m.lastScan.Store(time.Now().UnixNano())
	m.lastMissingCount.Store(missing)
	m.totalScans.Add(1)

	// Invoke repair callbacks outside the BadgerDB transaction so MVCC versions
	// are not pinned during network I/O.
	for _, r := range repairs {
		if ctx.Err() != nil {
			break
		}
		m.onMissing(r.target, r.shardIdx)
	}
	for _, c := range corrupt {
		if ctx.Err() != nil {
			break
		}
		m.onCorrupt(c.target, c.shardIdx, c.err)
	}

	return int(missing), nil
}

// scanRecord verifies, for the effective shardKey + placement of target t, that
// every shard assigned to this node exists on disk. Missing shards are queued
// into repairs, non-ENOENT read errors into corrupt. The originating target is
// threaded into the pending entries so callbacks receive it.
func (m *ShardPlacementMonitor) scanRecord(ctx context.Context, t ECShardScanTarget, shardKey string, rec PlacementRecord, missing *int64, repairs *[]pendingRepair, corrupt *[]pendingCorruptShard) {
	if ctx.Err() != nil {
		return
	}
	for shardIdx, holder := range rec.Nodes {
		if holder != m.nodeID {
			continue // someone else's shard; their monitor handles it
		}
		if _, rerr := m.shardSvc.ReadLocalShard(t.Bucket, shardKey, shardIdx); rerr != nil {
			if os.IsNotExist(rerr) {
				atomic.AddInt64(missing, 1)
				m.logger.Warn().Str("bucket", t.Bucket).Str("key", shardKey).Int("shard_idx", shardIdx).Msg("missing local shard")
				if m.onMissing != nil {
					*repairs = append(*repairs, pendingRepair{target: t, shardIdx: shardIdx})
				}
				continue
			}
			// Non-ENOENT read error — log but don't count as missing (could be
			// a transient I/O issue; a future scan will catch persistent corruption).
			m.logger.Warn().Str("bucket", t.Bucket).Str("key", shardKey).Int("shard_idx", shardIdx).Err(rerr).Msg("shard read error during scan")
			if m.onCorrupt != nil {
				*corrupt = append(*corrupt, pendingCorruptShard{target: t, shardIdx: shardIdx, err: rerr})
			}
		}
	}
}

// Start runs Scan in a loop until ctx is cancelled. Errors are logged and do
// not stop the loop — transient FSM read failures should not kill monitoring.
func (m *ShardPlacementMonitor) Start(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	m.logger.Info().Dur("interval", m.interval).Msg("shard placement monitor started")
	for {
		select {
		case <-ctx.Done():
			m.logger.Info().Msg("shard placement monitor stopped")
			return
		case <-ticker.C:
			if _, err := m.Scan(ctx); err != nil {
				m.logger.Warn().Err(err).Msg("placement scan failed")
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
