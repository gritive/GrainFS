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
	onMissing        func(bucket, key string, shardIdx int) // Slice 5 hook
}

type pendingRepair struct {
	bucket   string
	key      string
	shardIdx int
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
func (m *ShardPlacementMonitor) SetOnMissing(fn func(bucket, key string, shardIdx int)) {
	m.onMissing = fn
}

// Scan walks every placement record once, verifies each shard assigned to
// this node exists on disk, and returns the number of missing shards it
// found. Callers can hook repair logic via SetOnMissing.
//
// IterShardPlacements passes key=shardKey (object_key+"/"+versionID) as
// stored by putObjectEC, so shards can be read directly without an
// additional version lookup.
//
// onMissing callbacks are invoked AFTER the BadgerDB iterator closes so
// the read transaction does not stay open during potentially long-running
// network repair calls.
func (m *ShardPlacementMonitor) Scan(ctx context.Context) (int, error) {
	if m.shardSvc == nil {
		return 0, errors.New("shard service not configured")
	}

	var repairs []pendingRepair
	var missing int64
	seen := make(map[string]struct{})

	if m.resolver != nil {
		var refs []ObjectMetaRef
		err := m.fsm.IterObjectMetas(func(ref ObjectMetaRef) error {
			refs = append(refs, ref)
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("scan object metas: %w", err)
		}
		for _, ref := range refs {
			if ctx.Err() != nil {
				return int(missing), ctx.Err()
			}
			resolved, rerr := m.resolver.ResolvePlacement(ctx, ref.Bucket, ref.Key, PlacementMeta{
				VersionID:   ref.VersionID,
				RingVersion: ref.RingVersion,
				ECData:      ref.ECData,
				ECParity:    ref.ECParity,
				NodeIDs:     ref.NodeIDs,
			})
			if errors.Is(rerr, ErrNotEC) {
				continue
			}
			if rerr != nil {
				m.logger.Warn().Str("bucket", ref.Bucket).Str("key", ref.Key).Err(rerr).Msg("resolve placement during scan failed")
				continue
			}
			seen[ref.Bucket+"\x00"+resolved.ShardKey] = struct{}{}
			m.scanRecord(ctx, ref.Bucket, resolved.ShardKey, resolved.Record, &missing, &repairs)
		}
	}

	err := m.fsm.IterShardPlacements(func(bucket, key string, rec PlacementRecord) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, ok := seen[bucket+"\x00"+key]; ok {
			return nil
		}
		m.scanRecord(ctx, bucket, key, rec, &missing, &repairs)
		return nil
	})
	m.lastScan.Store(time.Now().UnixNano())
	m.lastMissingCount.Store(missing)
	m.totalScans.Add(1)
	if err != nil {
		return int(missing), fmt.Errorf("scan placements: %w", err)
	}

	// Invoke repair callbacks outside the BadgerDB transaction so MVCC versions
	// are not pinned during network I/O.
	for _, r := range repairs {
		if ctx.Err() != nil {
			break
		}
		m.onMissing(r.bucket, r.key, r.shardIdx)
	}

	return int(missing), nil
}

func (m *ShardPlacementMonitor) scanRecord(ctx context.Context, bucket, key string, rec PlacementRecord, missing *int64, repairs *[]pendingRepair) {
	if ctx.Err() != nil {
		return
	}
	for shardIdx, holder := range rec.Nodes {
		if holder != m.nodeID {
			continue // someone else's shard; their monitor handles it
		}
		if _, rerr := m.shardSvc.ReadLocalShard(bucket, key, shardIdx); rerr != nil {
			if os.IsNotExist(rerr) {
				atomic.AddInt64(missing, 1)
				m.logger.Warn().Str("bucket", bucket).Str("key", key).Int("shard_idx", shardIdx).Msg("missing local shard")
				if m.onMissing != nil {
					*repairs = append(*repairs, pendingRepair{bucket, key, shardIdx})
				}
				continue
			}
			// Non-ENOENT read error — log but don't count as missing (could be
			// a transient I/O issue; a future scan will catch persistent corruption).
			m.logger.Warn().Str("bucket", bucket).Str("key", key).Int("shard_idx", shardIdx).Err(rerr).Msg("shard read error during scan")
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
