package cluster

// Phase 18 Cluster EC — Slice 5: background N×→EC re-placement.
//
// When a cluster grows past MinECNodes (3), existing objects stored via the
// legacy N× path need to be converted to EC placement. The ReshardManager
// runs on the Raft leader and walks object metadata, converting any object
// without a placement record.
//
// Leader-only: since ConvertObjectToEC proposes through Raft, only the leader
// can propose without a redirect. Followers skip the scan to avoid wasted work.
//
// Concurrent PUT safety: ConvertObjectToEC uses an ETag check before commit.
// If a PUT overwrites the object mid-conversion, the convert aborts cleanly
// and the next scan picks up the new state (whether it went N× or EC through
// the normal PutObject path).

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Leader is a minimal interface onto raft.Node for the "am I leader?" check.
// Extracted so the manager can be unit tested without a real Raft cluster.
type Leader interface {
	IsLeader() bool
}

// Converter is the subset of DistributedBackend the reshard manager needs.
// Extracted for testability.
type Converter interface {
	ConvertObjectToEC(ctx context.Context, bucket, key string) error
	// FSMRef returns the FSM for iterating objects + checking placement.
	FSMRef() *FSM
	// ECActive returns whether EC is enabled AND cluster has enough nodes.
	ECActive() bool
	// EffectiveECConfig returns the ECConfig proportional to current cluster size.
	EffectiveECConfig() ECConfig
	// upgradeObjectEC re-encodes an EC object from oldRec's k,m to newCfg's k,m.
	upgradeObjectEC(ctx context.Context, bucket, key string, oldRec PlacementRecord, newCfg ECConfig) error
	// CurrentRingVersion returns the current ring version (0 if no ring).
	CurrentRingVersion() RingVersion
	// ReshardToRing reshards the object to the current ring layout.
	ReshardToRing(ctx context.Context, bucket, key string, oldRingVer RingVersion) error
}

// ReshardManager walks objects and triggers conversion from N× to EC.
type ReshardManager struct {
	backend  Converter
	leader   Leader
	interval time.Duration
	logger   zerolog.Logger

	totalConverted atomic.Uint64
	totalSkipped   atomic.Uint64
	totalErrors    atomic.Uint64
	lastRunNanos   atomic.Int64
	totalRuns      atomic.Uint64
}

// NewReshardManager creates the manager. interval is how often Start runs a
// full pass; use longer intervals (≥ 5 min) to limit I/O on large clusters.
func NewReshardManager(backend Converter, leader Leader, interval time.Duration) *ReshardManager {
	return &ReshardManager{
		backend:  backend,
		leader:   leader,
		interval: interval,
		logger:   log.With().Str("component", "reshard-manager").Logger(),
	}
}

// Run performs one full scan + conversion pass. Returns (converted, skipped, errs).
// Safe to invoke manually (e.g., from an admin endpoint) in addition to the
// periodic loop.
func (m *ReshardManager) Run(ctx context.Context) (converted, skipped, errs int) {
	m.totalRuns.Add(1)
	m.lastRunNanos.Store(time.Now().UnixNano())

	if m.leader != nil && !m.leader.IsLeader() {
		m.logger.Debug().Msg("reshard: skipping — not leader")
		return 0, 0, 0
	}
	if !m.backend.ECActive() {
		m.logger.Debug().Msg("reshard: skipping — EC not active on this cluster")
		return 0, 0, 0
	}
	fsm := m.backend.FSMRef()
	if fsm == nil {
		return 0, 0, 0
	}

	currentCfg := m.backend.EffectiveECConfig()

	err := fsm.IterObjectMetas(func(ref ObjectMetaRef) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ecRec, lookupErr := fsm.LookupShardPlacement(ref.Bucket, ref.Key)
		if lookupErr != nil {
			return nil // transient read error; skip this object
		}
		if len(ecRec.Nodes) == 0 {
			// No placement yet: N× replication object, convert to EC.
			if cerr := m.backend.ConvertObjectToEC(ctx, ref.Bucket, ref.Key); cerr != nil {
				errs++
				m.totalErrors.Add(1)
				m.logger.Warn().Str("bucket", ref.Bucket).Str("key", ref.Key).Err(cerr).Msg("reshard: convert failed")
				return nil
			}
			converted++
			m.totalConverted.Add(1)
			m.logger.Info().Str("bucket", ref.Bucket).Str("key", ref.Key).Int64("size", ref.Size).Msg("reshard: converted to EC")
			return nil
		}
		// Already EC. Check if stored k,m matches current effective config (EC→EC upgrade).
		recK, recM := ecRec.K, ecRec.M
		if recK == 0 {
			// Legacy record without stored k,m; use current effective config as baseline.
			recK, recM = currentCfg.DataShards, currentCfg.ParityShards
		}
		if recK == currentCfg.DataShards && recM == currentCfg.ParityShards {
			skipped++
			return nil
		}
		// k,m mismatch: cluster grew, upgrade this object's EC encoding.
		if uerr := m.backend.upgradeObjectEC(ctx, ref.Bucket, ref.Key, ecRec, currentCfg); uerr != nil {
			errs++
			m.totalErrors.Add(1)
			m.logger.Warn().Str("bucket", ref.Bucket).Str("key", ref.Key).
				Int("old_k", recK).Int("old_m", recM).
				Int("new_k", currentCfg.DataShards).Int("new_m", currentCfg.ParityShards).
				Err(uerr).Msg("reshard: EC upgrade failed")
			return nil
		}
		converted++
		m.totalConverted.Add(1)
		m.logger.Info().Str("bucket", ref.Bucket).Str("key", ref.Key).
			Int("old_k", recK).Int("old_m", recM).
			Int("new_k", currentCfg.DataShards).Int("new_m", currentCfg.ParityShards).
			Msg("reshard: upgraded EC encoding")
		return nil
	})
	m.totalSkipped.Add(uint64(skipped))
	if err != nil {
		m.logger.Warn().Err(err).Msg("reshard: iter failed")
	}

	// 링 리샤드 패스: RingVersion < currentRingVersion인 오브젝트를 현재 링으로 리샤드
	currentRingVer := m.backend.CurrentRingVersion()
	if currentRingVer > 0 {
		rerr := fsm.IterObjectMetas(func(ref ObjectMetaRef) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if ref.RingVersion >= currentRingVer {
				skipped++
				return nil
			}
			if err := m.backend.ReshardToRing(ctx, ref.Bucket, ref.Key, ref.RingVersion); err != nil {
				errs++
				m.totalErrors.Add(1)
				m.logger.Warn().Str("bucket", ref.Bucket).Str("key", ref.Key).
					Uint64("ring_ver", uint64(ref.RingVersion)).Err(err).Msg("reshard: ring-based reshard failed")
				return nil
			}
			converted++
			m.totalConverted.Add(1)
			m.logger.Info().Str("bucket", ref.Bucket).Str("key", ref.Key).
				Uint64("old_ring", uint64(ref.RingVersion)).Uint64("new_ring", uint64(currentRingVer)).
				Msg("reshard: resharded to new ring")
			return nil
		})
		if rerr != nil {
			m.logger.Warn().Err(rerr).Msg("reshard: ring iter failed")
		}
	}

	if converted > 0 || errs > 0 {
		m.logger.Info().Int("converted", converted).Int("skipped", skipped).Int("errors", errs).Msg("reshard: pass complete")
	}
	return converted, skipped, errs
}

// Start runs Run in a loop until ctx is cancelled.
func (m *ReshardManager) Start(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	m.logger.Info().Dur("interval", m.interval).Msg("reshard manager started")
	for {
		select {
		case <-ctx.Done():
			m.logger.Info().Msg("reshard manager stopped")
			return
		case <-ticker.C:
			_, _, _ = m.Run(ctx)
		}
	}
}

// ReshardStats is a point-in-time snapshot for observability.
type ReshardStats struct {
	TotalConverted uint64
	TotalSkipped   uint64
	TotalErrors    uint64
	TotalRuns      uint64
	LastRunNanos   int64
}

// Stats returns atomic counters. Safe to call concurrently.
func (m *ReshardManager) Stats() ReshardStats {
	return ReshardStats{
		TotalConverted: m.totalConverted.Load(),
		TotalSkipped:   m.totalSkipped.Load(),
		TotalErrors:    m.totalErrors.Load(),
		TotalRuns:      m.totalRuns.Load(),
		LastRunNanos:   m.lastRunNanos.Load(),
	}
}

// String for debug.
func (s ReshardStats) String() string {
	return fmt.Sprintf("reshard{converted=%d skipped=%d errors=%d runs=%d}",
		s.TotalConverted, s.TotalSkipped, s.TotalErrors, s.TotalRuns)
}
