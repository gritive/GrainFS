package cluster

// Phase 18 Cluster EC — Slice 5: background N×→EC re-placement.
//
// When a cluster crosses the ec-data+ec-parity node threshold (or the operator
// flips --cluster-ec to true on an existing deployment), existing objects that
// were stored via the legacy N× path need to be converted. The ReshardManager
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
	"log/slog"
	"sync/atomic"
	"time"
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
}

// ReshardManager walks objects and triggers conversion from N× to EC.
type ReshardManager struct {
	backend  Converter
	leader   Leader
	interval time.Duration
	logger   *slog.Logger

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
		logger:   slog.With("component", "reshard-manager"),
	}
}

// Run performs one full scan + conversion pass. Returns (converted, skipped, errs).
// Safe to invoke manually (e.g., from an admin endpoint) in addition to the
// periodic loop.
func (m *ReshardManager) Run(ctx context.Context) (converted, skipped, errs int) {
	m.totalRuns.Add(1)
	m.lastRunNanos.Store(time.Now().UnixNano())

	if m.leader != nil && !m.leader.IsLeader() {
		m.logger.Debug("reshard: skipping — not leader")
		return 0, 0, 0
	}
	if !m.backend.ECActive() {
		m.logger.Debug("reshard: skipping — EC not active on this cluster")
		return 0, 0, 0
	}
	fsm := m.backend.FSMRef()
	if fsm == nil {
		return 0, 0, 0
	}

	err := fsm.IterObjectMetas(func(ref ObjectMetaRef) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Already has placement? Skip. Cheap in-memory lookup via FSM.
		if nodes, lookupErr := fsm.LookupShardPlacement(ref.Bucket, ref.Key); lookupErr == nil && len(nodes) > 0 {
			skipped++
			return nil
		}
		if cerr := m.backend.ConvertObjectToEC(ctx, ref.Bucket, ref.Key); cerr != nil {
			errs++
			m.totalErrors.Add(1)
			m.logger.Warn("reshard: convert failed",
				"bucket", ref.Bucket, "key", ref.Key, "error", cerr)
			return nil // continue scanning — don't abort the whole pass
		}
		converted++
		m.totalConverted.Add(1)
		m.logger.Info("reshard: converted to EC",
			"bucket", ref.Bucket, "key", ref.Key, "size", ref.Size)
		return nil
	})
	m.totalSkipped.Add(uint64(skipped))
	if err != nil {
		m.logger.Warn("reshard: iter failed", "error", err)
	}
	if converted > 0 || errs > 0 {
		m.logger.Info("reshard: pass complete",
			"converted", converted, "skipped", skipped, "errors", errs)
	}
	return converted, skipped, errs
}

// Start runs Run in a loop until ctx is cancelled.
func (m *ReshardManager) Start(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	m.logger.Info("reshard manager started", "interval", m.interval)
	for {
		select {
		case <-ctx.Done():
			m.logger.Info("reshard manager stopped")
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
