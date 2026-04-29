package cluster

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

// LoadProposer is the subset of MetaRaft used by LoadReporter.
type LoadProposer interface {
	ProposeLoadSnapshot(ctx context.Context, entries []LoadStatEntry) error
}

// LeaderChecker reports whether this node is the meta-Raft leader.
type LeaderChecker interface {
	IsLeader() bool
}

// LoadReporter periodically commits local NodeStatsStore contents to the
// meta-Raft log. Only fires when this node is the meta-Raft leader.
//
// Default interval is 30s (P1: ~44 MB/day at 3-node cluster).
type LoadReporter struct {
	nodeID   string
	store    *NodeStatsStore
	proposer LoadProposer
	leader   LeaderChecker
	interval time.Duration
}

const DefaultLoadReportInterval = 30 * time.Second

// NewLoadReporter creates a reporter where proposer also implements LeaderChecker.
func NewLoadReporter(nodeID string, store *NodeStatsStore, proposer interface {
	LoadProposer
	LeaderChecker
}, interval time.Duration) *LoadReporter {
	return NewLoadReporterWithLeaderCheck(nodeID, store, proposer, proposer, interval)
}

// NewLoadReporterWithLeaderCheck creates a reporter with separate proposer and leader checker.
func NewLoadReporterWithLeaderCheck(nodeID string, store *NodeStatsStore, proposer LoadProposer, leader LeaderChecker, interval time.Duration) *LoadReporter {
	return &LoadReporter{
		nodeID:   nodeID,
		store:    store,
		proposer: proposer,
		leader:   leader,
		interval: interval,
	}
}

// Run starts the reporter loop. Blocks until ctx is cancelled.
func (r *LoadReporter) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reportOnce(ctx)
		}
	}
}

func (r *LoadReporter) reportOnce(ctx context.Context) {
	if !r.leader.IsLeader() {
		return
	}
	all := r.store.GetAll()
	entries := make([]LoadStatEntry, 0, len(all))
	for _, ns := range all {
		if ns.DiskAvailBytes == 0 {
			continue // skip zero/stale entries (P1)
		}
		entries = append(entries, LoadStatEntry{
			NodeID:         ns.NodeID,
			DiskUsedPct:    ns.DiskUsedPct,
			DiskAvailBytes: ns.DiskAvailBytes,
			RequestsPerSec: ns.RequestsPerSec,
			UpdatedAt:      ns.UpdatedAt,
		})
	}
	if len(entries) == 0 {
		return
	}
	if err := r.proposer.ProposeLoadSnapshot(ctx, entries); err != nil {
		log.Warn().Err(err).Str("component", "load_reporter").Msg("ProposeLoadSnapshot failed")
	}
}
