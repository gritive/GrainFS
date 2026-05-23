package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// FSM apply-batching metrics. See
// docs/superpowers/specs/2026-05-23-raft-fsm-apply-batching-design.md § Metrics.
var (
	// ApplyBatchSize records how many committed Raft entries were applied per
	// BadgerDB transaction commit. A distribution skewed toward 1 means no
	// batching is happening (low load, or batching disabled); higher buckets
	// confirm the actor is coalescing under load.
	ApplyBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_apply_batch_size",
		Help:    "Committed Raft entries applied per BadgerDB transaction commit.",
		Buckets: []float64{1, 2, 4, 8, 16, 32, 64},
	})

	// ApplyBatchCommitFallbackTotal counts batches that fell back to
	// individual-transaction replay (Commit failure or mid-batch ErrTxnTooBig).
	// Zero in healthy operation; a rising value means the validate-before-write
	// invariant broke or the size proxy is chronically underestimating.
	ApplyBatchCommitFallbackTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_apply_batch_commit_fallback_total",
		Help: "FSM apply batches that fell back to per-entry transaction replay.",
	})
)
