package receipt

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// BroadcastMetrics holds the Phase 16 Slice 2 counters tracking the
// RoutingCache-miss → cluster broadcast fallback path.
//
// The counters record the SRE-relevant question: when the dashboard asked
// for a receipt the local node didn't have, what did the cluster do?
//   - hit: exactly one peer returned the receipt.
//   - miss: every peer said "not found".
//   - timeout: 3s window elapsed before any peer answered.
//   - partial_success: a hit arrived but some peers hadn't responded yet
//     (observability signal — not a failure mode, just an SLO-relevant
//     hint that the cluster had degraded nodes during the query).
//
// Counters live in the receipt package so tests don't need Prometheus
// running. The cluster.BroadcastMetricsRecorder interface (S4) is
// satisfied by BroadcastMetrics.
var (
	BroadcastTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_receipt_broadcast_total",
		Help: "Total number of cluster-wide receipt broadcast queries initiated.",
	})
	BroadcastHit = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_receipt_broadcast_hit_total",
		Help: "Number of broadcast queries where at least one peer returned the receipt.",
	})
	BroadcastMiss = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_receipt_broadcast_miss_total",
		Help: "Number of broadcast queries where all peers reported not-found.",
	})
	BroadcastTimeout = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_receipt_broadcast_timeout_total",
		Help: "Number of broadcast queries that exceeded the timeout before any peer answered.",
	})
	BroadcastPartialSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_receipt_broadcast_partial_success_total",
		Help: "Broadcasts where fewer than all peers responded before the hit was counted — flags degraded peers during audit.",
	})
)

// BroadcastMetrics is the production implementation of
// cluster.BroadcastMetricsRecorder. Plumb it into the broadcaster via
// SetMetrics. A zero value is safe to use — Prometheus counters are
// package-level globals.
type BroadcastMetrics struct{}

// OnBroadcastStart increments the total counter for every query initiated.
func (BroadcastMetrics) OnBroadcastStart() { BroadcastTotal.Inc() }

// OnBroadcastHit is called when a peer answered with the receipt.
func (BroadcastMetrics) OnBroadcastHit() { BroadcastHit.Inc() }

// OnBroadcastMiss is called when every peer reported not-found.
func (BroadcastMetrics) OnBroadcastMiss() { BroadcastMiss.Inc() }

// OnBroadcastTimeout is called when the per-query deadline elapsed.
func (BroadcastMetrics) OnBroadcastTimeout() { BroadcastTimeout.Inc() }

// OnBroadcastPartialSuccess is called when a hit arrived before all peers
// had responded. responded/total arguments are currently unused (the
// counter is a simple Inc) but the interface preserves them for a future
// histogram that would track responded/total ratio distribution.
func (BroadcastMetrics) OnBroadcastPartialSuccess(_, _ int) {
	BroadcastPartialSuccess.Inc()
}
