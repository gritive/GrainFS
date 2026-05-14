package audit

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// auditDropsTotal counts events dropped due to ring overflow (per node).
	auditDropsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "audit_drops_total",
		Help: "Total audit events dropped due to ring overflow.",
	}, []string{"node"})

	// auditCommitLagSeconds measures time between event creation and Iceberg commit.
	auditCommitLagSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "audit_commit_lag_seconds",
		Help:    "Time between event creation and Iceberg CommitTable completion.",
		Buckets: []float64{1, 5, 15, 30, 60, 120, 300},
	}, []string{"node"})

	// auditCommitterState is 1 when acting as leader (committing), 0 when follower (shipping).
	auditCommitterState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "audit_committer_state",
		Help: "1=leader (committing), 0=follower (shipping). Per node.",
	}, []string{"node"})

	auditOutboxBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "audit_outbox_backlog",
		Help: "Number of durable audit events pending commit.",
	})

	auditOutboxOldestPendingUS = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "audit_outbox_oldest_pending_us",
		Help: "Oldest pending durable audit event timestamp in Unix microseconds.",
	})
)
