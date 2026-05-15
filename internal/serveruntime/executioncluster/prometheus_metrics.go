package executioncluster

import (
	"time"

	grainmetrics "github.com/gritive/GrainFS/internal/metrics"
)

type PrometheusMetrics struct{}

func NewPrometheusMetrics() PrometheusMetrics {
	return PrometheusMetrics{}
}

func (PrometheusMetrics) RecordQueueDepth(depth int) {
	grainmetrics.ExecutionClusterQueueDepth.Set(float64(depth))
}

func (PrometheusMetrics) RecordJobDuration(duration time.Duration) {
	grainmetrics.ExecutionClusterJobDuration.Observe(duration.Seconds())
}

func (PrometheusMetrics) RecordRetry() {
	grainmetrics.ExecutionClusterRetriesTotal.Inc()
}

func (PrometheusMetrics) RecordTimeout() {
	grainmetrics.ExecutionClusterTimeoutsTotal.Inc()
}

func (PrometheusMetrics) RecordWorkerFailure() {
	grainmetrics.ExecutionClusterWorkerFailuresTotal.Inc()
}

func (PrometheusMetrics) RecordAggregationFailure() {
	grainmetrics.ExecutionClusterAggregationFailuresTotal.Inc()
}
