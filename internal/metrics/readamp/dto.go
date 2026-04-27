package readamp

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// dtoFromMetric extracts the protobuf payload from a Prometheus metric.
// We need it so Snapshot can read the counter value back out for tests
// without setting up a full registry/Gather pipeline. Returns nil on
// the unlikely error path (collector misimplementation); tests assert
// on Snapshot returning zero in that case.
func dtoFromMetric(m prometheus.Metric) *dto.Metric {
	out := &dto.Metric{}
	if err := m.Write(out); err != nil {
		return nil
	}
	return out
}
