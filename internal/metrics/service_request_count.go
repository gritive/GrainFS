package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// ServiceRequestCount returns the cumulative number of service requests counted
// across all label combinations of ServiceRequestsTotal.
//
// It is sampled off the hot path (the cluster request-rate collector calls it
// once per gossip interval) so it adds zero per-request cost: ServiceRequestsTotal
// is already incremented per request and sharded by label set, and this only sums
// the existing children. The returned value is monotonic; callers derive a rate by
// differencing successive samples over elapsed time.
func ServiceRequestCount() float64 {
	ch := make(chan prometheus.Metric, 256)
	go func() {
		ServiceRequestsTotal.Collect(ch)
		close(ch)
	}()
	var sum float64
	for m := range ch {
		var out dto.Metric
		if err := m.Write(&out); err != nil {
			continue
		}
		if c := out.GetCounter(); c != nil {
			sum += c.GetValue()
		}
	}
	return sum
}
