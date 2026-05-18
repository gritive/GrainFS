package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// AppendSizeCapRejectedTotal counts AppendObject requests rejected by the
// FSM-side authoritative size cap check.
var AppendSizeCapRejectedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "grainfs_append_size_cap_rejected_total",
	Help: "Number of AppendObject requests rejected because the object would exceed SizeCapBytes.",
})
