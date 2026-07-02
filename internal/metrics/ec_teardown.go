package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ECDetachedTeardowns counts EC read teardown goroutines currently parked
// waiting for background shard-body producers to exit after an aborted GET.
// A sustained high value means abort/retry pressure against a slow or
// trickling peer is pinning connections and admission slots.
var ECDetachedTeardowns = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "grainfs_ec_detached_teardowns",
	Help: "EC read teardown goroutines parked awaiting producer exit after abort.",
})
