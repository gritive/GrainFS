package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// UnknownMetaCmdTotal counts raft MetaCmd entries ignored because this binary
// does not recognize or handle their command type.
var UnknownMetaCmdTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "grainfs_unknown_metacmd_total",
	Help: "Raft MetaCmd entries ignored because this binary does not recognize or handle their command type.",
}, []string{"type"})
