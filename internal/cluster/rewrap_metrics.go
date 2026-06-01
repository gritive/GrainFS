package cluster

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// RewrapECShardsTotal counts EC shards re-encrypted onto the active DEK
// generation by the EC rewrap lane, labelled by the active generation.
var RewrapECShardsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "grainfs_rewrap_ec_shards_total",
	Help: "EC shards re-encrypted onto the active DEK generation.",
}, []string{"active_gen"})

// RewrapFSMValuesTotal counts FSM-state values (bucket policy, object meta)
// re-sealed onto the active DEK generation by the FSM-value rewrap lane. S7-1a.
var RewrapFSMValuesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "grainfs_rewrap_fsm_values_total",
	Help: "FSM-state values (bucket policy, object meta) re-sealed onto the active DEK generation.",
}, []string{"active_gen"})
