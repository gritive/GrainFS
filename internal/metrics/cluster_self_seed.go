package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ClusterSelfSeeded is set to 1 when a genesis node self-generated its cluster
// transport key at boot (no cluster-key / invite-bundle / peers, empty data
// dir). Operators running unattended automation alert on this to catch an
// accidental fork (e.g. a node meant to invite-join but missing its bundle): the
// startup WARN log is invisible to automation, this gauge is not.
var ClusterSelfSeeded = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "grainfs_cluster_self_seeded",
	Help: "1 if this node self-generated its cluster key at genesis bootstrap, else 0.",
})
