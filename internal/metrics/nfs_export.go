package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	NFSExportsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_nfs_exports_total",
		Help: "Active NFS bucket exports by state.",
	}, []string{"state"})

	NFSExportPropagationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "grainfs_nfs_export_propagation_seconds",
		Help:    "Time to propagate NFS export registry change to all NFS-serving nodes.",
		Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5},
	})

	NFSLookupUnknownExportTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_nfs_lookup_unknown_export_total",
		Help: "Cumulative LOOKUPs at pseudo-root for unregistered buckets.",
	})

	NFSRevokedStateIDs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "grainfs_nfs_revoked_stateids_total",
		Help: "Cumulative NFSv4 stateids revoked due to export changes.",
	}, []string{"reason"})
)
