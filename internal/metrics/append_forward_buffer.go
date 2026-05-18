package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	AppendForwardBufferInflightBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_cluster_append_forward_buffer_inflight_bytes",
		Help: "Bytes currently reserved in the AppendObject forward buffer pool.",
	})

	AppendForwardBufferRejectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "grainfs_cluster_append_forward_buffer_rejected_total",
		Help: "Number of AppendObject forward attempts rejected due to full forward buffer.",
	})
)
