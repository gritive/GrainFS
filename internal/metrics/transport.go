package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// TransportCECounter counts QUIC mux capability exchange outcomes.
//
//	role:    "dialer" | "acceptor"
//	outcome: "success" | "failure"
//	reason:  "" on success; one of version_mismatch, wrong_first_stream,
//	         payload_length, feature_unsupported, timeout, io_error on failure.
var TransportCECounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "grainfs_transport_ce_total",
	Help: "QUIC mux capability exchange outcomes by role/outcome/reason.",
}, []string{"role", "outcome", "reason"})
