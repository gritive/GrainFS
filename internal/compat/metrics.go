package compat

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var CapabilityRejectTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "grainfs_capability_reject_total",
	Help: "Capability gate rejections by capability, scope, severity, operation, and forced status.",
}, []string{"capability", "scope", "severity", "operation", "forced"})

func RejectLabels(plan GatePlan, forced bool) map[string]string {
	return map[string]string{
		"capability": plan.Capability,
		"scope":      string(plan.Scope),
		"severity":   string(plan.Severity),
		"operation":  string(plan.Operation),
		"forced":     strconv.FormatBool(forced),
	}
}

func RecordReject(plan GatePlan, forced bool) {
	labels := RejectLabels(plan, forced)
	CapabilityRejectTotal.WithLabelValues(
		labels["capability"],
		labels["scope"],
		labels["severity"],
		labels["operation"],
		labels["forced"],
	).Inc()
}
