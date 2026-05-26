package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// KEK envelope lifecycle metrics (Task 13). These are refreshed on every GET
// /v1/encrypt/kek/status — the diagnostic API is the single source of truth
// for the FSM-derived state, so operators scraping Prometheus and operators
// running `grainfs encrypt kek status` see the same numbers.
//
// seal_count / lease_count carry a kek_version label so per-version
// nonce-collision risk and lease-drain progress are visible.
var (
	kekActiveVersion = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_kek_active_version",
		Help: "Current active cluster KEK version (the version new DEK seals are wrapped under).",
	})

	kekSealCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_kek_seal_count",
		Help: "Active-AEAD seals attributed to each KEK version. Monitor the active version against nonce-collision thresholds (warn 1e8, alert 1e9).",
	}, []string{"kek_version"})

	kekLeaseCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "grainfs_kek_lease_count",
		Help: "In-flight KEK consumer leases per version. Must reach 0 before a retired version can be pruned.",
	}, []string{"kek_version"})

	kekRetiredCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grainfs_kek_retired_count",
		Help: "Number of KEK versions in retiring or pruned lifecycle state.",
	})
)

// SetKEKActiveVersion publishes the current active KEK version gauge.
func SetKEKActiveVersion(v uint32) { kekActiveVersion.Set(float64(v)) }

// SetKEKSealCount publishes the seal count for a KEK version.
func SetKEKSealCount(version uint32, count uint64) {
	kekSealCount.WithLabelValues(strconv.FormatUint(uint64(version), 10)).Set(float64(count))
}

// SetKEKLeaseCount publishes the in-flight lease count for a KEK version.
func SetKEKLeaseCount(version uint32, count uint64) {
	kekLeaseCount.WithLabelValues(strconv.FormatUint(uint64(version), 10)).Set(float64(count))
}

// SetKEKRetiredCount publishes the number of retiring/pruned KEK versions.
func SetKEKRetiredCount(n int) { kekRetiredCount.Set(float64(n)) }
