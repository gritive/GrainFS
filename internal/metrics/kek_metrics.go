package metrics

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// KEK envelope lifecycle metrics (Task 13).
//
// These are emitted by a custom prometheus.Collector that reads live state at
// scrape time — NOT a set of gauges poked on an HTTP call. Prometheus scrapes
// /metrics, so the collector reads the DEKKeeper (active version + per-version
// seal counts) and the KEKLeaseTracker (per-version lease counts) directly on
// every Collect(). This keeps grainfs_kek_seal_count scrape-fresh so the
// runbook's PromQL nonce-collision alerts fire autonomously, with no polling
// of the admin status endpoint required.
//
// The reads are off the hot path: SealCountSnapshot takes the keeper's
// retiredSeals RLock + two atomic loads; the lease tracker snapshot takes its
// own RLock. The encrypt seal/open hot path is untouched.

// kekSealReader is the subset of *encrypt.DEKKeeper the collector needs.
// Defined here (consumer side) so the metrics package does not import encrypt.
type kekSealReader interface {
	ActiveKEKVersion() uint32
	SealCountSnapshot() map[uint32]uint64
}

// kekLeaseReader is the subset of *encrypt.KEKLeaseTracker the collector needs.
type kekLeaseReader interface {
	Snapshot() map[uint32]uint64
}

var (
	kekActiveVersionDesc = prometheus.NewDesc(
		"grainfs_kek_active_version",
		"Current active cluster KEK version (the version new DEK seals are wrapped under).",
		nil, nil,
	)
	kekSealCountDesc = prometheus.NewDesc(
		"grainfs_kek_seal_count",
		"Active-AEAD seals attributed to each KEK version (monotonic per version). Monitor the active version against nonce-collision thresholds (warn 1e8, alert 1e9).",
		[]string{"kek_version"}, nil,
	)
	kekLeaseCountDesc = prometheus.NewDesc(
		"grainfs_kek_lease_count",
		"In-flight KEK consumer leases per version. Must reach 0 before a retired version can be pruned.",
		[]string{"kek_version"}, nil,
	)
	kekRetiredCountDesc = prometheus.NewDesc(
		"grainfs_kek_retired_count",
		"Number of non-active KEK versions the keeper still tracks (versions rotated away from).",
		nil, nil,
	)
)

// kekCollector reads live KEK state at scrape time. keeper may be nil
// (encryption disabled / Phase A) — Collect then emits nothing. tracker may be
// nil independently; lease counts are then omitted (seal counts still emit).
type kekCollector struct {
	keeper  kekSealReader
	tracker kekLeaseReader
}

// NewKEKCollector builds the collector. keeper and tracker are the live
// sources; either may be nil. Register the result with prometheus.MustRegister
// once at boot (see RegisterKEKCollector).
func NewKEKCollector(keeper kekSealReader, tracker kekLeaseReader) prometheus.Collector {
	return &kekCollector{keeper: keeper, tracker: tracker}
}

// RegisterKEKCollector registers a KEK collector on the default registry. A nil
// keeper is a no-op (encryption disabled): nothing is registered, so no empty
// metric families appear on /metrics.
//
// Registration is idempotent: a second call (e.g. multiple boots within one
// test process) returns prometheus.AlreadyRegisteredError, which is swallowed.
// Production boots exactly once. Uses Register (not MustRegister) so a repeat
// registration does not panic.
func RegisterKEKCollector(keeper kekSealReader, tracker kekLeaseReader) {
	if keeper == nil {
		return
	}
	if err := prometheus.Register(NewKEKCollector(keeper, tracker)); err != nil {
		var already prometheus.AlreadyRegisteredError
		if !errors.As(err, &already) {
			panic(fmt.Errorf("RegisterKEKCollector: %w", err))
		}
	}
}

func (c *kekCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- kekActiveVersionDesc
	ch <- kekSealCountDesc
	ch <- kekLeaseCountDesc
	ch <- kekRetiredCountDesc
}

func (c *kekCollector) Collect(ch chan<- prometheus.Metric) {
	if c.keeper == nil {
		return
	}
	active := c.keeper.ActiveKEKVersion()
	ch <- prometheus.MustNewConstMetric(kekActiveVersionDesc, prometheus.GaugeValue, float64(active))

	seals := c.keeper.SealCountSnapshot()
	retired := 0
	for v, count := range seals {
		ch <- prometheus.MustNewConstMetric(
			kekSealCountDesc, prometheus.CounterValue, float64(count), kekVerLabel(v),
		)
		if v != active {
			retired++
		}
	}
	ch <- prometheus.MustNewConstMetric(kekRetiredCountDesc, prometheus.GaugeValue, float64(retired))

	if c.tracker != nil {
		for v, count := range c.tracker.Snapshot() {
			ch <- prometheus.MustNewConstMetric(
				kekLeaseCountDesc, prometheus.GaugeValue, float64(count), kekVerLabel(v),
			)
		}
	}
}

func kekVerLabel(v uint32) string { return strconv.FormatUint(uint64(v), 10) }
