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

// kekLifecycleReader is the subset of *cluster.MetaFSM the collector needs to
// report retired_count. The FSM lifecycle table is the source of truth for
// retiring/pruned status, the same source the admin status endpoint renders —
// so the metric and the JSON agree by construction.
type kekLifecycleReader interface {
	RetiredKEKVersionCount() int
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
		"Number of KEK versions in retiring or pruned lifecycle state.",
		nil, nil,
	)
)

// kekCollector reads live KEK state at scrape time. keeper may be nil
// (encryption disabled / Phase A) — Collect then emits nothing. tracker and
// lifecycle may each be nil independently; the corresponding metric is then
// omitted (seal counts still emit).
type kekCollector struct {
	keeper    kekSealReader
	tracker   kekLeaseReader
	lifecycle kekLifecycleReader
}

// NewKEKCollector builds the collector. keeper, tracker, and lifecycle are the
// live sources; any may be nil. Register the result once at boot (see
// RegisterKEKCollector).
func NewKEKCollector(keeper kekSealReader, tracker kekLeaseReader, lifecycle kekLifecycleReader) prometheus.Collector {
	return &kekCollector{keeper: keeper, tracker: tracker, lifecycle: lifecycle}
}

// RegisterKEKCollector registers a KEK collector on the default registry. A nil
// keeper is a no-op (encryption disabled): nothing is registered, so no empty
// metric families appear on /metrics.
//
// Registration is idempotent: a second call (e.g. multiple boots within one
// test process) returns prometheus.AlreadyRegisteredError, which is swallowed.
// Production boots exactly once. Uses Register (not MustRegister) so a repeat
// registration does not panic.
func RegisterKEKCollector(keeper kekSealReader, tracker kekLeaseReader, lifecycle kekLifecycleReader) {
	if keeper == nil {
		return
	}
	if err := prometheus.Register(NewKEKCollector(keeper, tracker, lifecycle)); err != nil {
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
	for v, count := range seals {
		ch <- prometheus.MustNewConstMetric(
			kekSealCountDesc, prometheus.CounterValue, float64(count), kekVerLabel(v),
		)
	}

	// retired_count comes from the FSM lifecycle table (retiring/pruned), NOT
	// from the seal snapshot — a version rotated away from is "previous active",
	// not "retired", until an operator runs `encrypt kek retire`. Omitted (not
	// 0) when no lifecycle source is wired, matching active_version's behaviour.
	if c.lifecycle != nil {
		ch <- prometheus.MustNewConstMetric(
			kekRetiredCountDesc, prometheus.GaugeValue, float64(c.lifecycle.RetiredKEKVersionCount()),
		)
	}

	if c.tracker != nil {
		for v, count := range c.tracker.Snapshot() {
			ch <- prometheus.MustNewConstMetric(
				kekLeaseCountDesc, prometheus.GaugeValue, float64(count), kekVerLabel(v),
			)
		}
	}
}

func kekVerLabel(v uint32) string { return strconv.FormatUint(uint64(v), 10) }
