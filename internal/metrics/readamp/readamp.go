// Package readamp simulates how a unified buffer cache would behave on the
// real read paths so we can decide whether to build one. It does NOT cache
// data — each Tracker keeps only the keys that would have been resident
// under a given cache size, and every Record call answers one question:
// "would a cache of this size have hit on this key?"
//
// We run several trackers in parallel (different sizes, different paths)
// and expose hit/miss counters as Prometheus metrics. Looking at the
// resulting hit-rate curve (small cache → large cache) tells us:
//
//   - flat-and-low (e.g. 2% across all sizes): workload has no temporal
//     locality on this path; even an unbounded cache would not help.
//     A unified buffer cache would be wasted memory.
//   - rising sharply with size: workload has a working set that fits in
//     a reachable cache size. Building a UBC is worth it; the curve
//     directly tells us how much memory to budget.
//   - already saturated at small sizes: tiny per-path caches would do.
//     Skip the architecture-level UBC; just add a narrow shard cache.
//
// The simulator is OFF by default. Call ENABLE only when --measure-read-amp
// is set on serve, or in benchmarks. Production overhead when disabled is
// a single atomic.Bool load per Record call.
package readamp

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var enabled atomic.Bool

// Enable turns on every registered tracker. Default is off so production
// builds pay only an atomic.Bool load per Record.
func Enable() { enabled.Store(true) }

// Disable turns trackers off. Used by tests that want a clean baseline.
func Disable() { enabled.Store(false) }

// IsEnabled reports the current state.
func IsEnabled() bool { return enabled.Load() }

// Tracker simulates an LRU-bounded cache by tracking which keys would still
// be resident if a real cache of `capacity` entries had observed the same
// access stream. It records per-tracker hit/miss counters as Prometheus
// metrics, labeled by the tracker name (e.g. "volume_64mb").
//
// Tracker is safe for concurrent use. The internal LRU is mutex-guarded;
// the lock is held only for the brief window of a map lookup + list move,
// so contention on a single tracker is bounded by access rate, not by
// fan-out.
type Tracker struct {
	name     string
	capacity int

	mu      sync.Mutex
	entries *list.List
	index   map[string]*list.Element

	hits   prometheus.Counter
	misses prometheus.Counter
}

// New builds a tracker with the given name and capacity (in entries).
// The Prometheus counters are registered eagerly so dashboards can
// reference them even before traffic arrives.
//
// Capacity is in entries, not bytes. Caller picks entries as
// (cache_bytes / typical_block_bytes); for GrainFS the relevant
// granularity is 4 KB (volume blocks) and ~1 MB (EC shards).
func New(name string, capacity int) *Tracker {
	if capacity <= 0 {
		capacity = 1
	}
	return &Tracker{
		name:     name,
		capacity: capacity,
		entries:  list.New(),
		index:    make(map[string]*list.Element, capacity),
		hits: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "grainfs_readamp_hits_total",
			Help:        "Simulated cache hits — how often a key Record-ed had been seen recently and would still be resident under this tracker's capacity.",
			ConstLabels: prometheus.Labels{"tracker": name},
		}),
		misses: promauto.NewCounter(prometheus.CounterOpts{
			Name:        "grainfs_readamp_misses_total",
			Help:        "Simulated cache misses — how often a key Record-ed was not in the simulator's resident set under this tracker's capacity.",
			ConstLabels: prometheus.Labels{"tracker": name},
		}),
	}
}

// Record observes one access for `key`. Returns true when the simulator
// considers this a hit (key was already resident). Records the access
// in the LRU regardless, so the next Record sees the updated state.
//
// When the simulator is disabled globally (Enable not called), Record
// returns false immediately without touching the LRU — the goal is to
// pay zero overhead on production read paths.
func (t *Tracker) Record(key string) bool {
	if !enabled.Load() {
		return false
	}
	t.mu.Lock()
	if elem, ok := t.index[key]; ok {
		t.entries.MoveToFront(elem)
		t.mu.Unlock()
		t.hits.Inc()
		return true
	}
	elem := t.entries.PushFront(key)
	t.index[key] = elem
	if t.entries.Len() > t.capacity {
		oldest := t.entries.Back()
		if oldest != nil {
			t.entries.Remove(oldest)
			delete(t.index, oldest.Value.(string))
		}
	}
	t.mu.Unlock()
	t.misses.Inc()
	return false
}

// Reset clears the resident set. Counters are NOT reset (Prometheus
// counters are monotonic by contract). Tests that want zero counters
// should record their assertions as deltas, not absolutes.
func (t *Tracker) Reset() {
	t.mu.Lock()
	t.entries.Init()
	for k := range t.index {
		delete(t.index, k)
	}
	t.mu.Unlock()
}

// Snapshot reads the current hit/miss counts and the resident set size.
// Useful for tests; production reads go through Prometheus scraping.
func (t *Tracker) Snapshot() (hits, misses uint64, resident int) {
	t.mu.Lock()
	resident = t.entries.Len()
	t.mu.Unlock()
	return readCounter(t.hits), readCounter(t.misses), resident
}

func readCounter(c prometheus.Counter) uint64 {
	var m prometheus.Metric = c
	dto := dtoFromMetric(m)
	if dto == nil {
		return 0
	}
	return uint64(dto.GetCounter().GetValue())
}
