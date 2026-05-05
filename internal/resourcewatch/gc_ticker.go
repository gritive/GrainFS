package resourcewatch

import (
	"context"
	"errors"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// gcMaxIterPerDBPerTick caps per-DB rewrites in a single ticker tick. Bounds
// the goroutine so write-churn DBs cannot starve other entries (Arch #3
// plan-eng-review).
const gcMaxIterPerDBPerTick = 8

// GCMetricsRecorder receives per-tick GC outcomes for Prometheus counters.
// Implementations are expected to be safe for concurrent calls. Defined as
// an interface so the resourcewatch package stays free of a metrics import.
type GCMetricsRecorder interface {
	IncRuns(category Category)                   // every tick attempt (success, ErrNoRewrite, or error)
	IncFailures(category Category)               // failure only (excludes ErrNoRewrite)
	SetConsecutive(category Category, n float64) // current consecutiveGCFailures gauge
}

// GCTickerConfig configures the BadgerDB vlog GC ticker. OnFailIncident is
// invoked at most once per leak episode (Arch #2 transition-only via
// incidentFired); subsequent ErrNoRewrite re-arms the flag.
type GCTickerConfig struct {
	Interval       time.Duration
	FailThreshold  int32
	OnFailIncident func(Category, error)
	Registry       *Registry
	Metrics        GCMetricsRecorder // optional; nil disables counters
}

// RunGCTicker drives RunValueLogGC across all registered DBs sequentially every
// cfg.Interval. Snapshot-then-unlock (Arch #1) ensures Register/Deregister are
// not blocked by GC duration.
func RunGCTicker(ctx context.Context, cfg GCTickerConfig) {
	r := cfg.Registry
	if r == nil {
		r = Default
	}
	t := time.NewTicker(cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, e := range r.Snapshot() {
				gc := func(entry *RegisteredDB) func() error {
					return func() error { return entry.DB.RunValueLogGC(0.5) }
				}(e)
				gcOnceWith(e, cfg, gc)
			}
		}
	}
}

// gcOnceWith runs the GC fn up to gcMaxIterPerDBPerTick times. ErrNoRewrite
// resets failure counter and re-arms incidentFired (so a future failure can
// fire again). Other errors increment the counter and fire OnFailIncident
// only on the first transition past threshold.
func gcOnceWith(e *RegisteredDB, cfg GCTickerConfig, gc func() error) {
	for i := 0; i < gcMaxIterPerDBPerTick; i++ {
		err := gc()
		if cfg.Metrics != nil {
			cfg.Metrics.IncRuns(e.Category)
		}
		if errors.Is(err, badger.ErrNoRewrite) {
			e.consecutiveGCFailures.Store(0)
			e.incidentFired.Store(false)
			if cfg.Metrics != nil {
				cfg.Metrics.SetConsecutive(e.Category, 0)
			}
			return
		}
		if err != nil {
			if cfg.Metrics != nil {
				cfg.Metrics.IncFailures(e.Category)
			}
			n := e.consecutiveGCFailures.Add(1)
			if cfg.Metrics != nil {
				cfg.Metrics.SetConsecutive(e.Category, float64(n))
			}
			if n >= cfg.FailThreshold && cfg.OnFailIncident != nil {
				if e.incidentFired.CompareAndSwap(false, true) {
					cfg.OnFailIncident(e.Category, err)
				}
			}
			return
		}
		// success — proceed to the next file (bounded by gcMaxIterPerDBPerTick)
	}
}
