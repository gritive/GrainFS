package resourcewatch

import (
	"context"
	"runtime"
	"time"
)

// GoroutineProviderOptions configure GoroutineProvider. Limit is the
// "critical" goroutine count — Sample.Open / Limit ratio drives Detector
// thresholds (WarnRatio < 1.0, CriticalRatio = 1.0).
type GoroutineProviderOptions struct {
	Limit int
}

// GoroutineProvider samples runtime.NumGoroutine() against a soft Limit.
// No category breakdown — operators inspect goroutine stacks via pprof
// when an alert fires.
type GoroutineProvider struct {
	limit int
}

// NewGoroutineProvider constructs a GoroutineProvider. Limit must be > 0;
// negative or zero limits force Snapshot to return ErrInvalidSample.
func NewGoroutineProvider(opts GoroutineProviderOptions) *GoroutineProvider {
	return &GoroutineProvider{limit: opts.Limit}
}

// Snapshot returns the current goroutine count vs the configured Limit.
func (p *GoroutineProvider) Snapshot(ctx context.Context) (Sample, error) {
	_ = ctx
	if p.limit <= 0 {
		return Sample{}, ErrInvalidSample
	}
	return Sample{
		Open:        runtime.NumGoroutine(),
		Limit:       p.limit,
		Categories:  nil,
		CollectedAt: time.Now().UTC(),
	}, nil
}
