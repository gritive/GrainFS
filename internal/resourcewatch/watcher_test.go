package resourcewatch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeFDProvider struct {
	snapshots []FDSnapshot
	err       error
	calls     int
}

func (p *fakeFDProvider) Snapshot(ctx context.Context) (FDSnapshot, error) {
	if p.err != nil {
		return FDSnapshot{}, p.err
	}
	if p.calls >= len(p.snapshots) {
		return p.snapshots[len(p.snapshots)-1], nil
	}
	snapshot := p.snapshots[p.calls]
	p.calls++
	return snapshot, nil
}

type sequenceFDProvider struct {
	results []fdProviderResult
	calls   int
}

type fdProviderResult struct {
	snapshot FDSnapshot
	err      error
}

func (p *sequenceFDProvider) Snapshot(ctx context.Context) (FDSnapshot, error) {
	if p.calls >= len(p.results) {
		return p.results[len(p.results)-1].snapshot, p.results[len(p.results)-1].err
	}
	result := p.results[p.calls]
	p.calls++
	return result.snapshot, result.err
}

func TestWatcher_PollOnceEmitsMetricsAndDecision(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	provider := &fakeFDProvider{snapshots: []FDSnapshot{
		{Open: 850, Limit: 1000, CollectedAt: start, Categories: map[FDCategory]int{FDCategorySocket: 800}},
	}}
	var metricsSnapshot FDSnapshot
	var metricsDecision *Decision
	var received *Decision
	watcher := NewWatcher(
		WatcherConfig{},
		provider,
		NewDetector(DetectorConfig{WarnRatio: 0.80, CriticalRatio: 0.90, MinSamples: 1}),
		func(snapshot FDSnapshot, decision *Decision) {
			metricsSnapshot = snapshot
			metricsDecision = decision
		},
		func(ctx context.Context, decision *Decision) error {
			received = decision
			return nil
		},
	)

	require.NoError(t, watcher.PollOnce(context.Background()))
	require.NotNil(t, received)
	assert.Equal(t, FDLevelWarn, received.Level)
	assert.Equal(t, 850, metricsSnapshot.Open)
	assert.Same(t, received, metricsDecision)
}

func TestWatcher_SuppressesNilDecision(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	provider := &fakeFDProvider{snapshots: []FDSnapshot{{Open: 100, Limit: 1000, CollectedAt: start}}}
	called := false
	watcher := NewWatcher(
		WatcherConfig{},
		provider,
		NewDetector(DetectorConfig{WarnRatio: 0.80, CriticalRatio: 0.90, MinSamples: 1}),
		func(snapshot FDSnapshot, decision *Decision) {},
		func(ctx context.Context, decision *Decision) error {
			called = true
			return nil
		},
	)

	require.NoError(t, watcher.PollOnce(context.Background()))
	assert.False(t, called)
}

func TestWatcher_PropagatesProviderError(t *testing.T) {
	wantErr := errors.New("boom")
	watcher := NewWatcher(
		WatcherConfig{},
		&fakeFDProvider{err: wantErr},
		NewDetector(DetectorConfig{}),
		nil,
		nil,
	)

	err := watcher.PollOnce(context.Background())
	assert.ErrorIs(t, err, wantErr)
}

func TestWatcher_RunStopsOnContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Unix(100, 0).UTC()
	watcher := NewWatcher(
		WatcherConfig{PollInterval: time.Millisecond},
		&fakeFDProvider{snapshots: []FDSnapshot{{Open: 100, Limit: 1000, CollectedAt: start}}},
		NewDetector(DetectorConfig{}),
		nil,
		nil,
	)

	err := watcher.Run(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestWatcher_RunContinuesAfterTransientProviderError(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wantErr := errors.New("temporary fd read failure")
	provider := &sequenceFDProvider{results: []fdProviderResult{
		{err: wantErr},
		{snapshot: FDSnapshot{Open: 850, Limit: 1000, CollectedAt: start}},
	}}
	var observedErrors []error
	var received *Decision
	watcher := NewWatcher(
		WatcherConfig{
			PollInterval:  time.Millisecond,
			ErrorInterval: time.Millisecond,
			OnError: func(err error) {
				observedErrors = append(observedErrors, err)
			},
		},
		provider,
		NewDetector(DetectorConfig{WarnRatio: 0.80, CriticalRatio: 0.90, MinSamples: 1}),
		nil,
		func(ctx context.Context, decision *Decision) error {
			received = decision
			cancel()
			return nil
		},
	)

	err := watcher.Run(ctx)
	assert.ErrorIs(t, err, context.Canceled)
	require.Len(t, observedErrors, 1)
	assert.ErrorIs(t, observedErrors[0], wantErr)
	require.NotNil(t, received)
	assert.Equal(t, FDLevelWarn, received.Level)
}
