package resourcewatch

import (
	"context"
	"time"
)

type WatcherConfig struct {
	PollInterval time.Duration
}

type MetricsSink func(FDSnapshot, *Decision)

type DecisionSink func(context.Context, *Decision) error

type Watcher struct {
	cfg        WatcherConfig
	provider   FDProvider
	detector   *Detector
	onMetrics  MetricsSink
	onDecision DecisionSink
}

func NewWatcher(cfg WatcherConfig, provider FDProvider, detector *Detector, onMetrics MetricsSink, onDecision DecisionSink) *Watcher {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Second
	}
	return &Watcher{
		cfg:        cfg,
		provider:   provider,
		detector:   detector,
		onMetrics:  onMetrics,
		onDecision: onDecision,
	}
}

func (w *Watcher) PollOnce(ctx context.Context) error {
	snapshot, err := w.provider.Snapshot(ctx)
	if err != nil {
		return err
	}
	decision, err := w.detector.Observe(snapshot)
	if err != nil {
		return err
	}
	if w.onMetrics != nil {
		w.onMetrics(snapshot, decision)
	}
	if decision != nil && w.onDecision != nil {
		if err := w.onDecision(ctx, decision); err != nil {
			return err
		}
	}
	return nil
}

func (w *Watcher) Run(ctx context.Context) error {
	if err := w.PollOnce(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := w.PollOnce(ctx); err != nil {
				return err
			}
		}
	}
}
