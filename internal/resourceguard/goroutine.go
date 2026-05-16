package resourceguard

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/resourcewatch"
)

// StartGoroutine spawns the goroutine-runaway watcher. Invalid thresholds
// (CriticalCount ≤ 0, WarnCount ≤ 0, WarnCount ≥ CriticalCount) log a
// warning and disable the watcher without spawning anything.
func StartGoroutine(ctx context.Context, opts GoroutineOptions, deps Deps) {
	if opts.CriticalCount <= 0 || opts.WarnCount <= 0 || opts.WarnCount >= opts.CriticalCount {
		log.Warn().Int("warn", opts.WarnCount).Int("critical", opts.CriticalCount).Msg("goroutine watcher: invalid thresholds, disabled")
		return
	}

	warnRatio := float64(opts.WarnCount) / float64(opts.CriticalCount)
	detector := resourcewatch.NewDetector(resourcewatch.DetectorConfig{
		WarnRatio:      warnRatio,
		CriticalRatio:  1.0,
		ETAWindow:      opts.ETAWindow,
		RecoveryWindow: opts.RecoveryWindow,
		MinSamples:     2,
		MaxSamples:     20,
		ResourceLabel:  "goroutine",
	})
	provider := resourcewatch.NewGoroutineProvider(resourcewatch.GoroutineProviderOptions{Limit: opts.CriticalCount})
	watcher := resourcewatch.NewWatcher(
		resourcewatch.WatcherConfig{
			PollInterval:  opts.PollInterval,
			ErrorInterval: time.Minute,
			OnError: func(err error) {
				log.Warn().Err(err).Msg("goroutine resource watcher poll failed")
			},
		},
		provider,
		detector,
		func(sample resourcewatch.Sample, decision *resourcewatch.Decision) {
			recordGoroutineMetrics(deps.NodeID, sample, decision)
		},
		func(ctx context.Context, decision *resourcewatch.Decision) error {
			if err := recordGoroutineDecision(ctx, deps.Recorder, deps.NodeID, decision); err != nil {
				return err
			}
			sendGoroutineAlert(deps.NodeID, deps.Alerts, decision)
			return nil
		},
	)
	go func() {
		if err := watcher.Run(ctx); err != nil && ctx.Err() == nil {
			log.Warn().Err(err).Msg("goroutine resource watcher stopped")
		}
	}()
	log.Info().Dur("interval", opts.PollInterval).Int("warn", opts.WarnCount).Int("critical", opts.CriticalCount).Msg("goroutine resource watcher started")
}

func recordGoroutineMetrics(nodeID string, sample resourcewatch.Sample, decision *resourcewatch.Decision) {
	metrics.GoroutineCount.WithLabelValues(nodeID).Set(float64(sample.Open))
	metrics.GoroutineLimit.WithLabelValues(nodeID).Set(float64(sample.Limit))
	if sample.Limit > 0 {
		metrics.GoroutineUsedRatio.WithLabelValues(nodeID).Set(float64(sample.Open) / float64(sample.Limit))
	}
	metrics.GoroutineETASeconds.WithLabelValues(nodeID, "warn").Set(-1)
	metrics.GoroutineETASeconds.WithLabelValues(nodeID, "critical").Set(-1)
	if decision != nil && decision.ETA > 0 && decision.Threshold != "" {
		metrics.GoroutineETASeconds.WithLabelValues(nodeID, decision.Threshold).Set(decision.ETA.Seconds())
	}
}

func recordGoroutineDecision(ctx context.Context, recorder IncidentRecorder, nodeID string, decision *resourcewatch.Decision) error {
	if recorder == nil || decision == nil {
		return nil
	}
	at := decision.Snapshot.CollectedAt
	if at.IsZero() {
		at = time.Now()
	}
	facts := []incident.Fact{{
		CorrelationID: goroutineIncidentID(nodeID),
		Type:          incident.FactObserved,
		Cause:         incident.CauseGoroutineRunaway,
		Scope:         incident.Scope{Kind: incident.ScopeNode, NodeID: nodeID},
		Message:       decision.Message,
		At:            at,
	}}
	switch decision.Level {
	case resourcewatch.LevelOK:
		facts = append(facts, incident.Fact{
			CorrelationID: goroutineIncidentID(nodeID),
			Type:          incident.FactResolved,
			Message:       decision.Message,
			At:            at,
		})
	case resourcewatch.LevelWarn:
		facts = append(facts, incident.Fact{
			CorrelationID: goroutineIncidentID(nodeID),
			Type:          incident.FactDiagnosed,
			Action:        incident.ActionResourceWarning,
			Message:       decision.Message,
			At:            at,
		})
	case resourcewatch.LevelCritical:
		facts = append(facts, incident.Fact{
			CorrelationID: goroutineIncidentID(nodeID),
			Type:          incident.FactDiagnosed,
			Action:        incident.ActionResourceWarning,
			Message:       decision.Message,
			At:            at,
		}, incident.Fact{
			CorrelationID: goroutineIncidentID(nodeID),
			Type:          incident.FactActionFailed,
			Action:        incident.ActionResourceWarning,
			ErrorCode:     "goroutine_critical",
			Message:       decision.Message,
			At:            at,
		})
	}
	return recorder.Record(ctx, facts)
}

func sendGoroutineAlert(nodeID string, sender AlertsSender, decision *resourcewatch.Decision) {
	if sender == nil || decision == nil || decision.Level == resourcewatch.LevelOK {
		return
	}
	severity := alerts.SeverityWarning
	if decision.Level == resourcewatch.LevelCritical {
		severity = alerts.SeverityCritical
	}
	log.Warn().Str("level", string(decision.Level)).Float64("ratio", decision.Ratio).Msg(decision.Message)
	sender.Send(alerts.Alert{
		Type:     "goroutine_" + string(decision.Level),
		Severity: severity,
		Resource: nodeID,
		Message:  decision.Message,
	})
}

func goroutineIncidentID(nodeID string) string {
	return fmt.Sprintf("goroutine-%s", nodeID)
}
