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

// StartFD spawns the FD-exhaustion watcher goroutine. Returns immediately;
// the goroutine exits when ctx is cancelled.
func StartFD(ctx context.Context, opts FDOptions, deps Deps) {
	detector := resourcewatch.NewDetector(resourcewatch.DetectorConfig{
		WarnRatio:         opts.WarnRatio,
		CriticalRatio:     opts.CriticalRatio,
		ETAWindow:         opts.ETAWindow,
		RecoveryWindow:    opts.RecoveryWindow,
		MinSamples:        2,
		MaxSamples:        20,
		ClassificationCap: opts.ClassificationCap,
		ResourceLabel:     "FD",
	})
	provider := resourcewatch.NewFDProvider(resourcewatch.FDProviderOptions{ClassificationCap: opts.ClassificationCap})
	watcher := resourcewatch.NewWatcher(
		resourcewatch.WatcherConfig{
			PollInterval:  opts.PollInterval,
			ErrorInterval: time.Minute,
			OnError: func(err error) {
				log.Warn().Err(err).Msg("fd resource watcher poll failed")
			},
		},
		provider,
		detector,
		func(snapshot resourcewatch.Sample, decision *resourcewatch.Decision) {
			recordFDMetrics(deps.NodeID, snapshot, decision)
		},
		func(ctx context.Context, decision *resourcewatch.Decision) error {
			if err := recordFDDecision(ctx, deps.Recorder, deps.NodeID, decision); err != nil {
				return err
			}
			sendFDAlert(deps.NodeID, deps.Alerts, decision)
			return nil
		},
	)
	go func() {
		if err := watcher.Run(ctx); err != nil && ctx.Err() == nil {
			log.Warn().Err(err).Msg("fd resource watcher stopped")
		}
	}()
	log.Info().Dur("interval", opts.PollInterval).Float64("warn_ratio", opts.WarnRatio).Float64("critical_ratio", opts.CriticalRatio).Msg("fd resource watcher started")
}

func recordFDMetrics(nodeID string, snapshot resourcewatch.Sample, decision *resourcewatch.Decision) {
	metrics.FDOpen.WithLabelValues(nodeID).Set(float64(snapshot.Open))
	metrics.FDLimit.WithLabelValues(nodeID).Set(float64(snapshot.Limit))
	if snapshot.Limit > 0 {
		metrics.FDUsedRatio.WithLabelValues(nodeID).Set(float64(snapshot.Open) / float64(snapshot.Limit))
	}
	metrics.FDETASeconds.WithLabelValues(nodeID, "warn").Set(-1)
	metrics.FDETASeconds.WithLabelValues(nodeID, "critical").Set(-1)
	if decision != nil && decision.ETA > 0 && decision.Threshold != "" {
		metrics.FDETASeconds.WithLabelValues(nodeID, decision.Threshold).Set(decision.ETA.Seconds())
	}
	for _, category := range fdMetricCategories() {
		metrics.FDOpenByCategory.WithLabelValues(nodeID, string(category)).Set(0)
	}
	for category, count := range snapshot.Categories {
		metrics.FDOpenByCategory.WithLabelValues(nodeID, string(category)).Set(float64(count))
	}
}

func fdMetricCategories() []resourcewatch.Category {
	return []resourcewatch.Category{
		resourcewatch.FDCategorySocket,
		resourcewatch.FDCategoryBadger,
		resourcewatch.FDCategoryReceiptOrEventStore,
		resourcewatch.FDCategoryNFSSession,
		resourcewatch.FDCategoryRegularFile,
		resourcewatch.FDCategoryUnknown,
	}
}

func recordFDDecision(ctx context.Context, recorder IncidentRecorder, nodeID string, decision *resourcewatch.Decision) error {
	if recorder == nil || decision == nil {
		return nil
	}
	at := decision.Snapshot.CollectedAt
	if at.IsZero() {
		at = time.Now()
	}
	facts := []incident.Fact{{
		CorrelationID: fdIncidentID(nodeID),
		Type:          incident.FactObserved,
		Cause:         incident.CauseFDExhaustionRisk,
		Scope:         incident.Scope{Kind: incident.ScopeNode, NodeID: nodeID},
		Message:       decision.Message,
		At:            at,
	}}
	switch decision.Level {
	case resourcewatch.LevelOK:
		facts = append(facts, incident.Fact{
			CorrelationID: fdIncidentID(nodeID),
			Type:          incident.FactResolved,
			Message:       decision.Message,
			At:            at,
		})
	case resourcewatch.LevelWarn:
		facts = append(facts, incident.Fact{
			CorrelationID: fdIncidentID(nodeID),
			Type:          incident.FactDiagnosed,
			Action:        incident.ActionResourceWarning,
			Message:       decision.Message,
			At:            at,
		})
	case resourcewatch.LevelCritical:
		facts = append(facts, incident.Fact{
			CorrelationID: fdIncidentID(nodeID),
			Type:          incident.FactDiagnosed,
			Action:        incident.ActionResourceWarning,
			Message:       decision.Message,
			At:            at,
		}, incident.Fact{
			CorrelationID: fdIncidentID(nodeID),
			Type:          incident.FactActionFailed,
			Action:        incident.ActionResourceWarning,
			ErrorCode:     "fd_critical",
			Message:       decision.Message,
			At:            at,
		})
	}
	return recorder.Record(ctx, facts)
}

func sendFDAlert(nodeID string, sender AlertsSender, decision *resourcewatch.Decision) {
	if sender == nil || decision == nil || decision.Level == resourcewatch.LevelOK {
		return
	}
	severity := alerts.SeverityWarning
	if decision.Level == resourcewatch.LevelCritical {
		severity = alerts.SeverityCritical
	}
	log.Warn().Str("level", string(decision.Level)).Float64("ratio", decision.Ratio).Msg(decision.Message)
	go func() {
		_ = sender.Send(alerts.Alert{
			Type:     "fd_" + string(decision.Level),
			Severity: severity,
			Resource: nodeID,
			Message:  decision.Message,
		})
	}()
}

func fdIncidentID(nodeID string) string {
	return fmt.Sprintf("fd-%s", nodeID)
}
