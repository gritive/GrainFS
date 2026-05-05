package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/resourcewatch"
	"github.com/gritive/GrainFS/internal/server"
)

func fdWatchEnabled(cmd *cobra.Command) bool {
	enabled, _ := cmd.Flags().GetBool("fd-watch-enabled")
	return enabled
}

func startFDResourceMonitor(ctx context.Context, cmd *cobra.Command, nodeID string, recorder *incident.Recorder, clusterAlerts *server.AlertsState) {
	if !fdWatchEnabled(cmd) {
		return
	}

	pollInterval, _ := cmd.Flags().GetDuration("fd-watch-interval")
	warnRatio, _ := cmd.Flags().GetFloat64("fd-warn-threshold")
	criticalRatio, _ := cmd.Flags().GetFloat64("fd-critical-threshold")
	etaWindow, _ := cmd.Flags().GetDuration("fd-eta-window")
	recoveryWindow, _ := cmd.Flags().GetDuration("fd-recovery-window")
	classificationCap, _ := cmd.Flags().GetInt("fd-classification-cap")

	detector := resourcewatch.NewDetector(resourcewatch.DetectorConfig{
		WarnRatio:         warnRatio,
		CriticalRatio:     criticalRatio,
		ETAWindow:         etaWindow,
		RecoveryWindow:    recoveryWindow,
		MinSamples:        2,
		MaxSamples:        20,
		ClassificationCap: classificationCap,
	})
	provider := resourcewatch.NewFDProvider(resourcewatch.FDProviderOptions{ClassificationCap: classificationCap})
	watcher := resourcewatch.NewWatcher(
		resourcewatch.WatcherConfig{
			PollInterval:  pollInterval,
			ErrorInterval: time.Minute,
			OnError: func(err error) {
				log.Warn().Err(err).Msg("fd resource watcher poll failed")
			},
		},
		provider,
		detector,
		func(snapshot resourcewatch.Sample, decision *resourcewatch.Decision) {
			recordFDMetrics(nodeID, snapshot, decision)
		},
		func(ctx context.Context, decision *resourcewatch.Decision) error {
			if err := recordFDDecision(ctx, recorder, nodeID, decision); err != nil {
				return err
			}
			sendFDAlert(nodeID, clusterAlerts, decision)
			return nil
		},
	)
	go func() {
		if err := watcher.Run(ctx); err != nil && ctx.Err() == nil {
			log.Warn().Err(err).Msg("fd resource watcher stopped")
		}
	}()
	log.Info().Dur("interval", pollInterval).Float64("warn_ratio", warnRatio).Float64("critical_ratio", criticalRatio).Msg("fd resource watcher started")
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

func recordFDDecision(ctx context.Context, recorder *incident.Recorder, nodeID string, decision *resourcewatch.Decision) error {
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

func sendFDAlert(nodeID string, clusterAlerts *server.AlertsState, decision *resourcewatch.Decision) {
	if clusterAlerts == nil || decision == nil || decision.Level == resourcewatch.LevelOK {
		return
	}
	severity := alerts.SeverityWarning
	if decision.Level == resourcewatch.LevelCritical {
		severity = alerts.SeverityCritical
	}
	log.Warn().Str("level", string(decision.Level)).Float64("ratio", decision.Ratio).Msg(decision.Message)
	go func() {
		_ = clusterAlerts.Send(alerts.Alert{
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

func goroutineWatchEnabled(cmd *cobra.Command) bool {
	enabled, _ := cmd.Flags().GetBool("goroutine-watch-enabled")
	return enabled
}

func startGoroutineResourceMonitor(ctx context.Context, cmd *cobra.Command, nodeID string, recorder *incident.Recorder, clusterAlerts *server.AlertsState) {
	if !goroutineWatchEnabled(cmd) {
		return
	}

	pollInterval, _ := cmd.Flags().GetDuration("goroutine-poll-interval")
	warnCount, _ := cmd.Flags().GetInt("goroutine-warn")
	criticalCount, _ := cmd.Flags().GetInt("goroutine-critical")
	etaWindow, _ := cmd.Flags().GetDuration("goroutine-eta-window")
	recoveryWindow, _ := cmd.Flags().GetDuration("goroutine-recovery-window")

	if criticalCount <= 0 || warnCount <= 0 || warnCount >= criticalCount {
		log.Warn().Int("warn", warnCount).Int("critical", criticalCount).Msg("goroutine watcher: invalid thresholds, disabled")
		return
	}

	warnRatio := float64(warnCount) / float64(criticalCount)
	detector := resourcewatch.NewDetector(resourcewatch.DetectorConfig{
		WarnRatio:      warnRatio,
		CriticalRatio:  1.0,
		ETAWindow:      etaWindow,
		RecoveryWindow: recoveryWindow,
		MinSamples:     2,
		MaxSamples:     20,
	})
	provider := resourcewatch.NewGoroutineProvider(resourcewatch.GoroutineProviderOptions{Limit: criticalCount})
	watcher := resourcewatch.NewWatcher(
		resourcewatch.WatcherConfig{
			PollInterval:  pollInterval,
			ErrorInterval: time.Minute,
			OnError: func(err error) {
				log.Warn().Err(err).Msg("goroutine resource watcher poll failed")
			},
		},
		provider,
		detector,
		func(sample resourcewatch.Sample, decision *resourcewatch.Decision) {
			recordGoroutineMetrics(nodeID, sample, decision)
		},
		func(ctx context.Context, decision *resourcewatch.Decision) error {
			if err := recordGoroutineDecision(ctx, recorder, nodeID, decision); err != nil {
				return err
			}
			sendGoroutineAlert(nodeID, clusterAlerts, decision)
			return nil
		},
	)
	go func() {
		if err := watcher.Run(ctx); err != nil && ctx.Err() == nil {
			log.Warn().Err(err).Msg("goroutine resource watcher stopped")
		}
	}()
	log.Info().Dur("interval", pollInterval).Int("warn", warnCount).Int("critical", criticalCount).Msg("goroutine resource watcher started")
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

func recordGoroutineDecision(ctx context.Context, recorder *incident.Recorder, nodeID string, decision *resourcewatch.Decision) error {
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

func sendGoroutineAlert(nodeID string, clusterAlerts *server.AlertsState, decision *resourcewatch.Decision) {
	if clusterAlerts == nil || decision == nil || decision.Level == resourcewatch.LevelOK {
		return
	}
	severity := alerts.SeverityWarning
	if decision.Level == resourcewatch.LevelCritical {
		severity = alerts.SeverityCritical
	}
	log.Warn().Str("level", string(decision.Level)).Float64("ratio", decision.Ratio).Msg(decision.Message)
	go func() {
		_ = clusterAlerts.Send(alerts.Alert{
			Type:     "goroutine_" + string(decision.Level),
			Severity: severity,
			Resource: nodeID,
			Message:  decision.Message,
		})
	}()
}

func goroutineIncidentID(nodeID string) string {
	return fmt.Sprintf("goroutine-%s", nodeID)
}
