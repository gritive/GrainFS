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
		resourcewatch.WatcherConfig{PollInterval: pollInterval},
		provider,
		detector,
		func(snapshot resourcewatch.FDSnapshot, decision *resourcewatch.Decision) {
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

func recordFDMetrics(nodeID string, snapshot resourcewatch.FDSnapshot, decision *resourcewatch.Decision) {
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
	for category, count := range snapshot.Categories {
		metrics.FDOpenByCategory.WithLabelValues(nodeID, string(category)).Set(float64(count))
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
	case resourcewatch.FDLevelOK:
		facts = append(facts, incident.Fact{
			CorrelationID: fdIncidentID(nodeID),
			Type:          incident.FactResolved,
			Message:       decision.Message,
			At:            at,
		})
	case resourcewatch.FDLevelWarn:
		facts = append(facts, incident.Fact{
			CorrelationID: fdIncidentID(nodeID),
			Type:          incident.FactDiagnosed,
			Action:        incident.ActionResourceWarning,
			Message:       decision.Message,
			At:            at,
		})
	case resourcewatch.FDLevelCritical:
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
	if clusterAlerts == nil || decision == nil || decision.Level == resourcewatch.FDLevelOK {
		return
	}
	severity := alerts.SeverityWarning
	if decision.Level == resourcewatch.FDLevelCritical {
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
