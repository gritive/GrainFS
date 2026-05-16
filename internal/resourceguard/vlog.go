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

// gcMetricsRecorder bridges resourcewatch GC ticks to Prometheus counters,
// labeled by node + category. Stateless — safe for concurrent calls.
type gcMetricsRecorder struct{ nodeID string }

func (r gcMetricsRecorder) IncRuns(cat resourcewatch.Category) {
	metrics.BadgerGCRunsTotal.WithLabelValues(r.nodeID, string(cat)).Inc()
}

func (r gcMetricsRecorder) IncFailures(cat resourcewatch.Category) {
	metrics.BadgerGCFailuresTotal.WithLabelValues(r.nodeID, string(cat)).Inc()
}

func (r gcMetricsRecorder) SetConsecutive(cat resourcewatch.Category, n float64) {
	metrics.BadgerGCConsecutiveFailures.WithLabelValues(r.nodeID, string(cat)).Set(n)
}

// StartVlog spawns the vlog-pressure watcher plus the deferred smoke verifier
// and (unless GCDisable) the GC ticker. Invalid ratios disable the watcher
// without spawning anything.
func StartVlog(ctx context.Context, opts VlogOptions, deps Deps) {
	if opts.WarnRatio <= 0 || opts.CriticalRatio <= 0 || opts.WarnRatio >= opts.CriticalRatio || opts.CriticalRatio >= 1.0 {
		log.Warn().Float64("warn", opts.WarnRatio).Float64("critical", opts.CriticalRatio).Msg("vlog watcher: invalid ratios, disabled")
		return
	}

	detector := resourcewatch.NewDetector(resourcewatch.DetectorConfig{
		WarnRatio:      opts.WarnRatio,
		CriticalRatio:  opts.CriticalRatio,
		ETAWindow:      opts.ETAWindow,
		RecoveryWindow: opts.RecoveryWindow,
		MinSamples:     2,
		MaxSamples:     20,
		ResourceLabel:  "vlog",
	})
	provider := resourcewatch.NewVlogProvider(resourcewatch.VlogProviderOptions{DataDir: opts.DataDir})
	watcher := resourcewatch.NewWatcher(
		resourcewatch.WatcherConfig{
			PollInterval:  opts.PollInterval,
			ErrorInterval: time.Minute,
			OnError: func(err error) {
				log.Warn().Err(err).Msg("vlog resource watcher poll failed")
			},
		},
		provider,
		detector,
		func(sample resourcewatch.Sample, decision *resourcewatch.Decision) {
			recordVlogMetrics(deps.NodeID, sample, decision)
		},
		func(ctx context.Context, decision *resourcewatch.Decision) error {
			if err := recordVlogDecision(ctx, deps.Recorder, deps.NodeID, decision); err != nil {
				return err
			}
			sendVlogAlert(deps.NodeID, deps.Alerts, decision)
			return nil
		},
	)
	go func() {
		if err := watcher.Run(ctx); err != nil && ctx.Err() == nil {
			log.Warn().Err(err).Msg("vlog resource watcher stopped")
		}
	}()

	// Perf #1: deferred smoke so async-loaded group-raft DBs can register.
	// Default 60s; e2e tests pass a lower value via --vlog-smoke-defer.
	smokeDefer := opts.SmokeDefer
	if smokeDefer <= 0 {
		smokeDefer = 60 * time.Second
	}
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(smokeDefer):
		}
		report, err := resourcewatch.VerifyVlogRegistry(opts.DataDir, nil, opts.StrictRegistry)
		if err != nil {
			if opts.StrictRegistry {
				log.Fatal().Err(err).Msg("vlog registry smoke verification failed (strict mode)")
			}
			log.Warn().Err(err).Msg("vlog registry smoke verification error")
		}
		if len(report.Live) > 0 {
			recordSmokeUnderPopulatedIncident(ctx, deps.Recorder, deps.NodeID, report.Live)
		}
	}()

	if !opts.GCDisable {
		go resourcewatch.RunGCTicker(ctx, resourcewatch.GCTickerConfig{
			Interval:      opts.GCInterval,
			FailThreshold: opts.GCFailThreshold,
			Metrics:       gcMetricsRecorder{nodeID: deps.NodeID},
			OnFailIncident: func(cat resourcewatch.Category, err error) {
				recordBadgerGCFailedIncident(ctx, deps.Recorder, deps.NodeID, cat, err)
				if deps.Alerts != nil {
					deps.Alerts.Send(alerts.Alert{
						Type:     "badger_gc_failed",
						Severity: alerts.SeverityCritical,
						Resource: fmt.Sprintf("%s/%s", deps.NodeID, cat),
						Message:  fmt.Sprintf("BadgerDB vlog GC failed %d times for category=%s: %v", opts.GCFailThreshold, cat, err),
					})
				}
			},
		})
	}

	log.Info().
		Dur("interval", opts.PollInterval).
		Float64("warn", opts.WarnRatio).
		Float64("critical", opts.CriticalRatio).
		Dur("gc_interval", opts.GCInterval).
		Bool("gc_disable", opts.GCDisable).
		Bool("strict_smoke", opts.StrictRegistry).
		Msg("vlog resource watcher started")
}

func recordVlogMetrics(nodeID string, sample resourcewatch.Sample, decision *resourcewatch.Decision) {
	metrics.VlogBytes.WithLabelValues(nodeID).Set(float64(sample.Open))
	metrics.VlogLimitBytes.WithLabelValues(nodeID).Set(float64(sample.Limit))
	if sample.Limit > 0 {
		metrics.VlogUsedRatio.WithLabelValues(nodeID).Set(float64(sample.Open) / float64(sample.Limit))
	}
	for cat, bytes := range sample.Categories {
		metrics.VlogBytesByCategory.WithLabelValues(nodeID, string(cat)).Set(float64(bytes))
	}
	metrics.VlogETASeconds.WithLabelValues(nodeID, "warn").Set(-1)
	metrics.VlogETASeconds.WithLabelValues(nodeID, "critical").Set(-1)
	if decision != nil && decision.ETA > 0 && decision.Threshold != "" {
		metrics.VlogETASeconds.WithLabelValues(nodeID, decision.Threshold).Set(decision.ETA.Seconds())
	}
}

func recordVlogDecision(ctx context.Context, recorder IncidentRecorder, nodeID string, decision *resourcewatch.Decision) error {
	if recorder == nil || decision == nil {
		return nil
	}
	at := decision.Snapshot.CollectedAt
	if at.IsZero() {
		at = time.Now()
	}
	facts := []incident.Fact{{
		CorrelationID: vlogIncidentID(nodeID),
		Type:          incident.FactObserved,
		Cause:         incident.CauseVlogPressure,
		Scope:         incident.Scope{Kind: incident.ScopeNode, NodeID: nodeID},
		Message:       decision.Message,
		At:            at,
	}}
	switch decision.Level {
	case resourcewatch.LevelOK:
		facts = append(facts, incident.Fact{
			CorrelationID: vlogIncidentID(nodeID),
			Type:          incident.FactResolved,
			Cause:         incident.CauseVlogPressure,
			Message:       decision.Message,
			At:            at,
		})
	case resourcewatch.LevelWarn:
		facts = append(facts, incident.Fact{
			CorrelationID: vlogIncidentID(nodeID),
			Type:          incident.FactDiagnosed,
			Cause:         incident.CauseVlogPressure,
			Action:        incident.ActionResourceWarning,
			Message:       vlogDecisionMessageWithBreakdown(decision),
			At:            at,
		})
	case resourcewatch.LevelCritical:
		msg := vlogDecisionMessageWithBreakdown(decision)
		facts = append(facts, incident.Fact{
			CorrelationID: vlogIncidentID(nodeID),
			Type:          incident.FactDiagnosed,
			Cause:         incident.CauseVlogPressure,
			Action:        incident.ActionResourceWarning,
			Message:       msg,
			At:            at,
		}, incident.Fact{
			CorrelationID: vlogIncidentID(nodeID),
			Type:          incident.FactActionFailed,
			Cause:         incident.CauseVlogPressure,
			Action:        incident.ActionResourceWarning,
			ErrorCode:     "vlog_critical",
			Message:       msg,
			At:            at,
		})
	}
	return recorder.Record(ctx, facts)
}

// vlogDecisionMessageWithBreakdown appends the top-3 categories sorted desc by
// bytes (D7 sub-decision) so the operator sees which DB dominates.
func vlogDecisionMessageWithBreakdown(decision *resourcewatch.Decision) string {
	if decision == nil {
		return ""
	}
	type kv struct {
		Cat   resourcewatch.Category
		Bytes int
	}
	var sorted []kv
	for c, b := range decision.Snapshot.Categories {
		sorted = append(sorted, kv{c, b})
	}
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j].Bytes > sorted[j-1].Bytes; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	if len(sorted) > 3 {
		sorted = sorted[:3]
	}
	if len(sorted) == 0 {
		return decision.Message
	}
	parts := decision.Message + " — top:"
	for i, e := range sorted {
		if i > 0 {
			parts += ","
		}
		parts += fmt.Sprintf(" %s=%d", e.Cat, e.Bytes)
	}
	return parts
}

func sendVlogAlert(nodeID string, sender AlertsSender, decision *resourcewatch.Decision) {
	if sender == nil || decision == nil || decision.Level == resourcewatch.LevelOK {
		return
	}
	severity := alerts.SeverityWarning
	if decision.Level == resourcewatch.LevelCritical {
		severity = alerts.SeverityCritical
	}
	log.Warn().Str("level", string(decision.Level)).Float64("ratio", decision.Ratio).Msg(decision.Message)
	sender.Send(alerts.Alert{
		Type:     "vlog_" + string(decision.Level),
		Severity: severity,
		Resource: nodeID,
		Message:  decision.Message,
	})
}

func recordBadgerGCFailedIncident(ctx context.Context, recorder IncidentRecorder, nodeID string, cat resourcewatch.Category, gcErr error) {
	if recorder == nil {
		return
	}
	at := time.Now()
	id := badgerGCIncidentID(nodeID, cat)
	facts := []incident.Fact{
		{
			CorrelationID: id,
			Type:          incident.FactObserved,
			Cause:         incident.CauseBadgerGCFailed,
			Scope:         incident.Scope{Kind: incident.ScopeNode, NodeID: nodeID},
			Message:       fmt.Sprintf("BadgerDB vlog GC failed for category=%s: %v", cat, gcErr),
			At:            at,
		},
		{
			CorrelationID: id,
			Type:          incident.FactActionFailed,
			Cause:         incident.CauseBadgerGCFailed,
			ErrorCode:     "gc_failed",
			Message:       fmt.Sprintf("RunValueLogGC failed N consecutive times for category=%s: %v", cat, gcErr),
			At:            at,
		},
	}
	if err := recorder.Record(ctx, facts); err != nil {
		log.Warn().Err(err).Str("category", string(cat)).Msg("badger gc failed: incident record error")
	}
}

func recordSmokeUnderPopulatedIncident(ctx context.Context, recorder IncidentRecorder, nodeID string, live []string) {
	if recorder == nil || len(live) == 0 {
		return
	}
	at := time.Now()
	id := smokeIncidentID(nodeID)
	facts := []incident.Fact{
		{
			CorrelationID: id,
			Type:          incident.FactObserved,
			Cause:         incident.CauseRegistryUnderPopulated,
			Scope:         incident.Scope{Kind: incident.ScopeNode, NodeID: nodeID},
			Message:       fmt.Sprintf("vlog smoke detected unregistered DB dirs: %v", live),
			At:            at,
		},
		{
			CorrelationID: id,
			Type:          incident.FactDiagnosed,
			Cause:         incident.CauseRegistryUnderPopulated,
			Action:        incident.ActionResourceWarning,
			Message:       fmt.Sprintf("vlog smoke detected unregistered DB dirs: %v", live),
			At:            at,
		},
	}
	if err := recorder.Record(ctx, facts); err != nil {
		log.Warn().Err(err).Strs("live", live).Msg("smoke under-populated: incident record error")
	}
}

func vlogIncidentID(nodeID string) string {
	return fmt.Sprintf("vlog-%s", nodeID)
}

func badgerGCIncidentID(nodeID string, cat resourcewatch.Category) string {
	return fmt.Sprintf("badger-gc-%s-%s", nodeID, cat)
}

func smokeIncidentID(nodeID string) string {
	return fmt.Sprintf("vlog-smoke-%s", nodeID)
}
