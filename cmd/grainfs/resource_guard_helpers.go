package main

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/resourceguard"
	"github.com/gritive/GrainFS/internal/server"
)

func fdWatchEnabled(cmd *cobra.Command) bool {
	enabled, _ := cmd.Flags().GetBool("fd-watch-enabled")
	return enabled
}

func goroutineWatchEnabled(cmd *cobra.Command) bool {
	enabled, _ := cmd.Flags().GetBool("goroutine-watch-enabled")
	return enabled
}

func vlogWatchEnabled(cmd *cobra.Command) bool {
	enabled, _ := cmd.Flags().GetBool("vlog-watch-enabled")
	return enabled
}

func fdOptionsFromCmd(cmd *cobra.Command) resourceguard.FDOptions {
	pollInterval, _ := cmd.Flags().GetDuration("fd-watch-interval")
	warnRatio, _ := cmd.Flags().GetFloat64("fd-warn-threshold")
	criticalRatio, _ := cmd.Flags().GetFloat64("fd-critical-threshold")
	etaWindow, _ := cmd.Flags().GetDuration("fd-eta-window")
	recoveryWindow, _ := cmd.Flags().GetDuration("fd-recovery-window")
	classificationCap, _ := cmd.Flags().GetInt("fd-classification-cap")
	return resourceguard.FDOptions{
		PollInterval:      pollInterval,
		WarnRatio:         warnRatio,
		CriticalRatio:     criticalRatio,
		ETAWindow:         etaWindow,
		RecoveryWindow:    recoveryWindow,
		ClassificationCap: classificationCap,
	}
}

func goroutineOptionsFromCmd(cmd *cobra.Command) resourceguard.GoroutineOptions {
	pollInterval, _ := cmd.Flags().GetDuration("goroutine-poll-interval")
	warnCount, _ := cmd.Flags().GetInt("goroutine-warn")
	criticalCount, _ := cmd.Flags().GetInt("goroutine-critical")
	etaWindow, _ := cmd.Flags().GetDuration("goroutine-eta-window")
	recoveryWindow, _ := cmd.Flags().GetDuration("goroutine-recovery-window")
	return resourceguard.GoroutineOptions{
		PollInterval:   pollInterval,
		WarnCount:      warnCount,
		CriticalCount:  criticalCount,
		ETAWindow:      etaWindow,
		RecoveryWindow: recoveryWindow,
	}
}

func vlogOptionsFromCmd(cmd *cobra.Command, dataDir string) resourceguard.VlogOptions {
	pollInterval, _ := cmd.Flags().GetDuration("vlog-poll-interval")
	warnRatio, _ := cmd.Flags().GetFloat64("vlog-warn-ratio")
	criticalRatio, _ := cmd.Flags().GetFloat64("vlog-critical-ratio")
	etaWindow, _ := cmd.Flags().GetDuration("vlog-eta-window")
	recoveryWindow, _ := cmd.Flags().GetDuration("vlog-recovery-window")
	gcInterval, _ := cmd.Flags().GetDuration("badger-gc-interval")
	gcDisable, _ := cmd.Flags().GetBool("badger-gc-disable")
	gcFailThreshold, _ := cmd.Flags().GetInt32("badger-gc-fail-threshold")
	strict, _ := cmd.Flags().GetBool("strict-vlog-registry")
	smokeDefer, _ := cmd.Flags().GetDuration("vlog-smoke-defer")
	return resourceguard.VlogOptions{
		DataDir:         dataDir,
		PollInterval:    pollInterval,
		WarnRatio:       warnRatio,
		CriticalRatio:   criticalRatio,
		ETAWindow:       etaWindow,
		RecoveryWindow:  recoveryWindow,
		GCInterval:      gcInterval,
		GCDisable:       gcDisable,
		GCFailThreshold: gcFailThreshold,
		StrictRegistry:  strict,
		SmokeDefer:      smokeDefer,
	}
}

// startResourceGuards reads watcher flags and dispatches to internal/resourceguard.
// Each monitor is independently enable-gated; disabled ones are silently skipped.
func startResourceGuards(ctx context.Context, cmd *cobra.Command, nodeID, dataDir string, recorder *incident.Recorder, clusterAlerts *server.AlertsState) {
	deps := resourceguard.Deps{
		NodeID:   nodeID,
		Alerts:   clusterAlerts,
		Recorder: recorder,
	}
	if fdWatchEnabled(cmd) {
		resourceguard.StartFD(ctx, fdOptionsFromCmd(cmd), deps)
	}
	if goroutineWatchEnabled(cmd) {
		resourceguard.StartGoroutine(ctx, goroutineOptionsFromCmd(cmd), deps)
	}
	if vlogWatchEnabled(cmd) {
		resourceguard.StartVlog(ctx, vlogOptionsFromCmd(cmd, dataDir), deps)
	}
}
