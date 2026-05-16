// Package resourceguard runs the FD / goroutine / vlog resource monitors that
// consume samples and decisions from internal/resourcewatch and fan them out
// to Prometheus metrics, the incident store, and the cluster alerts surface.
//
// resourcewatch is the producer (sampling + threshold detection); resourceguard
// is the consumer that records the response.
package resourceguard

import (
	"context"
	"time"

	"github.com/gritive/GrainFS/internal/alerts"
	"github.com/gritive/GrainFS/internal/incident"
)

// AlertsSender is the slim interface resourceguard needs from the cluster
// alerts plumbing. Implemented by *server.AlertsState; defined here so the
// package does not import internal/server. Send is fire-and-forget.
type AlertsSender interface {
	Send(alerts.Alert)
}

// IncidentRecorder is the slim interface resourceguard needs to record
// incident facts. Implemented by *incident.Recorder; defined here so tests
// can substitute a fake without spinning up a full reducer/store stack.
type IncidentRecorder interface {
	Record(ctx context.Context, facts []incident.Fact) error
}

// Deps bundles the shared dependencies every Start* function consumes.
type Deps struct {
	NodeID   string
	Alerts   AlertsSender     // optional; nil disables alert fan-out
	Recorder IncidentRecorder // optional; nil skips incident recording
}

// FDOptions configures StartFD.
type FDOptions struct {
	PollInterval      time.Duration
	WarnRatio         float64
	CriticalRatio     float64
	ETAWindow         time.Duration
	RecoveryWindow    time.Duration
	ClassificationCap int
}

// GoroutineOptions configures StartGoroutine. WarnCount and CriticalCount
// must satisfy 0 < WarnCount < CriticalCount.
type GoroutineOptions struct {
	PollInterval   time.Duration
	WarnCount      int
	CriticalCount  int
	ETAWindow      time.Duration
	RecoveryWindow time.Duration
}

// VlogOptions configures StartVlog. Ratios must satisfy
// 0 < WarnRatio < CriticalRatio < 1. SmokeDefer ≤ 0 falls back to 60s.
type VlogOptions struct {
	DataDir         string
	PollInterval    time.Duration
	WarnRatio       float64
	CriticalRatio   float64
	ETAWindow       time.Duration
	RecoveryWindow  time.Duration
	GCInterval      time.Duration
	GCDisable       bool
	GCFailThreshold int32
	StrictRegistry  bool
	SmokeDefer      time.Duration
}
