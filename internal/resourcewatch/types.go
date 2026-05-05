package resourcewatch

import (
	"context"
	"errors"
	"time"
)

var ErrInvalidSample = errors.New("resourcewatch: invalid sample")

// Category labels resource-specific subdivisions (e.g. FD socket vs badger,
// vlog raft vs incident). Opaque to Detector — used for diagnosis only.
type Category string

// Sample is one observation of an arbitrary resource: how much is in use vs
// the configured soft cap. Categories optional; nil OK.
type Sample struct {
	Open        int
	Limit       int
	Categories  map[Category]int
	CollectedAt time.Time
}

// Provider produces Samples. Implementations must be safe for concurrent use.
type Provider interface {
	Snapshot(ctx context.Context) (Sample, error)
}

type Level string

const (
	LevelOK       Level = "ok"
	LevelWarn     Level = "warn"
	LevelCritical Level = "critical"
)

type DetectorConfig struct {
	WarnRatio         float64
	CriticalRatio     float64
	ETAWindow         time.Duration
	RecoveryWindow    time.Duration
	MinSamples        int
	MaxSamples        int
	ClassificationCap int
}

type Decision struct {
	Level      Level
	Threshold  string
	Ratio      float64
	ETA        time.Duration
	Message    string
	Snapshot   Sample
	Categories map[Category]int
}
