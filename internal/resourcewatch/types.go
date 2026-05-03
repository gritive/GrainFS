package resourcewatch

import (
	"context"
	"errors"
	"time"
)

var ErrInvalidFDSample = errors.New("resourcewatch: invalid fd sample")

type FDCategory string

const (
	FDCategorySocket              FDCategory = "socket"
	FDCategoryBadger              FDCategory = "badger"
	FDCategoryReceiptOrEventStore FDCategory = "receipt_or_event_store"
	FDCategoryNFSSession          FDCategory = "nfs_session"
	FDCategoryRegularFile         FDCategory = "regular_file"
	FDCategoryUnknown             FDCategory = "unknown"
)

type FDSnapshot struct {
	Open        int
	Limit       int
	Categories  map[FDCategory]int
	CollectedAt time.Time
}

type FDProvider interface {
	Snapshot(ctx context.Context) (FDSnapshot, error)
}

type FDLevel string

const (
	FDLevelOK       FDLevel = "ok"
	FDLevelWarn     FDLevel = "warn"
	FDLevelCritical FDLevel = "critical"
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
	Level      FDLevel
	Threshold  string
	Ratio      float64
	ETA        time.Duration
	Message    string
	Snapshot   FDSnapshot
	Categories map[FDCategory]int
}
