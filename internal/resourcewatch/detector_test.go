package resourcewatch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetector_PredictsWarningFromPositiveTrend(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	d := NewDetector(DetectorConfig{
		WarnRatio:         0.80,
		CriticalRatio:     0.90,
		ETAWindow:         30 * time.Minute,
		RecoveryWindow:    time.Minute,
		MinSamples:        2,
		ClassificationCap: 128,
	})
	decision, err := d.Observe(FDSnapshot{Open: 600, Limit: 1000, CollectedAt: start})
	require.NoError(t, err)
	require.Nil(t, decision)
	decision, err = d.Observe(FDSnapshot{Open: 700, Limit: 1000, CollectedAt: start.Add(10 * time.Minute), Categories: map[FDCategory]int{FDCategorySocket: 500}})
	require.NoError(t, err)
	require.NotNil(t, decision)
	assert.Equal(t, FDLevelWarn, decision.Level)
	assert.Equal(t, "warn", decision.Threshold)
	assert.InDelta(t, 600, decision.ETA.Seconds(), 1)
	assert.Contains(t, decision.Message, "projected")
}

func TestDetector_SuppressesFlatAndDecreasingTrend(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	d := NewDetector(DetectorConfig{WarnRatio: 0.80, CriticalRatio: 0.90, ETAWindow: 30 * time.Minute, RecoveryWindow: time.Minute, MinSamples: 2})
	decision, err := d.Observe(FDSnapshot{Open: 700, Limit: 1000, CollectedAt: start})
	require.NoError(t, err)
	require.Nil(t, decision)
	decision, err = d.Observe(FDSnapshot{Open: 690, Limit: 1000, CollectedAt: start.Add(10 * time.Minute)})
	require.NoError(t, err)
	assert.Nil(t, decision)
}

func TestDetector_FiresWarnAndCriticalTransitionsOnce(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	d := NewDetector(DetectorConfig{WarnRatio: 0.80, CriticalRatio: 0.90, ETAWindow: 30 * time.Minute, RecoveryWindow: time.Minute, MinSamples: 1})
	decision, err := d.Observe(FDSnapshot{Open: 850, Limit: 1000, CollectedAt: start})
	require.NoError(t, err)
	require.NotNil(t, decision)
	assert.Equal(t, FDLevelWarn, decision.Level)
	decision, err = d.Observe(FDSnapshot{Open: 860, Limit: 1000, CollectedAt: start.Add(30 * time.Second)})
	require.NoError(t, err)
	assert.Nil(t, decision, "same warning level must not refire")
	decision, err = d.Observe(FDSnapshot{Open: 930, Limit: 1000, CollectedAt: start.Add(time.Minute)})
	require.NoError(t, err)
	require.NotNil(t, decision)
	assert.Equal(t, FDLevelCritical, decision.Level)
}

func TestDetector_ResolvesAfterStableRecoveryWindow(t *testing.T) {
	start := time.Unix(100, 0).UTC()
	d := NewDetector(DetectorConfig{WarnRatio: 0.80, CriticalRatio: 0.90, ETAWindow: 30 * time.Minute, RecoveryWindow: time.Minute, MinSamples: 1})
	_, err := d.Observe(FDSnapshot{Open: 850, Limit: 1000, CollectedAt: start})
	require.NoError(t, err)
	decision, err := d.Observe(FDSnapshot{Open: 700, Limit: 1000, CollectedAt: start.Add(30 * time.Second)})
	require.NoError(t, err)
	assert.Nil(t, decision, "recovery must wait for stable window")
	decision, err = d.Observe(FDSnapshot{Open: 690, Limit: 1000, CollectedAt: start.Add(91 * time.Second)})
	require.NoError(t, err)
	require.NotNil(t, decision)
	assert.Equal(t, FDLevelOK, decision.Level)
}

func TestDetector_RejectsInvalidSamples(t *testing.T) {
	d := NewDetector(DetectorConfig{WarnRatio: 0.80, CriticalRatio: 0.90, ETAWindow: 30 * time.Minute, RecoveryWindow: time.Minute, MinSamples: 1})
	_, err := d.Observe(FDSnapshot{Open: 10, Limit: 0, CollectedAt: time.Unix(100, 0)})
	assert.ErrorIs(t, err, ErrInvalidFDSample)
	_, err = d.Observe(FDSnapshot{Open: -1, Limit: 100, CollectedAt: time.Unix(101, 0)})
	assert.ErrorIs(t, err, ErrInvalidFDSample)
}
