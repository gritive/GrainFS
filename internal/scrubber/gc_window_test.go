package scrubber

import (
	"testing"
	"time"
)

func TestSweepRespectsRetentionWindow(t *testing.T) {
	now := time.Unix(10000, 0)
	window := time.Hour
	if evalGCCandidate(retentionInput{tZero: now.Add(-30 * time.Minute), hasTZero: true, now: now, window: window}) {
		t.Fatalf("young tombstoned chunk should be kept (within retention window)")
	}
	if !evalGCCandidate(retentionInput{tZero: now.Add(-2 * time.Hour), hasTZero: true, now: now, window: window}) {
		t.Fatalf("old tombstoned chunk should be deletable (window elapsed)")
	}
}

func TestSweepNoTombstoneFallsBackToAgeGate(t *testing.T) {
	if !evalGCCandidate(retentionInput{hasTZero: false}) {
		t.Fatalf("orphan without tombstone should be deletable (age gate already applied)")
	}
}
