package scrubber_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// segmentBackend embeds mockBackend and adds segment orphan support.
type segmentBackend struct {
	*mockBackend
	orphanSegments  map[string]time.Time // path → creation time
	deletedSegments []string
	appendableRecs  map[string][]scrubber.AppendableRecord
}

func newSegmentBackend() *segmentBackend {
	return &segmentBackend{
		mockBackend:    newMockBackend(),
		orphanSegments: make(map[string]time.Time),
		appendableRecs: make(map[string][]scrubber.AppendableRecord),
	}
}

func (b *segmentBackend) addOrphanSegment(path string, age time.Duration) {
	b.orphanSegments[path] = time.Now().Add(-age)
}

// WalkOrphanSegments implements OrphanSegmentWalkable (mock).
func (b *segmentBackend) WalkOrphanSegments(bucket string, known map[string]bool, fn func(string) error) error {
	const minAge = 5 * time.Minute
	now := time.Now()
	for p, t := range b.orphanSegments {
		if !strings.HasPrefix(p, bucket+"/") {
			continue
		}
		if now.Sub(t) < minAge {
			continue
		}
		if !known[p] {
			if err := fn(p); err != nil {
				return err
			}
		}
	}
	return nil
}

// DeleteOrphanSegment implements OrphanSegmentWalkable (mock).
func (b *segmentBackend) DeleteOrphanSegment(p string) error {
	if _, ok := b.orphanSegments[p]; !ok {
		return nil
	}
	delete(b.orphanSegments, p)
	b.deletedSegments = append(b.deletedSegments, p)
	return nil
}

// ScanAppendableObjects implements AppendableScannable (mock).
func (b *segmentBackend) ScanAppendableObjects(bucket string) (<-chan scrubber.AppendableRecord, error) {
	ch := make(chan scrubber.AppendableRecord, len(b.appendableRecs[bucket]))
	for _, rec := range b.appendableRecs[bucket] {
		ch <- rec
	}
	close(ch)
	return ch, nil
}

func TestSegmentSweep_Tombstone(t *testing.T) {
	b := newSegmentBackend()
	b.records["bucket"] = nil // bucket exists; no EC records
	b.addOrphanSegment("bucket/key_segments/blob1", 10*time.Minute)

	s := scrubber.New(b, time.Hour, scrubber.WithNoRetry())
	s.RunOnce(context.Background())
	require.Empty(t, b.deletedSegments, "first cycle should only tombstone")

	s.RunOnce(context.Background())
	require.Len(t, b.deletedSegments, 1, "second cycle should delete")
}
