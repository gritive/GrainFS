package scrubber_test

import (
	"context"
	"fmt"
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

func TestSegmentSweep_AgeGate(t *testing.T) {
	b := newSegmentBackend()
	b.records["bucket"] = nil
	b.addOrphanSegment("bucket/key_segments/young", 1*time.Minute) // < 5m
	b.addOrphanSegment("bucket/key_segments/old", 10*time.Minute)

	s := scrubber.New(b, time.Hour, scrubber.WithNoRetry())
	s.RunOnce(context.Background())
	s.RunOnce(context.Background())

	require.Len(t, b.deletedSegments, 1)
	require.Equal(t, "bucket/key_segments/old", b.deletedSegments[0])
}

func TestSegmentSweep_Cap(t *testing.T) {
	b := newSegmentBackend()
	b.records["bucket"] = nil
	for i := 0; i < 100; i++ {
		b.addOrphanSegment(fmt.Sprintf("bucket/key_segments/blob-%d", i), 10*time.Minute)
	}
	s := scrubber.New(b, time.Hour, scrubber.WithNoRetry())
	s.RunOnce(context.Background()) // tombstone all 100
	s.RunOnce(context.Background()) // delete 50, defer 50

	require.Len(t, b.deletedSegments, 50)
}

func TestSegmentSweep_RecoveredBetweenCycles(t *testing.T) {
	b := newSegmentBackend()
	b.records["bucket"] = nil
	b.addOrphanSegment("bucket/key_segments/blob1", 10*time.Minute)

	s := scrubber.New(b, time.Hour, scrubber.WithNoRetry())
	s.RunOnce(context.Background()) // tombstone

	// Between cycles: segment becomes "known" (metadata commit caught up).
	b.appendableRecs["bucket"] = []scrubber.AppendableRecord{{
		Bucket:         "bucket",
		Key:            "key",
		SegmentBlobIDs: []string{"blob1"},
	}}

	s.RunOnce(context.Background())
	require.Empty(t, b.deletedSegments, "recovered segment should not be deleted")
}

func TestSegmentSweep_CapAcrossBuckets(t *testing.T) {
	b := newSegmentBackend()
	b.records["bucketA"] = nil
	b.records["bucketB"] = nil
	for i := 0; i < 40; i++ {
		b.addOrphanSegment(fmt.Sprintf("bucketA/k%d_segments/b%d", i, i), 10*time.Minute)
	}
	for i := 0; i < 20; i++ {
		b.addOrphanSegment(fmt.Sprintf("bucketB/k%d_segments/b%d", i, i), 10*time.Minute)
	}
	s := scrubber.New(b, time.Hour, scrubber.WithNoRetry())
	s.RunOnce(context.Background()) // tombstone 60
	s.RunOnce(context.Background()) // cap 50: A 40 + B 10

	require.Len(t, b.deletedSegments, 50)
}
