package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// fakeRedundancyUpgradeDeps is an in-memory redundancyUpgradeDeps so the
// sweep selection/cap/gate logic is unit-testable without a real cluster.
type fakeRedundancyUpgradeDeps struct {
	redundant   bool
	buckets     []string
	listErr     error
	caughtUp    map[string]bool                    // bucket -> caught-up owner
	objects     map[string][]scrubber.ObjectRecord // bucket -> records
	scanErr     map[string]error                   // bucket -> scan error
	relocateErr map[string]error                   // bucket/key -> relocate error
	relocated   []relocateInput
}

func (f *fakeRedundancyUpgradeDeps) clusterRedundant() bool { return f.redundant }

func (f *fakeRedundancyUpgradeDeps) listBuckets() ([]string, error) {
	return f.buckets, f.listErr
}

func (f *fakeRedundancyUpgradeDeps) caughtUpOwner(bucket string) bool {
	return f.caughtUp[bucket]
}

func (f *fakeRedundancyUpgradeDeps) scanObjects(bucket string) (<-chan scrubber.ObjectRecord, error) {
	if err := f.scanErr[bucket]; err != nil {
		return nil, err
	}
	ch := make(chan scrubber.ObjectRecord, len(f.objects[bucket]))
	for _, rec := range f.objects[bucket] {
		ch <- rec
	}
	close(ch)
	return ch, nil
}

func (f *fakeRedundancyUpgradeDeps) relocate(_ context.Context, in relocateInput) error {
	if err := f.relocateErr[in.Bucket+"/"+in.Key]; err != nil {
		return err
	}
	f.relocated = append(f.relocated, in)
	return nil
}

// candidate is a 1+0 EC object older than the age gate.
func candidate(bucket, key string) scrubber.ObjectRecord {
	return scrubber.ObjectRecord{Bucket: bucket, Key: key, VersionID: "v-" + key, ETag: "e-" + key,
		DataShards: 1, ParityShards: 0, LastModified: 0}
}

// TestRunRedundancyUpgradeSweep_SelectsOnlyCandidates verifies a mix of
// candidate (1+0 aged) and non-candidate (redundant / delete-marker / fresh)
// objects across two buckets relocates ONLY the candidates.
func TestRunRedundancyUpgradeSweep_SelectsOnlyCandidates(t *testing.T) {
	const now, minAge = int64(1000), int64(60)
	d := &fakeRedundancyUpgradeDeps{
		redundant: true,
		buckets:   []string{"b1", "b2"},
		caughtUp:  map[string]bool{"b1": true, "b2": true},
		objects: map[string][]scrubber.ObjectRecord{
			"b1": {
				candidate("b1", "good1"),
				{Bucket: "b1", Key: "redundant", DataShards: 2, ParityShards: 2, LastModified: 0},
				{Bucket: "b1", Key: "tombstone", DataShards: 1, ParityShards: 0, IsDeleteMarker: true, LastModified: 0},
			},
			"b2": {
				candidate("b2", "good2"),
				{Bucket: "b2", Key: "fresh", DataShards: 1, ParityShards: 0, LastModified: now}, // too young
			},
		},
	}

	n, err := runRedundancyUpgradeSweep(context.Background(), d, now, minAge, 100)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Len(t, d.relocated, 2)
	keys := []string{d.relocated[0].Key, d.relocated[1].Key}
	require.ElementsMatch(t, []string{"good1", "good2"}, keys)
}

// TestRunRedundancyUpgradeSweep_Cap stops at maxPerCycle.
func TestRunRedundancyUpgradeSweep_Cap(t *testing.T) {
	const now, minAge = int64(1000), int64(60)
	d := &fakeRedundancyUpgradeDeps{
		redundant: true,
		buckets:   []string{"b1"},
		caughtUp:  map[string]bool{"b1": true},
		objects: map[string][]scrubber.ObjectRecord{
			"b1": {
				candidate("b1", "k1"), candidate("b1", "k2"), candidate("b1", "k3"),
				candidate("b1", "k4"), candidate("b1", "k5"),
			},
		},
	}

	n, err := runRedundancyUpgradeSweep(context.Background(), d, now, minAge, 2)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Len(t, d.relocated, 2)
}

// blockingScanDeps streams records over an UNBUFFERED channel from a producer
// goroutine, so abandoning the channel mid-range would block (leak) the producer.
// completed is closed only when the producer drains every record — proving the
// sweep fully consumes the channel even after hitting the cap.
type blockingScanDeps struct {
	recs      []scrubber.ObjectRecord
	completed chan struct{}
	relocated int
}

func (d *blockingScanDeps) clusterRedundant() bool         { return true }
func (d *blockingScanDeps) listBuckets() ([]string, error) { return []string{"b1"}, nil }
func (d *blockingScanDeps) caughtUpOwner(string) bool      { return true }
func (d *blockingScanDeps) relocate(context.Context, relocateInput) error {
	d.relocated++
	return nil
}
func (d *blockingScanDeps) scanObjects(string) (<-chan scrubber.ObjectRecord, error) {
	ch := make(chan scrubber.ObjectRecord) // unbuffered: producer blocks until consumed
	go func() {
		defer close(d.completed)
		defer close(ch)
		for _, r := range d.recs {
			ch <- r
		}
	}()
	return ch, nil
}

// TestRunRedundancyUpgradeSweep_DrainsChannelOnCap proves that hitting the
// per-cycle cap does NOT abandon the ScanObjects channel: the producer goroutine
// runs to completion (releasing its metadata txn) rather than blocking forever.
func TestRunRedundancyUpgradeSweep_DrainsChannelOnCap(t *testing.T) {
	const now, minAge = int64(1000), int64(60)
	d := &blockingScanDeps{
		recs:      []scrubber.ObjectRecord{candidate("b1", "k1"), candidate("b1", "k2"), candidate("b1", "k3"), candidate("b1", "k4")},
		completed: make(chan struct{}),
	}

	n, err := runRedundancyUpgradeSweep(context.Background(), d, now, minAge, 2)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, 2, d.relocated)
	// The producer must finish (channel fully drained), not block forever.
	select {
	case <-d.completed:
	case <-time.After(2 * time.Second):
		t.Fatal("producer goroutine leaked: channel not drained after cap")
	}
}

// TestRunRedundancyUpgradeSweep_CaughtUpGate skips a bucket whose owning group
// is not the locally-hosted caught-up leader.
func TestRunRedundancyUpgradeSweep_CaughtUpGate(t *testing.T) {
	const now, minAge = int64(1000), int64(60)
	d := &fakeRedundancyUpgradeDeps{
		redundant: true,
		buckets:   []string{"owned", "notleader"},
		caughtUp:  map[string]bool{"owned": true, "notleader": false},
		objects: map[string][]scrubber.ObjectRecord{
			"owned":     {candidate("owned", "k1")},
			"notleader": {candidate("notleader", "k2")},
		},
	}

	n, err := runRedundancyUpgradeSweep(context.Background(), d, now, minAge, 100)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Len(t, d.relocated, 1)
	require.Equal(t, "k1", d.relocated[0].Key)
}

// TestRunRedundancyUpgradeSweep_NotRedundant short-circuits to zero.
func TestRunRedundancyUpgradeSweep_NotRedundant(t *testing.T) {
	const now, minAge = int64(1000), int64(60)
	d := &fakeRedundancyUpgradeDeps{
		redundant: false,
		buckets:   []string{"b1"},
		caughtUp:  map[string]bool{"b1": true},
		objects:   map[string][]scrubber.ObjectRecord{"b1": {candidate("b1", "k1")}},
	}

	n, err := runRedundancyUpgradeSweep(context.Background(), d, now, minAge, 100)
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Empty(t, d.relocated)
}

// TestRunRedundancyUpgradeSweep_SkippedAndErrorContinue treats ErrRelocateSkipped
// as benign (not counted, continue) and a hard error as failed-but-continue.
func TestRunRedundancyUpgradeSweep_SkippedAndErrorContinue(t *testing.T) {
	const now, minAge = int64(1000), int64(60)
	d := &fakeRedundancyUpgradeDeps{
		redundant: true,
		buckets:   []string{"b1"},
		caughtUp:  map[string]bool{"b1": true},
		objects: map[string][]scrubber.ObjectRecord{
			"b1": {candidate("b1", "skip"), candidate("b1", "hard"), candidate("b1", "ok")},
		},
		relocateErr: map[string]error{
			"b1/skip": fmt.Errorf("%w: drift", ErrRelocateSkipped),
			"b1/hard": errors.New("boom"),
		},
	}

	n, err := runRedundancyUpgradeSweep(context.Background(), d, now, minAge, 100)
	require.NoError(t, err)
	require.Equal(t, 1, n) // only "ok" counted
	require.Len(t, d.relocated, 1)
	require.Equal(t, "ok", d.relocated[0].Key)
}
