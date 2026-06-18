package scrubber

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/metrics"
)

// fakeBackfillable is an in-memory PerVersionBackfillable for testing.
type fakeBackfillable struct {
	buckets    []string
	candidates []BackfillCandidate

	backfillErr map[string]error // versionID → error to return from BackfillPerVersionBlob
	backfilled  []string         // versionIDs successfully backfilled (in order)
}

func (f *fakeBackfillable) ListBackfillBuckets(_ context.Context) ([]string, error) {
	return f.buckets, nil
}

func (f *fakeBackfillable) WalkPerVersionBackfillCandidates(_ context.Context, bucket string, fn func(BackfillCandidate) error) error {
	for _, c := range f.candidates {
		if c.Bucket != bucket {
			continue
		}
		if err := fn(c); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeBackfillable) BackfillPerVersionBlob(_ context.Context, c BackfillCandidate) error {
	if err, ok := f.backfillErr[c.VersionID]; ok {
		return err
	}
	f.backfilled = append(f.backfilled, c.VersionID)
	return nil
}

func newBackfillScrubber() *BackgroundScrubber {
	return &BackgroundScrubber{orphanVersionTombstone: map[string]struct{}{}}
}

func TestPerVersionBackfillSweep_MigratesUpToCap(t *testing.T) {
	// 5 candidates, maxPerCycle=3 → 3 migrated, 2 capped.
	b := &fakeBackfillable{
		buckets: []string{"bkt"},
		candidates: []BackfillCandidate{
			{Bucket: "bkt", Key: "k", VersionID: "v1"},
			{Bucket: "bkt", Key: "k", VersionID: "v2"},
			{Bucket: "bkt", Key: "k", VersionID: "v3"},
			{Bucket: "bkt", Key: "k", VersionID: "v4"},
			{Bucket: "bkt", Key: "k", VersionID: "v5"},
		},
	}
	s := newBackfillScrubber()

	beforeFound := testutil.ToFloat64(metrics.QuorumMetaVersionBackfillFoundTotal)
	beforeMigrated := testutil.ToFloat64(metrics.QuorumMetaVersionBackfillMigratedTotal)
	beforeCapped := testutil.ToFloat64(metrics.QuorumMetaVersionBackfillCappedTotal)

	s.perVersionBackfillSweep(context.Background(), b, 3)

	require.Equal(t, 3, len(b.backfilled), "should have backfilled exactly 3 (cap)")
	require.InDelta(t, 5.0, testutil.ToFloat64(metrics.QuorumMetaVersionBackfillFoundTotal)-beforeFound, 0.001, "found counter")
	require.InDelta(t, 3.0, testutil.ToFloat64(metrics.QuorumMetaVersionBackfillMigratedTotal)-beforeMigrated, 0.001, "migrated counter")
	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.QuorumMetaVersionBackfillCappedTotal)-beforeCapped, 0.001, "capped counter")
}

func TestPerVersionBackfillSweep_ContinuesPastPerObjectError(t *testing.T) {
	// v2 errors; sweep should continue and backfill v3.
	b := &fakeBackfillable{
		buckets: []string{"bkt"},
		candidates: []BackfillCandidate{
			{Bucket: "bkt", Key: "k", VersionID: "v1"},
			{Bucket: "bkt", Key: "k", VersionID: "v2"},
			{Bucket: "bkt", Key: "k", VersionID: "v3"},
		},
		backfillErr: map[string]error{
			"v2": errors.New("transient write failure"),
		},
	}
	s := newBackfillScrubber()

	beforeMigrated := testutil.ToFloat64(metrics.QuorumMetaVersionBackfillMigratedTotal)
	beforeError := testutil.ToFloat64(metrics.QuorumMetaVersionBackfillErrorTotal)

	s.perVersionBackfillSweep(context.Background(), b, 100)

	require.Equal(t, []string{"v1", "v3"}, b.backfilled, "v1 and v3 must be backfilled; v2 errored")
	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.QuorumMetaVersionBackfillMigratedTotal)-beforeMigrated, 0.001, "migrated counter")
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.QuorumMetaVersionBackfillErrorTotal)-beforeError, 0.001, "error counter")
}

func TestPerVersionBackfillSweep_NoCandidates(t *testing.T) {
	b := &fakeBackfillable{
		buckets:    []string{"bkt"},
		candidates: []BackfillCandidate{},
	}
	s := newBackfillScrubber()

	beforeMigrated := testutil.ToFloat64(metrics.QuorumMetaVersionBackfillMigratedTotal)
	s.perVersionBackfillSweep(context.Background(), b, 10)
	require.InDelta(t, 0.0, testutil.ToFloat64(metrics.QuorumMetaVersionBackfillMigratedTotal)-beforeMigrated, 0.001)
}
