package scrubber

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/metrics"
)

// contextForTest returns a background context for use in sweep tests.
func contextForTest() context.Context { return context.Background() }

// fakeCutoverVerifiable is an in-memory PerVersionCutoverVerifiable for testing.
type fakeCutoverVerifiable struct {
	buckets   []string
	readiness map[string]CutoverReadiness // bucket → result
	verifyErr map[string]error            // bucket → error to return
}

func (f *fakeCutoverVerifiable) ListCutoverBuckets(_ context.Context) ([]string, error) {
	return f.buckets, nil
}

func (f *fakeCutoverVerifiable) VerifyBucketCutover(_ context.Context, bucket string) (CutoverReadiness, error) {
	if err, ok := f.verifyErr[bucket]; ok {
		return CutoverReadiness{}, err
	}
	if r, ok := f.readiness[bucket]; ok {
		return r, nil
	}
	return CutoverReadiness{}, nil
}

func TestPerVersionCutoverVerifySweep_SumsAcrossBuckets(t *testing.T) {
	// Two buckets with known tallies; sweep must sum and Set each gauge.
	f := &fakeCutoverVerifiable{
		buckets: []string{"bkt1", "bkt2"},
		readiness: map[string]CutoverReadiness{
			"bkt1": {Complete: 10, Gaps: 2, Stuck: 1, Unknown: 0, Excluded: 3},
			"bkt2": {Complete: 5, Gaps: 1, Stuck: 0, Unknown: 2, Excluded: 7},
		},
	}
	s := &BackgroundScrubber{}

	s.perVersionCutoverVerifySweep(contextForTest(), f)

	require.InDelta(t, 15.0, testutil.ToFloat64(metrics.PerVersionCutoverComplete), 0.001, "complete")
	require.InDelta(t, 3.0, testutil.ToFloat64(metrics.PerVersionCutoverGaps), 0.001, "gaps")
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.PerVersionCutoverStuck), 0.001, "stuck")
	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.PerVersionCutoverUnknown), 0.001, "unknown")
	require.InDelta(t, 10.0, testutil.ToFloat64(metrics.PerVersionCutoverExcluded), 0.001, "excluded")
}

func TestPerVersionCutoverVerifySweep_FailSoftPerBucket(t *testing.T) {
	// bkt1 errors; bkt2 succeeds. The sweep must continue and set gauges from bkt2 only.
	f := &fakeCutoverVerifiable{
		buckets: []string{"bkt1", "bkt2"},
		readiness: map[string]CutoverReadiness{
			"bkt2": {Complete: 7, Gaps: 0, Stuck: 0, Unknown: 1, Excluded: 2},
		},
		verifyErr: map[string]error{
			"bkt1": context.DeadlineExceeded,
		},
	}
	s := &BackgroundScrubber{}

	// Should not panic; should set gauges to bkt2's values only.
	s.perVersionCutoverVerifySweep(contextForTest(), f)

	require.InDelta(t, 7.0, testutil.ToFloat64(metrics.PerVersionCutoverComplete), 0.001, "complete")
	require.InDelta(t, 0.0, testutil.ToFloat64(metrics.PerVersionCutoverGaps), 0.001, "gaps")
	require.InDelta(t, 0.0, testutil.ToFloat64(metrics.PerVersionCutoverStuck), 0.001, "stuck")
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.PerVersionCutoverUnknown), 0.001, "unknown")
	require.InDelta(t, 2.0, testutil.ToFloat64(metrics.PerVersionCutoverExcluded), 0.001, "excluded")
}

func TestPerVersionCutoverVerifySweep_NoBuckets(t *testing.T) {
	f := &fakeCutoverVerifiable{
		buckets: []string{},
	}
	s := &BackgroundScrubber{}

	// Must not panic; all gauges stay at 0.
	s.perVersionCutoverVerifySweep(contextForTest(), f)

	require.InDelta(t, 0.0, testutil.ToFloat64(metrics.PerVersionCutoverComplete), 0.001)
	require.InDelta(t, 0.0, testutil.ToFloat64(metrics.PerVersionCutoverGaps), 0.001)
	require.InDelta(t, 0.0, testutil.ToFloat64(metrics.PerVersionCutoverStuck), 0.001)
	require.InDelta(t, 0.0, testutil.ToFloat64(metrics.PerVersionCutoverUnknown), 0.001)
	require.InDelta(t, 0.0, testutil.ToFloat64(metrics.PerVersionCutoverExcluded), 0.001)
}

// fakeCutoverVerifiableWithListErr is a PerVersionCutoverVerifiable whose
// ListCutoverBuckets always returns an error.
type fakeCutoverVerifiableWithListErr struct {
	listErr error
}

func (f *fakeCutoverVerifiableWithListErr) ListCutoverBuckets(_ context.Context) ([]string, error) {
	return nil, f.listErr
}

func (f *fakeCutoverVerifiableWithListErr) VerifyBucketCutover(_ context.Context, _ string) (CutoverReadiness, error) {
	return CutoverReadiness{}, nil
}

// TestPerVersionCutoverVerifySweep_BucketVerifyError asserts that when one
// bucket's VerifyBucketCutover returns an error, verify_errors gauge is ≥ 1
// and the sweep continues (the other bucket's counts ARE set).
func TestPerVersionCutoverVerifySweep_BucketVerifyError(t *testing.T) {
	f := &fakeCutoverVerifiable{
		buckets: []string{"bkt-err", "bkt-ok"},
		readiness: map[string]CutoverReadiness{
			"bkt-ok": {Complete: 3, Gaps: 1},
		},
		verifyErr: map[string]error{
			"bkt-err": context.DeadlineExceeded,
		},
	}
	s := &BackgroundScrubber{}

	s.perVersionCutoverVerifySweep(contextForTest(), f)

	// The erroring bucket must be counted in verify_errors.
	require.GreaterOrEqual(t, testutil.ToFloat64(metrics.PerVersionCutoverVerifyErrors), 1.0,
		"verify_errors must be ≥1 when a bucket verify fails")
	// The successful bucket's counts must still be reflected.
	require.InDelta(t, 3.0, testutil.ToFloat64(metrics.PerVersionCutoverComplete), 0.001, "complete from good bucket")
	require.InDelta(t, 1.0, testutil.ToFloat64(metrics.PerVersionCutoverGaps), 0.001, "gaps from good bucket")
}

// TestPerVersionCutoverVerifySweep_ListBucketsError asserts that when
// ListCutoverBuckets returns an error, verify_errors gauge is set ≥1 (not left
// stale-zero). This prevents a list failure from looking like a false READY.
func TestPerVersionCutoverVerifySweep_ListBucketsError(t *testing.T) {
	f := &fakeCutoverVerifiableWithListErr{listErr: context.DeadlineExceeded}
	s := &BackgroundScrubber{}

	// Pre-set the gauge to 0 so we can distinguish "was set to 0" from "never set".
	metrics.PerVersionCutoverVerifyErrors.Set(0)

	s.perVersionCutoverVerifySweep(contextForTest(), f)

	require.GreaterOrEqual(t, testutil.ToFloat64(metrics.PerVersionCutoverVerifyErrors), 1.0,
		"verify_errors must be ≥1 when ListCutoverBuckets fails (not left stale-zero)")
}
