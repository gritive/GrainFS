package scrubber

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
)

// CutoverReadiness summarizes per-version quorum-meta coverage for a bucket.
// It is the scrubber-facing alias for cluster.CutoverReadiness, redeclared here
// so the scrubber package does not import cluster (dependency inversion).
type CutoverReadiness struct {
	Complete int // VID found in readQuorumMetaVersionsStrict union
	Gaps     int // VID missing but placement resolvable (backfill can fix)
	Stuck    int // VID missing AND placement unresolvable
	Unknown  int // decode error or strict-readback error (fail-closed)
	Excluded int // internal bucket, appendable, or coalesced (intentionally skipped)
}

// PerVersionCutoverVerifiable is the optional backend extension for verifying
// per-version quorum-meta coverage in preparation for the S4 cutover gate.
// Defined here (consumer) per the repo's "인터페이스는 사용처에서 정의" rule.
// DistributedBackend implements it.
type PerVersionCutoverVerifiable interface {
	// ListCutoverBuckets returns the union of all locally-hosted generation
	// groups' buckets (mirrors ListBackfillBuckets / SegmentSweepBuckets).
	ListCutoverBuckets(ctx context.Context) ([]string, error)
	// VerifyBucketCutover runs the read-only per-version coverage check for
	// bucket and returns a CutoverReadiness tally. The call is fail-closed:
	// any decode or RPC error is counted as Unknown rather than silently ignored.
	VerifyBucketCutover(ctx context.Context, bucket string) (CutoverReadiness, error)
}

// perVersionCutoverVerifySweep iterates all hosted buckets, sums the per-bucket
// CutoverReadiness tallies, and SETs each readiness gauge to the summed value.
// It is read-only: no writes or deletes.
//
// Fail-closed on verification errors: a verify error on one bucket is logged,
// that bucket's records are uncounted (partial totals are unsafe for the gate),
// and verifyErrors is incremented. verifyErrors is always SET before returning —
// even when ListCutoverBuckets itself fails — so a list/verify failure never
// leaves the gauges at stale-zero, which would look like a false READY.
//
// Gate semantics: gaps+stuck+unknown+verify_errors == 0 across all nodes ⇒ safe.
//
// ctx is threaded so the sweep is cancellable on scrubber shutdown.
func (s *BackgroundScrubber) perVersionCutoverVerifySweep(ctx context.Context, v PerVersionCutoverVerifiable) {
	// Pessimistic sentinel: mark not-ready for the entire duration of the sweep
	// so a Prometheus scrape mid-sweep never observes a false READY (all-zero)
	// state. The real count is published atomically at the very end.
	metrics.PerVersionCutoverVerifyErrors.Set(1)

	var verifyErrors float64

	buckets, err := v.ListCutoverBuckets(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("scrub: per-version cutover verify list buckets failed")
		verifyErrors = 1
		metrics.PerVersionCutoverVerifyErrors.Set(verifyErrors)
		return
	}

	var total CutoverReadiness
	for _, bucket := range buckets {
		r, verr := v.VerifyBucketCutover(ctx, bucket)
		if verr != nil {
			log.Warn().Str("bucket", bucket).Err(verr).
				Msg("scrub: per-version cutover verify failed for bucket (records uncounted)")
			verifyErrors++
			continue
		}
		total.Complete += r.Complete
		total.Gaps += r.Gaps
		total.Stuck += r.Stuck
		total.Unknown += r.Unknown
		total.Excluded += r.Excluded
	}

	metrics.PerVersionCutoverComplete.Set(float64(total.Complete))
	metrics.PerVersionCutoverGaps.Set(float64(total.Gaps))
	metrics.PerVersionCutoverStuck.Set(float64(total.Stuck))
	metrics.PerVersionCutoverUnknown.Set(float64(total.Unknown))
	metrics.PerVersionCutoverExcluded.Set(float64(total.Excluded))
	// Publish the real verify_errors count last. A clean sweep sets this to 0,
	// which is the only way the gate can read READY. Any mid-sweep scrape still
	// sees the pessimistic 1 set at the top of the function.
	metrics.PerVersionCutoverVerifyErrors.Set(verifyErrors)
}
