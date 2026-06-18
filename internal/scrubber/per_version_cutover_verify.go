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
// CutoverReadiness tallies, and SETs each of the five readiness gauges to the
// summed value. It is read-only: no writes or deletes. Fail-soft per bucket: a
// verify error on one bucket is logged and that bucket's records are uncounted,
// but the sweep continues to the next bucket.
func (s *BackgroundScrubber) perVersionCutoverVerifySweep(v PerVersionCutoverVerifiable) {
	ctx := context.Background()
	buckets, err := v.ListCutoverBuckets(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("scrub: per-version cutover verify list buckets failed")
		return
	}

	var total CutoverReadiness
	for _, bucket := range buckets {
		r, verr := v.VerifyBucketCutover(ctx, bucket)
		if verr != nil {
			log.Warn().Str("bucket", bucket).Err(verr).
				Msg("scrub: per-version cutover verify failed for bucket (records uncounted)")
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
}
