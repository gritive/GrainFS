package scrubber

import (
	"context"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
)

// BackfillCandidate identifies a versioned object whose per-version
// quorum-meta blob is absent and needs to be written. The Opaque field carries
// backend-private state (e.g. objectMeta bytes) so the backend's Backfill
// method can skip a second lookup.
type BackfillCandidate struct {
	Bucket    string
	Key       string
	VersionID string
	// Opaque carries backend-private data threaded from Walk to Backfill,
	// avoiding a second metadata lookup. It is nil-safe: a nil Opaque causes
	// Backfill to re-derive the data from the live record.
	Opaque interface{}
}

// PerVersionBackfillable is the optional backend extension for writing missing
// per-version quorum-meta blobs. Defined here (consumer) per the repo's
// "인터페이스는 사용처에서 정의" rule. DistributedBackend implements it.
type PerVersionBackfillable interface {
	// ListBackfillBuckets returns the union of all locally-hosted generation
	// groups' buckets (mirrors the orphan/redundancy sweep's bucket enumeration).
	ListBackfillBuckets(ctx context.Context) ([]string, error)
	// WalkPerVersionBackfillCandidates calls fn for each versioned object whose
	// per-version blob is absent and which passes the placement and age gates.
	// The walker is callback-based; the fn may return an error to abort the walk.
	WalkPerVersionBackfillCandidates(ctx context.Context, bucket string, fn func(BackfillCandidate) error) error
	// BackfillPerVersionBlob fans out the missing per-version blob to the
	// object's placement nodes. Idempotent: safe to call if the blob already exists.
	// The Opaque field of c carries the backend-private objectMeta from Walk.
	BackfillPerVersionBlob(ctx context.Context, c BackfillCandidate) error
}

// maxBackfillPerCycle caps per-version blob writes per scrub cycle to prevent
// I/O storms. Mirrors maxOrphansPerCycle / maxSegmentsPerCycle.
const maxBackfillPerCycle = 50

// perVersionBackfillSweep iterates all hosted buckets and backfills missing
// per-version quorum-meta blobs up to maxPerCycle per sweep. It mirrors
// orphanVersionSweep's structure but ADDS blobs instead of deleting them:
//   - fail-soft per object: one error logs + increments ErrorTotal, continues
//   - cap reached: remaining candidates are counted as CappedTotal and skipped
//   - no tombstone delay needed: writes are idempotent and safe on first sight
func (s *BackgroundScrubber) perVersionBackfillSweep(ctx context.Context, b PerVersionBackfillable, maxPerCycle int) {
	buckets, err := b.ListBackfillBuckets(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("scrub: per-version backfill list buckets failed")
		return
	}

	migrated := 0
	for _, bucket := range buckets {
		capped := false
		walkErr := b.WalkPerVersionBackfillCandidates(ctx, bucket, func(c BackfillCandidate) error {
			metrics.QuorumMetaVersionBackfillFoundTotal.Inc()

			if capped || migrated >= maxPerCycle {
				capped = true
				metrics.QuorumMetaVersionBackfillCappedTotal.Inc()
				return nil // drain remaining candidates without backfilling
			}

			if berr := b.BackfillPerVersionBlob(ctx, c); berr != nil {
				metrics.QuorumMetaVersionBackfillErrorTotal.Inc()
				log.Warn().Str("bucket", c.Bucket).Str("key", c.Key).Str("version", c.VersionID).Err(berr).
					Msg("scrub: per-version backfill failed")
				return nil // fail-soft: continue to next candidate
			}

			metrics.QuorumMetaVersionBackfillMigratedTotal.Inc()
			migrated++
			return nil
		})
		if walkErr != nil {
			log.Warn().Str("bucket", bucket).Err(walkErr).Msg("scrub: per-version backfill walk failed")
		}
		if capped {
			log.Warn().Int("limit", maxPerCycle).Int("migrated", migrated).
				Msg("scrub: per-version backfill sweep capped")
		}
	}
}
