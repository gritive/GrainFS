package scrubber

import (
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
)

// OrphanSegmentWalkable is an optional Scrubbable extension for raw segment
// orphan sweep. Independent from OrphanWalkable (EC shards) — they share
// only the tombstone-cap discipline pattern, not state.
type OrphanSegmentWalkable interface {
	// WalkOrphanSegments calls fn for each raw segment file in `bucket`
	// that exists on disk but is not in `known` and is older than the
	// backend's age gate (typically 5m).
	WalkOrphanSegments(bucket string, known map[string]bool, fn func(path string) error) error
	// DeleteOrphanSegment removes the orphan raw segment file. ENOENT
	// is swallowed (already removed by another path).
	DeleteOrphanSegment(path string) error
}

// maxSegmentsPerCycle caps segment deletions per scrub cycle, shared across
// all buckets in the cycle. Independent from the EC shard cap.
const maxSegmentsPerCycle = 50

// segmentSweepBucket finds and cleans up orphan raw segment files for one
// bucket. capRemaining is the cycle-shared cap; returns updated remaining.
func (s *BackgroundScrubber) segmentSweepBucket(
	walker OrphanSegmentWalkable,
	bucket string,
	known map[string]bool,
	capRemaining int,
) int {
	var candidates []string
	err := walker.WalkOrphanSegments(bucket, known, func(path string) error {
		candidates = append(candidates, path)
		return nil
	})
	if err != nil {
		metrics.OrphanSegmentWalkErrorsTotal.Inc()
		log.Warn().Str("bucket", bucket).Err(err).Msg("scrub: orphan segment walk failed")
		return capRemaining
	}

	currentSet := make(map[string]struct{}, len(candidates))
	for _, p := range candidates {
		currentSet[p] = struct{}{}
	}

	deferred := 0
	for p := range s.segmentTombstone {
		if !strings.HasPrefix(p, bucket+"/") {
			continue
		}
		if _, still := currentSet[p]; !still {
			delete(s.segmentTombstone, p)
			continue
		}
		if capRemaining <= 0 {
			deferred++
			continue
		}
		if err := walker.DeleteOrphanSegment(p); err != nil {
			metrics.OrphanSegmentDeleteErrorsTotal.Inc()
			log.Warn().Str("path", p).Err(err).Msg("scrub: orphan segment delete failed")
			continue
		}
		delete(s.segmentTombstone, p)
		metrics.OrphanSegmentsDeletedTotal.Inc()
		capRemaining--
		log.Info().Str("path", p).Msg("scrub: orphan segment deleted")
	}
	if deferred > 0 {
		metrics.OrphanSegmentSweepCappedTotal.Add(float64(deferred))
	}

	for _, p := range candidates {
		if _, already := s.segmentTombstone[p]; already {
			continue
		}
		metrics.OrphanSegmentsFoundTotal.Inc()
		s.segmentTombstone[p] = struct{}{}
		log.Info().Str("path", p).Msg("scrub: orphan segment tombstoned")
	}
	return capRemaining
}
