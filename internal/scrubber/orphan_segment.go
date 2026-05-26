package scrubber

import (
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/chunkref"
	"github.com/gritive/GrainFS/internal/metrics"
)

type retentionInput struct {
	tZero    time.Time
	hasTZero bool
	now      time.Time
	window   time.Duration
}

// evalGCCandidate decides whether an orphan segment (already confirmed absent
// from the known manifest set and past the walker's age gate) may be physically
// deleted. A chunk with a tombstone is deletable only after its retention window
// elapses (now - t_zero > window); a chunk with no tombstone has already passed
// the walker age gate and is deletable.
func evalGCCandidate(in retentionInput) bool {
	if !in.hasTZero {
		return true
	}
	return in.now.Sub(in.tZero) > in.window
}

// tombstoneSource yields the t_zero for an unreferenced chunk, if tracked.
type tombstoneSource interface {
	TombstoneTime(c chunkref.ChunkID) (t time.Time, ok bool, err error)
}

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
	tombstones tombstoneSource,
	window time.Duration,
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
		if tombstones != nil {
			blobID := p[strings.LastIndex(p, "/")+1:]
			if tZero, ok, err := tombstones.TombstoneTime(chunkref.ChunkID(blobID)); err == nil {
				if !evalGCCandidate(retentionInput{tZero: tZero, hasTZero: ok, now: time.Now(), window: window}) {
					continue // within retention window — keep
				}
			}
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
		log.Warn().Int("limit", maxSegmentsPerCycle).Int("deferred", deferred).Msg("scrub: orphan segment sweep capped")
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
