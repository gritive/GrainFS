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
//
// The hasTZero==false → deletable default is correct ONLY because the known-set
// (authoritative manifest-absence, the spec's third GC condition) already
// excluded every chunk a live object or snapshot references before a chunk ever
// reaches here. ACTIVATION CONSTRAINT (Plan 3.5): a segmentOrphanLog MUST NOT be
// wired without snapshot-frozen paths (segmentManifestSource) in the same change
// — otherwise a snapshot-pinned chunk (refcount>0, hence no tombstone) that is
// missing from the known-set would fall through to deletion (data loss). Wire
// both, or neither.
//
// (H) t_zero is the FIRST WALK observation of the unreferenced chunk, not the
// instant it first became unreferenced. The effective grace before deletion is
// therefore walker age-gate (≈5m) + retention window + cycle timing. A
// window<=0 leaves only the age-gate (now-t_zero > 0 is true on the next walk).
func evalGCCandidate(in retentionInput) bool {
	if !in.hasTZero {
		return true
	}
	return in.now.Sub(in.tZero) > in.window
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
//
// Retention gating, observe, and reconcile use s.orphanLog/s.orphanRetention
// (nil log = age-gate-only behavior). All orphan-log interactions are
// FAIL-CLOSED: a read error KEEPS the segment; an observe error DEFERS the
// tombstone (never delete without a recorded t_zero).
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

	// deletedThisCycle guards the re-observe loop below: a path deleted in the
	// tombstone loop is still present in `candidates` (captured before deletion),
	// so without this we would Observe a fresh t_zero for a blob we just removed.
	deletedThisCycle := make(map[string]struct{})
	deferred := 0
	for p := range s.segmentTombstone {
		if !strings.HasPrefix(p, bucket+"/") {
			continue
		}
		blobID := chunkref.ChunkID(p[strings.LastIndex(p, "/")+1:])
		if _, still := currentSet[p]; !still {
			delete(s.segmentTombstone, p)
			if s.orphanLog != nil {
				_ = s.orphanLog.Forget(blobID) // re-referenced/gone -> reset window
			}
			continue
		}
		if capRemaining <= 0 {
			deferred++
			continue
		}
		if s.orphanLog != nil {
			tZero, ok, err := s.orphanLog.TombstoneTime(blobID)
			if err != nil { // (B) fail-closed: KEEP on read error
				log.Warn().Str("path", p).Err(err).Msg("scrub: orphan-log read failed, keeping (fail-closed)")
				continue
			}
			if !evalGCCandidate(retentionInput{tZero: tZero, hasTZero: ok, now: time.Now(), window: s.orphanRetention}) {
				continue // within retention window — keep
			}
		}
		if err := walker.DeleteOrphanSegment(p); err != nil {
			metrics.OrphanSegmentDeleteErrorsTotal.Inc()
			log.Warn().Str("path", p).Err(err).Msg("scrub: orphan segment delete failed")
			continue
		}
		delete(s.segmentTombstone, p)
		deletedThisCycle[p] = struct{}{}
		if s.orphanLog != nil {
			_ = s.orphanLog.Forget(blobID)
		}
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
		if _, gone := deletedThisCycle[p]; gone {
			continue // deleted above; don't re-observe a fresh t_zero
		}
		blobID := chunkref.ChunkID(p[strings.LastIndex(p, "/")+1:])
		if s.orphanLog != nil {
			if err := s.orphanLog.Observe(blobID, time.Now()); err != nil { // (B) fail-closed: don't tombstone w/o t_zero
				log.Warn().Str("path", p).Err(err).Msg("scrub: orphan-log observe failed, deferring tombstone")
				continue
			}
		}
		metrics.OrphanSegmentsFoundTotal.Inc()
		s.segmentTombstone[p] = struct{}{}
		log.Info().Str("path", p).Msg("scrub: orphan segment tombstoned")
	}
	return capRemaining
}
