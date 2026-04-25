package scrubber

import (
	"path"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
)

// OrphanWalkable is an optional extension of Scrubbable for orphan shard detection.
// ECBackend implements this; the scrubber detects it via type assertion.
type OrphanWalkable interface {
	// WalkOrphanShards calls fn for each shard directory that exists on disk but
	// is not present in known and is older than the age gate (5 min by default).
	WalkOrphanShards(known map[string]bool, fn func(dir string) error) error
	// DeleteOrphanDir removes the orphan shard directory and returns the number of
	// shard files deleted.
	DeleteOrphanDir(dir string) (int, error)
}

// maxOrphansPerCycle caps deletions per scrub cycle to prevent I/O storms.
const maxOrphansPerCycle = 50

// orphanSweep finds and cleans up orphan shard directories.
// known is the set of shard dirs seen during the regular scan.
// Deletion requires the dir to appear as orphan in two consecutive cycles (tombstone delay).
func (s *BackgroundScrubber) orphanSweep(walker OrphanWalkable, known map[string]bool) {
	// Collect candidates from this cycle.
	var candidates []string
	err := walker.WalkOrphanShards(known, func(dir string) error {
		candidates = append(candidates, dir)
		return nil
	})
	if err != nil {
		log.Warn().Err(err).Msg("scrub: orphan walk failed")
		return
	}

	// Build a set of current-cycle candidates for tombstone promotion.
	currentSet := make(map[string]struct{}, len(candidates))
	for _, dir := range candidates {
		currentSet[dir] = struct{}{}
	}

	// Promote tombstoned dirs that are still orphan this cycle → delete.
	deleteCount := 0
	deferred := 0
	for dir := range s.orphanTombstone {
		if _, stillOrphan := currentSet[dir]; !stillOrphan {
			// Recovered or known — remove tombstone.
			delete(s.orphanTombstone, dir)
			continue
		}
		if deleteCount >= maxOrphansPerCycle {
			deferred++
			continue
		}
		n, err := walker.DeleteOrphanDir(dir)
		if err != nil {
			log.Warn().Str("dir", dir).Err(err).Msg("scrub: orphan delete failed")
			continue
		}
		delete(s.orphanTombstone, dir)
		metrics.OrphanShardsDeletedTotal.Inc()
		deleteCount++
		log.Info().Str("dir", dir).Int("shards_removed", n).Msg("scrub: orphan deleted")
	}

	if deferred > 0 {
		metrics.OrphanSweepCappedTotal.Add(float64(deferred))
		log.Warn().Int("limit", maxOrphansPerCycle).Int("deferred", deferred).Msg("scrub: orphan sweep capped")
	}

	// Tombstone newly found candidates (will be deleted next cycle if still orphan).
	for _, dir := range candidates {
		if _, alreadyTombstoned := s.orphanTombstone[dir]; alreadyTombstoned {
			continue
		}
		metrics.OrphanShardsFoundTotal.Inc()
		s.orphanTombstone[dir] = struct{}{}
		log.Info().Str("dir", dir).Msg("scrub: orphan tombstoned")
	}
}

// shardDir extracts the shard directory from a shard path (e.g. "bucket/key/0" → "bucket/key").
func shardDirFromPath(p string) string {
	return path.Dir(p)
}
