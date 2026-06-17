package scrubber

import (
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
)

// OrphanQuorumMetaVersionWalkable is an optional Scrubbable extension for
// reclaiming dangling per-version quorum-meta blobs. DistributedBackend
// implements it; the scrubber detects it via type assertion.
type OrphanQuorumMetaVersionWalkable interface {
	// WalkOrphanQuorumMetaVersions calls fn for each per-version blob whose FSM
	// record is certainly absent and which is older than the age gate.
	WalkOrphanQuorumMetaVersions(fn func(bucket, key, versionID, path string) error) error
	// DeleteOrphanQuorumMetaVersion re-confirms then removes the local blob copy.
	DeleteOrphanQuorumMetaVersion(bucket, key, versionID string) error
}

// versionTombstoneKey identifies a candidate across cycles. Blob paths are not
// stable across nodes, so key on the logical identity. \x00 is a safe separator:
// S3 keys, bucket names, and UUIDv7 version IDs never contain a NUL byte.
func versionTombstoneKey(bucket, key, versionID string) string {
	return bucket + "\x00" + key + "\x00" + versionID
}

func splitVersionTombstoneKey(tk string) (bucket, key, versionID string) {
	first := strings.IndexByte(tk, '\x00')
	last := strings.LastIndexByte(tk, '\x00')
	if first < 0 || last <= first {
		return tk, "", ""
	}
	return tk[:first], tk[first+1 : last], tk[last+1:]
}

// orphanVersionSweep reclaims dangling per-version blobs with a two-cycle
// tombstone delay, mirroring orphanSweep. A candidate must appear orphan in two
// consecutive cycles before deletion, capped at maxOrphansPerCycle per cycle.
func (s *BackgroundScrubber) orphanVersionSweep(walker OrphanQuorumMetaVersionWalkable) {
	type cand struct{ bucket, key, versionID string }
	var candidates []cand
	current := map[string]struct{}{}
	err := walker.WalkOrphanQuorumMetaVersions(func(bucket, key, versionID, _ string) error {
		candidates = append(candidates, cand{bucket, key, versionID})
		current[versionTombstoneKey(bucket, key, versionID)] = struct{}{}
		return nil
	})
	if err != nil {
		log.Warn().Err(err).Msg("scrub: orphan per-version walk failed")
		return
	}

	// Promote tombstoned candidates still orphan this cycle → delete (capped).
	deleteCount, deferred := 0, 0
	for tk := range s.orphanVersionTombstone {
		if _, still := current[tk]; !still {
			delete(s.orphanVersionTombstone, tk) // recovered
			continue
		}
		if deleteCount >= maxOrphansPerCycle {
			deferred++
			continue
		}
		bucket, key, versionID := splitVersionTombstoneKey(tk)
		if derr := walker.DeleteOrphanQuorumMetaVersion(bucket, key, versionID); derr != nil {
			log.Warn().Str("bucket", bucket).Str("key", key).Str("version", versionID).Err(derr).
				Msg("scrub: orphan per-version delete failed")
			continue
		}
		delete(s.orphanVersionTombstone, tk)
		metrics.OrphanQuorumMetaVersionsDeletedTotal.Inc()
		deleteCount++
		log.Info().Str("bucket", bucket).Str("key", key).Str("version", versionID).
			Msg("scrub: orphan per-version blob deleted")
	}
	if deferred > 0 {
		metrics.OrphanQuorumMetaVersionSweepCappedTotal.Add(float64(deferred))
		log.Warn().Int("limit", maxOrphansPerCycle).Int("deferred", deferred).
			Msg("scrub: orphan per-version sweep capped")
	}

	// Tombstone newly found candidates (deleted next cycle if still orphan).
	for _, c := range candidates {
		tk := versionTombstoneKey(c.bucket, c.key, c.versionID)
		if _, dup := s.orphanVersionTombstone[tk]; dup {
			continue
		}
		metrics.OrphanQuorumMetaVersionsFoundTotal.Inc()
		s.orphanVersionTombstone[tk] = struct{}{}
	}
}
