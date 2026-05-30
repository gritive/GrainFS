package cluster

import (
	"context"
	"strconv"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/scrubber"
)

// ECRewrapShardBackend is the per-data-group backend surface the EC rewrap lane
// drives. *GroupBackend satisfies it (it embeds *DistributedBackend, which
// carries all three methods).
//
// The lane enumerates every data group's OWN stored objects (ScanGroupObjects),
// not the router's per-bucket assignment: an object lives in the placement
// group recorded in its metadata, which can differ from the bucket's current
// routing after a reassignment. This mirrors the read path, which resolves an
// object by its stored PlacementGroupID — the source of truth for "where the
// data physically is" (the same principle as the packblob lane scanning its
// index rather than asking the router).
type ECRewrapShardBackend interface {
	// ScanGroupObjects streams every live object stored in this group (all
	// buckets), bypassing the per-bucket HeadBucket gate. The producer honors
	// ctx so it never leaks when the consumer stops draining.
	ScanGroupObjects(ctx context.Context) <-chan scrubber.ObjectRecord
	OwnedShards(bucket, key, versionID, nodeID string) []int
	RewrapShardIfStale(bucket, key, versionID string, shardIdx int, activeGen uint32) (bool, error)
	// ScanGroupSegmentShards streams one SegmentShardRecord per EC-distributed
	// segment or coalesced sub-object stored in this group. The producer honors
	// ctx so it never leaks when the consumer stops draining.
	ScanGroupSegmentShards(ctx context.Context) <-chan SegmentShardRecord
	// RewrapShardIfStaleAt migrates a single owned EC shard at canonicalKey
	// onto activeGen. It is the canonical-key variant of RewrapShardIfStale,
	// used for segment/coalesced shards whose key is not derived from
	// (key, versionID).
	RewrapShardIfStaleAt(bucket, canonicalKey string, shardIdx int, activeGen uint32) (bool, error)
}

var _ ECRewrapShardBackend = (*GroupBackend)(nil)

// ECRewrapLane is the encrypt.RewrapLane for EC shards. On a DEK rotation it
// sweeps every owned shard of every live object across every data group and
// migrates any whose on-disk gen differs from the active gen. The sweep IGNORES
// the rotation's oldGen, so it is idempotent and tolerant of multiple
// un-migrated generations.
type ECRewrapLane struct {
	nodeID string
	groups func() []ECRewrapShardBackend
}

// ECRewrapLane satisfies encrypt.RewrapLane (the wiring task registers it).
var _ encrypt.RewrapLane = (*ECRewrapLane)(nil)

// NewECRewrapLane constructs the EC rewrap lane. groups enumerates the node's
// data-group backends; nodeID is this node's stable id used for shard
// ownership (the identity the placement vector records — NOT the transport
// self-address).
func NewECRewrapLane(nodeID string, groups func() []ECRewrapShardBackend) *ECRewrapLane {
	return &ECRewrapLane{nodeID: nodeID, groups: groups}
}

// Name identifies the lane in rewrap orchestration.
func (l *ECRewrapLane) Name() string { return "ec" }

// RewrapByGen sweeps owned EC shards onto activeGen. oldGen is intentionally
// ignored (sweep semantics: migrate any shard whose gen != activeGen).
func (l *ECRewrapLane) RewrapByGen(ctx context.Context, _ uint32, activeGen uint32) error {
	for _, gb := range l.groups() {
		for rec := range gb.ScanGroupObjects(ctx) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			for _, idx := range gb.OwnedShards(rec.Bucket, rec.Key, rec.VersionID, l.nodeID) {
				did, err := gb.RewrapShardIfStale(rec.Bucket, rec.Key, rec.VersionID, idx, activeGen)
				if err != nil {
					// Log + continue: one shard's transient error must not abort
					// the whole sweep (and must not leave the ScanGroupObjects
					// producer blocked on a full channel — see MAJOR-1). The sweep
					// is idempotent; the next rotation re-attempts skipped shards.
					log.Warn().Err(err).Str("bucket", rec.Bucket).Str("key", rec.Key).
						Int("shard", idx).Uint32("active_gen", activeGen).
						Msg("ec rewrap: shard migration failed; skipping")
					continue
				}
				if did {
					RewrapECShardsTotal.WithLabelValues(strconv.FormatUint(uint64(activeGen), 10)).Inc()
				}
			}
		}

		for rec := range gb.ScanGroupSegmentShards(ctx) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			for idx, holder := range rec.NodeIDs {
				if holder != l.nodeID {
					continue
				}
				did, err := gb.RewrapShardIfStaleAt(rec.Bucket, rec.ShardKey, idx, activeGen)
				if err != nil {
					log.Warn().Err(err).Str("bucket", rec.Bucket).Str("shardKey", rec.ShardKey).
						Int("shard", idx).Uint32("active_gen", activeGen).
						Msg("ec rewrap: segment shard migration failed; skipping")
					continue
				}
				if did {
					RewrapECShardsTotal.WithLabelValues(strconv.FormatUint(uint64(activeGen), 10)).Inc()
				}
			}
		}
	}
	return nil
}
