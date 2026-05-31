package cluster

import (
	"context"
	"strconv"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// ECRewrapShardBackend is the per-data-group backend surface the EC rewrap lane
// drives. *GroupBackend satisfies it (it embeds *DistributedBackend, which
// carries both methods).
//
// The lane collects all EC shards across ALL stored object versions
// (CollectECRewrapTargets), not only the latest: a compromise-recovery rekey
// must re-encrypt every stored shard, including old versions that the previous
// lat:-only scan missed.
type ECRewrapShardBackend interface {
	// CollectECRewrapTargets enumerates every EC shard (all versions, all
	// buckets) stored in this data group and returns them as a flat slice.
	CollectECRewrapTargets() ([]ECRewrapTarget, error)
	// RewrapShardIfStaleAt migrates a single owned EC shard at canonicalKey
	// onto activeGen. It is key-agnostic: it works for object-version, segment,
	// and coalesced shards alike.
	RewrapShardIfStaleAt(bucket, canonicalKey string, shardIdx int, activeGen uint32) (bool, error)
}

var _ ECRewrapShardBackend = (*GroupBackend)(nil)

// ECRewrapLane is the encrypt.RewrapLane for EC shards. On a DEK rotation it
// sweeps every owned shard of every stored object version across every data
// group and migrates any whose on-disk gen differs from the active gen. The
// sweep IGNORES the rotation's oldGen, so it is idempotent and tolerant of
// multiple un-migrated generations.
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
		targets, err := gb.CollectECRewrapTargets()
		if err != nil {
			// Log + continue: one group's enumeration error must not starve the
			// remaining groups of this sweep. Idempotent — retried next rotation.
			log.Warn().Err(err).Uint32("active_gen", activeGen).
				Msg("ec rewrap: collect targets failed; skipping group")
			continue
		}
		for _, t := range targets {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			for idx, holder := range t.NodeIDs {
				if holder != l.nodeID {
					continue
				}
				did, err := gb.RewrapShardIfStaleAt(t.Bucket, t.ShardKey, idx, activeGen)
				if err != nil {
					// Log + continue: one shard's transient error must not abort
					// the whole sweep. The sweep is idempotent; the next rotation
					// re-attempts skipped shards.
					log.Warn().Err(err).Str("bucket", t.Bucket).Str("shardKey", t.ShardKey).
						Int("shard", idx).Uint32("active_gen", activeGen).
						Msg("ec rewrap: shard migration failed; skipping")
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
