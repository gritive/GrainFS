package serveruntime

import (
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
)

// EnsureShardGroupPlaceholder registers a placeholder DataGroup in the
// manager for entries that aren't yet locally instantiated. group-0 is
// always wired through the shared distributed backend so it skips this
// path. The placeholder stores PeerIDs only; backend hookup happens
// later in InstantiateLocalGroup.
func EnsureShardGroupPlaceholder(dgMgr *cluster.DataGroupManager, entry cluster.ShardGroupEntry) {
	if entry.ID == "group-0" {
		return
	}
	if existing := dgMgr.Get(entry.ID); existing == nil {
		dgMgr.Add(cluster.NewDataGroup(entry.ID, entry.PeerIDs))
	}
}

// ECShardCounterFor returns a per-object shard-count function for
// MigrationExecutor. Returns 1 for N× objects (no EC metadata) and k+m
// for EC objects. Returning 0 on lookup failure tells Execute to fall
// back to the cluster-wide k+m — returning 1 would copy only shard 0
// then delete all k+m source shards, causing data loss.
func ECShardCounterFor(fsm *cluster.FSM) func(bucket, key, versionID string) int {
	return func(bucket, key, versionID string) int {
		k, m, err := fsm.LookupObjectECShards(bucket, key, versionID)
		if err != nil {
			log.Warn().Err(err).Str("bucket", bucket).Str("key", key).Str("version", versionID).
				Msg("LookupObjectECShards failed, using numShards fallback")
			return 0
		}
		if k == 0 {
			return 1
		}
		return k + m
	}
}

// HandleRuntimeGroupInstantiationError wraps a per-group instantiation
// failure with a stable prefix so log filters and tests can match on the
// runtime path independently of the underlying cause.
func HandleRuntimeGroupInstantiationError(groupID string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("runtime group %s instantiation failed: %w", groupID, err)
}
