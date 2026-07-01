package serveruntime

import (
	"fmt"

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

// HandleRuntimeGroupInstantiationError wraps a per-group instantiation
// failure with a stable prefix so log filters and tests can match on the
// runtime path independently of the underlying cause.
func HandleRuntimeGroupInstantiationError(groupID string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("runtime group %s instantiation failed: %w", groupID, err)
}
