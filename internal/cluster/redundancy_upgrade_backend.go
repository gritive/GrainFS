package cluster

import (
	"context"
	"time"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// defaultRedundancyUpgradeMinAge is the age gate for the EC redundancy-upgrade
// sweep: an object must be at least this old before it is relocated, so the sweep
// never races a fresh write or a settling append. Mirrors the conservative
// orphan/scrub age idea.
const defaultRedundancyUpgradeMinAge = 5 * time.Minute

// redundancyUpgradeMinAge returns the minimum object age (in seconds) before the
// redundancy-upgrade sweep will relocate it.
func (b *DistributedBackend) redundancyUpgradeMinAge() int64 {
	return int64(defaultRedundancyUpgradeMinAge / time.Second)
}

// redundancyUpgradeAdapter binds a DistributedBackend (and the cycle's ctx, used
// by the caught-up check) to the redundancyUpgradeDeps surface the testable sweep
// core consumes.
type redundancyUpgradeAdapter struct {
	b   *DistributedBackend
	ctx context.Context
}

func (a redundancyUpgradeAdapter) clusterRedundant() bool {
	return clusterHasRedundantCapacity(a.b.shardGroup.ShardGroups(), a.b.currentECConfig(), metaNodeCount(a.b.shardGroup))
}

func (a redundancyUpgradeAdapter) listBuckets() ([]string, error) {
	// Prefer the multi-group union so the sweep reaches every hosted group's
	// buckets (mirrors the orphan-segment sweep); falls back to self for
	// single-group/un-wired backends.
	return a.b.SegmentSweepBuckets(a.ctx)
}

func (a redundancyUpgradeAdapter) caughtUpOwner(bucket string) bool {
	gb := a.b.owningGroupBackend(bucket)
	return gb != nil && gb.CaughtUp(a.ctx) && a.b.gcSingletonOwner(bucket)
}

func (a redundancyUpgradeAdapter) scanObjects(bucket string) (<-chan scrubber.ObjectRecord, error) {
	return a.b.ScanObjects(bucket)
}

func (a redundancyUpgradeAdapter) relocate(ctx context.Context, in relocateInput) error {
	return a.b.relocateObjectToRedundantGroup(ctx, in)
}

// RunRedundancyUpgradeSweep relocates up to maxPerCycle non-redundant (1+0) EC
// objects into a redundant placement group, returning the count relocated. Only
// the deterministic singleton owner of each fresh hosted group relocates that
// group's objects.
// minAge is the minimum object age before relocation (avoids racing in-flight
// writes); when minAge < 0 the conservative default is used, while minAge == 0
// relocates eligible objects immediately (used by tests and aggressive ops).
// Implements scrubber.RedundancyUpgrader.
func (b *DistributedBackend) RunRedundancyUpgradeSweep(ctx context.Context, maxPerCycle int, minAge time.Duration) (int, error) {
	adapter := redundancyUpgradeAdapter{b: b, ctx: ctx}
	minAgeSecs := int64(minAge.Seconds())
	if minAge < 0 {
		minAgeSecs = b.redundancyUpgradeMinAge()
	}
	return runRedundancyUpgradeSweep(ctx, adapter, time.Now().Unix(), minAgeSecs, maxPerCycle)
}
