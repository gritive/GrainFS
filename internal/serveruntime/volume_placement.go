package serveruntime

import (
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/gritive/GrainFS/internal/volume"
)

// aggregateVolumeReplicaLayout walks volume-bucket object-index entries and
// produces a per-volume admin.ReplicaLayoutFact by classifying each entry
// against its placement group via cluster.ClassifyObjectLayout. Pure: no I/O.
//
// Entries are filtered to those whose key parses to a volume name in names.
// Volumes in names that have no entries are not present in the result map;
// the caller treats absence the same as an all-zero fact (no replica signal).
func aggregateVolumeReplicaLayout(
	entries []cluster.ObjectIndexEntry,
	groups map[string]cluster.ShardGroupEntry,
	names []string,
) map[string]admin.ReplicaLayoutFact {
	if len(entries) == 0 || len(names) == 0 {
		return nil
	}
	wanted := make(map[string]bool, len(names))
	for _, n := range names {
		if n != "" {
			wanted[n] = true
		}
	}
	if len(wanted) == 0 {
		return nil
	}
	out := make(map[string]admin.ReplicaLayoutFact)
	for _, entry := range entries {
		name, ok := volume.NameFromBlockKey(entry.Key)
		if !ok || !wanted[name] {
			continue
		}
		group := groups[entry.PlacementGroupID]
		state := cluster.ClassifyObjectLayout(entry, group, cluster.RepairSignals{})
		fact := out[name]
		switch state {
		case cluster.LayoutCurrent:
			fact.CurrentCount++
		case cluster.LayoutPendingUpgrade:
			fact.PendingUpgradeCount++
		case cluster.LayoutDowngradeSkipped:
			fact.DowngradeSkippedCount++
		case cluster.LayoutRepairNeeded:
			fact.RepairNeededCount++
		case cluster.LayoutUnknown:
			fact.UnknownCount++
		}
		out[name] = fact
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
