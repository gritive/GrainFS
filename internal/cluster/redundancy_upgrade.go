package cluster

import "github.com/gritive/GrainFS/internal/scrubber"

// needsRedundancyUpgrade reports whether an EC object should be relocated into a
// redundant placement group. An object qualifies iff it is a non-redundant EC
// object (DataShards >= 1, ParityShards == 0 — the "written when single-node"
// fingerprint), the cluster currently has redundant placement capacity, it is
// not a delete marker, and it is older than minAge (the age gate avoids racing
// an in-flight write or a settling append).
//
//nolint:unused // wired up by the orphan/redundancy-upgrade sweep in a later task.
func needsRedundancyUpgrade(rec scrubber.ObjectRecord, clusterRedundant bool, now, minAge int64) bool {
	if !clusterRedundant || rec.IsDeleteMarker {
		return false
	}
	if rec.DataShards < 1 || rec.ParityShards != 0 {
		return false
	}
	return now-rec.LastModified >= minAge
}

// clusterHasRedundantCapacity reports whether the cluster can currently place an
// object durably: there are >=2 member nodes AND the widest placement-group
// candidate is itself redundant (parity > 0). This is the same condition the
// placement-redundancy gate (redundantPlacementGate) enforces for new writes,
// but stated positively — note redundantPlacementGate returns nil for a genuine
// single-node cluster (metaNodeCount < 2), which is NOT redundant capacity, so we
// guard metaNodeCount >= 2 explicitly rather than mapping its nil to true.
//
//nolint:unused // wired up by the orphan/redundancy-upgrade sweep in a later task.
func clusterHasRedundantCapacity(groups []ShardGroupEntry, cfg ECConfig, metaNodeCount int) bool {
	if metaNodeCount < 2 {
		return false
	}
	candidates, err := candidateGroupsFor(groups, cfg)
	if err != nil || len(candidates) == 0 {
		return false
	}
	return DesiredECConfigForGroup(candidates[0]).Redundant()
}
