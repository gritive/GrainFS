package cluster

// ECRewrapTarget identifies a single EC-distributed shard that a DEK rewrap
// lane must migrate. ShardKey is the canonical shard-service key; NodeIDs is
// positional: shard i lives on NodeIDs[i].
type ECRewrapTarget struct {
	Bucket   string
	ShardKey string
	NodeIDs  []string
}

// CollectECRewrapTargets enumerates every EC shard across ALL stored object
// versions (including non-latest and legacy unversioned) in this backend's data
// group and returns them as a flat slice of ECRewrapTarget. It replaces the
// old ScanGroupObjects+OwnedShards+ScanGroupSegmentShards triple so the DEK
// rewrap lane covers non-latest versions that the lat:-only scan missed.
//
// Owner-local (ECData==0) and malformed object-version refs are silently
// skipped (validated inside IterECShardScanTargetsAllVersions via
// buildECShardTargets). Segment/coalesced refs that fail validateECRefPlacement
// are also dropped by buildECShardTargets.
func (b *DistributedBackend) CollectECRewrapTargets() ([]ECRewrapTarget, error) {
	var out []ECRewrapTarget
	err := b.fsm.IterECShardScanTargetsAllVersions(func(t ECShardScanTarget) error {
		switch t.Kind {
		case ECShardObjectVersion:
			if !validateECRefPlacement(t.ECData, t.ECParity, t.NodeIDs) {
				return nil // owner-local or malformed — skip
			}
			out = append(out, ECRewrapTarget{
				Bucket:   t.Bucket,
				ShardKey: ecObjectShardKey(t.ObjectKey, t.VersionID),
				NodeIDs:  t.NodeIDs,
			})
		default: // ECShardSegment / ECShardCoalesced — ShardKey + Placement already set + validated
			out = append(out, ECRewrapTarget{
				Bucket:   t.Bucket,
				ShardKey: t.ShardKey,
				NodeIDs:  t.Placement.Nodes,
			})
		}
		return nil
	})
	return out, err
}
