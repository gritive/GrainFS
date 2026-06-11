package cluster

// placementGeneration captures one topology generation's pinned object→group
// placement set. Objects written under this generation hash (modulo) into
// groupIDs. groupIDs is the candidateGroupsFor-derived, sorted candidate ID
// list, pinned for the generation's lifetime.
type placementGeneration struct {
	epoch    uint64
	groupIDs []string
}

// GenerationPlacement owns the ordered (ascending-epoch) list of topology
// generations used for deterministic object→group metadata placement.
// generations[0] is the base generation (epoch 0). At a single generation it is
// byte-identical to the legacy frozen-snapshot placement: currentGroupIDs
// returns the same candidate set NewOpRouter froze at construction.
//
// S7-3 appends generations (group-count growth); S7-4 adds newest-first probe
// over them. Segment/EC placement is NOT generation-routed — it is recorded in
// the object metadata (storage.SegmentRef self-describes PlacementGroupID +
// NodeIDs) and read record-driven, so a generation change never reroutes an
// existing object's shards. Only object→group metadata placement is recomputed
// on read (Phase 4 index-free), so only it needs the generation seam.
type GenerationPlacement struct {
	generations []placementGeneration
}

// newGenerationPlacement builds the base (single-generation) placement from the
// frozen candidate ID list. An empty list yields a placement whose
// currentGroupIDs returns nil, preserving the legacy empty-list short-circuit
// (ErrObjectIndexRequired on bootstrap clusters).
func newGenerationPlacement(baseGroupIDs []string) *GenerationPlacement {
	if len(baseGroupIDs) == 0 {
		return &GenerationPlacement{}
	}
	return &GenerationPlacement{
		generations: []placementGeneration{{epoch: 0, groupIDs: baseGroupIDs}},
	}
}

// currentGroupIDs returns the latest generation's pinned candidate group-ID set
// used for object→group write/read placement. Returns nil when no generation is
// configured (bootstrap), matching the legacy nil placementGroupIDs guard.
func (g *GenerationPlacement) currentGroupIDs() []string {
	if g == nil || len(g.generations) == 0 {
		return nil
	}
	return g.generations[len(g.generations)-1].groupIDs
}
