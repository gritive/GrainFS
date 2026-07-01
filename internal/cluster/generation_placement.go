package cluster

// placementGeneration captures one topology generation's pinned object→group
// placement set. Objects written under this generation hash (modulo) into
// groupIDs. groupIDs is the candidateGroupsFor-derived, sorted candidate ID
// list, pinned for the generation's lifetime.
type placementGeneration struct {
	epoch    uint64
	groupIDs []string
	retired  bool
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

// newGenerationPlacementFromList builds a placement from the FSM topology
// generation list (ascending epoch, as PlacementGenerations() returns it). Used
// by OpRouter when the cluster has recorded explicit generations; an empty list
// leaves the caller on the legacy single-generation path. The slice is taken
// as-is (PlacementGenerations() already returns a deep copy).
func newGenerationPlacementFromList(gens []placementGeneration) *GenerationPlacement {
	if len(gens) == 0 {
		return &GenerationPlacement{}
	}
	return &GenerationPlacement{generations: gens}
}

// currentGroupIDs returns the latest generation's pinned candidate group-ID set
// used for object→group write/read placement. Returns nil when no generation is
// configured (bootstrap), matching the legacy nil placementGroupIDs guard.
func (g *GenerationPlacement) currentGroupIDs() []string {
	if g == nil || len(g.generations) == 0 {
		return nil
	}
	for i := len(g.generations) - 1; i >= 0; i-- {
		if !g.generations[i].retired {
			return g.generations[i].groupIDs
		}
	}
	return nil
}

// generationCount returns the number of recorded topology generations. The
// default (bootstrap or single-generation) returns 0 or 1; a value > 1 means an
// operator has added at least one new generation, which arms the cross-generation
// LWW read merge (S7-6). Used by the coordinator to propagate the multi-generation
// flag to backends so reads fan out for last-writer-wins across generations
// instead of taking the local-first fast path.
func (g *GenerationPlacement) generationCount() int {
	if g == nil {
		return 0
	}
	return len(g.generations)
}

// readGenerationGroupIDs returns each generation's pinned group-ID set in
// newest-first probe order (latest generation first, base generation last). At
// a single generation this is one element equal to currentGroupIDs(), so the
// read path takes exactly one attempt — byte-identical to legacy routing.
func (g *GenerationPlacement) readGenerationGroupIDs() [][]string {
	if g == nil || len(g.generations) == 0 {
		return nil
	}
	out := make([][]string, 0, len(g.generations))
	for i := len(g.generations) - 1; i >= 0; i-- {
		if g.generations[i].retired {
			continue
		}
		out = append(out, g.generations[i].groupIDs)
	}
	return out
}
