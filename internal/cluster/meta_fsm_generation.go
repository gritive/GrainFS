package cluster

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// Phase 7 placement-generation command family. A placement generation records
// one topology epoch's pinned object→group candidate set. The FSM stores an
// ordered (ascending-epoch) list; AddPlacementGeneration appends to it. This is
// control-plane state (linearizable, snapshotted), separate from the shard-group
// registry. The list is empty by default (single-generation legacy clusters) and
// no production path proposes the command yet — S7-4 wires OpRouter to consume
// PlacementGenerations(); S7-6 is the irreversible flip that first appends one.
//
// The generation record type (placementGeneration{epoch, groupIDs}) is shared
// with GenerationPlacement in generation_placement.go.

// applyAddPlacementGeneration appends a placement generation. The epoch is not
// carried on the wire — apply assigns it monotonically from the current list
// length, so replay and re-encode are deterministic. Empty group_ids is
// rejected (an empty generation has no placement meaning).
func (f *MetaFSM) applyAddPlacementGeneration(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("meta_fsm: AddPlacementGeneration: empty payload")
	}
	var (
		c      *clusterpb.MetaAddPlacementGenerationCmd
		decErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				decErr = fmt.Errorf("meta_fsm: invalid MetaAddPlacementGenerationCmd flatbuffer: %v", r)
			}
		}()
		c = clusterpb.GetRootAsMetaAddPlacementGenerationCmd(data, 0)
	}()
	if decErr != nil {
		return decErr
	}
	groupIDs := make([]string, c.GroupIdsLength())
	for i := 0; i < c.GroupIdsLength(); i++ {
		groupIDs[i] = string(c.GroupIds(i))
	}
	if len(groupIDs) == 0 {
		return fmt.Errorf("meta_fsm: AddPlacementGeneration: empty group_ids")
	}
	f.mu.Lock()
	epoch := uint64(len(f.placementGenerations))
	f.placementGenerations = append(f.placementGenerations, placementGeneration{epoch: epoch, groupIDs: groupIDs})
	f.mu.Unlock()
	return nil
}

// PlacementGenerations returns a deep copy of the ordered placement-generation
// list (ascending epoch). Empty for single-generation legacy clusters. groupIDs
// slices are copied so callers cannot mutate FSM state.
func (f *MetaFSM) PlacementGenerations() []placementGeneration {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]placementGeneration, len(f.placementGenerations))
	for i, g := range f.placementGenerations {
		ids := make([]string, len(g.groupIDs))
		copy(ids, g.groupIDs)
		out[i] = placementGeneration{epoch: g.epoch, groupIDs: ids}
	}
	return out
}

// encodeMetaAddPlacementGenerationCmd builds the inner MetaAddPlacementGenerationCmd
// payload (wrap in a MetaCmd envelope via encodeMetaCmd before proposing/applying).
// Consumed by MetaRaft.ProposeAddPlacementGeneration (S7-6 production proposer).
func encodeMetaAddPlacementGenerationCmd(groupIDs []string) []byte {
	b := clusterBuilderPool.Get()
	idOffs := make([]flatbuffers.UOffsetT, len(groupIDs))
	for i := len(groupIDs) - 1; i >= 0; i-- {
		idOffs[i] = b.CreateString(groupIDs[i])
	}
	clusterpb.MetaAddPlacementGenerationCmdStartGroupIdsVector(b, len(groupIDs))
	for i := len(groupIDs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(idOffs[i])
	}
	gidVec := b.EndVector(len(groupIDs))
	clusterpb.MetaAddPlacementGenerationCmdStart(b)
	clusterpb.MetaAddPlacementGenerationCmdAddGroupIds(b, gidVec)
	return fbFinish(b, clusterpb.MetaAddPlacementGenerationCmdEnd(b))
}
