package cluster

import (
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/raft"
)

// makeAddPlacementGenerationCmd builds a full MetaCmd envelope so tests exercise
// the real applyCmd dispatch (not the leaf applier directly) — a missing
// dispatch case would be silently ignored, so this is the non-vacuous path.
func makeAddPlacementGenerationCmd(t *testing.T, groupIDs []string) []byte {
	t.Helper()
	cmd, err := encodeMetaCmd(MetaCmdTypeAddPlacementGeneration, encodeMetaAddPlacementGenerationCmd(groupIDs))
	require.NoError(t, err)
	return cmd
}

// TestApplyAddPlacementGeneration_AppendsMonotonic proves the command appends
// generations with monotonically-assigned epochs through the real apply
// dispatch, and rejects empty group_ids.
func TestApplyAddPlacementGeneration_AppendsMonotonic(t *testing.T) {
	f := NewMetaFSM()
	require.Empty(t, f.PlacementGenerations(), "fresh FSM has no generations")

	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, []string{"group-1", "group-2"})))
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, []string{"group-1", "group-2", "group-3"})))

	gens := f.PlacementGenerations()
	require.Len(t, gens, 2)
	require.Equal(t, uint64(0), gens[0].epoch)
	require.Equal(t, []string{"group-1", "group-2"}, gens[0].groupIDs)
	require.Equal(t, uint64(1), gens[1].epoch)
	require.Equal(t, []string{"group-1", "group-2", "group-3"}, gens[1].groupIDs)

	// Empty group_ids is rejected.
	require.Error(t, f.applyCmd(makeAddPlacementGenerationCmd(t, nil)))
	require.Len(t, f.PlacementGenerations(), 2, "rejected command does not append")

	// Accessor returns a deep copy: mutating the result must not affect FSM state.
	gens[0].groupIDs[0] = "MUTATED"
	require.Equal(t, "group-1", f.PlacementGenerations()[0].groupIDs[0])
}

// TestPlacementGenerations_SnapshotRestore proves the generation registry
// survives a snapshot→restore round-trip with order, epoch, and group IDs intact.
func TestPlacementGenerations_SnapshotRestore(t *testing.T) {
	f := NewMetaFSM()
	wireTestKEK(t, f)
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, []string{"group-1", "group-2"})))
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, []string{"group-1", "group-2", "group-3", "group-4"})))

	snap, err := f.Snapshot()
	require.NoError(t, err)

	f2 := NewMetaFSM()
	wireTestKEK(t, f2)
	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))

	got := f2.PlacementGenerations()
	require.Len(t, got, 2)
	require.Equal(t, uint64(0), got[0].epoch)
	require.Equal(t, []string{"group-1", "group-2"}, got[0].groupIDs)
	require.Equal(t, uint64(1), got[1].epoch)
	require.Equal(t, []string{"group-1", "group-2", "group-3", "group-4"}, got[1].groupIDs)
}

// TestPlacementGenerations_LegacySnapshotMissingSlot proves a snapshot built
// without the placement_generations field (an old binary, or a fresh cluster)
// restores to an empty registry with no error — the dormant-compat guarantee.
func TestPlacementGenerations_LegacySnapshotMissingSlot(t *testing.T) {
	// A MetaStateSnapshot with no placement_generations field at all (slot 19
	// absent, as an older binary would emit). FB returns Length()==0 for the
	// absent field; restore must read it as nil, not panic or error.
	b := flatbuffers.NewBuilder(64)
	clusterpb.MetaStateSnapshotStart(b)
	b.Finish(clusterpb.MetaStateSnapshotEnd(b))
	legacy := b.FinishedBytes()

	f := NewMetaFSM()
	wireTestKEK(t, f)
	require.NoError(t, f.Restore(raft.SnapshotMeta{}, legacy))
	require.Empty(t, f.PlacementGenerations(), "legacy snapshot → empty generation registry")
}
