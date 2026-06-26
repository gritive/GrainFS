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

func makeRetirePlacementGenerationCmd(t *testing.T, epoch uint64) []byte {
	t.Helper()
	cmd, err := encodeMetaCmd(MetaCmdTypeRetirePlacementGeneration, encodeMetaRetirePlacementGenerationCmd(epoch))
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

// TestApplyAddPlacementGeneration_DedupsIdenticalLatest pins the idempotency the
// lazy-first-write gen-0 establishment relies on: concurrent first writes both
// propose the same converged candidate set, so the FSM apply must no-op when the
// proposed group set equals the current LATEST generation — otherwise the registry
// would accumulate duplicate generations ([set, set, ...]), inflating the read
// fan-out and (for gen-0) violating the "one gen-0" contract. A genuinely different
// set (growth) still appends.
func TestApplyAddPlacementGeneration_DedupsIdenticalLatest(t *testing.T) {
	f := NewMetaFSM()

	// First record establishes gen-0.
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, []string{"group-1", "group-2", "group-3"})))
	require.Len(t, f.PlacementGenerations(), 1)

	// Re-proposing the identical set (concurrent first-writer / retry) is a no-op.
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, []string{"group-1", "group-2", "group-3"})))
	require.Len(t, f.PlacementGenerations(), 1, "identical-to-latest generation must dedup, not append")

	// A genuinely different set (growth) appends gen-1.
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, []string{"group-1", "group-2", "group-3", "group-4"})))
	require.Len(t, f.PlacementGenerations(), 2, "a changed set is real growth and must append")

	// Re-proposing the new latest is again a no-op.
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, []string{"group-1", "group-2", "group-3", "group-4"})))
	require.Len(t, f.PlacementGenerations(), 2, "identical-to-latest after growth must also dedup")
}

// TestApplyAddPlacementGeneration_DedupsAnyExisting pins the registry invariant
// that closes the lazy-gen-0-vs-operator-growth race: a proposal equal to ANY
// existing generation (not just the latest) is a no-op. The race forms a
// [A, B, A] history when a stale lazy gen-0 proposal (set A) is ordered AFTER an
// operator expansion appended B — dedup-against-latest (last=B≠A) would append A,
// silently reverting the expansion (currentGroupIDs would become the pre-growth
// set A and the registry would never shrink). Deduping against the whole registry
// keeps it [A, B], preserving the expansion.
//
// RED-on-revert: compare only against the last entry and the third apply appends
// A → registry becomes [A, B, A].
func TestApplyAddPlacementGeneration_DedupsAnyExisting(t *testing.T) {
	f := NewMetaFSM()
	setA := []string{"group-1", "group-2"}
	setB := []string{"group-1", "group-2", "group-3", "group-4"}

	// Operator growth captures gen-0 = A, then appends the expanded set B.
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, setA)))
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, setB)))
	require.Len(t, f.PlacementGenerations(), 2)

	// A stale lazy gen-0 proposal (set A) is ordered last. It equals an EARLIER
	// generation, not the latest — it must still dedup, or it reverts the growth.
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, setA)))
	gens := f.PlacementGenerations()
	require.Len(t, gens, 2, "a set equal to ANY existing generation must dedup, not append")
	require.Equal(t, setB, gens[len(gens)-1].groupIDs,
		"latest generation stays the expanded set B; the expansion is not reverted")
}

// TestPlacementGenerations_SnapshotRestore proves the generation registry
// survives a snapshot→restore round-trip with order, epoch, and group IDs intact.
func TestPlacementGenerations_SnapshotRestore(t *testing.T) {
	f := NewMetaFSM()
	wireTestKEK(t, f)
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, []string{"group-1", "group-2"})))
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, []string{"group-1", "group-2", "group-3", "group-4"})))
	require.NoError(t, f.applyCmd(makeRetirePlacementGenerationCmd(t, 0)))

	snap, err := f.Snapshot()
	require.NoError(t, err)

	f2 := NewMetaFSM()
	wireTestKEK(t, f2)
	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))

	got := f2.PlacementGenerations()
	require.Len(t, got, 2)
	require.Equal(t, uint64(0), got[0].epoch)
	require.Equal(t, []string{"group-1", "group-2"}, got[0].groupIDs)
	require.True(t, got[0].retired)
	require.Equal(t, uint64(1), got[1].epoch)
	require.Equal(t, []string{"group-1", "group-2", "group-3", "group-4"}, got[1].groupIDs)
	require.False(t, got[1].retired)
}

func TestApplyRetirePlacementGeneration(t *testing.T) {
	f := NewMetaFSM()
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, []string{"group-1", "group-2"})))
	require.NoError(t, f.applyCmd(makeAddPlacementGenerationCmd(t, []string{"group-3", "group-4"})))

	require.NoError(t, f.applyCmd(makeRetirePlacementGenerationCmd(t, 0)))
	gens := f.PlacementGenerations()
	require.True(t, gens[0].retired)
	require.False(t, gens[1].retired)

	require.NoError(t, f.applyCmd(makeRetirePlacementGenerationCmd(t, 0)), "retire is idempotent")
	require.ErrorContains(t, f.applyCmd(makeRetirePlacementGenerationCmd(t, 1)), "latest generation")
	require.ErrorContains(t, f.applyCmd(makeRetirePlacementGenerationCmd(t, 99)), "unknown epoch")
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
