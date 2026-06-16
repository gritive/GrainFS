package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPlanPlacementExpansion proves S7-7's planning step: Base is the OpRouter's
// boot-frozen placement set (the set objects are currently routed under), and
// Expanded is the candidate set derived from the LIVE shard groups — which
// includes groups formed by node joins after boot (no rebuild fires on
// PutShardGroup, so the OpRouter base stays frozen). Added = Expanded − Base.
//
// RED-on-revert: source Base from the live shard groups (candidateGroupsFor)
// instead of the frozen OpRouter → Base would already include the joined groups,
// Added would be empty, and the gen-0 capture would freeze the wrong (expanded)
// set — the corruption S7-6/S7-7 exist to prevent.
func TestPlanPlacementExpansion(t *testing.T) {
	ec := ECConfig{DataShards: 2, ParityShards: 1}
	meta := newFakeShardGroupSourceN(t, 2) // group-1, group-2 at construction
	c := NewClusterCoordinator(&fakeBackend{}, NewDataGroupManager(), nil, meta, "node-1").
		WithECConfig(ec)

	// Two new groups join after boot (e.g. via expandShardGroupsForJoinedNode).
	// No rebuild fires, so the OpRouter base stays frozen at {group-1, group-2}.
	meta.groups["group-3"] = ShardGroupEntry{ID: "group-3", PeerIDs: []string{"node-1", "node-2", "node-3"}}
	meta.groups["group-4"] = ShardGroupEntry{ID: "group-4", PeerIDs: []string{"node-1", "node-2", "node-3"}}

	plan, err := c.PlanPlacementExpansion()
	require.NoError(t, err)
	require.False(t, plan.NoOp, "new candidate groups present → not a no-op")
	require.Equal(t, []string{"group-1", "group-2"}, plan.Base, "Base is the boot-frozen placement set, not the live (grown) candidate set")
	require.Equal(t, []string{"group-1", "group-2", "group-3", "group-4"}, plan.Expanded)
	require.Equal(t, []string{"group-3", "group-4"}, plan.Added)
}

// TestPlanPlacementExpansion_WiderGroupNarrows proves the Removed reporting:
// candidateGroupsFor keeps only the widest-peer-count groups, so when a
// newly-joined group is wider than the Base groups, the narrower Base groups
// drop out of the active set. The plan must surface them in Removed so the
// operator is not misled by an Added-only report.
func TestPlanPlacementExpansion_WiderGroupNarrows(t *testing.T) {
	ec := ECConfig{DataShards: 2, ParityShards: 1}
	meta := newFakeShardGroupSourceN(t, 2) // group-1, group-2 @ 3 peers
	c := NewClusterCoordinator(&fakeBackend{}, NewDataGroupManager(), nil, meta, "node-1").
		WithECConfig(ec)

	// A wider group joins (5 peers). candidateGroupsFor keeps only the widest,
	// so group-1/group-2 (3 peers) drop out of the active placement set.
	meta.groups["group-5"] = ShardGroupEntry{ID: "group-5", PeerIDs: []string{"n1", "n2", "n3", "n4", "n5"}}

	plan, err := c.PlanPlacementExpansion()
	require.NoError(t, err)
	require.False(t, plan.NoOp)
	require.Equal(t, []string{"group-1", "group-2"}, plan.Base)
	require.Equal(t, []string{"group-5"}, plan.Expanded, "widest-only candidate set")
	require.Equal(t, []string{"group-5"}, plan.Added)
	require.Equal(t, []string{"group-1", "group-2"}, plan.Removed, "narrower Base groups must be surfaced as Removed")
}

// TestPlanPlacementExpansion_NoOp proves the degenerate guard: when no new
// candidate groups have appeared (live candidate set equals the frozen base),
// the plan is a no-op so the caller records no useless generation.
func TestPlanPlacementExpansion_NoOp(t *testing.T) {
	ec := ECConfig{DataShards: 2, ParityShards: 1}
	meta := newFakeShardGroupSourceN(t, 3)
	c := NewClusterCoordinator(&fakeBackend{}, NewDataGroupManager(), nil, meta, "node-1").
		WithECConfig(ec)

	plan, err := c.PlanPlacementExpansion()
	require.NoError(t, err)
	require.True(t, plan.NoOp, "no new candidate groups → no-op")
	require.Empty(t, plan.Added)
}

// TestPlanPlacementExpansion_RejectsNonRedundant pins the durability guard on the
// operator expand-placement path: it refuses to record a non-redundant generation
// in a multi-node cluster. This protects ensureGenZero's self-heal invariant (the
// latest generation regresses to non-redundant only at gen-0/boot). A redundant
// base is frozen, then the wide group vanishes leaving only a single-peer group —
// the operator expansion to that 1+0 set is rejected.
//
// RED-on-revert: drop the redundantPlacementGate in PlanPlacementExpansion and it
// returns a normal plan over the single-peer set.
func TestPlanPlacementExpansion_RejectsNonRedundant(t *testing.T) {
	ec := ECConfig{DataShards: 2, ParityShards: 2}
	meta := &fakeGenShardSource{
		nodeCount: 4,
		groups: map[string]ShardGroupEntry{
			"group-wide": {ID: "group-wide", PeerIDs: []string{"n1", "n2", "n3", "n4"}},
		},
	}
	c := NewClusterCoordinator(&fakeBackend{}, NewDataGroupManager(), nil, meta, "n1").
		WithECConfig(ec)
	require.Equal(t, []string{"group-wide"}, c.runtimeState().opRouter.currentPlacementGroupIDs(),
		"redundant base is boot-frozen")

	// The wide group vanishes; only a single-peer group remains live.
	delete(meta.groups, "group-wide")
	meta.groups["group-narrow"] = ShardGroupEntry{ID: "group-narrow", PeerIDs: []string{"n1"}}

	_, err := c.PlanPlacementExpansion()
	require.ErrorIs(t, err, ErrPlacementNotRedundant,
		"operator expansion to a non-redundant set is refused")
}
