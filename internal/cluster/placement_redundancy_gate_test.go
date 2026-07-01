package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// peersN builds a ShardGroupEntry with n synthetic peers, used to drive the
// redundancy predicate (DesiredECConfigForGroup keys off len(PeerIDs)).
func peersN(id string, n int) ShardGroupEntry {
	p := make([]string, n)
	for i := 0; i < n; i++ {
		p[i] = string(rune('a' + i))
	}
	return ShardGroupEntry{ID: id, PeerIDs: p}
}

// TestRedundantPlacementGate pins the durability gate that prevents objects from
// being placed in a non-redundant (single-peer, 1+0) group when the cluster has
// the node capacity for redundancy. The widest candidate (candidates[0] after
// candidateGroupsFor) represents the set; a 1-peer widest with >=2 member nodes
// means redundant groups have not formed yet -> defer (error). A genuine 1-node
// cluster keeps the 1+0 group (best available). A redundant widest always passes.
func TestRedundantPlacementGate(t *testing.T) {
	tests := []struct {
		name      string
		widest    ShardGroupEntry
		nodeCount int
		wantErr   bool
	}{
		{"4-peer widest, 4-node cluster -> redundant ok", peersN("group-12", 4), 4, false},
		{"2-peer widest, 2-node cluster -> 1+1 redundant ok", peersN("group-8", 2), 2, false},
		{"3-peer widest, 3-node cluster -> 2+1 redundant ok", peersN("group-9", 3), 3, false},
		{"1-peer widest, 4-node cluster -> defer (forming)", peersN("group-5", 1), 4, true},
		{"1-peer widest, 2-node cluster -> defer (forming)", peersN("group-5", 1), 2, true},
		{"1-peer widest, 1-node cluster -> single-node 1+0 ok", peersN("group-5", 1), 1, false},
		{"1-peer group-0 only, 2-node cluster -> defer (Trap D)", peersN("group-0", 1), 2, true},
		{"empty candidates -> no gate (caller already errored)", ShardGroupEntry{}, 4, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var candidates []ShardGroupEntry
			if tc.widest.ID != "" {
				candidates = []ShardGroupEntry{tc.widest}
			}
			err := redundantPlacementGate(candidates, tc.nodeCount)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrPlacementNotRedundant)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestErrPlacementNotRedundant_IsSentinel proves the error is a stable sentinel
// callers can errors.Is against to map to ServiceUnavailable.
func TestErrPlacementNotRedundant_IsSentinel(t *testing.T) {
	require.True(t, errors.Is(ErrPlacementNotRedundant, ErrPlacementNotRedundant))
	require.NotEmpty(t, ErrPlacementNotRedundant.Error())
}

// TestRouteObjectWrite_FrozenPathGated pins the frozen/gen-path gate (the code-gate
// MAJOR): a poisoned non-redundant placement generation (a 1+0 single-peer gen-0 from
// the formation race) must NOT route objects non-redundantly via currentGroupIDs().
// The frozen pick is gated; a non-redundant frozen group falls through to the live
// path, which selects the redundant wide group instead.
//
// RED-on-revert: remove the redundantPlacementGate on the currentGroupIDs() branch
// of RouteObjectWrite and the object routes to the single-peer group "g-narrow".
func TestRouteObjectWrite_FrozenPathGated(t *testing.T) {
	meta := &fakeGenShardSource{
		nodeCount: 4,
		groups: map[string]ShardGroupEntry{
			"g-narrow": {ID: "g-narrow", PeerIDs: []string{"n1"}},
			"g-wide":   {ID: "g-wide", PeerIDs: []string{"n1", "n2", "n3", "n4"}},
		},
	}
	r := NewOpRouter(nil, meta, nil, nil, ECConfig{DataShards: 2, ParityShards: 2}, "writer", nil)
	// Poison the placement: a generation pinned to the single-peer group, as the
	// formation-race would record before self-heal advances it.
	r.applyGenerations([]placementGeneration{{epoch: 0, groupIDs: []string{"g-narrow"}}})
	require.Equal(t, []string{"g-narrow"}, r.currentPlacementGroupIDs(), "poisoned gen is in effect")

	_, group, err := r.RouteObjectWrite("bucket", "obj")
	require.NoError(t, err)
	require.Equal(t, "g-wide", group.ID,
		"frozen non-redundant pick is gated; object routes to the redundant wide group, not the single-peer one")
}
