package cluster

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGenerationPlacement_CurrentGroupIDs covers the single-generation seam:
// the base generation's IDs are returned verbatim, and the empty/nil guards
// preserve the legacy nil-placementGroupIDs short-circuit.
func TestGenerationPlacement_CurrentGroupIDs(t *testing.T) {
	ids := []string{"group-1", "group-2", "group-3"}
	gp := newGenerationPlacement(ids)
	got := gp.currentGroupIDs()
	if len(got) != len(ids) {
		t.Fatalf("currentGroupIDs len = %d, want %d", len(got), len(ids))
	}
	for i := range ids {
		if got[i] != ids[i] {
			t.Fatalf("currentGroupIDs[%d] = %q, want %q", i, got[i], ids[i])
		}
	}

	if empty := newGenerationPlacement(nil).currentGroupIDs(); empty != nil {
		t.Fatalf("empty placement currentGroupIDs = %v, want nil", empty)
	}
	if empty := newGenerationPlacement([]string{}).currentGroupIDs(); empty != nil {
		t.Fatalf("zero-len placement currentGroupIDs = %v, want nil", empty)
	}
	var nilGP *GenerationPlacement
	if got := nilGP.currentGroupIDs(); got != nil {
		t.Fatalf("nil receiver currentGroupIDs = %v, want nil", got)
	}
}

func TestGenerationPlacement_ReadGenerationGroupIDsSkipsRetired(t *testing.T) {
	gp := newGenerationPlacementFromList([]placementGeneration{
		{epoch: 0, groupIDs: []string{"group-old"}, retired: true},
		{epoch: 1, groupIDs: []string{"group-current"}},
	})

	require.Equal(t, [][]string{{"group-current"}}, gp.readGenerationGroupIDs())
	require.Equal(t, []string{"group-current"}, gp.currentGroupIDs())
}

// TestGenerationPlacement_ByteIdentical is the S7-2 neutrality anchor: at a
// single generation, the OpRouter seam selects exactly the same group that the
// legacy direct computation (groupIDForObject over the sorted candidate IDs)
// would. RED-on-revert: if the seam routed through a different ID set, the
// per-key group assignments would diverge and this fails.
func TestGenerationPlacement_ByteIdentical(t *testing.T) {
	ec := ECConfig{DataShards: 2, ParityShards: 1}
	groups := newFakeShardGroupSourceN(t, 5) // group-1..group-5, RF=3
	r := NewOpRouter(nil, groups, nil, nil, ec, "node-1", nil)

	// Legacy reference: the same candidate IDs the router froze, computed
	// independently from the live group set.
	candidates, err := candidateGroupsFor(groups.ShardGroups(), ec)
	if err != nil {
		t.Fatalf("candidateGroupsFor: %v", err)
	}
	legacyIDs := make([]string, len(candidates))
	for i, c := range candidates {
		legacyIDs[i] = c.ID
	}
	sort.Strings(legacyIDs) // candidateGroupsFor already sorts; defensive

	const bucket = "b"
	for _, key := range []string{"a", "obj/1", "k-zzz", "longer/key/path", "", "x"} {
		want := groupIDForObject(bucket, key, legacyIDs)

		readTarget, _, rerr := r.RouteObjectRead(bucket, key, "")
		if rerr != nil {
			t.Fatalf("RouteObjectRead(%q): %v", key, rerr)
		}
		if readTarget.GroupID != want {
			t.Fatalf("read seam group for %q = %q, want %q (byte-identical broken)", key, readTarget.GroupID, want)
		}

		_, writeGroup, werr := r.RouteObjectWrite(bucket, key)
		if werr != nil {
			t.Fatalf("RouteObjectWrite(%q): %v", key, werr)
		}
		if writeGroup.ID != want {
			t.Fatalf("write seam group for %q = %q, want %q (read/write divergence)", key, writeGroup.ID, want)
		}
	}
}

// newFakeShardGroupSourceN builds a deterministic EC-capable shard group source
// with group-1..group-N, each RF=3, for placement determinism tests.
func newFakeShardGroupSourceN(t *testing.T, n int) *fakeShardGroupSource {
	t.Helper()
	m := make(map[string]ShardGroupEntry, n)
	for i := 1; i <= n; i++ {
		id := "group-" + string(rune('0'+i))
		m[id] = ShardGroupEntry{ID: id, PeerIDs: []string{"node-1", "node-2", "node-3"}}
	}
	return &fakeShardGroupSource{groups: m}
}
