package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// fakeLeaderProbe satisfies dataGroupLeaderProbe for OpRouter tests.
type fakeLeaderProbe struct {
	leaderGroups map[string]bool
}

func (f *fakeLeaderProbe) GroupLeaderIsSelf(groupID string) bool {
	return f.leaderGroups[groupID]
}

// fakeNodeAddressBook satisfies NodeAddressBook for OpRouter tests.
type fakeNodeAddressBook struct {
	nodes []MetaNodeEntry
}

func (f *fakeNodeAddressBook) Nodes() []MetaNodeEntry { return f.nodes }

// routerWithGroups builds a *Router whose RouteKey can return non-nil
// DataGroups for the supplied group IDs. Required because Router.RouteKey
// dereferences mgr.Get(gid).
func routerWithGroups(t *testing.T, groups map[string][]string) *Router {
	t.Helper()
	mgr := NewDataGroupManager()
	for id, peers := range groups {
		mgr.Add(NewDataGroup(id, peers))
	}
	return NewRouter(mgr)
}

func routerForTestWithBucket(t *testing.T, leaderProbe *fakeLeaderProbe) *OpRouter {
	t.Helper()
	r := routerWithGroups(t, map[string][]string{
		"g1": {"node-1", "node-2", "node-3"},
	})
	r.AssignBucket("b1", "g1")
	groups := &fakeShardGroupSource{
		groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{"node-1", "node-2", "node-3"}},
		},
	}
	addr := &fakeNodeAddressBook{nodes: []MetaNodeEntry{
		{ID: "node-1", Address: "10.0.0.1:7000"},
		{ID: "node-2", Address: "10.0.0.2:7000"},
		{ID: "node-3", Address: "10.0.0.3:7000"},
	}}
	return NewOpRouter(r, groups, addr, leaderProbe, ECConfig{}, "node-2", nil)
}

func TestOpRouter_RouteBucket_HappyNonLeader(t *testing.T) {
	probe := &fakeLeaderProbe{leaderGroups: map[string]bool{}}
	r := routerForTestWithBucket(t, probe)
	got, err := r.RouteBucket("b1")
	require.NoError(t, err)
	require.Equal(t, "g1", got.GroupID)
	require.True(t, got.SelfIsVoter, "node-2 is in PeerIDs")
	require.False(t, got.SelfIsLeader)
	require.Equal(t, []string{"10.0.0.1:7000", "10.0.0.3:7000", "10.0.0.2:7000"}, got.Peers)
}

func TestOpRouter_RouteBucket_NoRouter(t *testing.T) {
	r := &OpRouter{}
	_, err := r.RouteBucket("b1")
	require.ErrorIs(t, err, ErrCoordinatorNoRouter)
}

func TestOpRouter_RouteBucket_UnassignedBucket(t *testing.T) {
	probe := &fakeLeaderProbe{}
	r := routerForTestWithBucket(t, probe)
	_, err := r.RouteBucket("never-assigned")
	require.ErrorIs(t, err, storage.ErrNoSuchBucket)
}

func TestOpRouter_RouteBucket_LeaderShortCircuit(t *testing.T) {
	probe := &fakeLeaderProbe{leaderGroups: map[string]bool{"g1": true}}
	r := routerForTestWithBucket(t, probe)
	got, err := r.RouteBucket("b1")
	require.NoError(t, err)
	require.True(t, got.SelfIsLeader)
	require.Empty(t, got.Peers, "leader path skips peer resolution")
}

func TestOpRouter_RouteBucket_DuplicateSelfIsOnlyVoter(t *testing.T) {
	r := routerWithGroups(t, map[string][]string{
		"g1": {"node-1", "node-1", "node-1"},
	})
	r.AssignBucket("b1", "g1")
	groups := &fakeShardGroupSource{
		groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{"node-1", "node-1", "node-1"}},
		},
	}
	router := NewOpRouter(r, groups, nil, &fakeLeaderProbe{}, ECConfig{}, "node-1", nil)

	got, err := router.RouteBucket("b1")
	require.NoError(t, err)
	require.True(t, got.SelfIsOnlyVoter)
}

func TestOpRouter_RouteObjectRead_DeterministicWhenGroupsExist(t *testing.T) {
	// Deterministic hash placement: EC-capable groups → route succeeds without index.
	probe := &fakeLeaderProbe{}
	r := routerForTestWithBucket(t, probe)
	_, entry, err := r.RouteObjectRead("b1", "k1", "")
	require.NoError(t, err)
	require.NotEmpty(t, entry.PlacementGroupID)
}

func TestOpRouter_RouteObjectRead_NilIndexNoCandidatesReturnsError(t *testing.T) {
	// nil index + no EC-capable groups (bootstrap) → ErrObjectIndexRequired.
	groups := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{}}
	r := NewOpRouter(nil, groups, nil, nil, ECConfig{DataShards: 2, ParityShards: 1}, "n1", nil)
	_, _, err := r.RouteObjectRead("b1", "k1", "")
	require.ErrorIs(t, err, ErrObjectIndexRequired)
}

func routerForTestWithECGroups(t *testing.T, ec ECConfig) *OpRouter {
	t.Helper()
	rt := routerWithGroups(t, map[string][]string{
		"g1": {"node-1", "node-2", "node-3", "node-4", "node-5", "node-6"},
		"g2": {"node-1", "node-2", "node-3", "node-4", "node-5", "node-6"},
	})
	groups := &fakeShardGroupSource{
		groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{"node-1", "node-2", "node-3", "node-4", "node-5", "node-6"}},
			"g2": {ID: "g2", PeerIDs: []string{"node-1", "node-2", "node-3", "node-4", "node-5", "node-6"}},
		},
	}
	addr := &fakeNodeAddressBook{nodes: []MetaNodeEntry{
		{ID: "node-1", Address: "10.0.0.1:7000"}, {ID: "node-2", Address: "10.0.0.2:7000"},
		{ID: "node-3", Address: "10.0.0.3:7000"}, {ID: "node-4", Address: "10.0.0.4:7000"},
		{ID: "node-5", Address: "10.0.0.5:7000"}, {ID: "node-6", Address: "10.0.0.6:7000"},
	}}
	probe := &fakeLeaderProbe{}
	return NewOpRouter(rt, groups, addr, probe, ec, "node-2", nil)
}

func TestOpRouter_RouteObjectWrite_PicksECCapableGroup(t *testing.T) {
	r := routerForTestWithECGroups(t, ECConfig{DataShards: 4, ParityShards: 2})
	target, group, err := r.RouteObjectWrite("b1", "key-1")
	require.NoError(t, err)
	require.Contains(t, []string{"g1", "g2"}, target.GroupID)
	require.Equal(t, target.GroupID, group.ID)
	require.GreaterOrEqual(t, len(group.PeerIDs), 6)
}

func TestOpRouter_RouteObjectWrite_PreservesForwardPeersWhenSelfIsLeader(t *testing.T) {
	r := routerForTestWithECGroups(t, ECConfig{DataShards: 4, ParityShards: 2})
	r.leaderProbe = &fakeLeaderProbe{leaderGroups: map[string]bool{
		"g1": true,
		"g2": true,
	}}

	target, _, err := r.RouteObjectWrite("b1", "key-1")

	require.NoError(t, err)
	require.True(t, target.SelfIsLeader)
	require.NotEmpty(t, target.Peers, "forward candidates must survive route-to-execute leadership races")
	require.NotContains(t, target.Peers[:len(target.Peers)-1], "10.0.0.2:7000")
}

func TestOpRouter_RouteObjectOwnerWriteGroup_UsesOwnerOnlyPeer(t *testing.T) {
	groups := &fakeShardGroupSource{
		groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{"node-1"}},
		},
	}
	addr := &fakeNodeAddressBook{nodes: []MetaNodeEntry{
		{ID: "node-1", Address: "10.0.0.1:7000"},
		{ID: "node-2", Address: "10.0.0.2:7000"},
	}}
	r := NewOpRouter(nil, groups, addr, &fakeLeaderProbe{}, ECConfig{}, "node-2", nil)

	target, group, err := r.RouteObjectOwnerWriteGroup("g1")

	require.NoError(t, err)
	require.Equal(t, target.GroupID, group.ID)
	require.False(t, target.SelfIsWriteOwner)
	require.Equal(t, "node-1", target.OwnerPeer)
	require.Equal(t, []string{"10.0.0.1:7000"}, target.Peers)
}

func TestOpRouter_RouteObjectOwnerWriteGroup_LocalOwnerDoesNotRequireLeader(t *testing.T) {
	groups := &fakeShardGroupSource{
		groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{"node-1"}},
		},
	}
	r := NewOpRouter(nil, groups, nil, &fakeLeaderProbe{}, ECConfig{}, "node-1", nil)

	target, group, err := r.RouteObjectOwnerWriteGroup("g1")

	require.NoError(t, err)
	require.Equal(t, group.ID, target.GroupID)
	require.True(t, target.SelfIsWriteOwner)
	require.True(t, target.SelfIsVoter)
	require.False(t, target.SelfIsLeader)
	require.Empty(t, target.Peers)
}

func TestRouteObjectRead_IndexFree(t *testing.T) {
	groups := &fakeShardGroupSource{
		groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{"n1", "n2", "n3"}},
			"g2": {ID: "g2", PeerIDs: []string{"n1", "n2", "n3"}},
		},
	}
	ec := ECConfig{DataShards: 2, ParityShards: 1}

	// EC-capable groups → deterministic route succeeds
	r := NewOpRouter(nil, groups, nil, nil, ec, "n1", nil)
	_, entry, err := r.RouteObjectRead("b", "k", "")
	require.NoError(t, err)
	require.Contains(t, []string{"g1", "g2"}, entry.PlacementGroupID)

	// empty group source (no EC candidates) → ErrObjectIndexRequired
	r2 := NewOpRouter(nil, &fakeShardGroupSource{groups: map[string]ShardGroupEntry{}}, nil, nil, ec, "n1", nil)
	_, _, err2 := r2.RouteObjectRead("b", "k", "")
	require.ErrorIs(t, err2, ErrObjectIndexRequired)
}

func TestPlacementEquivalence(t *testing.T) {
	groups := &fakeShardGroupSource{
		groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{"n1", "n2", "n3"}},
			"g2": {ID: "g2", PeerIDs: []string{"n1", "n2", "n3"}},
		},
	}
	ec := ECConfig{DataShards: 2, ParityShards: 1}
	r := NewOpRouter(nil, groups, nil, nil, ec, "n1", nil)

	for _, key := range []string{"obj/a", "obj/b", "obj/c", "obj/123"} {
		_, writeGroup, err := r.RouteObjectWrite("b", key)
		require.NoError(t, err, "key=%s", key)

		_, readEntry, err := r.RouteObjectRead("b", key, "")
		require.NoError(t, err, "key=%s", key)

		require.Equal(t, writeGroup.ID, readEntry.PlacementGroupID,
			"write and index-free read must land on same group for key=%s", key)
	}
}

func TestOpRouter_RouteObjectWrite_NoECCapableFallsBackToBucketRoute(t *testing.T) {
	rt := routerWithGroups(t, map[string][]string{
		"g-small": {"node-1", "node-2", "node-3"},
	})
	rt.AssignBucket("b-fallback", "g-small")
	groups := &fakeShardGroupSource{
		groups: map[string]ShardGroupEntry{
			"g-small": {ID: "g-small", PeerIDs: []string{"node-1", "node-2", "node-3"}},
		},
	}
	addr := &fakeNodeAddressBook{nodes: []MetaNodeEntry{
		{ID: "node-1", Address: "10.0.0.1:7000"}, {ID: "node-2", Address: "10.0.0.2:7000"},
		{ID: "node-3", Address: "10.0.0.3:7000"},
	}}
	r := NewOpRouter(rt, groups, addr, &fakeLeaderProbe{}, ECConfig{DataShards: 4, ParityShards: 2}, "node-2", nil)
	target, group, err := r.RouteObjectWrite("b-fallback", "k")
	require.NoError(t, err)
	require.Equal(t, "g-small", target.GroupID)
	require.Equal(t, "g-small", group.ID)
}
