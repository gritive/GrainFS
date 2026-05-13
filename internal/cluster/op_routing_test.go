package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// fakeObjectIndex satisfies objectIndexLookup for OpRouter tests.
type fakeObjectIndex struct {
	latest  map[string]ObjectIndexEntry // key = bucket+"/"+key
	version map[string]ObjectIndexEntry // key = bucket+"/"+key+"#"+versionID
}

func (f *fakeObjectIndex) ObjectIndexLatest(bucket, key string) (ObjectIndexEntry, bool) {
	e, ok := f.latest[bucket+"/"+key]
	return e, ok
}

func (f *fakeObjectIndex) ObjectIndexVersion(bucket, key, versionID string) (ObjectIndexEntry, bool) {
	e, ok := f.version[bucket+"/"+key+"#"+versionID]
	return e, ok
}

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
	idx := &fakeObjectIndex{latest: map[string]ObjectIndexEntry{}, version: map[string]ObjectIndexEntry{}}
	return NewOpRouter(r, groups, idx, addr, leaderProbe, ECConfig{}, "node-2", nil)
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
	idx := &fakeObjectIndex{latest: map[string]ObjectIndexEntry{}, version: map[string]ObjectIndexEntry{}}
	router := NewOpRouter(r, groups, idx, nil, &fakeLeaderProbe{}, ECConfig{}, "node-1", nil)

	got, err := router.RouteBucket("b1")
	require.NoError(t, err)
	require.True(t, got.SelfIsOnlyVoter)
}

func TestOpRouter_RouteObjectRead_LatestFromIndex(t *testing.T) {
	probe := &fakeLeaderProbe{}
	r := routerForTestWithBucket(t, probe)
	r.index.(*fakeObjectIndex).latest["b1/k1"] = ObjectIndexEntry{
		Bucket: "b1", Key: "k1", VersionID: "v-A", PlacementGroupID: "g1",
		Size: 100, ETag: "etag-A",
	}
	got, entry, err := r.RouteObjectRead("b1", "k1", "")
	require.NoError(t, err)
	require.Equal(t, "g1", got.GroupID)
	require.Equal(t, "v-A", entry.VersionID)
	require.Equal(t, "etag-A", entry.ETag)
}

func TestOpRouter_RouteObjectRead_VersionFromIndex(t *testing.T) {
	probe := &fakeLeaderProbe{}
	r := routerForTestWithBucket(t, probe)
	r.index.(*fakeObjectIndex).version["b1/k1#v-B"] = ObjectIndexEntry{
		Bucket: "b1", Key: "k1", VersionID: "v-B", PlacementGroupID: "g1", Size: 200,
	}
	got, entry, err := r.RouteObjectRead("b1", "k1", "v-B")
	require.NoError(t, err)
	require.Equal(t, "g1", got.GroupID)
	require.Equal(t, int64(200), entry.Size)
}

func TestOpRouter_RouteObjectRead_MissingLatest(t *testing.T) {
	probe := &fakeLeaderProbe{}
	r := routerForTestWithBucket(t, probe)
	_, _, err := r.RouteObjectRead("b1", "missing", "")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestOpRouter_RouteObjectRead_InternalBucketBypassesIndex(t *testing.T) {
	probe := &fakeLeaderProbe{}
	r := routerForTestWithBucket(t, probe)
	// Register the internal bucket's group with the underlying DataGroupManager
	// and the ShardGroupSource so routeGroup can resolve peers.
	r.router = routerWithGroups(t, map[string][]string{
		"g1": {"node-1", "node-2", "node-3"},
	})
	r.router.AssignBucket("__grainfs_vfs_default", "g1")
	got, entry, err := r.RouteObjectRead("__grainfs_vfs_default", "obj", "")
	require.NoError(t, err)
	require.Equal(t, "g1", got.GroupID)
	require.Equal(t, "g1", entry.PlacementGroupID)
	require.Equal(t, "__grainfs_vfs_default", entry.Bucket)
}

func TestOpRouter_RouteObjectWrite_InternalBucketPreservesGroupPeers(t *testing.T) {
	probe := &fakeLeaderProbe{}
	r := routerForTestWithBucket(t, probe)
	r.router.AssignBucket("__grainfs_vfs_default", "g1")

	target, group, err := r.RouteObjectWrite("__grainfs_vfs_default", "obj")
	require.NoError(t, err)
	require.Equal(t, "g1", target.GroupID)
	require.Equal(t, "g1", group.ID)
	require.Equal(t, []string{"node-1", "node-2", "node-3"}, group.PeerIDs)
}

func TestOpRouter_RouteObjectRead_NilIndexReturnsError(t *testing.T) {
	probe := &fakeLeaderProbe{}
	r := routerForTestWithBucket(t, probe)
	r.index = nil
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
	idx := &fakeObjectIndex{latest: map[string]ObjectIndexEntry{}, version: map[string]ObjectIndexEntry{}}
	probe := &fakeLeaderProbe{}
	return NewOpRouter(rt, groups, idx, addr, probe, ec, "node-2", nil)
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

func TestOpRouter_RouteObjectWrite_InternalBucketUsesBucketRoute(t *testing.T) {
	r := routerForTestWithECGroups(t, ECConfig{DataShards: 4, ParityShards: 2})
	r.router.AssignBucket("__grainfs_vfs_default", "g1")
	target, group, err := r.RouteObjectWrite("__grainfs_vfs_default", "k")
	require.NoError(t, err)
	require.Equal(t, "g1", target.GroupID)
	require.Equal(t, "g1", group.ID)
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
	idx := &fakeObjectIndex{}
	r := NewOpRouter(rt, groups, idx, addr, &fakeLeaderProbe{}, ECConfig{DataShards: 4, ParityShards: 2}, "node-2", nil)
	target, group, err := r.RouteObjectWrite("b-fallback", "k")
	require.NoError(t, err)
	require.Equal(t, "g-small", target.GroupID)
	require.Equal(t, "g-small", group.ID)
}
