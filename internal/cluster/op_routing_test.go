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
	r.router.AssignBucket(storage.NFS4BucketName, "g1")
	got, entry, err := r.RouteObjectRead(storage.NFS4BucketName, "obj", "")
	require.NoError(t, err)
	require.Equal(t, "g1", got.GroupID)
	require.Equal(t, "g1", entry.PlacementGroupID)
	require.Equal(t, storage.NFS4BucketName, entry.Bucket)
}

func TestOpRouter_RouteObjectRead_NilIndexReturnsError(t *testing.T) {
	probe := &fakeLeaderProbe{}
	r := routerForTestWithBucket(t, probe)
	r.index = nil
	_, _, err := r.RouteObjectRead("b1", "k1", "")
	require.ErrorIs(t, err, ErrObjectIndexRequired)
}
