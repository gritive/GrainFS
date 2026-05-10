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
