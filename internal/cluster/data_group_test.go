package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataGroup_Fields(t *testing.T) {
	g := NewDataGroup("group-0", []string{"node-0", "node-1"})
	assert.Equal(t, "group-0", g.ID())
	assert.Equal(t, []string{"node-0", "node-1"}, g.PeerIDs())
}

func TestDataGroupManager_Empty_ReturnsNil(t *testing.T) {
	mgr := NewDataGroupManager()
	assert.Nil(t, mgr.Get("group-0"))
	assert.Empty(t, mgr.All())
}

func TestDataGroupManager_Add_Get(t *testing.T) {
	mgr := NewDataGroupManager()
	g := NewDataGroup("group-0", []string{"node-0"})
	mgr.Add(g)

	found := mgr.Get("group-0")
	require.NotNil(t, found)
	assert.Equal(t, "group-0", found.ID())
}

func TestDataGroupManager_Add_ReplacesExisting(t *testing.T) {
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-0", []string{"node-0"}))
	mgr.Add(NewDataGroup("group-0", []string{"node-0", "node-1"}))

	all := mgr.All()
	require.Len(t, all, 1)
	assert.Equal(t, []string{"node-0", "node-1"}, all[0].PeerIDs())
}

func TestDataGroupManager_ConcurrentAdd(t *testing.T) {
	mgr := NewDataGroupManager()
	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.Add(NewDataGroup("group-a", []string{"node-0"}))
	}()
	mgr.Add(NewDataGroup("group-b", []string{"node-1"}))
	<-done
}

// TestDataGroupManager_GroupForBucket verifies the bucket→group lookup used by
// ClusterCoordinator's bucket-scoped routing path. Three cases:
//   - happy path: assigned bucket returns its group
//   - unassigned bucket returns (nil, false) (no default set in this test)
//   - nil router returns (nil, false) (defensive — coordinator may be wired
//     before router exists during startup race)
func TestDataGroupManager_GroupForBucket(t *testing.T) {
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-1", []string{"a", "b", "c"}))

	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")

	dg, ok := mgr.GroupForBucket("photos", router)
	require.True(t, ok)
	require.Equal(t, "group-1", dg.ID())

	_, ok = mgr.GroupForBucket("not-assigned", router)
	require.False(t, ok)

	_, ok = mgr.GroupForBucket("photos", nil)
	require.False(t, ok)
}
