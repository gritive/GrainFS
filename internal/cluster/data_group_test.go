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
