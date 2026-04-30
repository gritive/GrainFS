package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeShardGroupSource implements shardGroupSource for unit testing.
type fakeShardGroupSource struct {
	groups map[string]ShardGroupEntry
}

func (f *fakeShardGroupSource) ShardGroup(id string) (ShardGroupEntry, bool) {
	g, ok := f.groups[id]
	return g, ok
}

func TestLookupForwardTarget_ReturnsFirstPeer(t *testing.T) {
	src := &fakeShardGroupSource{
		groups: map[string]ShardGroupEntry{
			"group-3": {ID: "group-3", PeerIDs: []string{"node-2", "node-4", "node-7"}},
		},
	}
	got, err := lookupForwardTarget(src, "group-3")
	require.NoError(t, err)
	require.Equal(t, "node-2", got)
}

func TestLookupForwardTarget_UnknownGroup(t *testing.T) {
	src := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{}}
	_, err := lookupForwardTarget(src, "group-99")
	require.ErrorIs(t, err, ErrUnknownGroup)
}

func TestLookupForwardTarget_EmptyPeers(t *testing.T) {
	src := &fakeShardGroupSource{
		groups: map[string]ShardGroupEntry{
			"group-0": {ID: "group-0", PeerIDs: []string{}},
		},
	}
	_, err := lookupForwardTarget(src, "group-0")
	require.ErrorIs(t, err, ErrUnknownGroup)
}
