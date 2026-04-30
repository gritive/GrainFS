package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeShardGroupSource implements ShardGroupSource for unit testing.
type fakeShardGroupSource struct {
	groups map[string]ShardGroupEntry
}

func (f *fakeShardGroupSource) ShardGroup(id string) (ShardGroupEntry, bool) {
	g, ok := f.groups[id]
	return g, ok
}

func (f *fakeShardGroupSource) ShardGroups() []ShardGroupEntry {
	out := make([]ShardGroupEntry, 0, len(f.groups))
	for _, g := range f.groups {
		out = append(out, g)
	}
	return out
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

// TestPeersForForward_SelfLast verifies that self is moved to the END of the
// attempt order — non-self peers are tried first to encourage cross-node load
// distribution. Self only handles the call when every other peer is unreachable.
func TestPeersForForward_SelfLast(t *testing.T) {
	entry := ShardGroupEntry{ID: "g", PeerIDs: []string{"a", "self", "b"}}
	got := PeersForForward(entry, "self")
	require.Equal(t, []string{"a", "b", "self"}, got)
}

// TestPeersForForward_SelfNotInPeers verifies that when this node is NOT a
// member of the group, the original peer order is preserved unchanged.
func TestPeersForForward_SelfNotInPeers(t *testing.T) {
	entry := ShardGroupEntry{ID: "g", PeerIDs: []string{"a", "b", "c"}}
	got := PeersForForward(entry, "self")
	require.Equal(t, []string{"a", "b", "c"}, got)
}

// TestPeersForForward_EmptyPeers verifies the degenerate case — caller will
// observe an empty list and return ErrNoReachablePeer without dialing.
func TestPeersForForward_EmptyPeers(t *testing.T) {
	entry := ShardGroupEntry{ID: "g", PeerIDs: nil}
	got := PeersForForward(entry, "self")
	require.Empty(t, got)
}
