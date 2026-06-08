package cluster

import (
	"testing"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/stretchr/testify/require"
)

func TestIndexGroupRegistry_ApplyAndSeparation(t *testing.T) {
	f := NewMetaFSM()
	entry := IndexGroupEntry{ID: "index-0", PeerIDs: []string{"n1", "n2", "n3"}}
	data, err := encodeMetaIndexGroupCmd(entry) // returns a full PutIndexGroup MetaCmd byte slice
	require.NoError(t, err)
	require.NoError(t, f.applyPutIndexGroup(extractIndexGroupPayload(data)))
	got := f.IndexGroups()
	require.Len(t, got, 1)
	require.Equal(t, "index-0", got[0].ID)
	require.ElementsMatch(t, []string{"n1", "n2", "n3"}, got[0].PeerIDs)
	require.Empty(t, f.ShardGroups()) // separation invariant: index entries never in data-group registry
}

func TestIndexGroupRegistry_SnapshotRoundTrip(t *testing.T) {
	f := NewMetaFSM()
	wireTestKEK(t, f)
	for _, id := range []string{"index-0", "index-1"} {
		data, err := encodeMetaIndexGroupCmd(IndexGroupEntry{ID: id, PeerIDs: []string{"n1"}})
		require.NoError(t, err)
		require.NoError(t, f.applyPutIndexGroup(extractIndexGroupPayload(data)))
	}
	snap, err := f.Snapshot()
	require.NoError(t, err)
	f2 := NewMetaFSM()
	wireTestKEK(t, f2)
	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))
	ids := make([]string, 0)
	for _, g := range f2.IndexGroups() {
		ids = append(ids, g.ID)
	}
	require.ElementsMatch(t, []string{"index-0", "index-1"}, ids)
}
