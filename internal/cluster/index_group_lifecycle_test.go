package cluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// TestInstantiateLocalIndexGroup_SoloRoundTrip boots a SOLO (single-node) index
// group via the lifecycle constructor, starts it, round-trips a PUT through raft,
// reads it back, and closes cleanly.
func TestInstantiateLocalIndexGroup_SoloRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db := openTestFSMStore(t, dir)

	g, err := instantiateLocalIndexGroup(IndexGroupLifecycleConfig{
		NodeID:   "n1",
		DataDir:  dir,
		FSMStore: db,
		// Deterministic K0 active KEK — mirrors wireTestKEK's store, the minimum
		// the index-group MetaFSM needs to seal/open its raft-snapshot envelope.
		KEKStore: newTestKEKStore(t, bytes.Repeat([]byte{0xA0}, encrypt.KEKSize)),
	}, IndexGroupEntry{ID: "index-0", PeerIDs: []string{"n1"}})
	require.NoError(t, err)
	require.NoError(t, g.Start(context.Background()))
	defer g.Close()

	// Solo node must elect itself leader before a local propose can succeed
	// (mirrors startSoloIndexGroup's wait in index_group_raft_test.go).
	require.Eventually(t, g.node.IsLeader, 5*time.Second, 20*time.Millisecond,
		"solo index group should elect itself leader")

	ctx := context.Background()
	entry := ObjectIndexEntry{Bucket: "b", Key: "k", VersionID: "v1", PlacementGroupID: "index-0"}
	require.NoError(t, g.ProposeObjectIndex(ctx, entry, false))

	got, ok := g.ObjectIndexLatest("b", "k")
	require.True(t, ok)
	require.Equal(t, "v1", got.VersionID)
}
