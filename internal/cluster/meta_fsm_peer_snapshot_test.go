package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestMetaFSM_SnapshotRestore_RoundTripsPeers verifies the zero-CA peer
// registry survives Snapshot/Restore (Task 5, codex P1): after log compaction
// / snapshot install the per-node SPKIs must be rebuilt AND onPeersChanged must
// fire so the transport composer rebuilds the accept-set union.
func TestMetaFSM_SnapshotRestore_RoundTripsPeers(t *testing.T) {
	spki1 := [32]byte{1, 2, 3}
	spki2 := [32]byte{4, 5, 6}

	f := NewMetaFSM()
	// Register a member (with PresentsPerNode=true to verify it round-trips) and
	// a pending-learner directly on the registry.
	require.NoError(t, f.peers.registerMember("a", spki1, "addr-a", true))
	require.NoError(t, f.peers.registerPendingLearner("b", spki2, "addr-b"))

	snap, err := f.Snapshot()
	require.NoError(t, err)

	f2 := NewMetaFSM()
	fired := false
	var firedSet [][32]byte
	f2.SetOnPeersChanged(func(s [][32]byte) {
		fired = true
		firedSet = s
	})

	require.NoError(t, f2.Restore(raft.SnapshotMeta{}, snap))

	// Member "a" restored as member with correct SPKI + PresentsPerNode.
	ea, ok := f2.peers.lookupByNodeID("a")
	require.True(t, ok, "node a should be present after restore")
	require.Equal(t, peerStateMember, ea.State)
	require.Equal(t, spki1, ea.SPKI)
	require.Equal(t, "addr-a", ea.Address)
	require.True(t, ea.PresentsPerNode, "PresentsPerNode should round-trip")

	// Pending-learner "b" restored as pendingLearner.
	eb, ok := f2.peers.lookupByNodeID("b")
	require.True(t, ok, "node b should be present after restore")
	require.Equal(t, peerStatePendingLearner, eb.State)
	require.Equal(t, spki2, eb.SPKI)
	require.False(t, eb.PresentsPerNode)

	// SPKI index rebuilt (catches index desync vs byNodeID).
	require.Len(t, f2.peers.acceptSPKIs(), 2)
	owner, ok := f2.peers.spkiOwner(spki1)
	require.True(t, ok)
	require.Equal(t, "a", owner)

	// Restore fired onPeersChanged so the composer rebuilds the union.
	require.True(t, fired, "Restore must fire onPeersChanged")
	require.Len(t, firedSet, 2, "onPeersChanged must receive the full accept-set")
}
