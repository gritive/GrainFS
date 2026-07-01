package cluster

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
)

// wireSnapshotKEK gives an FSM the active KEK that Snapshot/Restore's encryption
// envelope (master's sealSnapshotEnvelope) requires. Both the snapshotting and
// restoring FSM must share the same KEK + (zero) ClusterID for the AAD to match.
func wireSnapshotKEK(t *testing.T, f *MetaFSM) {
	t.Helper()
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(0, bytes.Repeat([]byte{0x5A}, encrypt.KEKSize)))
	f.SetKEKStore(store)
}

// TestMetaFSM_SnapshotRestore_RoundTripsPeers verifies the zero-CA peer
// registry survives Snapshot/Restore (Task 5, codex P1): after log compaction
// / snapshot install the per-node SPKIs must be rebuilt AND onPeersChanged must
// fire so the transport composer rebuilds the accept-set union.
func TestMetaFSM_SnapshotRestore_RoundTripsPeers(t *testing.T) {
	spki1 := [32]byte{1, 2, 3}
	spki2 := [32]byte{4, 5, 6}

	f := NewMetaFSM()
	wireSnapshotKEK(t, f)
	// Register a member (with PresentsPerNode=true and NodeKeyKEKGen set to
	// verify both round-trip) and a pending-learner directly on the registry.
	require.NoError(t, f.peers.registerMember("a", spki1, "addr-a", true, 7))
	require.NoError(t, f.peers.registerPendingLearner("b", spki2, "addr-b"))

	snap, err := f.Snapshot()
	require.NoError(t, err)

	f2 := NewMetaFSM()
	wireSnapshotKEK(t, f2)
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
	require.Equal(t, uint32(7), ea.NodeKeyKEKGen, "NodeKeyKEKGen should round-trip")

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

// TestMetaFSM_SnapshotRestore_CorruptPeers_NoPartialMutation verifies the
// Finding-B fix: a Restore whose peer vector is corrupt (duplicate SPKI) must
// fail BEFORE committing any core FSM state, so the target FSM is left
// un-restored (the meta-raft invariant). Previously peer validation ran AFTER
// f.nodes/shardGroups/objectIndex were swapped in, leaving partial mutation.
func TestMetaFSM_SnapshotRestore_CorruptPeers_NoPartialMutation(t *testing.T) {
	dupSPKI := [32]byte{9, 9, 9}

	// Source FSM: inject TWO peers sharing one SPKI directly into the registry
	// maps (registerMember rejects duplicate SPKI, so we bypass it to forge a
	// corrupt snapshot). Snapshot.export() serializes both.
	f := NewMetaFSM()
	wireSnapshotKEK(t, f)
	f.peers.byNodeID["a"] = peerEntry{NodeID: "a", SPKI: dupSPKI, Address: "addr-a", State: peerStateMember}
	f.peers.byNodeID["b"] = peerEntry{NodeID: "b", SPKI: dupSPKI, Address: "addr-b", State: peerStateMember}
	f.peers.bySPKI[dupSPKI] = "a"

	snap, err := f.Snapshot()
	require.NoError(t, err)

	// Target FSM: pre-populate a SENTINEL f.nodes so we can assert it is
	// UNCHANGED after the failed Restore (no partial mutation of core state).
	f2 := NewMetaFSM()
	wireSnapshotKEK(t, f2)
	sentinel := MetaNodeEntry{ID: "preexisting", Address: "10.0.0.1:7000"}
	f2.nodes = map[string]MetaNodeEntry{"preexisting": sentinel}

	err = f2.Restore(raft.SnapshotMeta{}, snap)
	require.Error(t, err, "Restore with duplicate-SPKI peers must fail")
	require.Contains(t, err.Error(), "duplicate SPKI")

	// Core FSM state must be UNCHANGED — the failed Restore left it un-restored.
	require.Len(t, f2.nodes, 1, "f.nodes must be untouched after failed Restore")
	got, ok := f2.nodes["preexisting"]
	require.True(t, ok, "sentinel node must still be present")
	require.Equal(t, sentinel, got)

	// Peer registry must also be untouched (commit never ran).
	_, ok = f2.peers.lookupByNodeID("a")
	require.False(t, ok, "peer registry must not have been mutated")
}
