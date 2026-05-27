package serveruntime

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
)

// fsmWithWrappedDEKs returns a fresh MetaFSM whose pendingDEKVersions has been
// populated by a Restore from a snapshot taken on a sibling FSM that held the
// given KEK. This mirrors the production sequence: snapshot on the leader, ship
// the snapshot to a peer, peer Restores, pendingDEKVersions is set. The runtime
// then has to unwrap those bytes with the local kek.key — which is exactly the
// failure surface §7 T57 covers.
// restoreTestClusterID returns the deterministic 16-byte clusterID used by the
// restore fixtures. DEK wraps are AAD-bound to (clusterID, gen, kekVer), so the
// same clusterID must be staged on disk (cluster.id) for the restore path to
// reconstruct the AAD.
func restoreTestClusterID() []byte {
	id := make([]byte, 16)
	for i := range id {
		id[i] = byte(i + 1)
	}
	return id
}

func fsmWithWrappedDEKs(t *testing.T, kek []byte) *cluster.MetaFSM {
	t.Helper()

	// Build wraps via the replication primitives so the wrap AAD is bound to
	// (clusterID, gen, kekVer=0) — consistent with what the restore path
	// reconstructs. Two generations confirm LoadFromFSM exercises >1 unwrap.
	keeper, err := encrypt.NewEmptyDEKKeeper(kek, restoreTestClusterID())
	require.NoError(t, err, "NewEmptyDEKKeeper")
	for gen := uint32(0); gen <= 1; gen++ {
		w, kv, err := keeper.GenerateWrappedDEK(gen)
		require.NoError(t, err, "GenerateWrappedDEK")
		require.NoError(t, keeper.InstallReplicatedDEK(gen, w, kv), "InstallReplicatedDEK")
	}

	src := cluster.NewMetaFSM()
	src.SetDEKKeeper(keeper)

	snap, err := src.Snapshot()
	require.NoError(t, err, "Snapshot")

	dst := cluster.NewMetaFSM()
	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, snap), "Restore")

	pending, _ := dst.PendingDEKVersions()
	require.NotEmpty(t, pending, "pendingDEKVersions must be populated by Restore")
	return dst
}

// writeKEKFile stages keys/0.key under dataDir using the new KEKStore
// disk layout. Returns the path written.
func writeKEKFile(t *testing.T, dir string, kek []byte) string {
	t.Helper()
	keysDir := filepath.Join(dir, "keys")
	require.NoError(t, os.MkdirAll(keysDir, 0o700))
	path := filepath.Join(keysDir, "0.key")
	require.NoError(t, os.WriteFile(path, kek, 0o600))
	return path
}

// writeClusterID stages cluster.id under dataDir, simulating an operator
// scp'ing it from a healthy peer alongside the active KEK. Join-mode
// wiring uses strict LoadClusterID, so this file must be present for
// wireDEKKeeper to succeed.
func writeClusterID(t *testing.T, dir string, id []byte) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.Len(t, id, 16)
	path := filepath.Join(dir, "cluster.id")
	require.NoError(t, os.WriteFile(path, id, 0o600))
	return path
}

// TestStartup_RefusesWhenKEKMissing covers F#21: the FSM holds wrapped DEKs
// (because the snapshot trailer carried them) but the node's kek.key file is
// gone. The runtime must refuse to start with an explicit remediation message
// rather than silently auto-generating a fresh KEK that can never unwrap the
// cluster's DEKs.
func TestStartup_RefusesWhenKEKMissing(t *testing.T) {
	dataDir := t.TempDir()
	state := &bootState{cfg: Config{DataDir: dataDir}}

	kek := make([]byte, encrypt.KEKSize)
	_, err := rand.Read(kek)
	require.NoError(t, err)
	fsm := fsmWithWrappedDEKs(t, kek)

	// Deliberately do NOT stage <dataDir>/keys/0.key. Phase A always loads
	// the KEK from <dataDir>/keys/<V>.key (no env override) so the keystore
	// is absent here.

	err = rebuildDEKKeeperFromRestore(state, fsm)
	require.Error(t, err, "missing KEK with FSM-wrapped DEKs must refuse startup")
	assert.Contains(t, err.Error(), "KEK not found",
		"error must include the literal 'KEK not found' for operator grep-ability")
	// Remediation hints from spec.
	assert.Contains(t, err.Error(), "scp", "error must point at the scp-from-peer remediation")
	assert.Contains(t, err.Error(), "decommission", "error must offer the decommission-and-rejoin option")
	assert.Nil(t, state.dekKeeper, "no keeper installed when refusal fires")
}

// TestStartup_RefusesWhenKEKDoesNotDecryptDEK covers F#22: a kek.key file is
// present on disk but it is not the cluster's KEK (typically the operator
// rotated it out from under the node). LoadFromFSM's AEAD-open fails; the
// runtime must wrap that failure with a "rotated KEK / scp from peer" message.
func TestStartup_RefusesWhenKEKDoesNotDecryptDEK(t *testing.T) {
	dataDir := t.TempDir()
	state := &bootState{cfg: Config{DataDir: dataDir}}

	clusterKEK := make([]byte, encrypt.KEKSize)
	_, err := rand.Read(clusterKEK)
	require.NoError(t, err)
	fsm := fsmWithWrappedDEKs(t, clusterKEK)

	// Write an UNMATCHED KEK to the node's kek.key — the file exists (so the
	// missing-KEK branch is not taken) but its bytes do not decrypt the FSM's
	// wrapped DEKs.
	wrongKEK := make([]byte, encrypt.KEKSize)
	_, err = rand.Read(wrongKEK)
	require.NoError(t, err)
	require.NotEqual(t, clusterKEK, wrongKEK, "test fixture must use distinct KEK bytes")
	writeKEKFile(t, dataDir, wrongKEK)
	// cluster.id must be present so the path reaches the DEK unwrap (the
	// failure under test) rather than erroring earlier on a missing cluster.id.
	writeClusterID(t, dataDir, restoreTestClusterID())

	err = rebuildDEKKeeperFromRestore(state, fsm)
	require.Error(t, err, "wrong KEK with FSM-wrapped DEKs must refuse startup")
	assert.Contains(t, err.Error(), "decrypt",
		"error must include the literal 'decrypt' so operators can grep the cause")
	assert.Contains(t, err.Error(), "rotated", "error must point at the KEK-rotated diagnosis")
	assert.Contains(t, err.Error(), "scp", "error must point at the scp-from-peer remediation")
	assert.Nil(t, state.dekKeeper, "no keeper installed when refusal fires")
}

// TestStartup_NoOpWhenSnapshotHasNoWrappedDEKs covers the first-boot path:
// no DKVS trailer in the snapshot (or no snapshot at all) means
// PendingDEKVersions is empty and the fresh keeper from wireDEKKeeper stays in
// place. The post-Restore phase must not require kek.key or touch state.
func TestStartup_NoOpWhenSnapshotHasNoWrappedDEKs(t *testing.T) {
	dataDir := t.TempDir()
	state := &bootState{cfg: Config{DataDir: dataDir}}
	fsm := cluster.NewMetaFSM() // no Restore → pendingDEKVersions nil

	// No kek.key written, no env var — the no-op branch must NOT touch the
	// filesystem, so this still succeeds.
	require.NoError(t, rebuildDEKKeeperFromRestore(state, fsm))
	assert.Nil(t, state.dekKeeper, "no-op branch must leave state untouched")
}

// fsmWithWrappedDEKsAtKEKVersion builds a snapshot whose DKVS trailer records
// activeKEKVersion=kekVersion. The wrapped DEKs are sealed under kek. The
// returned FSM is the restore target; callers populate state.kekStore manually.
func fsmWithWrappedDEKsAtKEKVersion(t *testing.T, kek []byte, kekVersion uint32) *cluster.MetaFSM {
	t.Helper()

	// Seal both gens under kekVersion so the wrap AAD matches the version the
	// snapshot records (production: InstallKEKRotation re-seals on rotation).
	keeper, err := encrypt.NewEmptyDEKKeeper(kek, restoreTestClusterID())
	require.NoError(t, err)
	keeper.SetActiveKEKVersion(kekVersion)
	for gen := uint32(0); gen <= 1; gen++ {
		w, kv, err := keeper.GenerateWrappedDEK(gen)
		require.NoError(t, err)
		require.Equal(t, kekVersion, kv, "GenerateWrappedDEK must seal under the labeled KEK version")
		require.NoError(t, keeper.InstallReplicatedDEK(gen, w, kv))
	}

	src := cluster.NewMetaFSM()
	src.SetDEKKeeper(keeper)
	src.SetActiveKEKVersion(kekVersion)

	snap, err := src.Snapshot()
	require.NoError(t, err)

	dst := cluster.NewMetaFSM()
	require.NoError(t, dst.Restore(raft.SnapshotMeta{}, snap))

	pending, _ := dst.PendingDEKVersions()
	require.NotEmpty(t, pending, "pendingDEKVersions must be populated by Restore")
	return dst
}

// TestRestore_UsesSnapshotActiveKEKVersion_NotKeystoreActive is the Task 4c
// regression guard (codex Pass-9 H2). It verifies that rebuildDEKKeeperFromRestore
// uses the snapshot-captured active_kek_version to select the unwrap KEK, not
// the keystore's current active version.
//
// Scenario: cluster rotated KEK from V=1 to V=2. A raft snapshot was taken
// when active was V=1 (wraps sealed under K_1). The keystore now has V=2 active.
// Restore on a lagging node must succeed by loading K_1 (from the snapshot field),
// NOT K_2 (the current keystore active) which would fail AES-GCM open.
func TestRestore_UsesSnapshotActiveKEKVersion_NotKeystoreActive(t *testing.T) {
	k1 := make([]byte, encrypt.KEKSize)
	k2 := make([]byte, encrypt.KEKSize)
	_, err := rand.Read(k1)
	require.NoError(t, err)
	_, err = rand.Read(k2)
	require.NoError(t, err)
	require.NotEqual(t, k1, k2, "test fixture must use distinct KEK bytes")

	// Build source FSM: DEKs wrapped under K_1, activeKEKVersion=1.
	fsm := fsmWithWrappedDEKsAtKEKVersion(t, k1, 1)

	// Verify snapshot captured KEK version 1.
	require.Equal(t, uint32(1), fsm.SnapshotCapturedKEKVersion(),
		"SnapshotCapturedKEKVersion must return the value from the DKVS trailer")

	// Build a KEKStore with both V=1 and V=2; current active is V=2.
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(1, k1))
	require.NoError(t, store.Add(2, k2))
	require.NoError(t, store.SetActiveVersion(2))
	require.Equal(t, uint32(2), store.ActiveVersion(), "keystore active must be V=2")

	// rebuildDEKKeeperFromRestore must use K_1 (snapshot-captured), not K_2 (current active).
	dataDir := t.TempDir()
	writeClusterID(t, dataDir, restoreTestClusterID())
	state := &bootState{cfg: Config{DataDir: dataDir}, kekStore: store}
	require.NoError(t, rebuildDEKKeeperFromRestore(state, fsm),
		"must succeed by selecting K_1 per snapshot active_kek_version=1")
	require.NotNil(t, state.dekKeeper,
		"dekKeeper must be installed after successful restore")

	// Confirm the keeper was built with K_1: the active DEK gen must unseal
	// under the AAD bound to (clusterID, gen, kekVer=1).
	gen, wrapped := state.dekKeeper.Active()
	require.Equal(t, uint32(1), gen, "keeper active gen must be 1 (last gen)")
	kekForVerify, err := store.Get(1)
	require.NoError(t, err)
	aad := encrypt.BuildAAD(encrypt.DomainDEKFSMWrap, restoreTestClusterID(), encrypt.FieldUint32(gen), encrypt.FieldUint32(1))
	_, err = encrypt.AESGCMOpenWithAAD(kekForVerify, wrapped, aad)
	require.NoError(t, err, "active DEK wrap must unseal under K_1")
}

// TestRestore_FallsBackToKeystoreActiveWhenSnapshotVersionIsZero guards
// backward compat: Phase A snapshots record active_kek_version=0 (the
// proto default). The restore path must treat zero the same as before —
// Get(0) or ActiveKEK() (both return the same key when V=0 is active).
func TestRestore_FallsBackToKeystoreActiveWhenSnapshotVersionIsZero(t *testing.T) {
	k0 := make([]byte, encrypt.KEKSize)
	_, err := rand.Read(k0)
	require.NoError(t, err)

	// Phase-A-style FSM: activeKEKVersion not set → 0 default.
	fsm := fsmWithWrappedDEKsAtKEKVersion(t, k0, 0)
	require.Equal(t, uint32(0), fsm.SnapshotCapturedKEKVersion())

	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(0, k0))
	// Active stays 0 by default.

	dataDir := t.TempDir()
	writeClusterID(t, dataDir, restoreTestClusterID())
	state := &bootState{cfg: Config{DataDir: dataDir}, kekStore: store}
	require.NoError(t, rebuildDEKKeeperFromRestore(state, fsm))
	require.NotNil(t, state.dekKeeper)
}
