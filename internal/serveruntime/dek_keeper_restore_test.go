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
func fsmWithWrappedDEKs(t *testing.T, kek []byte) *cluster.MetaFSM {
	t.Helper()

	keeper, err := encrypt.NewDEKKeeper(kek)
	require.NoError(t, err, "NewDEKKeeper")
	// Rotate once so we have multiple wrapped generations in the trailer —
	// confirms the LoadFromFSM path exercises >1 unwrap.
	require.NoError(t, keeper.Rotate(), "Rotate")

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

	// Deliberately do NOT write kek.key under dataDir. GRAINFS_KEK_SOURCE is
	// also unset (test environment) so KEKSource() defaults to dataDir/kek.key,
	// which does not exist.

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
