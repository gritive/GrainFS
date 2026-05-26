package serveruntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// TestDEKKeeperWiring verifies that the API contract exercised by the
// production boot path (C2) holds:
//
//   - LoadOrInitKEKStoreDir generates a fresh KEK v0 when keys/ is empty.
//   - NewDEKKeeper succeeds with the active KEK.
//   - SetDEKKeeper injects the keeper into a MetaFSM without error.
//
// This does not spin up meta-raft; the test covers the wiring surface so a
// regression in any of the three calls surfaces here without a full boot
// fixture.
func TestDEKKeeperWiring(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")

	store, err := encrypt.LoadOrInitKEKStoreDir(keysDir)
	require.NoError(t, err, "LoadOrInitKEKStoreDir should succeed on first call (generates v0)")

	kek, err := store.ActiveKEK()
	require.NoError(t, err)
	require.Len(t, kek, encrypt.KEKSize, "KEK must be %d bytes", encrypt.KEKSize)

	keeper, err := encrypt.NewDEKKeeper(kek)
	require.NoError(t, err, "NewDEKKeeper must succeed with a valid KEK")
	require.NotNil(t, keeper)

	fsm := cluster.NewMetaFSM()
	fsm.SetDEKKeeper(keeper)

	gen, wrapped := keeper.Active()
	assert.Equal(t, uint32(0), gen, "initial active generation must be 0")
	assert.NotEmpty(t, wrapped, "wrapped DEK must not be empty")
}

// TestDEKKeeperWiring_LoadIdempotent verifies that a second
// LoadOrInitKEKStoreDir call on an existing keys/ directory returns the
// same active KEK bytes (no overwrite).
func TestDEKKeeperWiring_LoadIdempotent(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")

	store1, err := encrypt.LoadOrInitKEKStoreDir(keysDir)
	require.NoError(t, err)
	kek1, err := store1.ActiveKEK()
	require.NoError(t, err)

	store2, err := encrypt.LoadOrInitKEKStoreDir(keysDir)
	require.NoError(t, err)
	kek2, err := store2.ActiveKEK()
	require.NoError(t, err)

	assert.Equal(t, kek1, kek2, "second load must return identical KEK bytes")
}

// TestWireDEKKeeper_InjectsAndRegistersHook drives the production
// `wireDEKKeeper` function directly. We assert:
//
//  1. `state.dekKeeper` is non-nil after the call (boot state observable).
//  2. `state.kekStore` is non-nil (loaded from <dataDir>/keys/).
//  3. `state.handshakeVerifier` is non-nil and carries cluster.id from
//     <dataDir>/cluster.id.
//  4. The FSM applies a DEKRotate cmd successfully and the generation
//     increments from 0 to 1 — proving the keeper is wired into the apply
//     path (a no-op nil keeper would have stayed at gen 0).
func TestWireDEKKeeper_InjectsAndRegistersHook(t *testing.T) {
	dir := t.TempDir()
	state := &bootState{cfg: Config{DataDir: dir}}
	fsm := cluster.NewMetaFSM()

	require.NoError(t, wireDEKKeeper(state, fsm))
	require.NotNil(t, state.dekKeeper, "wireDEKKeeper must inject keeper into state for downstream phases")
	require.NotNil(t, state.kekStore, "wireDEKKeeper must populate state.kekStore")
	require.NotNil(t, state.handshakeVerifier, "wireDEKKeeper must construct the handshake verifier")
	require.Equal(t, uint32(0), state.kekStore.ActiveVersion(), "Phase A: active KEK version must be 0")

	// cluster.id was persisted.
	_, err := os.Stat(filepath.Join(dir, "cluster.id"))
	require.NoError(t, err, "wireDEKKeeper must persist cluster.id on first boot")

	// Active generation starts at 0 (initial DEK seeded by NewDEKKeeper).
	gen, _ := state.dekKeeper.Active()
	require.Equal(t, uint32(0), gen, "initial DEK generation must be 0")

	// Apply DEKRotate via the FSM apply path. If SetDEKKeeper was not called,
	// applyDEKRotate would error or no-op; the generation would not advance.
	rotateCmd, err := cluster.EncodeMetaCmdForTest(cluster.MetaCmdTypeDEKRotate, nil)
	require.NoError(t, err)
	require.NoError(t, fsm.ApplyCmdForTest(rotateCmd))

	gen2, _ := state.dekKeeper.Active()
	assert.Equal(t, uint32(1), gen2, "DEKRotate apply must advance active generation when keeper is wired")
}
