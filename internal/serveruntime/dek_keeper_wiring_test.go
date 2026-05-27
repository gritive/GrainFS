package serveruntime

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
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

	keeper, err := encrypt.NewDEKKeeper(kek, restoreTestClusterID())
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

	// Apply a Phase D replicated DEK rotate via the FSM apply path. Legacy
	// type-48 is now a deterministic no-op, so the wiring is proven by the
	// replicated path instead: the leader-generated wrapped bytes come from the
	// SAME keeper (state.dekKeeper); the FSM advances ONLY if SetDEKKeeper wired
	// that exact keeper into the apply path. A different/absent keeper would
	// leave state.dekKeeper at gen 0.
	wrapped, kekVer, err := state.dekKeeper.GenerateWrappedDEK(1)
	require.NoError(t, err)
	enc, err := cluster.EncodeDEKReplicatedRotateCmd(cluster.DEKReplicatedRotateCmd{
		Gen: 1, WrappedDEK: wrapped, ExpectedActiveGen: 0, ActiveKEKVer: kekVer,
	})
	require.NoError(t, err)
	rotateCmd, err := cluster.EncodeMetaCmdForTest(cluster.MetaCmdTypeDEKReplicatedRotate, enc)
	require.NoError(t, err)
	require.NoError(t, fsm.ApplyCmdForTest(rotateCmd))

	gen2, _ := state.dekKeeper.Active()
	assert.Equal(t, uint32(1), gen2, "replicated DEK rotate must advance active generation when keeper is wired")
}

// TestWireDEKKeeper_RestartRefusesMissingKEK is the P3-H1 regression
// guard. Simulates a restart of an existing node (priorState=true was
// captured by bootValidateConfig) where the operator accidentally
// deleted keys/0.key. The strict-restart path must refuse to
// auto-generate a fresh KEK — otherwise raft Restore would unwrap
// FSM-stored DEKs with the wrong key (silent corruption).
func TestWireDEKKeeper_RestartRefusesMissingKEK(t *testing.T) {
	dir := t.TempDir()
	// DELIBERATELY do not stage keys/0.key.
	// Stage cluster.id so this test isolates the KEK refusal.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cluster.id"), bytes.Repeat([]byte{0x42}, 16), 0o600))

	// priorState=true mirrors bootValidateConfig observing existing
	// raft/meta content from a prior boot.
	state := &bootState{cfg: Config{DataDir: dir}, priorState: true}
	fsm := cluster.NewMetaFSM()

	err := wireDEKKeeper(state, fsm)
	require.Error(t, err, "wireDEKKeeper must refuse to auto-generate KEK in restart mode")
	// Verify keys/0.key was NOT created.
	_, statErr := os.Stat(filepath.Join(dir, "keys", "0.key"))
	require.Error(t, statErr, "keys/0.key was auto-created in restart mode")
}

// TestWireDEKKeeper_RestartRefusesMissingClusterID guards against
// silent cluster.id drift on restart. priorState=true + a valid KEK
// present, but cluster.id missing — strict load must surface
// ErrClusterIDMissing rather than minting a fresh UUID.
func TestWireDEKKeeper_RestartRefusesMissingClusterID(t *testing.T) {
	dir := t.TempDir()
	// KEK staged, cluster.id missing.
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "keys"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "keys", "0.key"),
		bytes.Repeat([]byte{0x55}, encrypt.KEKSize), 0o600))

	state := &bootState{cfg: Config{DataDir: dir}, priorState: true}
	fsm := cluster.NewMetaFSM()

	err := wireDEKKeeper(state, fsm)
	require.Error(t, err, "wireDEKKeeper must refuse to auto-generate cluster.id in restart mode")
	// Verify cluster.id was NOT created.
	_, statErr := os.Stat(filepath.Join(dir, "cluster.id"))
	require.Error(t, statErr, "cluster.id was auto-created in restart mode")
}

// TestWireDEKKeeper_RefusesLegacyKEKSourceEnv pins the P6-M1 fix: an
// operator setting GRAINFS_KEK_SOURCE expecting an override must get a
// clear refusal, not silent ignore. The env var was honored by the old
// NodeConfig.KEKSource() helper but no production code path reads it
// after the KEKStore migration.
func TestWireDEKKeeper_RefusesLegacyKEKSourceEnv(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GRAINFS_KEK_SOURCE", "file:///etc/grainfs/kek.key")

	state := &bootState{cfg: Config{DataDir: dir}}
	fsm := cluster.NewMetaFSM()
	err := wireDEKKeeper(state, fsm)
	if err == nil {
		t.Fatal("wireDEKKeeper accepted GRAINFS_KEK_SOURCE in env")
	}
	if !strings.Contains(err.Error(), "GRAINFS_KEK_SOURCE") {
		t.Errorf("error does not mention GRAINFS_KEK_SOURCE: %v", err)
	}
}

// TestWireDEKKeeper_NonGenesisStartsEmpty pins Task 5: a joining node
// (joinMode → not genesis) MUST start with an EMPTY DEK keeper. No local
// gen-0 is generated; gen-0 arrives via log replay / snapshot restore after
// the node joins. Seal must fail until a generation is installed.
func TestWireDEKKeeper_NonGenesisStartsEmpty(t *testing.T) {
	dir := t.TempDir()
	// Joiner must already hold the cluster KEK + cluster.id before serve
	// (wireDEKKeeper refuses auto-gen in join mode), so stage both.
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "keys"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "keys", "0.key"),
		bytes.Repeat([]byte{0x55}, encrypt.KEKSize), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cluster.id"),
		bytes.Repeat([]byte{0x42}, 16), 0o600))

	state := &bootState{cfg: Config{DataDir: dir}, joinMode: true}
	fsm := cluster.NewMetaFSM()
	require.NoError(t, wireDEKKeeper(state, fsm))

	_, _, err := state.dekKeeper.Seal([]byte("x"))
	require.Error(t, err, "non-genesis node must start with empty keeper (no local gen-0)")
}

// TestWireDEKKeeper_GenesisGeneratesGen0 pins Task 5: a fresh single-voter
// bootstrap (!joinMode, no peers, no priorState) IS genesis and generates a
// random gen-0 locally. Seal must succeed immediately.
func TestWireDEKKeeper_GenesisGeneratesGen0(t *testing.T) {
	dir := t.TempDir()
	state := &bootState{cfg: Config{DataDir: dir}}
	fsm := cluster.NewMetaFSM()
	require.NoError(t, wireDEKKeeper(state, fsm))

	_, _, err := state.dekKeeper.Seal([]byte("x"))
	require.NoError(t, err, "genesis node must have gen-0")
}

// TestWireDEKKeeper_SetsFSMClusterID is a production-path regression guard
// (HIGH 1 / Pass 3): wireDEKKeeper MUST bind the persisted cluster.id into the
// FSM so the KEK-rewrap AAD matches the DEK keeper's AAD. Without SetClusterID
// the FSM returns all-zero and the first KEK rotation over AAD-bound DEKs
// fails auth.
func TestWireDEKKeeper_SetsFSMClusterID(t *testing.T) {
	dir := t.TempDir()
	state := &bootState{cfg: Config{DataDir: dir}}
	fsm := cluster.NewMetaFSM()
	require.NoError(t, wireDEKKeeper(state, fsm))

	persisted, err := os.ReadFile(filepath.Join(dir, "cluster.id"))
	require.NoError(t, err, "wireDEKKeeper must persist cluster.id on first boot")
	got := fsm.ClusterID()
	require.NotEqual(t, [16]byte{}, got, "wireDEKKeeper must SetClusterID on the FSM (got all-zero)")
	require.True(t, bytes.Equal(got[:], persisted), "FSM clusterID %x != persisted cluster.id %x", got[:], persisted)
}

// TestWireDEKKeeper_StoresClusterID pins slice C: wireDEKKeeper must store the
// loaded 16-byte cluster.id on bootState.clusterID so the data-plane WRITE
// (putpipeline) and READ (ShardService) sides bind the identical clusterID.
func TestWireDEKKeeper_StoresClusterID(t *testing.T) {
	dir := t.TempDir()
	state := &bootState{cfg: Config{DataDir: dir}}
	fsm := cluster.NewMetaFSM()

	if err := wireDEKKeeper(state, fsm); err != nil {
		t.Fatalf("wireDEKKeeper: %v", err)
	}
	if len(state.clusterID) != 16 {
		t.Fatalf("expected state.clusterID 16 bytes, got %d", len(state.clusterID))
	}
}

// TestWireDEKKeeper_FreshBootStillAutoGenerates pins the fresh-boot
// behavior unchanged by the P3-H1 fix: priorState=false, no join, no
// peers → keys/0.key and cluster.id auto-generate.
func TestWireDEKKeeper_FreshBootStillAutoGenerates(t *testing.T) {
	dir := t.TempDir()
	// priorState=false (default) — pure fresh boot.
	state := &bootState{cfg: Config{DataDir: dir}}
	fsm := cluster.NewMetaFSM()

	require.NoError(t, wireDEKKeeper(state, fsm))
	// keys/0.key and cluster.id should be auto-generated.
	_, err := os.Stat(filepath.Join(dir, "keys", "0.key"))
	require.NoError(t, err, "fresh boot did not generate keys/0.key")
	_, err = os.Stat(filepath.Join(dir, "cluster.id"))
	require.NoError(t, err, "fresh boot did not generate cluster.id")
}
