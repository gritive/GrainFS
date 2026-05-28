package serveruntime

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
)

var testRaftStoreClusterID = []byte("cluster-12345678")

func testRaftStoreKEKStore(t *testing.T, gens ...uint32) *encrypt.KEKStore {
	t.Helper()
	store := encrypt.NewKEKStore()
	for _, gen := range gens {
		require.NoError(t, store.Add(gen, bytes.Repeat([]byte{byte(0x30 + gen)}, encrypt.KEKSize)))
	}
	return store
}

func TestRaftStoreKeyLoadOrCreateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store := testRaftStoreKEKStore(t, 3)

	key1, meta1, err := loadOrCreateRaftStoreKey(dir, store, testRaftStoreClusterID, "node-a", false)
	require.NoError(t, err)
	require.Len(t, key1, 32)
	require.Equal(t, uint32(3), meta1.KEKVersion)

	key2, meta2, err := loadOrCreateRaftStoreKey(dir, store, testRaftStoreClusterID, "node-a", false)
	require.NoError(t, err)
	require.Equal(t, key1, key2)
	require.Equal(t, meta1.KEKVersion, meta2.KEKVersion)
	require.Equal(t, meta1.Path, meta2.Path)
}

func TestRaftStoreKeyRejectsWrongNodeContext(t *testing.T) {
	dir := t.TempDir()
	store := testRaftStoreKEKStore(t, 0)

	_, _, err := loadOrCreateRaftStoreKey(dir, store, testRaftStoreClusterID, "node-a", false)
	require.NoError(t, err)

	_, _, err = loadOrCreateRaftStoreKey(dir, store, testRaftStoreClusterID, "node-b", false)
	require.Error(t, err)
}

func TestRaftStoreKeyMissingSidecarRefusesExistingStore(t *testing.T) {
	dir := t.TempDir()
	store := testRaftStoreKEKStore(t, 0)

	_, _, err := loadOrCreateRaftStoreKey(dir, store, testRaftStoreClusterID, "node-a", true)

	require.Error(t, err)
	require.Contains(t, err.Error(), "raft-store.key.enc")
	require.Contains(t, err.Error(), "in-place migration is unsupported")
}

func TestRaftStoreKeyRewrapKeepsPlaintextAndMovesKEKVersion(t *testing.T) {
	dir := t.TempDir()
	store := testRaftStoreKEKStore(t, 1)
	key1, meta1, err := loadOrCreateRaftStoreKey(dir, store, testRaftStoreClusterID, "node-a", false)
	require.NoError(t, err)
	require.Equal(t, uint32(1), meta1.KEKVersion)

	require.NoError(t, store.Add(2, bytes.Repeat([]byte{0x52}, encrypt.KEKSize)))
	rewrapped, err := rewrapRaftStoreKey(dir, store, testRaftStoreClusterID, "node-a")
	require.NoError(t, err)
	require.Equal(t, uint32(2), rewrapped)

	key2, meta2, err := loadOrCreateRaftStoreKey(dir, store, testRaftStoreClusterID, "node-a", false)
	require.NoError(t, err)
	require.Equal(t, key1, key2)
	require.Equal(t, uint32(2), meta2.KEKVersion)
}

func TestRaftStoreKeyRejectsMalformedClusterID(t *testing.T) {
	dir := t.TempDir()
	store := testRaftStoreKEKStore(t, 0)

	_, _, err := loadOrCreateRaftStoreKey(dir, store, []byte("short"), "node-a", false)

	require.Error(t, err)
	require.Contains(t, err.Error(), "cluster_id")
}

func TestRaftStoreKeyWritesPrivateSidecar(t *testing.T) {
	dir := t.TempDir()
	store := testRaftStoreKEKStore(t, 0)
	_, meta, err := loadOrCreateRaftStoreKey(dir, store, testRaftStoreClusterID, "node-a", false)
	require.NoError(t, err)

	info, err := os.Stat(meta.Path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.Equal(t, filepath.Join(dir, "keys.d", "raft-store.key.enc"), meta.Path)
}

func TestBootRaftStoreKeyCreatesSidecar(t *testing.T) {
	dir := t.TempDir()
	state := &bootState{
		cfg:     Config{DataDir: dir},
		raftDir: filepath.Join(dir, "raft"),
		nodeID:  "node-a",
	}

	require.NoError(t, bootRaftStoreKey(state))

	require.Len(t, state.raftStoreKey, 32)
	require.Equal(t, uint32(0), state.raftStoreKeyKEKVer.Load())
	require.NotNil(t, state.kekStore)
	require.Len(t, state.clusterID, 16)
	require.FileExists(t, filepath.Join(dir, "keys.d", "raft-store.key.enc"))
	require.FileExists(t, filepath.Join(dir, "keys", "0.key"))
	require.FileExists(t, filepath.Join(dir, "cluster.id"))
}

func TestBootRaftStoreKeyRefusesExistingRaftStoreWithoutSidecar(t *testing.T) {
	dir := t.TempDir()
	raftDir := filepath.Join(dir, "raft")
	require.NoError(t, os.MkdirAll(filepath.Join(raftDir, "raft-v2"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(raftDir, "raft-v2", "MANIFEST"), []byte("raft"), 0o600))
	state := &bootState{
		cfg:        Config{DataDir: dir},
		raftDir:    raftDir,
		nodeID:     "node-a",
		priorState: true,
	}
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "keys"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "keys", "0.key"), bytes.Repeat([]byte{0x44}, encrypt.KEKSize), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cluster.id"), testRaftStoreClusterID, 0o600))

	err := bootRaftStoreKey(state)

	require.Error(t, err)
	require.Contains(t, err.Error(), "raft-store.key.enc")
	require.Contains(t, err.Error(), "in-place migration is unsupported")
}

func TestBootRaftStoreKeyRefusesExistingMetaRaftStoreWithoutSidecar(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "meta_raft", "raft-v2"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "meta_raft", "raft-v2", "MANIFEST"), []byte("raft"), 0o600))
	state := &bootState{
		cfg:        Config{DataDir: dir},
		raftDir:    filepath.Join(dir, "raft"),
		nodeID:     "node-a",
		priorState: true,
	}
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "keys"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "keys", "0.key"), bytes.Repeat([]byte{0x44}, encrypt.KEKSize), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cluster.id"), testRaftStoreClusterID, 0o600))

	err := bootRaftStoreKey(state)

	require.Error(t, err)
	require.Contains(t, err.Error(), "raft-store.key.enc")
	require.Contains(t, err.Error(), "in-place migration is unsupported")
}

func TestRaftStoreKeyPruneRefRejectsReferencedKEK(t *testing.T) {
	state := &bootState{}
	state.raftStoreKeyKEKVer.Store(1)

	err := checkRaftStoreKeyPruneRef(state, 1)

	require.Error(t, err)
	require.Contains(t, err.Error(), "raft-store key is still sealed")
	require.NoError(t, checkRaftStoreKeyPruneRef(state, 2))
}

func TestRaftStoreKeyPostCommitRewrapsAfterKEKRotate(t *testing.T) {
	dir := t.TempDir()
	store := testRaftStoreKEKStore(t, 1)
	_, meta, err := loadOrCreateRaftStoreKey(dir, store, testRaftStoreClusterID, "node-a", false)
	require.NoError(t, err)
	require.Equal(t, uint32(1), meta.KEKVersion)
	require.NoError(t, store.Add(2, bytes.Repeat([]byte{0x52}, encrypt.KEKSize)))
	state := &bootState{
		cfg:       Config{DataDir: dir},
		kekStore:  store,
		clusterID: testRaftStoreClusterID,
		nodeID:    "node-a",
	}
	state.raftStoreKeyKEKVer.Store(1)

	raftStoreKeyPostCommitDispatcher{state: state}.Handle(clusterpb.MetaCmdTypeKEKRotate, nil)

	waitFor(t, 500*time.Millisecond, func() bool { return state.raftStoreKeyKEKVer.Load() == 2 })
	_, meta, err = loadOrCreateRaftStoreKey(dir, store, testRaftStoreClusterID, "node-a", false)
	require.NoError(t, err)
	require.Equal(t, uint32(2), meta.KEKVersion)
}
