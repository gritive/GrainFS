package serveruntime

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/transport"
)

const (
	testNIClusterID = "0011223344556677"
	testNINodeID    = "node-a"
)

// testNIEncKey returns a distinct 32-byte static encryption.key for tests.
func testNIEncKey() []byte {
	return bytes.Repeat([]byte{0xAB}, 32)
}

// newNIKEKStore builds a KEKStore with the given gens loaded; gen i gets KEK
// bytes filled with byte (0x10 + i) so each gen is distinct.
func newNIKEKStore(t *testing.T, gens ...uint32) *encrypt.KEKStore {
	t.Helper()
	s := encrypt.NewKEKStore()
	for _, g := range gens {
		kek := bytes.Repeat([]byte{byte(0x10 + g)}, encrypt.KEKSize)
		require.NoError(t, s.Add(g, kek), "KEKStore.Add(%d)", g)
	}
	return s
}

func nodeKeyEncPath(dataDir string) string {
	return filepath.Join(dataDir, "keys.d", "node.key.enc")
}

func nodeKeyGenPath(dataDir string) string {
	return filepath.Join(dataDir, "keys.d", "node.key.gen")
}

func TestEnsureNodeIdentity_GeneratesAndPersistsWhenAbsent(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()
	store := newNIKEKStore(t, 0, 1, 2)

	_, spki, gotGen, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store)
	require.NoError(t, err)
	require.Equal(t, store.ActiveVersion(), gotGen)
	require.NotEqual(t, [32]byte{}, spki, "expected non-zero SPKI")
	require.FileExists(t, nodeKeyEncPath(dir))
	gen, ok := readNodeKeyGen(dir)
	require.True(t, ok, "node.key.gen not written")
	require.Equal(t, store.ActiveVersion(), gen)
	activeKEK, err := store.ActiveKEK()
	require.NoError(t, err)
	_, reloaded, err := transport.LoadNodeKey(dir, activeKEK)
	require.NoError(t, err)
	require.Equal(t, spki, reloaded)
	_, _, err = transport.LoadNodeKey(dir, encKey)
	require.Error(t, err, "node.key.enc unexpectedly decrypts under static encryption key")
}

func TestEnsureNodeIdentity_GeneratesWithoutStaticEncryptionKey(t *testing.T) {
	dir := t.TempDir()
	store := newNIKEKStore(t, 0, 1, 2)

	_, spki, gotGen, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, nil, store)
	require.NoError(t, err)
	require.NotEqual(t, [32]byte{}, spki, "expected non-zero SPKI")
	require.Equal(t, store.ActiveVersion(), gotGen)
	require.FileExists(t, nodeKeyEncPath(dir))
	require.NoFileExists(t, filepath.Join(dir, "encryption.key"))

	activeKEK, err := store.ActiveKEK()
	require.NoError(t, err)
	_, reloaded, err := transport.LoadNodeKey(dir, activeKEK)
	require.NoError(t, err)
	require.Equal(t, spki, reloaded)
}

func TestEnsureNodeIdentity_ReloadsKEKSealedKeyWithoutStaticEncryptionKey(t *testing.T) {
	dir := t.TempDir()
	store := newNIKEKStore(t, 0, 1, 2)

	_, first, firstGen, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, nil, store)
	require.NoError(t, err)
	before, err := os.ReadFile(nodeKeyEncPath(dir))
	require.NoError(t, err)

	_, second, secondGen, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, nil, store)
	require.NoError(t, err)
	after, err := os.ReadFile(nodeKeyEncPath(dir))
	require.NoError(t, err)

	require.Equal(t, first, second)
	require.Equal(t, firstGen, secondGen)
	require.Equal(t, before, after)
	require.NoFileExists(t, filepath.Join(dir, "encryption.key"))
}

func TestEnsureNodeIdentity_ReusesPersisted(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()
	store := newNIKEKStore(t, 0, 1, 2)

	_, first, _, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store)
	require.NoError(t, err)
	before, err := os.ReadFile(nodeKeyEncPath(dir))
	require.NoError(t, err)
	_, second, _, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store)
	require.NoError(t, err)
	require.Equal(t, first, second)
	after, err := os.ReadFile(nodeKeyEncPath(dir))
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestEnsureNodeIdentity_ReSealsOlderKEKGenToActive(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()
	store := newNIKEKStore(t, 0, 1, 2, 3)
	kek2, err := store.Get(2)
	require.NoError(t, err)
	cert, want, err := transport.GenerateNodeIdentity(testNIClusterID, testNINodeID)
	require.NoError(t, err)
	require.NoError(t, transport.SealNodeKey(dir, kek2, cert))
	require.NoError(t, writeNodeKeyGen(dir, 2))

	_, got, gotGen, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store)
	require.NoError(t, err)
	require.Equal(t, store.ActiveVersion(), gotGen)
	require.Equal(t, want, got)
	activeKEK, err := store.ActiveKEK()
	require.NoError(t, err)
	_, migrated, err := transport.LoadNodeKey(dir, activeKEK)
	require.NoError(t, err)
	require.Equal(t, want, migrated)
	gen, ok := readNodeKeyGen(dir)
	require.True(t, ok, "node.key.gen not written after active re-seal")
	require.Equal(t, store.ActiveVersion(), gen)
}

func TestEnsureNodeIdentity_MigratesLegacyStaticSealedToActiveKEK(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()
	store := newNIKEKStore(t, 0, 1, 2)

	cert, want, err := transport.GenerateNodeIdentity(testNIClusterID, testNINodeID)
	require.NoError(t, err)
	require.NoError(t, transport.SealNodeKey(dir, encKey, cert))

	_, got, gotGen, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store)
	require.NoError(t, err)
	require.Equal(t, store.ActiveVersion(), gotGen)
	require.Equal(t, want, got)
	activeKEK, err := store.ActiveKEK()
	require.NoError(t, err)
	_, migrated, err := transport.LoadNodeKey(dir, activeKEK)
	require.NoError(t, err)
	require.Equal(t, want, migrated)
	gen, ok := readNodeKeyGen(dir)
	require.True(t, ok, "node.key.gen not written after static migration")
	require.Equal(t, store.ActiveVersion(), gen)
}

func TestEnsureNodeIdentity_RejectsMissingSidecarForKEKSealedKey(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()
	sealStore := newNIKEKStore(t, 2)
	kek2, err := sealStore.Get(2)
	require.NoError(t, err)
	cert, _, err := transport.GenerateNodeIdentity(testNIClusterID, testNINodeID)
	require.NoError(t, err)
	require.NoError(t, transport.SealNodeKey(dir, kek2, cert))
	before, err := os.ReadFile(nodeKeyEncPath(dir))
	require.NoError(t, err)

	store := newNIKEKStore(t, 0, 1, 2)
	_, _, _, err = ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store)
	require.Error(t, err, "expected error when KEK-sealed key lacks sidecar")
	after, err := os.ReadFile(nodeKeyEncPath(dir))
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestEnsureNodeIdentity_NeverRegeneratesOnPrunedRecordedGen(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()
	sealStore := newNIKEKStore(t, 2)
	kek2, err := sealStore.Get(2)
	require.NoError(t, err)
	cert, _, err := transport.GenerateNodeIdentity(testNIClusterID, testNINodeID)
	require.NoError(t, err)
	require.NoError(t, transport.SealNodeKey(dir, kek2, cert))
	require.NoError(t, writeNodeKeyGen(dir, 2))
	before, err := os.ReadFile(nodeKeyEncPath(dir))
	require.NoError(t, err)

	store := newNIKEKStore(t, 0, 1)
	_, _, _, err = ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store)
	require.Error(t, err, "expected error when recorded KEK gen is pruned")
	after, err := os.ReadFile(nodeKeyEncPath(dir))
	require.NoError(t, err)
	require.Equal(t, before, after)
}
