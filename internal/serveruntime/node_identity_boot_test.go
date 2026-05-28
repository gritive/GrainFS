package serveruntime

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

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
		if err := s.Add(g, kek); err != nil {
			t.Fatalf("KEKStore.Add(%d): %v", g, err)
		}
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

	_, spki, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store)
	if err != nil {
		t.Fatalf("ensureNodeIdentity: %v", err)
	}
	if spki == ([32]byte{}) {
		t.Fatal("expected non-zero SPKI")
	}
	if _, err := os.Stat(nodeKeyEncPath(dir)); err != nil {
		t.Fatalf("node.key.enc not written: %v", err)
	}
	gen, ok := readNodeKeyGen(dir)
	if !ok {
		t.Fatal("node.key.gen not written")
	}
	if gen != store.ActiveVersion() {
		t.Fatalf("node.key.gen=%d want active %d", gen, store.ActiveVersion())
	}
	activeKEK, err := store.ActiveKEK()
	if err != nil {
		t.Fatalf("ActiveKEK: %v", err)
	}
	if _, reloaded, err := transport.LoadNodeKey(dir, activeKEK); err != nil {
		t.Fatalf("LoadNodeKey under active KEK: %v", err)
	} else if reloaded != spki {
		t.Fatalf("reloaded SPKI %x != %x", reloaded, spki)
	}
	if _, _, err := transport.LoadNodeKey(dir, encKey); err == nil {
		t.Fatal("node.key.enc unexpectedly decrypts under static encryption key")
	}
}

func TestEnsureNodeIdentity_ReusesPersisted(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()
	store := newNIKEKStore(t, 0, 1, 2)

	_, first, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store)
	if err != nil {
		t.Fatalf("first ensureNodeIdentity: %v", err)
	}
	before, err := os.ReadFile(nodeKeyEncPath(dir))
	if err != nil {
		t.Fatalf("read node.key.enc: %v", err)
	}
	_, second, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store)
	if err != nil {
		t.Fatalf("second ensureNodeIdentity: %v", err)
	}
	if first != second {
		t.Fatalf("SPKI changed across calls: %x != %x", first, second)
	}
	after, err := os.ReadFile(nodeKeyEncPath(dir))
	if err != nil {
		t.Fatalf("read node.key.enc after: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("node.key.enc changed despite already using active KEK")
	}
}

func TestEnsureNodeIdentity_ReSealsOlderKEKGenToActive(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()
	store := newNIKEKStore(t, 0, 1, 2, 3)
	kek2, err := store.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	cert, want, err := transport.GenerateNodeIdentity(testNIClusterID, testNINodeID)
	if err != nil {
		t.Fatalf("GenerateNodeIdentity: %v", err)
	}
	if err := transport.SealNodeKey(dir, kek2, cert); err != nil {
		t.Fatalf("SealNodeKey: %v", err)
	}
	if err := writeNodeKeyGen(dir, 2); err != nil {
		t.Fatalf("writeNodeKeyGen: %v", err)
	}

	_, got, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store)
	if err != nil {
		t.Fatalf("ensureNodeIdentity: %v", err)
	}
	if got != want {
		t.Fatalf("SPKI mismatch: got %x want %x", got, want)
	}
	activeKEK, err := store.ActiveKEK()
	if err != nil {
		t.Fatalf("ActiveKEK: %v", err)
	}
	if _, migrated, err := transport.LoadNodeKey(dir, activeKEK); err != nil {
		t.Fatalf("post-migration LoadNodeKey under active KEK: %v", err)
	} else if migrated != want {
		t.Fatalf("migrated SPKI %x != %x", migrated, want)
	}
	gen, ok := readNodeKeyGen(dir)
	if !ok {
		t.Fatal("node.key.gen not written after active re-seal")
	}
	if gen != store.ActiveVersion() {
		t.Fatalf("node.key.gen=%d want active %d", gen, store.ActiveVersion())
	}
}

func TestEnsureNodeIdentity_MigratesLegacyStaticSealedToActiveKEK(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()
	store := newNIKEKStore(t, 0, 1, 2)

	cert, want, err := transport.GenerateNodeIdentity(testNIClusterID, testNINodeID)
	if err != nil {
		t.Fatalf("GenerateNodeIdentity: %v", err)
	}
	if err := transport.SealNodeKey(dir, encKey, cert); err != nil {
		t.Fatalf("SealNodeKey under encKey: %v", err)
	}

	_, got, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store)
	if err != nil {
		t.Fatalf("ensureNodeIdentity: %v", err)
	}
	if got != want {
		t.Fatalf("SPKI mismatch: got %x want %x", got, want)
	}
	activeKEK, err := store.ActiveKEK()
	if err != nil {
		t.Fatalf("ActiveKEK: %v", err)
	}
	if _, migrated, err := transport.LoadNodeKey(dir, activeKEK); err != nil {
		t.Fatalf("post-migration LoadNodeKey under active KEK: %v", err)
	} else if migrated != want {
		t.Fatalf("migrated SPKI %x != %x", migrated, want)
	}
	gen, ok := readNodeKeyGen(dir)
	if !ok {
		t.Fatal("node.key.gen not written after static migration")
	}
	if gen != store.ActiveVersion() {
		t.Fatalf("node.key.gen=%d want active %d", gen, store.ActiveVersion())
	}
}

func TestEnsureNodeIdentity_RejectsMissingSidecarForKEKSealedKey(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()
	sealStore := newNIKEKStore(t, 2)
	kek2, err := sealStore.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	cert, _, err := transport.GenerateNodeIdentity(testNIClusterID, testNINodeID)
	if err != nil {
		t.Fatalf("GenerateNodeIdentity: %v", err)
	}
	if err := transport.SealNodeKey(dir, kek2, cert); err != nil {
		t.Fatalf("SealNodeKey: %v", err)
	}
	before, err := os.ReadFile(nodeKeyEncPath(dir))
	if err != nil {
		t.Fatalf("read node.key.enc: %v", err)
	}

	store := newNIKEKStore(t, 0, 1, 2)
	if _, _, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store); err == nil {
		t.Fatal("expected error when KEK-sealed key lacks sidecar, got nil")
	}
	after, err := os.ReadFile(nodeKeyEncPath(dir))
	if err != nil {
		t.Fatalf("read node.key.enc after: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("node.key.enc was overwritten on decrypt failure")
	}
}

func TestEnsureNodeIdentity_NeverRegeneratesOnPrunedRecordedGen(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()
	sealStore := newNIKEKStore(t, 2)
	kek2, err := sealStore.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	cert, _, err := transport.GenerateNodeIdentity(testNIClusterID, testNINodeID)
	if err != nil {
		t.Fatalf("GenerateNodeIdentity: %v", err)
	}
	if err := transport.SealNodeKey(dir, kek2, cert); err != nil {
		t.Fatalf("SealNodeKey: %v", err)
	}
	if err := writeNodeKeyGen(dir, 2); err != nil {
		t.Fatalf("writeNodeKeyGen: %v", err)
	}
	before, err := os.ReadFile(nodeKeyEncPath(dir))
	if err != nil {
		t.Fatalf("read node.key.enc: %v", err)
	}

	store := newNIKEKStore(t, 0, 1)
	if _, _, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store); err == nil {
		t.Fatal("expected error when recorded KEK gen is pruned, got nil")
	}
	after, err := os.ReadFile(nodeKeyEncPath(dir))
	if err != nil {
		t.Fatalf("read node.key.enc after: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("node.key.enc was overwritten on pruned recorded gen")
	}
}
