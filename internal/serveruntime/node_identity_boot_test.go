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

	spki, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, nil)
	if err != nil {
		t.Fatalf("ensureNodeIdentity: %v", err)
	}
	if spki == ([32]byte{}) {
		t.Fatal("expected non-zero SPKI")
	}
	if _, err := os.Stat(nodeKeyEncPath(dir)); err != nil {
		t.Fatalf("node.key.enc not written: %v", err)
	}
	// The static-key path must NOT write the legacy sidecar.
	if _, err := os.Stat(nodeKeyGenPath(dir)); !os.IsNotExist(err) {
		t.Fatalf("node.key.gen must not be written, err=%v", err)
	}
	// The sealed key must decrypt under the static encryption key.
	if _, reloaded, err := transport.LoadNodeKey(dir, encKey); err != nil {
		t.Fatalf("LoadNodeKey under encKey: %v", err)
	} else if reloaded != spki {
		t.Fatalf("reloaded SPKI %x != %x", reloaded, spki)
	}
}

func TestEnsureNodeIdentity_ReusesPersisted(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()

	first, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, nil)
	if err != nil {
		t.Fatalf("first ensureNodeIdentity: %v", err)
	}
	second, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, nil)
	if err != nil {
		t.Fatalf("second ensureNodeIdentity: %v", err)
	}
	if first != second {
		t.Fatalf("SPKI changed across calls: %x != %x", first, second)
	}
}

func TestEnsureNodeIdentity_BackCompatKEKGenSealed(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()
	// An invite-join (Phase 2) sealed the key under KEK gen 2 and left a
	// node.key.gen sidecar. The cluster retains gens 0,1,2.
	store := newNIKEKStore(t, 0, 1, 2)
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
	// Simulate a leftover legacy sidecar from the Phase-2 seal.
	if err := os.WriteFile(nodeKeyGenPath(dir), []byte("2\n"), 0o600); err != nil {
		t.Fatalf("write legacy node.key.gen: %v", err)
	}

	got, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store)
	if err != nil {
		t.Fatalf("ensureNodeIdentity: %v", err)
	}
	if got != want {
		t.Fatalf("SPKI mismatch: got %x want %x", got, want)
	}
	// Migration: the key now decrypts under the static encryption key.
	if _, migrated, err := transport.LoadNodeKey(dir, encKey); err != nil {
		t.Fatalf("post-migration LoadNodeKey under encKey: %v", err)
	} else if migrated != want {
		t.Fatalf("migrated SPKI %x != %x", migrated, want)
	}
	// The stale sidecar must be deleted.
	if _, err := os.Stat(nodeKeyGenPath(dir)); !os.IsNotExist(err) {
		t.Fatalf("stale node.key.gen must be deleted, err=%v", err)
	}
}

func TestEnsureNodeIdentity_NeverRegeneratesOnDecryptFailure(t *testing.T) {
	dir := t.TempDir()
	encKey := testNIEncKey()
	// Seal under a gen whose KEK the store does NOT hold (gen 2), and use a
	// static encKey that also does not match. Neither encKey nor any retained
	// gen (0,1) can decrypt → must error, must not overwrite.
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

	store := newNIKEKStore(t, 0, 1)
	if _, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, encKey, store); err == nil {
		t.Fatal("expected error when nothing decrypts, got nil")
	}
	after, err := os.ReadFile(nodeKeyEncPath(dir))
	if err != nil {
		t.Fatalf("read node.key.enc after: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("node.key.enc was overwritten on decrypt failure")
	}
}
