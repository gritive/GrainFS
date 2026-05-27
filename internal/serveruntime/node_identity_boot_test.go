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
	store := newNIKEKStore(t, 0)

	spki, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, store)
	if err != nil {
		t.Fatalf("ensureNodeIdentity: %v", err)
	}
	if spki == ([32]byte{}) {
		t.Fatal("expected non-zero SPKI")
	}
	if _, err := os.Stat(nodeKeyEncPath(dir)); err != nil {
		t.Fatalf("node.key.enc not written: %v", err)
	}
	genBytes, err := os.ReadFile(nodeKeyGenPath(dir))
	if err != nil {
		t.Fatalf("node.key.gen not written: %v", err)
	}
	if got := string(bytes.TrimSpace(genBytes)); got != "0" {
		t.Fatalf("node.key.gen = %q, want %q", got, "0")
	}
}

func TestEnsureNodeIdentity_ReusesPersisted(t *testing.T) {
	dir := t.TempDir()
	store := newNIKEKStore(t, 0)

	first, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, store)
	if err != nil {
		t.Fatalf("first ensureNodeIdentity: %v", err)
	}
	second, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, store)
	if err != nil {
		t.Fatalf("second ensureNodeIdentity: %v", err)
	}
	if first != second {
		t.Fatalf("SPKI changed across calls: %x != %x", first, second)
	}
}

func TestEnsureNodeIdentity_BackCompatMissingGenFile(t *testing.T) {
	dir := t.TempDir()
	// Cluster retains gens 0,1,2; the key was sealed under gen 2 by an
	// invite-join that predates the node.key.gen sidecar.
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
	// No node.key.gen on disk (predates this task).
	if _, err := os.Stat(nodeKeyGenPath(dir)); !os.IsNotExist(err) {
		t.Fatalf("expected no node.key.gen, got err=%v", err)
	}

	got, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, store)
	if err != nil {
		t.Fatalf("ensureNodeIdentity: %v", err)
	}
	if got != want {
		t.Fatalf("SPKI mismatch: got %x want %x", got, want)
	}
	// Sidecar recreated with the gen that worked (2).
	genBytes, err := os.ReadFile(nodeKeyGenPath(dir))
	if err != nil {
		t.Fatalf("node.key.gen not recreated: %v", err)
	}
	if g := string(bytes.TrimSpace(genBytes)); g != "2" {
		t.Fatalf("node.key.gen = %q, want %q", g, "2")
	}
}

func TestEnsureNodeIdentity_BackCompatTriesGensNewestFirst(t *testing.T) {
	dir := t.TempDir()
	// Sealed under gen 1, but store retains 0,1,2. Newest-first means gen 2 is
	// tried first (wrong KEK → GCM failure), then gen 1 succeeds — exercising
	// the skip-and-retry loop, not just the first iteration.
	store := newNIKEKStore(t, 0, 1, 2)
	kek1, err := store.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	cert, want, err := transport.GenerateNodeIdentity(testNIClusterID, testNINodeID)
	if err != nil {
		t.Fatalf("GenerateNodeIdentity: %v", err)
	}
	if err := transport.SealNodeKey(dir, kek1, cert); err != nil {
		t.Fatalf("SealNodeKey: %v", err)
	}

	got, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, store)
	if err != nil {
		t.Fatalf("ensureNodeIdentity: %v", err)
	}
	if got != want {
		t.Fatalf("SPKI mismatch: got %x want %x", got, want)
	}
	genBytes, err := os.ReadFile(nodeKeyGenPath(dir))
	if err != nil {
		t.Fatalf("node.key.gen not recreated: %v", err)
	}
	if g := string(bytes.TrimSpace(genBytes)); g != "1" {
		t.Fatalf("node.key.gen = %q, want %q", g, "1")
	}
}

func TestEnsureNodeIdentity_NeverRegeneratesOnDecryptFailure(t *testing.T) {
	dir := t.TempDir()
	// Seal under a gen whose KEK the store does NOT hold: seal with a key for
	// gen 2, but give the store only gens 0,1 (with different KEK bytes). No
	// gen can decrypt → must error, must not overwrite.
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
	if _, err := ensureNodeIdentity(dir, testNIClusterID, testNINodeID, store); err == nil {
		t.Fatal("expected error when no gen decrypts, got nil")
	}
	after, err := os.ReadFile(nodeKeyEncPath(dir))
	if err != nil {
		t.Fatalf("read node.key.enc after: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("node.key.enc was overwritten on decrypt failure")
	}
	if _, err := os.Stat(nodeKeyGenPath(dir)); !os.IsNotExist(err) {
		t.Fatalf("node.key.gen must not be created on failure, err=%v", err)
	}
}
