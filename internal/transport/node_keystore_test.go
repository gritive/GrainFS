package transport

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestNodeKeystore_SealLoadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	cert, spki, err := GenerateNodeIdentity("cluster-x", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := SealNodeKey(dir, kek, cert); err != nil {
		t.Fatalf("SealNodeKey: %v", err)
	}
	path := filepath.Join(dir, "keys.d", "node.key.enc")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat sealed key: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("sealed key mode = %v, want 0600", info.Mode().Perm())
	}
	di, err := os.Stat(filepath.Join(dir, "keys.d"))
	if err != nil {
		t.Fatal(err)
	}
	if di.Mode().Perm() != 0o700 {
		t.Fatalf("keys.d mode = %v, want 0700", di.Mode().Perm())
	}
	raw, _ := os.ReadFile(path)
	if bytes.Contains(raw, []byte("PRIVATE KEY")) {
		t.Fatal("sealed key contains plaintext PEM; not encrypted")
	}
	loaded, loadedSPKI, err := LoadNodeKey(dir, kek)
	if err != nil {
		t.Fatalf("LoadNodeKey: %v", err)
	}
	if loadedSPKI != spki {
		t.Fatalf("loaded SPKI %x != sealed SPKI %x", loadedSPKI, spki)
	}
	if loaded.Leaf == nil || loaded.PrivateKey == nil {
		t.Fatal("loaded cert missing Leaf or PrivateKey")
	}
}

func TestNodeKeystore_WrongKEKFails(t *testing.T) {
	dir := t.TempDir()
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	cert, _, _ := GenerateNodeIdentity("cluster-x", "node-a")
	if err := SealNodeKey(dir, kek, cert); err != nil {
		t.Fatal(err)
	}
	wrong := make([]byte, 32)
	if _, err := rand.Read(wrong); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadNodeKey(dir, wrong); err == nil {
		t.Fatal("LoadNodeKey with wrong KEK succeeded; want auth failure")
	}
}

func TestNodeKeystore_LoadMissingReturnsNotExist(t *testing.T) {
	dir := t.TempDir()
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadNodeKey(dir, kek); !os.IsNotExist(err) {
		t.Fatalf("want os.IsNotExist, got %v", err)
	}
}

func TestNodeKeystore_ResealOverwrites(t *testing.T) {
	dir := t.TempDir()
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	cert1, _, err := GenerateNodeIdentity("cluster-x", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := SealNodeKey(dir, kek, cert1); err != nil {
		t.Fatalf("SealNodeKey (first): %v", err)
	}
	cert2, spki2, err := GenerateNodeIdentity("cluster-x", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := SealNodeKey(dir, kek, cert2); err != nil {
		t.Fatalf("SealNodeKey (second): %v", err)
	}
	_, loadedSPKI, err := LoadNodeKey(dir, kek)
	if err != nil {
		t.Fatalf("LoadNodeKey: %v", err)
	}
	if loadedSPKI != spki2 {
		t.Fatalf("loaded SPKI %x != second identity SPKI %x; re-seal did not overwrite", loadedSPKI, spki2)
	}
}

func TestNodeKeystore_RejectsOversizedFile(t *testing.T) {
	dir := t.TempDir()
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	keysDir := filepath.Join(dir, "keys.d")
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		t.Fatal(err)
	}
	// Write an oversized (>64KiB) file in place of a sealed key.
	if err := os.WriteFile(filepath.Join(keysDir, "node.key.enc"), make([]byte, (64<<10)+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadNodeKey(dir, kek); err == nil {
		t.Fatal("LoadNodeKey accepted an oversized file; want bounded-read rejection")
	}
}
