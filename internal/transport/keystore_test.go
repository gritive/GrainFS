package transport

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKeystore_WriteCurrent_AtomicAndPermissioned(t *testing.T) {
	dir := t.TempDir()
	ks := NewKeystore(dir)
	psk := strings.Repeat("a", 64)

	if err := ks.WriteCurrent(psk); err != nil {
		t.Fatal(err)
	}

	di, err := os.Stat(filepath.Join(dir, "keys.d"))
	if err != nil {
		t.Fatal(err)
	}
	if di.Mode().Perm() != 0o700 {
		t.Fatalf("keys.d mode: want 0700, got %o", di.Mode().Perm())
	}

	fi, err := os.Stat(filepath.Join(dir, "keys.d", "current.key"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("current.key mode: want 0600, got %o", fi.Mode().Perm())
	}

	got, err := ks.ReadCurrent()
	if err != nil {
		t.Fatal(err)
	}
	if got != psk {
		t.Fatalf("read mismatch: want %q, got %q", psk, got)
	}
}

func TestKeystore_NoSymlinkTraversal(t *testing.T) {
	dir := t.TempDir()
	keysd := filepath.Join(dir, "keys.d")
	if err := os.Mkdir(keysd, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "outside.txt")
	if err := os.WriteFile(target, []byte("attacker controlled"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(keysd, "current.key")); err != nil {
		t.Fatal(err)
	}

	ks := NewKeystore(dir)
	if _, err := ks.ReadCurrent(); err == nil {
		t.Fatal("expected error for symlinked current.key (O_NOFOLLOW)")
	}
}

func TestKeystore_NextAndPreviousSlots(t *testing.T) {
	dir := t.TempDir()
	ks := NewKeystore(dir)
	a := strings.Repeat("a", 64)
	b := strings.Repeat("b", 64)

	if err := ks.WriteCurrent(a); err != nil {
		t.Fatal(err)
	}
	if err := ks.WriteNext(b); err != nil {
		t.Fatal(err)
	}

	gotNext, err := ks.ReadNext()
	if err != nil || gotNext != b {
		t.Fatalf("ReadNext: %v %q", err, gotNext)
	}

	if err := ks.MoveNextToCurrent(); err != nil {
		t.Fatal(err)
	}
	gotCur, _ := ks.ReadCurrent()
	if gotCur != b {
		t.Fatalf("after move, current should be b, got %q", gotCur)
	}
	gotPrev, _ := ks.ReadPrevious()
	if gotPrev != a {
		t.Fatalf("after move, previous should be a, got %q", gotPrev)
	}
	if _, err := ks.ReadNext(); !os.IsNotExist(err) {
		t.Fatalf("after move, next should be gone, got err=%v", err)
	}
}

func TestKeystore_DeleteSlots(t *testing.T) {
	dir := t.TempDir()
	ks := NewKeystore(dir)
	if err := ks.WriteNext(strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if err := ks.DeleteNext(); err != nil {
		t.Fatal(err)
	}
	if _, err := ks.ReadNext(); !os.IsNotExist(err) {
		t.Fatalf("after delete, next should be gone, got err=%v", err)
	}
	// Idempotent — deleting again is a no-op.
	if err := ks.DeleteNext(); err != nil {
		t.Fatalf("delete-of-absent should be no-op, got %v", err)
	}
}
