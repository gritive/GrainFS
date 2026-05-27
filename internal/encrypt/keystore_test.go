package encrypt

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestKEKStore_AddGetActive(t *testing.T) {
	s := NewKEKStore()
	k0 := bytes.Repeat([]byte{0xAA}, KEKSize)
	k1 := bytes.Repeat([]byte{0xBB}, KEKSize)
	if err := s.Add(0, k0); err != nil {
		t.Fatalf("Add(0): %v", err)
	}
	if err := s.Add(1, k1); err != nil {
		t.Fatalf("Add(1): %v", err)
	}
	if v := s.ActiveVersion(); v != 1 {
		t.Errorf("ActiveVersion = %d, want 1", v)
	}
	got, err := s.Get(0)
	if err != nil {
		t.Fatalf("Get(0): %v", err)
	}
	if !bytes.Equal(got, k0) {
		t.Errorf("Get(0) returned wrong bytes")
	}
}

func TestKEKStore_AddRejectsBadLen(t *testing.T) {
	s := NewKEKStore()
	if err := s.Add(0, []byte{0x01}); err == nil {
		t.Fatalf("Add accepted KEK of wrong length")
	}
}

func TestKEKStore_AddRejectsDuplicate(t *testing.T) {
	s := NewKEKStore()
	k := bytes.Repeat([]byte{0xCC}, KEKSize)
	if err := s.Add(0, k); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := s.Add(0, k); err == nil {
		t.Fatalf("Add accepted duplicate version 0")
	}
}

func TestKEKStore_Versions_SortedCanonical(t *testing.T) {
	s := NewKEKStore()
	k := bytes.Repeat([]byte{0xDD}, KEKSize)
	_ = s.Add(5, k)
	_ = s.Add(1, k)
	_ = s.Add(3, k)
	got := s.Versions()
	want := []uint32{1, 3, 5}
	if len(got) != len(want) {
		t.Fatalf("Versions len = %d, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("Versions[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestKEKStore_Delete_RefuseActive(t *testing.T) {
	s := NewKEKStore()
	k := bytes.Repeat([]byte{0xEE}, KEKSize)
	_ = s.Add(0, k)
	if err := s.Delete(0); err == nil {
		t.Fatalf("Delete accepted active version")
	}
}

func TestKEKStore_Get_UnknownVersion(t *testing.T) {
	s := NewKEKStore()
	if _, err := s.Get(99); err == nil {
		t.Fatalf("Get(99) on empty store accepted")
	}
}

func TestKEKStore_Delete_OldVersionAfterRotate(t *testing.T) {
	s := NewKEKStore()
	k0 := bytes.Repeat([]byte{0x10}, KEKSize)
	k1 := bytes.Repeat([]byte{0x20}, KEKSize)
	_ = s.Add(0, k0)
	_ = s.Add(1, k1)
	if err := s.Delete(0); err != nil {
		t.Fatalf("Delete(0) after advancing to 1: %v", err)
	}
	if _, err := s.Get(0); err == nil {
		t.Fatalf("Get(0) after Delete still succeeds")
	}
	if v := s.ActiveVersion(); v != 1 {
		t.Errorf("ActiveVersion after delete of old = %d, want 1", v)
	}
}

func TestKEKStore_ActiveKEK_ReturnsCopy(t *testing.T) {
	s := NewKEKStore()
	k0 := bytes.Repeat([]byte{0x42}, KEKSize)
	_ = s.Add(0, k0)
	got, err := s.ActiveKEK()
	if err != nil {
		t.Fatalf("ActiveKEK: %v", err)
	}
	// Mutate the returned slice; the store's internal copy must NOT change.
	got[0] = 0x99
	got2, _ := s.Get(0)
	if got2[0] == 0x99 {
		t.Errorf("ActiveKEK returned a reference instead of a copy")
	}
}

func TestKEKStore_Get_ReturnsCopy(t *testing.T) {
	s := NewKEKStore()
	k0 := bytes.Repeat([]byte{0x5A}, KEKSize)
	_ = s.Add(0, k0)
	got, err := s.Get(0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	got[0] = 0x99
	again, _ := s.Get(0)
	if again[0] == 0x99 {
		t.Errorf("Get returned a reference, mutation leaked back into store")
	}
}

func TestKEKStore_Delete_UnknownReturnsSentinel(t *testing.T) {
	s := NewKEKStore()
	_ = s.Add(0, bytes.Repeat([]byte{0x77}, KEKSize))
	_ = s.Add(1, bytes.Repeat([]byte{0x88}, KEKSize)) // active=1; v0 now deletable
	err := s.Delete(42)
	if err == nil {
		t.Fatal("Delete(42) on absent version accepted")
	}
	if !errors.Is(err, ErrKEKVersionUnknown) {
		t.Errorf("expected ErrKEKVersionUnknown, got %v", err)
	}
}

func TestKEKStore_SentinelErrorsWrappedConsistently(t *testing.T) {
	s := NewKEKStore()
	k := bytes.Repeat([]byte{0x11}, KEKSize)
	_ = s.Add(0, k)

	if err := s.Add(0, k); !errors.Is(err, ErrKEKVersionDuplicate) {
		t.Errorf("Add(dup): expected ErrKEKVersionDuplicate, got %v", err)
	}
	if _, err := s.Get(99); !errors.Is(err, ErrKEKVersionUnknown) {
		t.Errorf("Get(unknown): expected ErrKEKVersionUnknown, got %v", err)
	}
	if err := s.Delete(0); !errors.Is(err, ErrKEKActiveInUse) {
		t.Errorf("Delete(active): expected ErrKEKActiveInUse, got %v", err)
	}
}

func TestKEKStore_LoadOrInitDir_FreshGeneratesV0(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	s, err := LoadOrInitKEKStoreDir(keysDir)
	if err != nil {
		t.Fatalf("LoadOrInitKEKStoreDir: %v", err)
	}
	if v := s.ActiveVersion(); v != 0 {
		t.Errorf("fresh store active version = %d, want 0", v)
	}
	info, err := os.Stat(filepath.Join(keysDir, "0.key"))
	if err != nil {
		t.Fatalf("0.key not written: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("0.key perm = %#o, want 0o600", perm)
	}
}

func TestKEKStore_LoadOrInitDir_FreshCreatesDurable(t *testing.T) {
	// Smoke test for the parent-dir fsync after MkdirAll. We can't easily
	// trigger a real power-loss to verify the fsync ran, but we can at
	// least verify the happy path still works when the keys/ directory
	// did not previously exist (including nested parents that MkdirAll
	// must create).
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "deeply", "nested", "keys")
	// keys/ does not exist; LoadOrInitKEKStoreDir must create it and v0.
	store, err := LoadOrInitKEKStoreDir(keysDir)
	if err != nil {
		t.Fatalf("LoadOrInitKEKStoreDir: %v", err)
	}
	if v := store.ActiveVersion(); v != 0 {
		t.Errorf("ActiveVersion = %d, want 0", v)
	}
	if _, err := os.Stat(filepath.Join(keysDir, "0.key")); err != nil {
		t.Errorf("0.key not created: %v", err)
	}
}

func TestKEKStore_LoadOrInitDir_ReloadsExisting(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	s1, err := LoadOrInitKEKStoreDir(keysDir)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	k0, _ := s1.Get(0)
	s2, err := LoadOrInitKEKStoreDir(keysDir)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, _ := s2.Get(0)
	if !bytes.Equal(got, k0) {
		t.Errorf("reloaded KEK does not match original")
	}
}

func TestKEKStore_LoadOrInitDir_RefuseLegacyKEKFile(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	// Legacy kek.key sits at <dataDir>/kek.key, which is the sibling of keys/.
	legacyPath := filepath.Join(filepath.Dir(keysDir), "kek.key")
	if err := os.WriteFile(legacyPath, bytes.Repeat([]byte{0x01}, KEKSize), 0o600); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	_, err := LoadOrInitKEKStoreDir(keysDir)
	if err == nil {
		t.Fatalf("expected refuse-boot error on legacy kek.key, got nil")
	}
	if !errors.Is(err, ErrLegacyKEKDetected) {
		t.Errorf("err = %v, want ErrLegacyKEKDetected", err)
	}
}

func TestKEKStore_AddAndPersist(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	s, _ := LoadOrInitKEKStoreDir(keysDir)
	k1 := bytes.Repeat([]byte{0x42}, KEKSize)
	if err := s.AddAndPersist(keysDir, 1, k1); err != nil {
		t.Fatalf("AddAndPersist: %v", err)
	}
	info, err := os.Stat(filepath.Join(keysDir, "1.key"))
	if err != nil {
		t.Fatalf("1.key not written: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("1.key perm = %#o, want 0o600", perm)
	}
	s2, _ := LoadOrInitKEKStoreDir(keysDir)
	if v := s2.ActiveVersion(); v != 1 {
		t.Errorf("reloaded active = %d, want 1", v)
	}
}

func TestKEKStore_LoadOrInitDir_RejectsBadFileSize(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	bad := filepath.Join(keysDir, "0.key")
	if err := os.WriteFile(bad, []byte{0x01, 0x02, 0x03}, 0o600); err != nil {
		t.Fatalf("write bad: %v", err)
	}
	if _, err := LoadOrInitKEKStoreDir(keysDir); err == nil {
		t.Fatalf("expected error for 3-byte KEK file, got nil")
	}
}

func TestKEKStore_LoadOrInitDir_RejectsLoosePerms(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	if err := os.MkdirAll(keysDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	loose := filepath.Join(keysDir, "0.key")
	if err := os.WriteFile(loose, bytes.Repeat([]byte{0x01}, KEKSize), 0o644); err != nil {
		t.Fatalf("write loose: %v", err)
	}
	if _, err := LoadOrInitKEKStoreDir(keysDir); err == nil {
		t.Fatalf("expected perm error for 0o644 KEK file, got nil")
	}
}

func TestKEKStore_ParseKeyFilename_RejectsLeadingZero(t *testing.T) {
	cases := []struct {
		in       string
		expectOK bool
		expectV  uint32
	}{
		{"0.key", true, 0},
		{"1.key", true, 1},
		{"42.key", true, 42},
		{"4294967295.key", true, 4294967295},
		{"01.key", false, 0},         // leading zero
		{"007.key", false, 0},        // leading zero
		{"+1.key", false, 0},         // explicit sign
		{"4294967296.key", false, 0}, // overflows uint32
		{"foo.key", false, 0},
		{".key", false, 0},
		{"0.key.bak", false, 0},
	}
	for _, c := range cases {
		v, ok := parseKeyFilename(c.in)
		if ok != c.expectOK {
			t.Errorf("parseKeyFilename(%q): ok = %v, want %v", c.in, ok, c.expectOK)
			continue
		}
		if ok && v != c.expectV {
			t.Errorf("parseKeyFilename(%q): v = %d, want %d", c.in, v, c.expectV)
		}
	}
}

func TestKEKStore_AddAndPersist_RefusesExistingDiskVersion(t *testing.T) {
	dir := t.TempDir()
	keysDir := filepath.Join(dir, "keys")
	s, _ := LoadOrInitKEKStoreDir(keysDir)
	originalBytes, _ := os.ReadFile(filepath.Join(keysDir, "0.key"))
	err := s.AddAndPersist(keysDir, 0, bytes.Repeat([]byte{0x99}, KEKSize))
	if err == nil {
		t.Fatal("AddAndPersist accepted version 0 (already on disk)")
	}
	if !errors.Is(err, ErrKEKVersionDuplicate) {
		t.Errorf("expected ErrKEKVersionDuplicate, got %v", err)
	}
	currentBytes, _ := os.ReadFile(filepath.Join(keysDir, "0.key"))
	if !bytes.Equal(originalBytes, currentBytes) {
		t.Errorf("AddAndPersist mutated existing 0.key on failure")
	}
	_ = s
}

func TestKeysDirIsEmpty(t *testing.T) {
	t.Run("missing dir counts as empty", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "does-not-exist")
		empty, err := KeysDirIsEmpty(dir)
		if err != nil {
			t.Fatalf("KeysDirIsEmpty: %v", err)
		}
		if !empty {
			t.Errorf("missing dir must be reported as empty")
		}
	})
	t.Run("empty dir is empty", func(t *testing.T) {
		dir := t.TempDir()
		empty, err := KeysDirIsEmpty(dir)
		if err != nil {
			t.Fatalf("KeysDirIsEmpty: %v", err)
		}
		if !empty {
			t.Errorf("empty dir must be reported as empty")
		}
	})
	t.Run("canonical 0.key makes dir non-empty", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "0.key"), bytes.Repeat([]byte{0x01}, KEKSize), 0o600); err != nil {
			t.Fatalf("seed 0.key: %v", err)
		}
		empty, err := KeysDirIsEmpty(dir)
		if err != nil {
			t.Fatalf("KeysDirIsEmpty: %v", err)
		}
		if empty {
			t.Errorf("dir with canonical 0.key must be reported as non-empty")
		}
	})
	t.Run("non-canonical *.key files are ignored", func(t *testing.T) {
		dir := t.TempDir()
		// Leading-zero stem and non-numeric stem are both rejected by
		// parseKeyFilename, so they must NOT make the dir non-empty.
		if err := os.WriteFile(filepath.Join(dir, "01.key"), []byte("x"), 0o600); err != nil {
			t.Fatalf("seed 01.key: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "abc.key"), []byte("x"), 0o600); err != nil {
			t.Fatalf("seed abc.key: %v", err)
		}
		empty, err := KeysDirIsEmpty(dir)
		if err != nil {
			t.Fatalf("KeysDirIsEmpty: %v", err)
		}
		if !empty {
			t.Errorf("dir with only non-canonical *.key files must be reported as empty")
		}
	})
}

func TestKEKStore_SealOpenWithActiveKEK_RoundTrip(t *testing.T) {
	s := NewKEKStore()
	kek := make([]byte, KEKSize)
	for i := range kek {
		kek[i] = byte(i + 1)
	}
	if err := s.Add(0, kek); err != nil {
		t.Fatalf("Add: %v", err)
	}
	plain := []byte("hello capability assertion")
	aad := []byte("test-aad")

	ct, err := s.SealWithActiveKEK(plain, aad)
	if err != nil {
		t.Fatalf("SealWithActiveKEK: %v", err)
	}
	got, err := s.OpenWithActiveKEK(ct, aad)
	if err != nil {
		t.Fatalf("OpenWithActiveKEK: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Errorf("round-trip mismatch: got %q, want %q", got, plain)
	}
}

func TestKEKStore_SealOpenWithActiveKEK_AADMismatchRejects(t *testing.T) {
	s := NewKEKStore()
	kek := make([]byte, KEKSize)
	for i := range kek {
		kek[i] = byte(i + 2)
	}
	if err := s.Add(0, kek); err != nil {
		t.Fatalf("Add: %v", err)
	}
	plain := []byte("some data")
	ct, err := s.SealWithActiveKEK(plain, []byte("aad-a"))
	if err != nil {
		t.Fatalf("SealWithActiveKEK: %v", err)
	}
	_, err = s.OpenWithActiveKEK(ct, []byte("aad-b"))
	if err == nil {
		t.Fatal("expected error on AAD mismatch, got nil")
	}
}

func TestKEKStore_SealOpenWithActiveKEK_WrongKEKRejects(t *testing.T) {
	s1 := NewKEKStore()
	s2 := NewKEKStore()
	kek1 := bytes.Repeat([]byte{0x11}, KEKSize)
	kek2 := bytes.Repeat([]byte{0x22}, KEKSize)
	if err := s1.Add(0, kek1); err != nil {
		t.Fatalf("s1.Add: %v", err)
	}
	if err := s2.Add(0, kek2); err != nil {
		t.Fatalf("s2.Add: %v", err)
	}
	aad := []byte("shared-aad")
	ct, err := s1.SealWithActiveKEK([]byte("secret"), aad)
	if err != nil {
		t.Fatalf("SealWithActiveKEK: %v", err)
	}
	_, err = s2.OpenWithActiveKEK(ct, aad)
	if err == nil {
		t.Fatal("expected error when opening with wrong KEK, got nil")
	}
}

// TestKEKStore_RemoveAndUnlink_DiskFirst verifies disk unlink + parent-dir
// fsync happens BEFORE the in-memory delete. We force a disk failure (read-only
// keysDir + missing file path) and assert the in-memory entry is preserved so
// a replay can retry deterministically.
func TestKEKStore_RemoveAndUnlink_DiskFirst(t *testing.T) {
	dir := t.TempDir()
	s, err := LoadOrInitKEKStoreDir(dir)
	if err != nil {
		t.Fatalf("LoadOrInitKEKStoreDir: %v", err)
	}
	// Add a second version so we can remove a non-active one.
	k1 := bytes.Repeat([]byte{0xB1}, KEKSize)
	if err := s.AddAndPersist(dir, 1, k1); err != nil {
		t.Fatalf("AddAndPersist v1: %v", err)
	}
	// Active is now 1. Remove version 0.
	if !s.HasVersion(0) {
		t.Fatalf("setup: expected version 0 present")
	}
	if err := s.RemoveAndUnlink(dir, 0); err != nil {
		t.Fatalf("RemoveAndUnlink(0): %v", err)
	}
	// In-memory gone.
	if s.HasVersion(0) {
		t.Errorf("HasVersion(0) = true after RemoveAndUnlink; want false")
	}
	// On-disk gone.
	if _, err := os.Stat(filepath.Join(dir, "0.key")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("0.key still on disk: %v", err)
	}
	// Refuses to remove active version.
	if err := s.RemoveAndUnlink(dir, 1); err == nil {
		t.Errorf("RemoveAndUnlink(active=1) accepted; want error")
	}
	// Idempotent: a second remove on already-removed version is a no-op.
	if err := s.RemoveAndUnlink(dir, 0); err != nil {
		t.Errorf("RemoveAndUnlink replay: %v", err)
	}

	// Disk-first ordering: when the directory does not exist, fsync fails and
	// the in-memory state must be preserved.
	k2 := bytes.Repeat([]byte{0xB2}, KEKSize)
	if err := s.AddAndPersist(dir, 2, k2); err != nil {
		t.Fatalf("AddAndPersist v2: %v", err)
	}
	if err := s.RemoveAndUnlink("/non/existent/dir", 1); err == nil {
		t.Errorf("RemoveAndUnlink with bad dir accepted; want fsync failure")
	}
	// In-memory state for v1 must still be present — replay-safe.
	if !s.HasVersion(1) {
		t.Errorf("HasVersion(1) = false after failed RemoveAndUnlink; in-memory state must be preserved")
	}
}

// TestCanonicalVoterSetHash_DeterministicAndSortInvariant verifies that
// CanonicalVoterSetHash is deterministic, order-invariant, and discriminates
// different voter sets.
func TestCanonicalVoterSetHash_DeterministicAndSortInvariant(t *testing.T) {
	a := []string{"node-c", "node-a", "node-b"}
	b := []string{"node-a", "node-b", "node-c"}
	c := []string{"node-b", "node-c", "node-a"}
	if CanonicalVoterSetHash(a) != CanonicalVoterSetHash(b) {
		t.Errorf("hash differs for same set in different order (a vs b)")
	}
	if CanonicalVoterSetHash(a) != CanonicalVoterSetHash(c) {
		t.Errorf("hash differs for same set in different order (a vs c)")
	}
	// Different membership must produce a different hash.
	d := []string{"node-a", "node-b"}
	if CanonicalVoterSetHash(a) == CanonicalVoterSetHash(d) {
		t.Errorf("hash equal for distinct sets {a,b,c} vs {a,b}")
	}
	// Distinct strings that would collide under naive concatenation must
	// produce different hashes (length-prefix discipline).
	x := []string{"ab", "c"}
	y := []string{"a", "bc"}
	if CanonicalVoterSetHash(x) == CanonicalVoterSetHash(y) {
		t.Errorf("hash collides for {ab,c} vs {a,bc}; length-prefix missing?")
	}
}
