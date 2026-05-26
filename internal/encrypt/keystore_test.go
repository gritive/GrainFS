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
