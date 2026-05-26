package encrypt

import (
	"bytes"
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
