package policystore

import (
	"context"
	"errors"
	"testing"
)

func TestPolicyStore_PutGetDelete(t *testing.T) {
	s := NewInMemoryStore()
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`)
	if err := s.Put(context.Background(), "my-read", doc, false); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.GetRaw(context.Background(), "my-read")
	if err != nil {
		t.Fatalf("GetRaw: %v", err)
	}
	if string(got) != string(doc) {
		t.Fatal("GetRaw returned different bytes")
	}
	if err := s.Delete(context.Background(), "my-read"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.GetRaw(context.Background(), "my-read"); !errors.Is(err, ErrPolicyNotFound) {
		t.Fatalf("GetRaw after Delete: got %v, want ErrPolicyNotFound", err)
	}
}

func TestPolicyStore_RefuseBuiltinMutation(t *testing.T) {
	s := NewInMemoryStore()
	doc := []byte(`{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`)
	if err := s.Put(context.Background(), "readonly", doc, true); err != nil {
		t.Fatalf("Put builtin (initial): %v", err)
	}
	if err := s.Put(context.Background(), "readonly", doc, false); !errors.Is(err, ErrBuiltinPolicy) {
		t.Fatalf("Put on builtin name with builtin=false: got %v, want ErrBuiltinPolicy", err)
	}
	if err := s.Delete(context.Background(), "readonly"); !errors.Is(err, ErrBuiltinPolicy) {
		t.Fatalf("Delete on builtin: got %v, want ErrBuiltinPolicy", err)
	}
}

// TestPolicyStore_GetRawIsIndependent verifies that the returned byte slice is a
// copy: mutating the returned slice must not corrupt the stored doc.
func TestPolicyStore_GetRawIsIndependent(t *testing.T) {
	s := NewInMemoryStore()
	doc := []byte(`{"Statement":[]}`)
	if err := s.Put(context.Background(), "iso", doc, false); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.GetRaw(context.Background(), "iso")
	if err != nil {
		t.Fatalf("GetRaw: %v", err)
	}
	// Corrupt the returned slice.
	for i := range got {
		got[i] ^= 0xFF
	}
	// A second GetRaw must still return the original bytes.
	got2, err := s.GetRaw(context.Background(), "iso")
	if err != nil {
		t.Fatalf("GetRaw (2nd): %v", err)
	}
	if string(got2) != string(doc) {
		t.Fatalf("GetRaw (2nd) returned %q, want %q", got2, doc)
	}
}

// TestPolicyStore_List verifies that List returns all stored names.
func TestPolicyStore_List(t *testing.T) {
	s := NewInMemoryStore()
	names := []string{"alpha", "beta", "gamma"}
	doc := []byte(`{"Statement":[]}`)
	for _, n := range names {
		if err := s.Put(context.Background(), n, doc, false); err != nil {
			t.Fatalf("Put %q: %v", n, err)
		}
	}
	got := s.List()
	if len(got) != len(names) {
		t.Fatalf("List len: got %d, want %d", len(got), len(names))
	}
}
