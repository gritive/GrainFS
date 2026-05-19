package bucketpolicy

import (
	"context"
	"errors"
	"testing"
)

func TestBucketPolicy_PutGet(t *testing.T) {
	s := NewInMemoryStore()
	raw := []byte(`{"Statement":[{"Effect":"Allow","Principal":{"AWS":["sa-1"]},"Action":"s3:GetObject","Resource":"arn:aws:s3:::a/*"}]}`)
	if err := s.Put(context.Background(), "a", raw); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(raw) {
		t.Fatal("roundtrip mismatch")
	}
}

func TestBucketPolicy_DeleteNoSuch(t *testing.T) {
	s := NewInMemoryStore()
	if err := s.Delete(context.Background(), "no-such"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestBucketPolicy_GetIsIndependent(t *testing.T) {
	s := NewInMemoryStore()
	raw := []byte(`{"x":1}`)
	_ = s.Put(context.Background(), "b", raw)
	got, _ := s.Get(context.Background(), "b")
	got[0] = 'X' // mutate caller's copy
	got2, _ := s.Get(context.Background(), "b")
	if got2[0] != '{' {
		t.Fatal("Get must return independent copy")
	}
}
