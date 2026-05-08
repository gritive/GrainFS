package iam

import (
	"bytes"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// newTestEncryptor builds a deterministic Encryptor for tests AND
// benchmarks. testing.TB so bench_test.go can reuse it.
func newTestEncryptor(t testing.TB) *encrypt.Encryptor {
	t.Helper()
	key := bytes.Repeat([]byte{0xab}, 32)
	enc, err := encrypt.NewEncryptor(key)
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	return enc
}

func TestSecretWrap_Roundtrip(t *testing.T) {
	enc := newTestEncryptor(t)
	saID := "sa-test-1"
	plain := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	ct, err := WrapSecret(enc, saID, plain)
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}
	if len(ct) == 0 || string(ct) == plain {
		t.Fatal("ciphertext empty or unencrypted")
	}

	got, err := UnwrapSecret(enc, saID, ct)
	if err != nil {
		t.Fatalf("UnwrapSecret: %v", err)
	}
	if got != plain {
		t.Fatalf("got %q want %q", got, plain)
	}
}

func TestSecretWrap_AADBindsToSA(t *testing.T) {
	enc := newTestEncryptor(t)
	plain := "secret"
	ct, err := WrapSecret(enc, "sa-A", plain)
	if err != nil {
		t.Fatalf("WrapSecret: %v", err)
	}
	if _, err := UnwrapSecret(enc, "sa-B", ct); err == nil {
		t.Fatal("expected AAD mismatch error, got nil")
	}
}

func TestSecretWrap_NilEncryptor(t *testing.T) {
	if _, err := WrapSecret(nil, "sa", "secret"); err == nil {
		t.Fatal("WrapSecret(nil) should error")
	}
	if _, err := UnwrapSecret(nil, "sa", []byte{1, 2, 3}); err == nil {
		t.Fatal("UnwrapSecret(nil) should error")
	}
}
