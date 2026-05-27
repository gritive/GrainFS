package encrypt

import (
	"sync"
	"testing"
)

// TestEncryptorIsXAES verifies the Encryptor now uses a 192-bit (24-byte)
// nonce and a 16-byte tag, as required by XAES-256-GCM.
func TestEncryptorIsXAES(t *testing.T) {
	e, err := NewEncryptor(make([]byte, 32))
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	if got := e.aead.NonceSize(); got != 24 {
		t.Errorf("NonceSize = %d, want 24", got)
	}
	if got := e.AEADOverhead(); got != 16 {
		t.Errorf("AEADOverhead = %d, want 16", got)
	}
}

// TestEncryptorRoundTripAllForms exercises EncryptWithAAD/DecryptWithAAD and
// SealValueAADTo/OpenValueAADTo with valid and invalid AAD.
func TestEncryptorRoundTripAllForms(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	e, err := NewEncryptor(key)
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}

	plaintext := []byte("hello XAES-256-GCM")
	aad := []byte("bucket/key/version/0")

	// EncryptWithAAD / DecryptWithAAD round-trip
	blob, err := e.EncryptWithAAD(plaintext, aad)
	if err != nil {
		t.Fatalf("EncryptWithAAD: %v", err)
	}
	got, err := e.DecryptWithAAD(blob, aad)
	if err != nil {
		t.Fatalf("DecryptWithAAD: %v", err)
	}
	if string(got) != string(plaintext) {
		t.Errorf("round-trip mismatch: got %q, want %q", got, plaintext)
	}

	// Wrong AAD must fail
	_, err = e.DecryptWithAAD(blob, []byte("wrong-aad"))
	if err == nil {
		t.Error("DecryptWithAAD with wrong AAD should fail")
	}

	// SealValueAADTo / OpenValueAADTo round-trip
	sealed, err := e.SealValueAADTo(nil, aad, plaintext)
	if err != nil {
		t.Fatalf("SealValueAADTo: %v", err)
	}
	got2, err := e.OpenValueAADTo(nil, aad, sealed)
	if err != nil {
		t.Fatalf("OpenValueAADTo: %v", err)
	}
	if string(got2) != string(plaintext) {
		t.Errorf("value round-trip mismatch: got %q, want %q", got2, plaintext)
	}

	// Wrong AAD must fail for value form
	_, err = e.OpenValueAADTo(nil, []byte("wrong-aad"), sealed)
	if err == nil {
		t.Error("OpenValueAADTo with wrong AAD should fail")
	}
}

// TestRejectsPreXAESFormats verifies that old AES-GCM blobs (0xAE 0xE1 magic
// or valueVersion1 0x01) are rejected loudly rather than mis-decoded.
func TestRejectsPreXAESFormats(t *testing.T) {
	e, err := NewEncryptor(make([]byte, 32))
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}

	// Old blob: magic 0xAE 0xE1 + 28 zero bytes (old 12-byte nonce AES-GCM format)
	oldBlob := make([]byte, 30)
	oldBlob[0] = 0xAE
	oldBlob[1] = 0xE1
	_, err = e.DecryptWithAAD(oldBlob, nil)
	if err == nil {
		t.Error("DecryptWithAAD with pre-XAES magic (0xE1) should fail")
	}

	// Old value: magic 0xAE 0xE2 + version 0x01 + 28 zero bytes
	oldValue := make([]byte, 31)
	oldValue[0] = 0xAE
	oldValue[1] = 0xE2
	oldValue[2] = 0x01
	_, err = e.OpenValueAADTo(nil, nil, oldValue)
	if err == nil {
		t.Error("OpenValueAADTo with pre-XAES version (0x01) should fail")
	}
}

// TestEncryptorConcurrent checks that the shared Encryptor is safe for
// concurrent use (no mutable state races). Run with -race.
func TestEncryptorConcurrent(t *testing.T) {
	e, err := NewEncryptor(make([]byte, 32))
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	aad := []byte("concurrent:test:aad")
	plaintext := []byte("concurrent plaintext payload")

	const goroutines = 16
	const iters = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				sealed, err := e.SealValueAADTo(nil, aad, plaintext)
				if err != nil {
					t.Errorf("SealValueAADTo: %v", err)
					return
				}
				got, err := e.OpenValueAADTo(nil, aad, sealed)
				if err != nil {
					t.Errorf("OpenValueAADTo: %v", err)
					return
				}
				if string(got) != string(plaintext) {
					t.Errorf("round-trip mismatch")
					return
				}
			}
		}()
	}
	wg.Wait()
}
