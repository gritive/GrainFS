package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

// ErrCiphertextTooShort is returned when the ciphertext is shorter than the GCM nonce.
var ErrCiphertextTooShort = errors.New("ciphertext shorter than GCM nonce")

// AESGCMSeal encrypts plain under key using AES-256-GCM with a random 12-byte
// nonce. The output format is: nonce(12) + ciphertext + tag(16).
// key must be exactly 32 bytes.
func AESGCMSeal(key, plain []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("AESGCMSeal: key must be 32 bytes, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("AESGCMSeal: create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("AESGCMSeal: create GCM: %w", err)
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("AESGCMSeal: generate nonce: %w", err)
	}

	return aead.Seal(nonce, nonce, plain, nil), nil
}

// AESGCMOpen decrypts ciphertext produced by AESGCMSeal. The input must be at
// least 12 bytes (nonce size); otherwise ErrCiphertextTooShort is returned.
// key must be exactly 32 bytes.
func AESGCMOpen(key, ct []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("AESGCMOpen: key must be 32 bytes, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("AESGCMOpen: create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("AESGCMOpen: create GCM: %w", err)
	}

	nonceSize := aead.NonceSize()
	if len(ct) < nonceSize {
		return nil, ErrCiphertextTooShort
	}

	nonce, ciphertext := ct[:nonceSize], ct[nonceSize:]
	plain, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("AESGCMOpen: %w", err)
	}

	return plain, nil
}
