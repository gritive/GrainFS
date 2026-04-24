package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

// Encryptor handles AES-256-GCM encryption/decryption for at-rest data.
type Encryptor struct {
	aead cipher.AEAD
}

// NewEncryptor creates a new AES-256-GCM encryptor from a 32-byte key.
func NewEncryptor(key []byte) (*Encryptor, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("encryption key must be 32 bytes, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	return &Encryptor{aead: aead}, nil
}

// Encrypt encrypts plaintext using AES-256-GCM.
// Returns nonce + ciphertext + tag.
func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	ciphertext := e.aead.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt decrypts data produced by Encrypt.
func (e *Encryptor) Decrypt(data []byte) ([]byte, error) {
	nonceSize := e.aead.NonceSize()
	if len(data) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := e.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}
	if plaintext == nil {
		plaintext = []byte{}
	}

	return plaintext, nil
}

// encMagic is a 2-byte header prepended by EncryptWithAAD to identify
// encrypted blobs and enable downgrade detection.
const encMagic0, encMagic1 = byte(0xAE), byte(0xE1)

// IsEncryptedBlob reports whether data was produced by EncryptWithAAD.
// Used to detect encrypted shards when the encryptor is nil (downgrade guard).
func IsEncryptedBlob(data []byte) bool {
	return len(data) >= 2 && data[0] == encMagic0 && data[1] == encMagic1
}

// EncryptWithAAD encrypts plaintext using AES-256-GCM with Additional
// Authenticated Data. The AAD binds the ciphertext to its storage location so
// that moving an encrypted shard to a different position causes decryption to
// fail. Output format: magic(2) + nonce(12) + ciphertext + tag(16).
func (e *Encryptor) EncryptWithAAD(plaintext, aad []byte) ([]byte, error) {
	// Single allocation: magic(2) + nonce(12) + plaintext + tag(16).
	// nonce is a sub-slice of out (already heap-allocated), so no 2nd alloc.
	out := make([]byte, 2+12, 2+12+len(plaintext)+16)
	out[0] = encMagic0
	out[1] = encMagic1
	nonce := out[2:14]
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	out = e.aead.Seal(out, nonce, plaintext, aad)
	return out, nil
}

// DecryptWithAAD decrypts data produced by EncryptWithAAD using the same AAD.
// Returns an error if the magic header is missing, the ciphertext is too short,
// or the GCM tag does not match (wrong key, wrong AAD, or data corruption).
func (e *Encryptor) DecryptWithAAD(data, aad []byte) ([]byte, error) {
	if !IsEncryptedBlob(data) {
		return nil, fmt.Errorf("not an encrypted blob (missing magic header)")
	}
	inner := data[2:]
	nonceSize := e.aead.NonceSize()
	if len(inner) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, ciphertext := inner[:nonceSize], inner[nonceSize:]
	plaintext, err := e.aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}
	if plaintext == nil {
		plaintext = []byte{}
	}
	return plaintext, nil
}
