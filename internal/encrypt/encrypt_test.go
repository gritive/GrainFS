package encrypt

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncryptDecrypt(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	enc, err := NewEncryptor(key)
	require.NoError(t, err)

	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("hello world")},
		{"medium", bytes.Repeat([]byte("x"), 1024)},
		{"large", bytes.Repeat([]byte("data"), 100000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encrypted, err := enc.Encrypt(tt.data)
			require.NoError(t, err)

			// Encrypted data should be different from plaintext
			if len(tt.data) > 0 {
				assert.NotEqual(t, tt.data, encrypted)
			}
			// Encrypted data should be larger (nonce + tag overhead)
			assert.Greater(t, len(encrypted), len(tt.data))

			decrypted, err := enc.Decrypt(encrypted)
			require.NoError(t, err)
			assert.Equal(t, tt.data, decrypted)
		})
	}
}

func TestWrongKeyDecrypt(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	key2[0] = 0xFF

	enc1, _ := NewEncryptor(key1)
	enc2, _ := NewEncryptor(key2)

	encrypted, err := enc1.Encrypt([]byte("secret"))
	require.NoError(t, err)

	_, err = enc2.Decrypt(encrypted)
	assert.Error(t, err, "decryption with wrong key should fail")
}

func TestInvalidKeySize(t *testing.T) {
	_, err := NewEncryptor([]byte("short"))
	assert.Error(t, err)
}

func TestEncryptDecryptWithAAD(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := NewEncryptor(key)

	plaintext := []byte("hello, world")
	aad := []byte("bucket/key/versionID/0")

	ciphertext, err := enc.EncryptWithAAD(plaintext, aad)
	require.NoError(t, err)

	assert.True(t, IsEncryptedBlob(ciphertext), "must have magic header")

	// correct AAD decrypts
	got, err := enc.DecryptWithAAD(ciphertext, aad)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got)

	// wrong AAD fails
	_, err = enc.DecryptWithAAD(ciphertext, []byte("bucket/other-key/0"))
	assert.Error(t, err, "wrong AAD must fail")

	// wrong key fails
	key2 := make([]byte, 32)
	key2[0] = 0xFF
	enc2, _ := NewEncryptor(key2)
	_, err = enc2.DecryptWithAAD(ciphertext, aad)
	assert.Error(t, err, "wrong key must fail")

	// no magic header fails
	_, err = enc.DecryptWithAAD([]byte("notencrypted"), aad)
	assert.Error(t, err, "missing magic must fail")
}

func TestEncryptWithAAD_AllocsBounded(t *testing.T) {
	e, err := NewEncryptor(make([]byte, 32))
	require.NoError(t, err)
	plaintext := make([]byte, 128*1024)
	aad := []byte("bucket/key/0")
	_, _ = e.EncryptWithAAD(plaintext, aad)
	allocs := testing.AllocsPerRun(100, func() {
		_, _ = e.EncryptWithAAD(plaintext, aad)
	})
	assert.LessOrEqual(t, allocs, 1.0, "EncryptWithAAD should allocate exactly 1 (output slice)")
}

func TestIsEncryptedBlob(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := NewEncryptor(key)

	ciphertext, _ := enc.EncryptWithAAD([]byte("data"), []byte("aad"))
	assert.True(t, IsEncryptedBlob(ciphertext))
	assert.False(t, IsEncryptedBlob([]byte("plaintext")))
	assert.False(t, IsEncryptedBlob([]byte{}))
}
