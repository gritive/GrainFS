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
	// XAES-256-GCM derives a per-call sub-key (AES-CMAC-based KDF over the 24-byte
	// nonce), which allocates 4 objects per Seal call on the nil-dst path. This is a
	// known regression vs. the old AES-256-GCM path (1 alloc). Bound is set at 5 to
	// catch any future regression beyond the inherent XAES CMAC-KDF baseline.
	assert.LessOrEqual(t, allocs, 5.0, "EncryptWithAAD allocs within XAES baseline (4)")
}

func TestIsEncryptedBlob(t *testing.T) {
	key := make([]byte, 32)
	enc, _ := NewEncryptor(key)

	ciphertext, _ := enc.EncryptWithAAD([]byte("data"), []byte("aad"))
	assert.True(t, IsEncryptedBlob(ciphertext))
	assert.False(t, IsEncryptedBlob([]byte("plaintext")))
	assert.False(t, IsEncryptedBlob([]byte{}))
}

func TestValueEnvelopeRoundTrip(t *testing.T) {
	enc, err := NewEncryptor(bytes.Repeat([]byte{0x11}, 32))
	require.NoError(t, err)

	plaintext := []byte("customer controlled value")
	sealed, err := enc.SealValueAADTo(nil, []byte("badger:meta:object"), plaintext)
	require.NoError(t, err)
	require.True(t, IsEncryptedValue(sealed))
	require.NotContains(t, string(sealed), string(plaintext))

	got, err := enc.OpenValueAAD([]byte("badger:meta:object"), sealed)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestValueEnvelopeSealToReusesDestination(t *testing.T) {
	enc, err := NewEncryptor(bytes.Repeat([]byte{0x11}, 32))
	require.NoError(t, err)
	plaintext := bytes.Repeat([]byte("x"), 64*1024)
	aad := []byte("local-object:physical:chunk:7")
	dst := make([]byte, 0, 3+24+len(plaintext)+enc.AEADOverhead())

	sealed, err := enc.SealValueAADTo(dst, aad, plaintext)
	require.NoError(t, err)
	require.True(t, IsEncryptedValue(sealed))
	require.Equal(t, cap(dst), cap(sealed))

	got, err := enc.OpenValueAAD(aad, sealed)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestValueEnvelopeRejectsWrongDomainAndTamper(t *testing.T) {
	enc, err := NewEncryptor(bytes.Repeat([]byte{0x22}, 32))
	require.NoError(t, err)

	sealed, err := enc.SealValueAADTo(nil, []byte("wal:record"), []byte("mutation body"))
	require.NoError(t, err)

	_, err = enc.OpenValueAAD([]byte("wal:other"), sealed)
	require.Error(t, err)

	sealed[len(sealed)-1] ^= 0x80
	_, err = enc.OpenValueAAD([]byte("wal:record"), sealed)
	require.Error(t, err)
}

func TestValueEnvelopeRejectsPlaintext(t *testing.T) {
	enc, err := NewEncryptor(bytes.Repeat([]byte{0x33}, 32))
	require.NoError(t, err)

	require.False(t, IsEncryptedValue([]byte("plain")))
	_, err = enc.OpenValueAAD([]byte("badger:meta:object"), []byte("plain"))
	require.Error(t, err)
}

func TestIsLegacyEncryptedValue(t *testing.T) {
	// Exact pre-XAES value envelope: 0xAE 0xE2 0x01
	require.True(t, IsLegacyEncryptedValue([]byte{0xAE, 0xE2, 0x01, 0x00})) // old version 1
	// Current XAES value envelope (0x02) is NOT legacy.
	require.False(t, IsLegacyEncryptedValue([]byte{0xAE, 0xE2, 0x02, 0x00}))
	// Value magic with an unrelated version byte must pass through (not legacy).
	require.False(t, IsLegacyEncryptedValue([]byte{0xAE, 0xE2, 0x05, 0x00}))
	require.False(t, IsLegacyEncryptedValue([]byte("plaintext")))      // no magic
	require.False(t, IsLegacyEncryptedValue([]byte{0xAE, 0xE2}))       // too short
	require.False(t, IsLegacyEncryptedValue([]byte{}))                 // empty
	require.False(t, IsLegacyEncryptedValue([]byte{0xAE, 0xE3, 0x01})) // blob magic, not value
}

func TestIsLegacyEncryptedBlob(t *testing.T) {
	// Exact pre-XAES EncryptWithAAD blob: 0xAE 0xE1
	require.True(t, IsLegacyEncryptedBlob([]byte{0xAE, 0xE1, 0x00})) // old blob format
	// Current XAES blob magic (0xE3) is NOT legacy.
	require.False(t, IsLegacyEncryptedBlob([]byte{0xAE, 0xE3, 0x00}))
	// Value magic (0xE2) is not the blob magic.
	require.False(t, IsLegacyEncryptedBlob([]byte{0xAE, 0xE2, 0x01}))
	require.False(t, IsLegacyEncryptedBlob([]byte("plaintext"))) // no magic
	require.False(t, IsLegacyEncryptedBlob([]byte{0xAE}))        // too short
	require.False(t, IsLegacyEncryptedBlob([]byte{}))            // empty
}
