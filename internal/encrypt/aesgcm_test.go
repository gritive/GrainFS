package encrypt

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAESGCMRoundTrip(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	plain := []byte("hello, DEK key wrapping")

	ct, err := AESGCMSeal(key, plain)
	require.NoError(t, err)
	require.NotEqual(t, plain, ct, "ciphertext must differ from plaintext")

	got, err := AESGCMOpen(key, ct)
	require.NoError(t, err)
	require.Equal(t, plain, got, "decrypted plaintext must match original")
}

func TestAESGCMOpen_RejectsTamperedTag(t *testing.T) {
	key := bytes.Repeat([]byte{0x11}, 32)
	ct, err := AESGCMSeal(key, []byte("sensitive"))
	require.NoError(t, err)

	ct[len(ct)-1] ^= 0xFF // flip last byte (tag)

	_, err = AESGCMOpen(key, ct)
	require.Error(t, err, "tampered tag must be rejected")
}

func TestAESGCMOpen_RejectsWrongKey(t *testing.T) {
	k1 := bytes.Repeat([]byte{0xAA}, 32)
	k2 := bytes.Repeat([]byte{0xBB}, 32)

	ct, err := AESGCMSeal(k1, []byte("secret payload"))
	require.NoError(t, err)

	_, err = AESGCMOpen(k2, ct)
	require.Error(t, err, "wrong key must be rejected")
}
