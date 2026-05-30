package encrypt

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func randSalt(t *testing.T) []byte {
	t.Helper()
	b := make([]byte, envKEKSaltSize)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

func TestDeriveEEK_Deterministic(t *testing.T) {
	ikm := []byte("machine-factors")
	salt := randSalt(t)
	k1, err := deriveEEK(ikm, salt)
	require.NoError(t, err)
	require.Len(t, k1, KEKSize)
	k2, err := deriveEEK(ikm, salt)
	require.NoError(t, err)
	require.True(t, bytes.Equal(k1, k2))

	k3, err := deriveEEK(ikm, randSalt(t))
	require.NoError(t, err)
	require.False(t, bytes.Equal(k1, k3), "different salt -> different key")
}

func TestDeriveRecoveryKey_Deterministic(t *testing.T) {
	salt := randSalt(t)
	k1 := deriveRecoveryKey([]byte("passphrase"), salt, argonTimeDefault, argonMemDefault, argonThreadsDefault)
	require.Len(t, k1, KEKSize)
	k2 := deriveRecoveryKey([]byte("passphrase"), salt, argonTimeDefault, argonMemDefault, argonThreadsDefault)
	require.True(t, bytes.Equal(k1, k2))

	k3 := deriveRecoveryKey([]byte("other"), salt, argonTimeDefault, argonMemDefault, argonThreadsDefault)
	require.False(t, bytes.Equal(k1, k3))
}

func TestDeriveKeysUsableAsAEAD(t *testing.T) {
	salt := randSalt(t)
	eek, err := deriveEEK([]byte("ikm"), salt)
	require.NoError(t, err)
	blob, err := AESGCMSealWithAAD(eek, []byte("secret"), []byte("aad"))
	require.NoError(t, err)
	pt, err := AESGCMOpenWithAAD(eek, blob, []byte("aad"))
	require.NoError(t, err)
	require.Equal(t, []byte("secret"), pt)
}
