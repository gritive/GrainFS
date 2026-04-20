package receipt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestKeyStore(t *testing.T) *KeyStore {
	t.Helper()
	ks, err := NewKeyStore(Key{ID: "psk-2026-04", Secret: []byte("super-secret-key-2026-04")})
	require.NoError(t, err)
	return ks
}

func TestSign_PopulatesPayloadAndSignature(t *testing.T) {
	ks := newTestKeyStore(t)
	r := sampleReceipt()

	err := Sign(r, ks)
	require.NoError(t, err)

	require.Equal(t, "psk-2026-04", r.KeyID, "Sign must overwrite KeyID with the active key id")
	require.NotEmpty(t, r.CanonicalPayload)
	require.NotEmpty(t, r.Signature)
}

func TestSign_RoundTripVerifies(t *testing.T) {
	ks := newTestKeyStore(t)
	r := sampleReceipt()

	require.NoError(t, Sign(r, ks))
	require.NoError(t, Verify(r, ks))
}

func TestVerify_DetectsTamperedField(t *testing.T) {
	ks := newTestKeyStore(t)
	r := sampleReceipt()
	require.NoError(t, Sign(r, ks))

	r.DurationMs = 999999 // mutate a signed field

	err := Verify(r, ks)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSignatureMismatch)
}

func TestVerify_DetectsTamperedSignature(t *testing.T) {
	ks := newTestKeyStore(t)
	r := sampleReceipt()
	require.NoError(t, Sign(r, ks))

	r.Signature = "deadbeef"

	err := Verify(r, ks)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSignatureMismatch)
}

func TestVerify_UnknownKeyID(t *testing.T) {
	ks := newTestKeyStore(t)
	r := sampleReceipt()
	require.NoError(t, Sign(r, ks))

	r.KeyID = "psk-does-not-exist"

	err := Verify(r, ks)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrUnknownKey)
}

func TestSign_FailsOnEmptyKeyStore(t *testing.T) {
	ks := &KeyStore{} // no active key
	r := sampleReceipt()

	err := Sign(r, ks)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNoActiveKey)
}
