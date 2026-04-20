package receipt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewKeyStore_RejectsEmptyKey(t *testing.T) {
	_, err := NewKeyStore(Key{ID: "", Secret: []byte("x")})
	require.Error(t, err)

	_, err = NewKeyStore(Key{ID: "k1", Secret: nil})
	require.Error(t, err)
}

func TestKeyStore_ActiveReturnsCurrent(t *testing.T) {
	ks, err := NewKeyStore(Key{ID: "psk-2026-04", Secret: []byte("secret-04")})
	require.NoError(t, err)

	got, ok := ks.Active()
	require.True(t, ok)
	require.Equal(t, "psk-2026-04", got.ID)
	require.Equal(t, []byte("secret-04"), got.Secret)
}

func TestKeyStore_LookupActive(t *testing.T) {
	ks, err := NewKeyStore(Key{ID: "k1", Secret: []byte("s1")})
	require.NoError(t, err)

	k, ok := ks.Lookup("k1")
	require.True(t, ok)
	require.Equal(t, []byte("s1"), k.Secret)
}

func TestKeyStore_Rotate_PreservesPrevious(t *testing.T) {
	ks, err := NewKeyStore(Key{ID: "k1", Secret: []byte("s1")})
	require.NoError(t, err)

	// Sign something with k1.
	r1 := sampleReceipt()
	require.NoError(t, Sign(r1, ks))
	require.Equal(t, "k1", r1.KeyID)

	// Rotate: k2 becomes active, k1 stays verifiable.
	err = ks.Rotate(Key{ID: "k2", Secret: []byte("s2")})
	require.NoError(t, err)

	active, _ := ks.Active()
	require.Equal(t, "k2", active.ID)

	// Old receipt still verifies via previous key.
	require.NoError(t, Verify(r1, ks))

	// New signing uses k2.
	r2 := sampleReceipt()
	require.NoError(t, Sign(r2, ks))
	require.Equal(t, "k2", r2.KeyID)
	require.NoError(t, Verify(r2, ks))
}

func TestKeyStore_Rotate_EvictsPastRetention(t *testing.T) {
	// Retention = 1 previous key; rotating twice drops the oldest.
	ks, err := NewKeyStore(Key{ID: "k1", Secret: []byte("s1")}, WithPreviousRetention(1))
	require.NoError(t, err)

	r1 := sampleReceipt()
	require.NoError(t, Sign(r1, ks))

	require.NoError(t, ks.Rotate(Key{ID: "k2", Secret: []byte("s2")}))
	require.NoError(t, Verify(r1, ks), "k1 still within retention (1 previous)")

	require.NoError(t, ks.Rotate(Key{ID: "k3", Secret: []byte("s3")}))
	err = Verify(r1, ks)
	require.Error(t, err, "k1 must be evicted past retention")
	require.ErrorIs(t, err, ErrUnknownKey)
}

func TestKeyStore_Rotate_RejectsDuplicateID(t *testing.T) {
	ks, err := NewKeyStore(Key{ID: "k1", Secret: []byte("s1")})
	require.NoError(t, err)

	err = ks.Rotate(Key{ID: "k1", Secret: []byte("s1-rotated")})
	require.Error(t, err, "rotation must not reuse the active key id")
}
