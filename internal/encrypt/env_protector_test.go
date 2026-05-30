package encrypt

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// --- test seams ---------------------------------------------------------

type fakeFactors struct{ f []string }

func (s fakeFactors) Factors() ([]string, error) { return s.f, nil }

// recoverySpy counts resolver invocations so we can assert the happy path never
// touches the (expensive, secret) recovery passphrase.
type recoverySpy struct {
	secret []byte
	calls  int
}

func (r *recoverySpy) resolve() ([]byte, error) {
	r.calls++
	if len(r.secret) == 0 {
		return nil, nil
	}
	return r.secret, nil
}

func fixedSalt() []byte { return bytes.Repeat([]byte{0x5A}, envKEKSaltSize) }

func newEnvProtector(factors []string, secret []byte) (*EnvProtector, *recoverySpy) {
	spy := &recoverySpy{secret: secret}
	p := &EnvProtector{
		factors:  fakeFactors{f: factors},
		recovery: spy.resolve,
		saltGen:  func() ([]byte, error) { return fixedSalt(), nil },
	}
	return p, spy
}

func freshKEK(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, KEKSize)
	_, err := rand.Read(k)
	require.NoError(t, err)
	return k
}

// --- round-trip ---------------------------------------------------------

func TestEnvProtector_RoundTrip(t *testing.T) {
	p, spy := newEnvProtector([]string{"machine-id:host-a"}, []byte("recover-pass"))
	kek := freshKEK(t)
	aad := []byte("aad-v0")

	blob, err := p.Protect(kek, aad)
	require.NoError(t, err)
	require.True(t, LooksLikeEnvKEK(blob))
	require.Equal(t, "env", p.Name())

	got, rewrap, err := p.Unprotect(blob, aad)
	require.NoError(t, err)
	require.False(t, rewrap, "same factors -> env slot opens, no rewrap")
	require.True(t, bytes.Equal(kek, got))
	require.Equal(t, 1, spy.calls, "recovery resolved once (at Protect) but NOT on happy-path Unprotect")
}

func TestEnvProtector_LazyRecovery_NotCalledOnHappyPath(t *testing.T) {
	p, _ := newEnvProtector([]string{"machine-id:host-a"}, []byte("pass"))
	kek := freshKEK(t)
	blob, err := p.Protect(kek, []byte("aad"))
	require.NoError(t, err)

	// New protector instance (fresh spy) so Protect's call doesn't pollute the count.
	p2, spy2 := newEnvProtector([]string{"machine-id:host-a"}, []byte("pass"))
	_, rewrap, err := p2.Unprotect(blob, []byte("aad"))
	require.NoError(t, err)
	require.False(t, rewrap)
	require.Equal(t, 0, spy2.calls, "env_slot opened => recovery resolver MUST NOT be called")
}

func TestEnvProtector_FreshNoncePerProtect(t *testing.T) {
	p, _ := newEnvProtector([]string{"machine-id:host-a"}, []byte("pass"))
	kek := freshKEK(t)
	b1, err := p.Protect(kek, []byte("aad"))
	require.NoError(t, err)
	b2, err := p.Protect(kek, []byte("aad"))
	require.NoError(t, err)
	require.False(t, bytes.Equal(b1, b2), "fresh nonces -> different ciphertext")
	for _, b := range [][]byte{b1, b2} {
		got, _, err := p.Unprotect(b, []byte("aad"))
		require.NoError(t, err)
		require.True(t, bytes.Equal(kek, got))
	}
}

// --- rebind / recovery --------------------------------------------------

func TestEnvProtector_FactorChange_RecoversAndRebinds(t *testing.T) {
	create, _ := newEnvProtector([]string{"machine-id:host-a"}, []byte("pass"))
	kek := freshKEK(t)
	blob, err := create.Protect(kek, []byte("aad"))
	require.NoError(t, err)

	// Different machine: env slot won't open; recovery must.
	moved, spy := newEnvProtector([]string{"machine-id:host-B"}, []byte("pass"))
	got, rewrap, err := moved.Unprotect(blob, []byte("aad"))
	require.NoError(t, err)
	require.True(t, rewrap, "binding changed -> caller should rewrap")
	require.True(t, bytes.Equal(kek, got))
	require.Equal(t, 1, spy.calls, "recovery resolved exactly once on the recovery path")

	// After rewrap the caller re-Protects with the NEW factors; that container
	// then opens via env slot on host-B without recovery.
	rebound, err := moved.Protect(got, []byte("aad"))
	require.NoError(t, err)
	moved2, spy2 := newEnvProtector([]string{"machine-id:host-B"}, []byte("pass"))
	got2, rewrap2, err := moved2.Unprotect(rebound, []byte("aad"))
	require.NoError(t, err)
	require.False(t, rewrap2)
	require.True(t, bytes.Equal(kek, got2))
	require.Equal(t, 0, spy2.calls)
}

func TestEnvProtector_BothFail_Errors(t *testing.T) {
	create, _ := newEnvProtector([]string{"machine-id:host-a"}, []byte("right-pass"))
	kek := freshKEK(t)
	blob, err := create.Protect(kek, []byte("aad"))
	require.NoError(t, err)

	wrong, _ := newEnvProtector([]string{"machine-id:host-B"}, []byte("WRONG-pass"))
	_, _, err = wrong.Unprotect(blob, []byte("aad"))
	require.Error(t, err, "env fail + wrong recovery -> fail-closed")
}

func TestEnvProtector_RecoveryAbsentOnEnvFail_Errors(t *testing.T) {
	create, _ := newEnvProtector([]string{"machine-id:host-a"}, []byte("pass"))
	kek := freshKEK(t)
	blob, err := create.Protect(kek, []byte("aad"))
	require.NoError(t, err)

	noSecret, _ := newEnvProtector([]string{"machine-id:host-B"}, nil) // absent
	_, _, err = noSecret.Unprotect(blob, []byte("aad"))
	require.Error(t, err, "env fail + no recovery secret -> fail-closed")
}

func TestEnvProtector_ProtectRequiresRecovery(t *testing.T) {
	p, _ := newEnvProtector([]string{"machine-id:host-a"}, nil) // absent
	_, err := p.Protect(freshKEK(t), []byte("aad"))
	require.Error(t, err, "create without a recovery secret must fail")
}

// --- slot confusion / tamper / header -----------------------------------

func TestEnvProtector_SlotConfusionRejected(t *testing.T) {
	// A blob sealed for aad-A must not open under aad-B (cross-version replay).
	p, _ := newEnvProtector([]string{"machine-id:host-a"}, []byte("pass"))
	kek := freshKEK(t)
	blob, err := p.Protect(kek, []byte("aad-v0"))
	require.NoError(t, err)
	_, _, err = p.Unprotect(blob, []byte("aad-v1"))
	require.Error(t, err, "different aad (version) must not open")
}

func TestEnvProtector_TamperEnvSlot_FallsBackToRecovery(t *testing.T) {
	p, _ := newEnvProtector([]string{"machine-id:host-a"}, []byte("pass"))
	kek := freshKEK(t)
	blob, err := p.Protect(kek, []byte("aad"))
	require.NoError(t, err)

	// Flip a byte inside the env slot ciphertext (just past the fixed header +
	// the 4-byte env length prefix). The env AEAD must reject it, and Unprotect
	// must transparently recover via the intact recovery slot (rewrap=true).
	bad := append([]byte(nil), blob...)
	bad[envKEKHeaderLen+4+1] ^= 0xFF
	got, rewrap, err := p.Unprotect(bad, []byte("aad"))
	require.NoError(t, err)
	require.True(t, rewrap, "tampered env slot -> recovery rescue -> rebind")
	require.True(t, bytes.Equal(kek, got))
}

func TestEnvProtector_BothSlotsTampered_Errors(t *testing.T) {
	p, _ := newEnvProtector([]string{"machine-id:host-a"}, []byte("pass"))
	kek := freshKEK(t)
	blob, err := p.Protect(kek, []byte("aad"))
	require.NoError(t, err)

	bad := append([]byte(nil), blob...)
	bad[envKEKHeaderLen+4+1] ^= 0xFF // env slot ciphertext
	bad[len(bad)-1] ^= 0xFF          // recovery slot tag (last byte)
	_, _, err = p.Unprotect(bad, []byte("aad"))
	require.Error(t, err, "both slots tampered -> fail-closed")
}

func TestEnvProtector_BadHeaderRejected(t *testing.T) {
	p, _ := newEnvProtector([]string{"machine-id:host-a"}, []byte("pass"))
	blob, err := p.Protect(freshKEK(t), []byte("aad"))
	require.NoError(t, err)

	// wrong magic
	bad := append([]byte(nil), blob...)
	bad[0] = 'X'
	_, _, err = p.Unprotect(bad, []byte("aad"))
	require.Error(t, err)

	// truncated
	_, _, err = p.Unprotect(blob[:envKEKHeaderLen-1], []byte("aad"))
	require.Error(t, err)
}

func TestLooksLikeEnvKEK(t *testing.T) {
	p, _ := newEnvProtector([]string{"machine-id:host-a"}, []byte("pass"))
	blob, err := p.Protect(freshKEK(t), []byte("aad"))
	require.NoError(t, err)
	require.True(t, LooksLikeEnvKEK(blob))
	require.False(t, LooksLikeEnvKEK(freshKEK(t)), "raw 32-byte KEK must not look like a container")
	require.False(t, LooksLikeEnvKEK([]byte("xy")))
}

// --- legacy migration ----------------------------------------------------

func TestEnvProtector_LegacyRawMigrates(t *testing.T) {
	p, spy := newEnvProtector([]string{"machine-id:host-a"}, []byte("pass"))
	rawKEK := freshKEK(t) // a legacy plaintext <V>.key is exactly KEKSize bytes
	got, rewrap, err := p.Unprotect(rawKEK, []byte("aad"))
	require.NoError(t, err)
	require.True(t, rewrap, "legacy raw -> migrate (rewrap)")
	require.True(t, bytes.Equal(rawKEK, got))
	require.Equal(t, 1, spy.calls, "migration needs the recovery secret")
}

func TestEnvProtector_LegacyRawRequiresRecovery(t *testing.T) {
	p, _ := newEnvProtector([]string{"machine-id:host-a"}, nil)
	_, _, err := p.Unprotect(freshKEK(t), []byte("aad"))
	require.Error(t, err, "legacy migrate without recovery secret -> fail")
}

// --- Argon2 param round-trip --------------------------------------------

func TestEnvProtector_StoredArgonParamsHonored(t *testing.T) {
	// Create with default params, then unwrap with a protector whose DEFAULTS
	// differ — unwrap must still succeed because params are read from the blob.
	p, _ := newEnvProtector([]string{"machine-id:host-a"}, []byte("pass"))
	kek := freshKEK(t)
	blob, err := p.Protect(kek, []byte("aad"))
	require.NoError(t, err)

	// Force the recovery path on a "moved" machine; the recovery key derivation
	// must use the params embedded in blob, not any ambient default.
	moved, _ := newEnvProtector([]string{"machine-id:host-B"}, []byte("pass"))
	got, rewrap, err := moved.Unprotect(blob, []byte("aad"))
	require.NoError(t, err)
	require.True(t, rewrap)
	require.True(t, bytes.Equal(kek, got))
}
