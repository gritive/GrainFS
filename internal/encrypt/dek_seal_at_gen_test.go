package encrypt

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// newTestKeeper returns a genesis DEKKeeper (gen 0 installed).
func newTestKeeper(t *testing.T) *DEKKeeper {
	t.Helper()
	kek := make([]byte, KEKSize)
	_, err := rand.Read(kek)
	require.NoError(t, err)
	k, err := NewDEKKeeper(kek, testClusterID())
	require.NoError(t, err)
	return k
}

// At gen == active, SealWithAADToAtGen must behave exactly like SealWithAADTo:
// same gen selection, same framing, decryptable with the same AAD.
func TestSealWithAADToAtGen_ActiveGenRoundTrips(t *testing.T) {
	k := newTestKeeper(t)
	plain := []byte("seal-at-active-gen payload")
	aad := []byte("aad-bind")

	ct, err := k.SealWithAADToAtGen(nil, plain, aad, k.ActiveDEKGeneration())
	require.NoError(t, err)

	got, err := k.OpenWithAAD(ct, k.ActiveDEKGeneration(), aad)
	require.NoError(t, err)
	require.Equal(t, plain, got)
}

// After a rotation the prior generation stays resident, so sealing AT the old
// gen still succeeds and round-trips — this is exactly the EC mid-shard race
// window where chunk 1+ must pin chunk 0's (now-retired) gen.
func TestSealWithAADToAtGen_RetiredGenStillResident(t *testing.T) {
	k := newTestKeeper(t)
	oldGen := k.ActiveDEKGeneration()

	require.NoError(t, k.Rotate())
	newGen := k.ActiveDEKGeneration()
	require.NotEqual(t, oldGen, newGen, "rotation must advance the active gen")

	plain := []byte("pinned to the retired gen")
	aad := []byte("aad")
	ct, err := k.SealWithAADToAtGen(nil, plain, aad, oldGen)
	require.NoError(t, err)

	got, err := k.OpenWithAAD(ct, oldGen, aad)
	require.NoError(t, err)
	require.Equal(t, plain, got)
}

// An unknown generation fails closed with ErrDEKGenUnknown (no silent fallback
// to the active gen).
func TestSealWithAADToAtGen_UnknownGenFailsClosed(t *testing.T) {
	k := newTestKeeper(t)
	_, err := k.SealWithAADToAtGen(nil, []byte("x"), []byte("a"), 999)
	require.ErrorIs(t, err, ErrDEKGenUnknown)
}

// Nonce-exhaustion accounting: a seal at the active gen bumps the live counter;
// a seal at a retired gen adds to that gen's frozen count (SealCount reflects
// each).
func TestSealWithAADToAtGen_AccountsUnderCorrectGen(t *testing.T) {
	k := newTestKeeper(t)
	oldGen := k.ActiveDEKGeneration()

	beforeActive := k.SealCount(oldGen)
	_, err := k.SealWithAADToAtGen(nil, []byte("a"), []byte("aad"), oldGen)
	require.NoError(t, err)
	require.Equal(t, beforeActive+1, k.SealCount(oldGen), "active-gen seal bumps the live count")

	require.NoError(t, k.Rotate()) // freezes oldGen's count, makes newGen active
	frozenOld := k.SealCount(oldGen)

	// Seal AT the now-retired oldGen: must add to its frozen count, not active's.
	_, err = k.SealWithAADToAtGen(nil, []byte("b"), []byte("aad"), oldGen)
	require.NoError(t, err)
	require.Equal(t, frozenOld+1, k.SealCount(oldGen), "retired-gen seal bumps the retired count")
}
