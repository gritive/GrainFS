package encrypt

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// newTestKEKStoreWithVersion builds a KEKStore with a single KEK at the
// supplied version using a deterministic key (suitable for tests).
func newTestKEKStoreWithVersion(t *testing.T, version uint32) *KEKStore {
	t.Helper()
	kek := bytes.Repeat([]byte{0x77}, KEKSize)
	store := NewKEKStore()
	require.NoError(t, store.Add(version, kek))
	return store
}

// TestTransientReadOnlyDEK_OpenMatchesLiveKeeper builds a live DEKKeeper
// with two gens, seals plaintext under the active gen, then opens the same
// ciphertext through a TransientReadOnlyDEK built from the keeper's wraps.
// Both must produce identical plaintext.
func TestTransientReadOnlyDEK_OpenMatchesLiveKeeper(t *testing.T) {
	store := newTestKEKStoreWithVersion(t, 0)
	kek, err := store.ActiveKEK()
	require.NoError(t, err)

	// Live keeper at gen 0, then rotate to gen 1.
	k, err := NewDEKKeeper(kek, testClusterID())
	require.NoError(t, err)
	require.NoError(t, k.Rotate())

	plaintext := []byte("hello, transient")
	aad := BuildAAD(DomainIAMCredential, testClusterID(), FieldString("sa-x"), FieldString("AK1"))
	ct, gen, err := k.SealWithAAD(plaintext, aad)
	require.NoError(t, err)
	require.Equal(t, uint32(1), gen, "active gen should be 1 after one rotation")

	// Build a transient over the same wraps.
	versions, active := k.VersionsAndActive()
	transient, err := NewTransientReadOnlyDEK(testClusterID(), versions, active, 0 /*activeKEK*/, store)
	require.NoError(t, err)

	got, err := transient.OpenWithAAD(ct, gen, aad)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestTransientReadOnlyDEK_OpenWithAADToMatchesLive(t *testing.T) {
	store := newTestKEKStoreWithVersion(t, 0)
	kek, err := store.ActiveKEK()
	require.NoError(t, err)

	k, err := NewDEKKeeper(kek, testClusterID())
	require.NoError(t, err)
	require.NoError(t, k.Rotate())

	plaintext := bytes.Repeat([]byte("z"), 5000)
	aad := BuildAAD(DomainIAMCredential, testClusterID(), FieldString("sa-x"), FieldString("AK1"))
	ct, gen, err := k.SealWithAAD(plaintext, aad)
	require.NoError(t, err)

	versions, active := k.VersionsAndActive()
	transient, err := NewTransientReadOnlyDEK(testClusterID(), versions, active, 0, store)
	require.NoError(t, err)

	got, err := transient.OpenWithAADTo(make([]byte, 0, 16), ct, gen, aad)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)

	// Byte-equivalent to the allocating OpenWithAAD.
	gotAlloc, err := transient.OpenWithAAD(ct, gen, aad)
	require.NoError(t, err)
	require.Equal(t, gotAlloc, got)
}

// TestTransientReadOnlyDEK_GenZeroOpens — codex P0 regression: gen 0 is a
// legitimate active gen on a fresh genesis-bootstrap cluster. The
// transient view must Open ciphertext sealed at gen 0.
func TestTransientReadOnlyDEK_GenZeroOpens(t *testing.T) {
	store := newTestKEKStoreWithVersion(t, 0)
	kek, err := store.ActiveKEK()
	require.NoError(t, err)

	k, err := NewDEKKeeper(kek, testClusterID())
	require.NoError(t, err)
	// No Rotate — active stays at 0.

	plaintext := []byte("gen-zero secret")
	aad := BuildAAD(DomainIAMCredential, testClusterID(), FieldString("sa-zero"), FieldString("AK0"))
	ct, gen, err := k.SealWithAAD(plaintext, aad)
	require.NoError(t, err)
	require.Equal(t, uint32(0), gen)

	versions, active := k.VersionsAndActive()
	require.Equal(t, uint32(0), active)

	transient, err := NewTransientReadOnlyDEK(testClusterID(), versions, active, 0, store)
	require.NoError(t, err)
	got, err := transient.OpenWithAAD(ct, gen, aad)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

// TestTransientReadOnlyDEK_RejectsEmptyVersions ensures the constructor's
// guard fires — the surrounding MetaFSM.Restore code relies on this to
// catch a tampered DEK snapshot trailer that decodes to an empty map.
func TestTransientReadOnlyDEK_RejectsEmptyVersions(t *testing.T) {
	store := newTestKEKStoreWithVersion(t, 0)
	_, err := NewTransientReadOnlyDEK(testClusterID(), map[uint32][]byte{}, 0, 0, store)
	require.Error(t, err)
}

// TestTransientReadOnlyDEK_NoLiveKeeperSideEffects guards the read-only
// invariant: building + Open-ing through the transient view must not
// disturb the live keeper's seal counter or active gen for the same
// versions.
func TestTransientReadOnlyDEK_NoLiveKeeperSideEffects(t *testing.T) {
	store := newTestKEKStoreWithVersion(t, 0)
	kek, err := store.ActiveKEK()
	require.NoError(t, err)

	k, err := NewDEKKeeper(kek, testClusterID())
	require.NoError(t, err)

	plaintext := []byte("payload")
	aad := BuildAAD(DomainIAMCredential, testClusterID(), FieldString("sa"), FieldString("AK"))
	ct, gen, err := k.SealWithAAD(plaintext, aad)
	require.NoError(t, err)

	beforeActive := k.ActiveDEKGeneration()
	beforeSeals := k.SealCount(gen)

	versions, active := k.VersionsAndActive()
	transient, err := NewTransientReadOnlyDEK(testClusterID(), versions, active, 0, store)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		_, err := transient.OpenWithAAD(ct, gen, aad)
		require.NoError(t, err)
	}

	require.Equal(t, beforeActive, k.ActiveDEKGeneration(), "transient Open must not advance live active gen")
	require.Equal(t, beforeSeals, k.SealCount(gen), "transient Open must not bump live seal counter")
}
