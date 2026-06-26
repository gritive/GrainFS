package storage

import (
	"bytes"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/require"
)

func testSeamClusterID() []byte {
	id := make([]byte, 16)
	for i := range id {
		id[i] = byte(i + 1)
	}
	return id
}

func newDEKKeeperAdapterForTest(t *testing.T) *DEKKeeperAdapter {
	t.Helper()
	kek := bytes.Repeat([]byte{0x11}, encrypt.KEKSize)
	keeper, err := encrypt.NewDEKKeeper(kek, testSeamClusterID())
	require.NoError(t, err, "NewDEKKeeper")
	return NewDEKKeeperAdapter(keeper, testSeamClusterID())
}

func TestDEKKeeperAdapter_RoundTrip(t *testing.T) {
	a := newDEKKeeperAdapterForTest(t)
	plain := []byte("segment chunk plaintext")
	fields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldString("key"), encrypt.FieldUint32(3)}

	ct, gen, err := a.Seal(encrypt.DomainShard, fields, plain)
	require.NoError(t, err, "Seal")
	got, err := a.Open(encrypt.DomainShard, fields, gen, ct)
	require.NoError(t, err, "Open")
	require.Equal(t, plain, got)
}

func TestDEKKeeperAdapter_WrongAADFails(t *testing.T) {
	a := newDEKKeeperAdapterForTest(t)
	plain := []byte("payload")
	sealFields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldUint32(3)}
	ct, gen, err := a.Seal(encrypt.DomainShard, sealFields, plain)
	require.NoError(t, err, "Seal")
	openFields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldUint32(4)}
	_, err = a.Open(encrypt.DomainShard, openFields, gen, ct)
	require.Error(t, err, "expected auth failure opening with mismatched AAD fields")
}

func TestDEKKeeperAdapter_CopiesClusterID(t *testing.T) {
	kek := bytes.Repeat([]byte{0x11}, encrypt.KEKSize)
	clusterID := testSeamClusterID()
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err, "NewDEKKeeper")
	wantClusterID := append([]byte(nil), clusterID...)
	a := NewDEKKeeperAdapter(keeper, clusterID)
	for i := range clusterID {
		clusterID[i] = 0xFF
	}
	opener := NewDEKKeeperAdapter(keeper, wantClusterID)

	fields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldUint32(3)}
	ct, gen, err := a.Seal(encrypt.DomainShard, fields, []byte("payload"))
	require.NoError(t, err, "Seal")
	got, err := opener.Open(encrypt.DomainShard, fields, gen, ct)
	require.NoError(t, err, "Open after caller clusterID mutation")
	require.Equal(t, []byte("payload"), got)
}

func TestDEKKeeperAdapter_OldGenOpensAfterRotate(t *testing.T) {
	kek := bytes.Repeat([]byte{0x11}, encrypt.KEKSize)
	keeper, err := encrypt.NewDEKKeeper(kek, testSeamClusterID())
	require.NoError(t, err, "NewDEKKeeper")
	a := NewDEKKeeperAdapter(keeper, testSeamClusterID())
	plain := []byte("payload")
	fields := []encrypt.AADField{encrypt.FieldString("b"), encrypt.FieldUint32(0)}

	ct, gen0, err := a.Seal(encrypt.DomainShard, fields, plain)
	require.NoError(t, err, "Seal")
	require.NoError(t, keeper.Rotate(), "Rotate")
	got, err := a.Open(encrypt.DomainShard, fields, gen0, ct)
	require.NoError(t, err, "Open(gen0) after rotate")
	require.Equal(t, plain, got, "plaintext mismatch after rotate")
}

func TestDEKKeeperAdapter_ImplementsDataEncryptor(t *testing.T) {
	var _ DataEncryptor = (*DEKKeeperAdapter)(nil)
}

// TestTransientDataEncryptor_OpensSameCiphertextAsLiveAdapter — the key
// invariant for MetaFSM.Restore: a transient adapter Open returns the same
// plaintext the live DEKKeeperAdapter would after boot wiring.
func TestTransientDataEncryptor_OpensSameCiphertextAsLiveAdapter(t *testing.T) {
	kek := bytes.Repeat([]byte{0x11}, encrypt.KEKSize)
	clusterID := testSeamClusterID()
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err, "NewDEKKeeper")
	require.NoError(t, keeper.Rotate(), "Rotate")
	live := NewDEKKeeperAdapter(keeper, clusterID)
	fields := []encrypt.AADField{encrypt.FieldString("sa"), encrypt.FieldString("AK")}
	ct, gen, err := live.Seal(encrypt.DomainIAMCredential, fields, []byte("creds"))
	require.NoError(t, err, "live Seal")

	// Build a KEKStore at the same version the keeper currently wraps under
	// (Phase A pins to 0; Rotate does not advance it).
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(0, kek), "KEKStore.Add")
	versions, active := keeper.VersionsAndActive()
	transient, err := encrypt.NewTransientReadOnlyDEK(clusterID, versions, active, 0, store)
	require.NoError(t, err, "NewTransientReadOnlyDEK")
	a := NewTransientDataEncryptor(transient, clusterID)
	got, err := a.Open(encrypt.DomainIAMCredential, fields, gen, ct)
	require.NoError(t, err, "transient Open")
	require.Equal(t, []byte("creds"), got)
}

// TestTransientDataEncryptor_SealIsRefused guards the read-only invariant
// at the adapter boundary: Seal must return encrypt.ErrTransientReadOnly so
// any caller that tries to mutate state during Restore hard-fails.
func TestTransientDataEncryptor_SealIsRefused(t *testing.T) {
	kek := bytes.Repeat([]byte{0x11}, encrypt.KEKSize)
	clusterID := testSeamClusterID()
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err, "NewDEKKeeper")
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(0, kek), "KEKStore.Add")
	versions, active := keeper.VersionsAndActive()
	transient, err := encrypt.NewTransientReadOnlyDEK(clusterID, versions, active, 0, store)
	require.NoError(t, err, "NewTransientReadOnlyDEK")
	a := NewTransientDataEncryptor(transient, clusterID)
	_, _, err = a.Seal(encrypt.DomainIAMCredential, nil, []byte("nope"))
	require.ErrorIs(t, err, encrypt.ErrTransientReadOnly)
}

func TestTransientDataEncryptor_ImplementsDataEncryptor(t *testing.T) {
	var _ DataEncryptor = (*TransientDataEncryptor)(nil)
}

func TestSealToRoundTripsAndMatchesSeal(t *testing.T) {
	cases := []struct {
		name string
		de   DataEncryptor
	}{
		{"dekkeeper", newDEKKeeperAdapterForTest(t)},
	}
	plain := []byte("segment chunk plaintext")
	fields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldString("key"), encrypt.FieldUint32(3)}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ct, gen, err := tc.de.SealTo(make([]byte, 0, 512), encrypt.DomainShard, fields, plain)
			require.NoError(t, err, "SealTo")
			got, err := tc.de.Open(encrypt.DomainShard, fields, gen, ct)
			require.NoError(t, err, "Open")
			require.Equal(t, plain, got)
			ctSeal, _, err := tc.de.Seal(encrypt.DomainShard, fields, plain)
			require.NoError(t, err, "Seal")
			require.Len(t, ctSeal, len(ct))
		})
	}
}

func TestOpenToRoundTripsAndMatchesOpen(t *testing.T) {
	cases := []struct {
		name string
		de   DataEncryptor
	}{
		{"dekkeeper", newDEKKeeperAdapterForTest(t)},
	}
	plain := []byte("segment chunk plaintext")
	fields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldString("key"), encrypt.FieldUint32(3)}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ct, gen, err := tc.de.Seal(encrypt.DomainShard, fields, plain)
			require.NoError(t, err, "Seal")
			gotTo, err := tc.de.OpenTo(make([]byte, 0, 512), encrypt.DomainShard, fields, gen, ct)
			require.NoError(t, err, "OpenTo")
			require.Equal(t, plain, gotTo)
			gotOpen, err := tc.de.Open(encrypt.DomainShard, fields, gen, ct)
			require.NoError(t, err, "Open")
			require.Equal(t, gotOpen, gotTo)
		})
	}
}

// TestOpenToReusesCapacity proves OpenTo writes into the caller's buffer when
// capacity suffices (the spool reader relies on this to avoid per-record allocs).
func TestOpenToReusesCapacity(t *testing.T) {
	de := newDEKKeeperAdapterForTest(t)
	fields := []encrypt.AADField{encrypt.FieldString("k")}
	plain := bytes.Repeat([]byte("p"), 64)
	ct, gen, err := de.Seal(encrypt.DomainShard, fields, plain)
	require.NoError(t, err, "Seal")
	buf := make([]byte, 0, 4096)
	got, err := de.OpenTo(buf, encrypt.DomainShard, fields, gen, ct)
	require.NoError(t, err, "OpenTo")
	require.Same(t, &buf[:1][0], &got[:1][0], "OpenTo reallocated despite sufficient capacity")
}

func TestTransientSealToUnsupported(t *testing.T) {
	de := &TransientDataEncryptor{}
	_, _, err := de.SealTo(nil, encrypt.DomainShard, nil, []byte("x"))
	require.ErrorIs(t, err, encrypt.ErrTransientReadOnly)
}
