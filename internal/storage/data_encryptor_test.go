package storage

import (
	"bytes"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func testSeamClusterID() []byte {
	id := make([]byte, 16)
	for i := range id {
		id[i] = byte(i + 1)
	}
	return id
}

func newEncryptorAdapterForTest(t *testing.T) *EncryptorAdapter {
	t.Helper()
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x07}, 32))
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	return NewEncryptorAdapter(enc, testSeamClusterID())
}

func TestEncryptorAdapter_RoundTrip(t *testing.T) {
	a := newEncryptorAdapterForTest(t)
	plain := []byte("segment chunk plaintext")
	fields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldString("key"), encrypt.FieldUint32(3)}

	ct, gen, err := a.Seal(encrypt.DomainShard, fields, plain)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if gen != 0 {
		t.Fatalf("Encryptor adapter must seal at sentinel gen 0, got %d", gen)
	}
	got, err := a.Open(encrypt.DomainShard, fields, gen, ct)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("plaintext mismatch: got %q want %q", got, plain)
	}
}

func TestEncryptorAdapter_WrongAADFails(t *testing.T) {
	a := newEncryptorAdapterForTest(t)
	plain := []byte("payload")
	sealFields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldString("key"), encrypt.FieldUint32(3)}
	ct, gen, err := a.Seal(encrypt.DomainShard, sealFields, plain)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	openFields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldString("key"), encrypt.FieldUint32(4)}
	if _, err := a.Open(encrypt.DomainShard, openFields, gen, ct); err == nil {
		t.Fatal("expected auth failure opening with mismatched AAD fields")
	}
	if _, err := a.Open(encrypt.DomainWAL, sealFields, gen, ct); err == nil {
		t.Fatal("expected auth failure opening under a different domain")
	}
}

func TestEncryptorAdapter_IgnoresGenOnOpen(t *testing.T) {
	a := newEncryptorAdapterForTest(t)
	plain := []byte("payload")
	fields := []encrypt.AADField{encrypt.FieldString("b"), encrypt.FieldUint32(0)}
	ct, _, err := a.Seal(encrypt.DomainShard, fields, plain)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	got, err := a.Open(encrypt.DomainShard, fields, 99, ct)
	if err != nil {
		t.Fatalf("Open with ignored gen: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatal("plaintext mismatch")
	}
}

func TestEncryptorAdapter_ImplementsDataEncryptor(t *testing.T) {
	var _ DataEncryptor = (*EncryptorAdapter)(nil)
}

func newDEKKeeperAdapterForTest(t *testing.T) *DEKKeeperAdapter {
	t.Helper()
	kek := bytes.Repeat([]byte{0x11}, encrypt.KEKSize)
	keeper, err := encrypt.NewDEKKeeper(kek, testSeamClusterID())
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	return NewDEKKeeperAdapter(keeper, testSeamClusterID())
}

func TestDEKKeeperAdapter_RoundTrip(t *testing.T) {
	a := newDEKKeeperAdapterForTest(t)
	plain := []byte("segment chunk plaintext")
	fields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldString("key"), encrypt.FieldUint32(3)}

	ct, gen, err := a.Seal(encrypt.DomainShard, fields, plain)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	got, err := a.Open(encrypt.DomainShard, fields, gen, ct)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("plaintext mismatch: got %q want %q", got, plain)
	}
}

func TestDEKKeeperAdapter_WrongAADFails(t *testing.T) {
	a := newDEKKeeperAdapterForTest(t)
	plain := []byte("payload")
	sealFields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldUint32(3)}
	ct, gen, err := a.Seal(encrypt.DomainShard, sealFields, plain)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	openFields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldUint32(4)}
	if _, err := a.Open(encrypt.DomainShard, openFields, gen, ct); err == nil {
		t.Fatal("expected auth failure opening with mismatched AAD fields")
	}
}

func TestDEKKeeperAdapter_CopiesClusterID(t *testing.T) {
	kek := bytes.Repeat([]byte{0x11}, encrypt.KEKSize)
	clusterID := testSeamClusterID()
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	wantClusterID := append([]byte(nil), clusterID...)
	a := NewDEKKeeperAdapter(keeper, clusterID)
	for i := range clusterID {
		clusterID[i] = 0xFF
	}
	opener := NewDEKKeeperAdapter(keeper, wantClusterID)

	fields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldUint32(3)}
	ct, gen, err := a.Seal(encrypt.DomainShard, fields, []byte("payload"))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	got, err := opener.Open(encrypt.DomainShard, fields, gen, ct)
	if err != nil {
		t.Fatalf("Open after caller clusterID mutation: %v", err)
	}
	if !bytes.Equal(got, []byte("payload")) {
		t.Fatalf("plaintext mismatch: got %q", got)
	}
}

func TestDEKKeeperAdapter_OldGenOpensAfterRotate(t *testing.T) {
	kek := bytes.Repeat([]byte{0x11}, encrypt.KEKSize)
	keeper, err := encrypt.NewDEKKeeper(kek, testSeamClusterID())
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	a := NewDEKKeeperAdapter(keeper, testSeamClusterID())
	plain := []byte("payload")
	fields := []encrypt.AADField{encrypt.FieldString("b"), encrypt.FieldUint32(0)}

	ct, gen0, err := a.Seal(encrypt.DomainShard, fields, plain)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := keeper.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	got, err := a.Open(encrypt.DomainShard, fields, gen0, ct)
	if err != nil {
		t.Fatalf("Open(gen0) after rotate: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatal("plaintext mismatch after rotate")
	}
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
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	if err := keeper.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	live := NewDEKKeeperAdapter(keeper, clusterID)
	fields := []encrypt.AADField{encrypt.FieldString("sa"), encrypt.FieldString("AK")}
	ct, gen, err := live.Seal(encrypt.DomainIAMCredential, fields, []byte("creds"))
	if err != nil {
		t.Fatalf("live Seal: %v", err)
	}

	// Build a KEKStore at the same version the keeper currently wraps under
	// (Phase A pins to 0; Rotate does not advance it).
	store := encrypt.NewKEKStore()
	if err := store.Add(0, kek); err != nil {
		t.Fatalf("KEKStore.Add: %v", err)
	}
	versions, active := keeper.VersionsAndActive()
	transient, err := encrypt.NewTransientReadOnlyDEK(clusterID, versions, active, 0, store)
	if err != nil {
		t.Fatalf("NewTransientReadOnlyDEK: %v", err)
	}
	a := NewTransientDataEncryptor(transient, clusterID)
	got, err := a.Open(encrypt.DomainIAMCredential, fields, gen, ct)
	if err != nil {
		t.Fatalf("transient Open: %v", err)
	}
	if !bytes.Equal(got, []byte("creds")) {
		t.Fatalf("plaintext mismatch: got %q", got)
	}
}

// TestTransientDataEncryptor_SealIsRefused guards the read-only invariant
// at the adapter boundary: Seal must return encrypt.ErrTransientReadOnly so
// any caller that tries to mutate state during Restore hard-fails.
func TestTransientDataEncryptor_SealIsRefused(t *testing.T) {
	kek := bytes.Repeat([]byte{0x11}, encrypt.KEKSize)
	clusterID := testSeamClusterID()
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	if err != nil {
		t.Fatalf("NewDEKKeeper: %v", err)
	}
	store := encrypt.NewKEKStore()
	if err := store.Add(0, kek); err != nil {
		t.Fatalf("KEKStore.Add: %v", err)
	}
	versions, active := keeper.VersionsAndActive()
	transient, err := encrypt.NewTransientReadOnlyDEK(clusterID, versions, active, 0, store)
	if err != nil {
		t.Fatalf("NewTransientReadOnlyDEK: %v", err)
	}
	a := NewTransientDataEncryptor(transient, clusterID)
	_, _, err = a.Seal(encrypt.DomainIAMCredential, nil, []byte("nope"))
	if !errors.Is(err, encrypt.ErrTransientReadOnly) {
		t.Fatalf("Seal must return ErrTransientReadOnly, got %v", err)
	}
}

func TestTransientDataEncryptor_ImplementsDataEncryptor(t *testing.T) {
	var _ DataEncryptor = (*TransientDataEncryptor)(nil)
}

func TestSealToRoundTripsAndMatchesSeal(t *testing.T) {
	cases := []struct {
		name string
		de   DataEncryptor
	}{
		{"encryptor", newEncryptorAdapterForTest(t)},
		{"dekkeeper", newDEKKeeperAdapterForTest(t)},
	}
	plain := []byte("segment chunk plaintext")
	fields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldString("key"), encrypt.FieldUint32(3)}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ct, gen, err := tc.de.SealTo(make([]byte, 0, 512), encrypt.DomainShard, fields, plain)
			if err != nil {
				t.Fatalf("SealTo: %v", err)
			}
			got, err := tc.de.Open(encrypt.DomainShard, fields, gen, ct)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if !bytes.Equal(got, plain) {
				t.Fatalf("plaintext mismatch: got %q want %q", got, plain)
			}
			ctSeal, _, err := tc.de.Seal(encrypt.DomainShard, fields, plain)
			if err != nil {
				t.Fatalf("Seal: %v", err)
			}
			if len(ctSeal) != len(ct) {
				t.Fatalf("ciphertext len mismatch: Seal=%d SealTo=%d", len(ctSeal), len(ct))
			}
		})
	}
}

func TestOpenToRoundTripsAndMatchesOpen(t *testing.T) {
	cases := []struct {
		name string
		de   DataEncryptor
	}{
		{"encryptor", newEncryptorAdapterForTest(t)},
		{"dekkeeper", newDEKKeeperAdapterForTest(t)},
	}
	plain := []byte("segment chunk plaintext")
	fields := []encrypt.AADField{encrypt.FieldString("bucket"), encrypt.FieldString("key"), encrypt.FieldUint32(3)}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ct, gen, err := tc.de.Seal(encrypt.DomainShard, fields, plain)
			if err != nil {
				t.Fatalf("Seal: %v", err)
			}
			gotTo, err := tc.de.OpenTo(make([]byte, 0, 512), encrypt.DomainShard, fields, gen, ct)
			if err != nil {
				t.Fatalf("OpenTo: %v", err)
			}
			if !bytes.Equal(gotTo, plain) {
				t.Fatalf("OpenTo plaintext mismatch: got %q want %q", gotTo, plain)
			}
			gotOpen, err := tc.de.Open(encrypt.DomainShard, fields, gen, ct)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if !bytes.Equal(gotOpen, gotTo) {
				t.Fatalf("OpenTo != Open: %q vs %q", gotTo, gotOpen)
			}
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
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	buf := make([]byte, 0, 4096)
	got, err := de.OpenTo(buf, encrypt.DomainShard, fields, gen, ct)
	if err != nil {
		t.Fatalf("OpenTo: %v", err)
	}
	if &buf[:1][0] != &got[:1][0] {
		t.Fatalf("OpenTo reallocated despite sufficient capacity")
	}
}

func TestTransientSealToUnsupported(t *testing.T) {
	de := &TransientDataEncryptor{}
	_, _, err := de.SealTo(nil, encrypt.DomainShard, nil, []byte("x"))
	if !errors.Is(err, encrypt.ErrTransientReadOnly) {
		t.Fatalf("SealTo must return ErrTransientReadOnly, got %v", err)
	}
}
