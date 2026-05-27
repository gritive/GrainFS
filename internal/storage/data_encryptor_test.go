package storage

import (
	"bytes"
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
