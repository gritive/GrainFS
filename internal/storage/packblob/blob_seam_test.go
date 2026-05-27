package packblob

import (
	"bytes"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

// fakeSeam seals via a real Encryptor so AEAD round-trips, and reports a gen
// (always ignored on Open) to prove the seam contract is honored.
type fakeSeam struct {
	enc *encrypt.Encryptor
	gen uint32
}

func newFakeSeam(t *testing.T) *fakeSeam {
	t.Helper()
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x07}, 32))
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	return &fakeSeam{enc: enc, gen: 3}
}

func (f *fakeSeam) aad(domain encrypt.AADDomain, fields []encrypt.AADField) []byte {
	return encrypt.BuildAAD(domain, make([]byte, 16), fields...)
}

func (f *fakeSeam) Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	ct, err := f.enc.SealValueAADTo(nil, f.aad(domain, fields), plain)
	return ct, f.gen, err
}

func (f *fakeSeam) Open(domain encrypt.AADDomain, fields []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	return f.enc.OpenValueAADTo(nil, f.aad(domain, fields), ct)
}

var _ storage.DataEncryptor = (*fakeSeam)(nil)

func TestBlobStore_SeamRoundTrip(t *testing.T) {
	bs, err := newBlobStore(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatalf("newBlobStore: %v", err)
	}
	defer bs.Close()
	bs.segEnc = newFakeSeam(t)

	loc, err := bs.Append("bucket/key", []byte("hello world"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, err := bs.Read(loc)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("round-trip mismatch: %q", got)
	}
}

func TestBlobStore_SeamRejectsWrongLocationAAD(t *testing.T) {
	bs, err := newBlobStore(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatalf("newBlobStore: %v", err)
	}
	defer bs.Close()
	bs.segEnc = newFakeSeam(t)

	loc, err := bs.Append("bucket/key", []byte("payload"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Tamper the recorded offset: the positional AAD must fail to open.
	bad := loc
	bad.Offset = loc.Offset + 1
	if _, err := bs.Read(bad); err == nil {
		t.Fatal("expected open to fail when offset AAD does not match")
	}
}
