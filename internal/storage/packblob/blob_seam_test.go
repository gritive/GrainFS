package packblob

import (
	"bytes"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err, "NewEncryptor")
	return &fakeSeam{enc: enc, gen: 3}
}

func (f *fakeSeam) aad(domain encrypt.AADDomain, fields []encrypt.AADField) []byte {
	return encrypt.BuildAAD(domain, make([]byte, 16), fields...)
}

func (f *fakeSeam) Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	ct, err := f.enc.SealValueAADTo(nil, f.aad(domain, fields), plain)
	return ct, f.gen, err
}

func (f *fakeSeam) SealTo(_ []byte, domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	return f.Seal(domain, fields, plain)
}

func (f *fakeSeam) SealAtGen(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte, _ uint32) ([]byte, error) {
	ct, _, err := f.Seal(domain, fields, plain)
	return ct, err
}

func (f *fakeSeam) SealAtGenTo(_ []byte, domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte, _ uint32) ([]byte, error) {
	ct, _, err := f.Seal(domain, fields, plain)
	return ct, err
}

func (f *fakeSeam) Open(domain encrypt.AADDomain, fields []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	return f.enc.OpenValueAADTo(nil, f.aad(domain, fields), ct)
}

func (f *fakeSeam) OpenTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	return f.enc.OpenValueAADTo(dst, f.aad(domain, fields), ct)
}

var _ storage.DataEncryptor = (*fakeSeam)(nil)

func TestBlobStore_SeamRoundTrip(t *testing.T) {
	bs, err := newBlobStore(t.TempDir(), 1<<20)
	require.NoError(t, err, "newBlobStore")
	defer bs.Close()
	bs.segEnc = newFakeSeam(t)

	loc, err := bs.Append("bucket/key", []byte("hello world"))
	require.NoError(t, err, "Append")
	got, err := bs.Read(loc)
	require.NoError(t, err, "Read")
	require.Equal(t, "hello world", string(got))
}

func TestBlobStore_SeamRejectsWrongLocationAAD(t *testing.T) {
	bs, err := newBlobStore(t.TempDir(), 1<<20)
	require.NoError(t, err, "newBlobStore")
	defer bs.Close()
	bs.segEnc = newFakeSeam(t)

	loc, err := bs.Append("bucket/key", []byte("payload"))
	require.NoError(t, err, "Append")
	// Tamper the recorded offset: the positional AAD must fail to open.
	bad := loc
	bad.Offset = loc.Offset + 1
	_, err = bs.Read(bad)
	require.Error(t, err, "expected open to fail when offset AAD does not match")
}
