package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// writeEncryptedSpoolRecordFile stages `payload` to disk as encrypted spool
// records via encryptedSpoolRecordWriter + copyToSpoolChunked — the exact codec
// multipart UploadPart uses (the PUT-body spool was removed; this per-record
// codec survives for disk-staged multipart parts). copyToSpoolChunked (not a
// single w.Write) is required so the payload is split into
// spoolCopyBufferSize-bounded records, which the multi-record reader-reuse and
// streaming tests below depend on. Returns the staged file path.
func writeEncryptedSpoolRecordFile(t *testing.T, dir string, seam storage.DataEncryptor, domain string, payload []byte) string {
	t.Helper()
	path := filepath.Join(dir, "encrypted-record")
	f, err := os.Create(path)
	require.NoError(t, err)
	w := &encryptedSpoolRecordWriter{w: f, seam: seam, domain: domain}
	n, err := copyToSpoolChunked(w, bytes.NewReader(payload))
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), n)
	require.NoError(t, f.Close())
	return path
}

func TestEncryptedSpoolRecordHidesPlaintext(t *testing.T) {
	seam := newClusterTestSeam(t)
	payload := []byte("sensitive cluster spool payload")
	path := writeEncryptedSpoolRecordFile(t, t.TempDir(), seam, "cluster-spool:test", payload)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(raw), string(payload))

	rc, err := openSpoolEncryptedRecordFile(path, seam, "cluster-spool:test")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got)
}

func TestEncryptedSpoolRecordRoundTripsViaDEKSeam(t *testing.T) {
	clusterID := bytes.Repeat([]byte("c"), 16)
	kek := bytes.Repeat([]byte{0x77}, encrypt.KEKSize)
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err)
	seam := storage.NewDEKKeeperAdapter(keeper, clusterID)

	plain := bytes.Repeat([]byte("spool-bytes-"), 200_000) // > 1 MiB, multiple records
	path := writeEncryptedSpoolRecordFile(t, t.TempDir(), seam, "cluster-spool:test", plain)

	// On-disk record file must be ciphertext, not the plaintext run.
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.False(t, bytes.Contains(raw, plain[:4096]), "record file must not contain plaintext")

	// Reads back to the exact plaintext via the seam.
	rc, err := openSpoolEncryptedRecordFile(path, seam, "cluster-spool:test")
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plain, got)
}

func TestEncryptedSpoolRecordOpenStreamsWithoutDecryptingFutureRecords(t *testing.T) {
	seam := newClusterTestSeam(t)
	payload := append(bytes.Repeat([]byte("a"), spoolCopyBufferSize), bytes.Repeat([]byte("b"), spoolCopyBufferSize)...)
	path := writeEncryptedSpoolRecordFile(t, t.TempDir(), seam, "cluster-spool:stream", payload)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var hdr [12]byte
	_, err = f.ReadAt(hdr[:], 0)
	require.NoError(t, err)
	firstBlobLen := binary.BigEndian.Uint32(hdr[4:8])
	secondBodyOffset := int64(12 + int(firstBlobLen) + 12)
	_, err = f.Seek(secondBodyOffset, io.SeekStart)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x00})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	rc, err := openSpoolEncryptedRecordFile(path, seam, "cluster-spool:stream")
	require.NoError(t, err)
	defer rc.Close()
	buf := make([]byte, 32)
	n, err := rc.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, bytes.Repeat([]byte("a"), len(buf)), buf)
}

func TestCopyToSpoolChunkedHandlesLargeReaders(t *testing.T) {
	// A naive io.Copy(spoolWriter, bytes.NewReader(large)) would invoke
	// bytes.Reader.WriteTo, producing one giant sealed record that the
	// reader rejects as "blob too large". multipart UploadPart hit this
	// when warp pushed 5 MiB parts through the encrypted spool path.
	// copyToSpoolChunked must keep every record within the invariant.
	seam := newClusterTestSeam(t)
	dir := t.TempDir()
	path := dir + "/part"
	f, err := os.Create(path)
	require.NoError(t, err)
	domain := "spool:test-large"
	w := &encryptedSpoolRecordWriter{w: f, seam: seam, domain: domain}

	// Use a bytes.Reader so WriteTo is implemented; the helper must still
	// chunk the copy through a spoolCopyBufferSize-sized buffer.
	payload := bytes.Repeat([]byte("multipart-part-byte"), (5*spoolCopyBufferSize)/19+1)
	n, err := copyToSpoolChunked(w, bytes.NewReader(payload))
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), n)
	require.NoError(t, f.Close())

	rc, err := openSpoolEncryptedRecordFile(path, seam, domain)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got)
}

func TestEncryptedSpoolRecordRejectsOversizedRecordHeader(t *testing.T) {
	seam := newClusterTestSeam(t)
	payload := []byte("sensitive cluster spool payload")
	path := writeEncryptedSpoolRecordFile(t, t.TempDir(), seam, "cluster-spool:oversized", payload)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var hdr [12]byte
	_, err = f.ReadAt(hdr[:], 0)
	require.NoError(t, err)
	binary.BigEndian.PutUint32(hdr[4:8], uint32(maxEncryptedSpoolBlobBytes+1))
	_, err = f.WriteAt(hdr[:], 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	rc, err := openSpoolEncryptedRecordFile(path, seam, "cluster-spool:oversized")
	require.NoError(t, err)
	_, err = io.ReadAll(rc)
	require.Error(t, err)
	require.NoError(t, rc.Close())
}

func TestECStreamBlockSizeScalesWithObjectSize(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}

	require.Equal(t, 64<<10, ecStreamBlockSize(cfg, 64<<10))
	require.Equal(t, 1<<20, ecStreamBlockSize(cfg, 2<<20))
	require.Equal(t, 1<<20, ecStreamBlockSize(cfg, 64<<20))
}

// TestEncryptedSpoolReader_MultiRecordByteExact reconstructs a payload spanning
// several 1 MiB records (last one smaller) byte-for-byte. This is the
// regression guard for the reader-owned plaintext/ciphertext buffer reuse
// (OpenTo + r.cipherBuf): a slice/cap bug shows up here and nowhere else.
func TestEncryptedSpoolReader_MultiRecordByteExact(t *testing.T) {
	seam := newClusterTestSeam(t)
	// 2.5 MiB → records of 1 MiB, 1 MiB, 0.5 MiB; the shrinking tail exercises
	// dst[:0] capacity reuse on a smaller record.
	payload := make([]byte, spoolCopyBufferSize*2+spoolCopyBufferSize/2)
	for i := range payload {
		payload[i] = byte(i*31 + 7)
	}
	path := writeEncryptedSpoolRecordFile(t, t.TempDir(), seam, "cluster-spool:reuse", payload)

	rc, err := openSpoolEncryptedRecordFile(path, seam, "cluster-spool:reuse")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got)
}

// TestEncryptedSpoolReader_ZeroizesPlaintextOnClose asserts the reader-owned
// plaintext buffer is wiped on Close (no plaintext residue), preserving the
// zeroization guarantee across the buffer-reuse refactor.
func TestEncryptedSpoolReader_ZeroizesPlaintextOnClose(t *testing.T) {
	seam := newClusterTestSeam(t)
	payload := bytes.Repeat([]byte("S"), 4096)
	path := writeEncryptedSpoolRecordFile(t, t.TempDir(), seam, "cluster-spool:zeroize", payload)

	rc, err := openSpoolEncryptedRecordFile(path, seam, "cluster-spool:zeroize")
	require.NoError(t, err)
	r, ok := rc.(*encryptedSpoolRecordReader)
	require.True(t, ok, "expected *encryptedSpoolRecordReader")

	// Load the first record by reading a few bytes (leaves undrained plaintext).
	tmp := make([]byte, 10)
	_, err = io.ReadFull(r, tmp)
	require.NoError(t, err)
	require.NoError(t, r.Close())

	for i, b := range r.plain {
		require.Zerof(t, b, "r.plain[%d] not zeroized after Close", i)
	}
}

// residueOpenSeam simulates an AEAD that overwrites dst up to capacity before
// returning an auth error — behavior the cipher.AEAD.Open contract explicitly
// permits ("the contents of dst, up to its capacity, may be overwritten" even
// on failure). Go's GCM happens to zero on failure, so this fake is how we
// prove the spool reader's defensive wipe independent of the live cipher.
type residueOpenSeam struct {
	storage.DataEncryptor
}

func (residueOpenSeam) OpenTo(dst []byte, _ encrypt.AADDomain, _ []encrypt.AADField, _ uint32, _ []byte) ([]byte, error) {
	d := dst[:cap(dst)]
	for i := range d {
		d[i] = 0xAA
	}
	return nil, errors.New("simulated auth failure")
}

// TestReadSpoolEncryptedRecord_WipesPlaintextOnOpenError is the regression
// guard for the code-gate finding: on an Open error the reader-owned plaintext
// buffer must be wiped to its full capacity, leaving no unauthenticated
// residue. Cipher-independent (uses residueOpenSeam): fails without the
// full-capacity clear on the error path.
func TestReadSpoolEncryptedRecord_WipesPlaintextOnOpenError(t *testing.T) {
	blob := bytes.Repeat([]byte{0x01}, 64)
	var hdr [12]byte
	binary.BigEndian.PutUint32(hdr[:4], 64) // plainLen (unused on error)
	binary.BigEndian.PutUint32(hdr[4:8], uint32(len(blob)))
	binary.BigEndian.PutUint32(hdr[8:], 0) // gen
	frame := append(append([]byte{}, hdr[:]...), blob...)

	plainDst := make([]byte, 0, 256) // reusable buffer the fake will dirty
	_, _, _, _, err := readSpoolEncryptedRecord(bytes.NewReader(frame), residueOpenSeam{}, "d", 0, plainDst, nil, nil)
	require.Error(t, err)

	full := plainDst[:cap(plainDst)]
	for i, b := range full {
		require.Zerof(t, b, "plainDst[%d]=%#x not wiped on Open error", i, b)
	}
}

type recordingOpenSeam struct {
	storage.DataEncryptor
	dstBacking    []*byte
	retBacking    []*byte
	fieldsBacking []*encrypt.AADField
}

func (s *recordingOpenSeam) OpenTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) ([]byte, error) {
	s.dstBacking = append(s.dstBacking, unsafe.SliceData(dst[:cap(dst)]))
	s.fieldsBacking = append(s.fieldsBacking, unsafe.SliceData(fields[:cap(fields)]))
	out, err := s.DataEncryptor.OpenTo(dst, domain, fields, gen, ct)
	s.retBacking = append(s.retBacking, unsafe.SliceData(out))
	return out, err
}

func TestEncryptedSpoolReader_ReusesPooledBuffersAndAADFields(t *testing.T) {
	seam := &recordingOpenSeam{DataEncryptor: newClusterTestSeam(t)}
	payload := append(bytes.Repeat([]byte("A"), spoolCopyBufferSize), bytes.Repeat([]byte("b"), 512)...)
	path := writeEncryptedSpoolRecordFile(t, t.TempDir(), seam, "cluster-spool:reader-reuse", payload)

	rc, err := openSpoolEncryptedRecordFile(path, seam, "cluster-spool:reader-reuse")
	require.NoError(t, err)
	r, ok := rc.(*encryptedSpoolRecordReader)
	require.True(t, ok, "expected *encryptedSpoolRecordReader")
	require.NotNil(t, r.plainRef, "reader must acquire a pooled plaintext buffer")
	require.NotNil(t, r.cipherRef, "reader must acquire a pooled ciphertext buffer")
	require.GreaterOrEqual(t, cap(*r.plainRef), spoolCopyBufferSize)
	require.GreaterOrEqual(t, cap(*r.cipherRef), encryptedSpoolCipherBufferSize)

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, payload, got)

	require.Len(t, seam.dstBacking, 2, "expected one OpenTo call per encrypted spool record")
	require.Len(t, seam.retBacking, 2)
	require.Len(t, seam.fieldsBacking, 2)
	require.Equal(t, seam.retBacking[0], seam.dstBacking[1],
		"second OpenTo dst must reuse the first record's plaintext backing array")
	require.Equal(t, seam.fieldsBacking[0], seam.fieldsBacking[1],
		"reader must reuse AAD fields backing across records")
}

// recordingSealSeam wraps a real seam, recording whether Seal (the
// fresh-allocation path) was ever used and, for each SealTo call, the backing
// array of the dst it received vs. the slice it returned. This lets the writer
// test prove the API switch (SealTo, never Seal) and the cipherBuf reuse
// (second SealTo is handed the first call's returned backing array) without a
// flaky AllocsPerRun assertion. No t.Fatal inside the fake — flags are recorded
// and asserted in the test goroutine after the Writes.
type recordingSealSeam struct {
	storage.DataEncryptor
	sealCalled bool
	dstBacking []*byte
	retBacking []*byte
}

func (f *recordingSealSeam) Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	f.sealCalled = true
	return f.DataEncryptor.Seal(domain, fields, plain)
}

func (f *recordingSealSeam) SealTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	f.dstBacking = append(f.dstBacking, unsafe.SliceData(dst[:cap(dst)]))
	blob, gen, err := f.DataEncryptor.SealTo(dst, domain, fields, plain)
	f.retBacking = append(f.retBacking, unsafe.SliceData(blob))
	return blob, gen, err
}

// TestEncryptedSpoolWriter_ReusesCipherBufViaSealTo is the regression guard for
// the writer-owned ciphertext buffer reuse (SealTo + w.cipherBuf): it proves the
// writer (a) uses SealTo and never the fresh-allocating Seal, and (b) feeds the
// first record's returned buffer back as the second record's dst (backing-array
// identity), i.e. the per-record ciphertext allocation is eliminated. Pointer
// identity — not cap>0 — is required: a buggy impl that re-allocates a same-sized
// buffer each call would pass a cap>0 check but is not reuse.
func TestEncryptedSpoolWriter_ReusesCipherBufViaSealTo(t *testing.T) {
	fake := &recordingSealSeam{DataEncryptor: newClusterTestSeam(t)}
	w := &encryptedSpoolRecordWriter{w: io.Discard, seam: fake, domain: "spool:reuse-test"}

	// Larger record first, then a smaller one that fits in the retained capacity.
	big := bytes.Repeat([]byte("A"), 4096)
	small := bytes.Repeat([]byte("b"), 512)
	_, err := w.Write(big)
	require.NoError(t, err)
	_, err = w.Write(small)
	require.NoError(t, err)

	require.False(t, fake.sealCalled, "writer must use SealTo, never the fresh-allocating Seal")
	require.Len(t, fake.dstBacking, 2, "expected exactly two SealTo calls")
	require.Len(t, fake.retBacking, 2)
	require.NotNil(t, fake.dstBacking[1], "second SealTo got a nil dst — cipherBuf was not retained")
	require.Equal(t, fake.retBacking[0], fake.dstBacking[1],
		"second SealTo dst must reuse the first call's returned backing array (cipherBuf reuse)")
}

type spoolAADAllocProbeSeam struct{}

func (spoolAADAllocProbeSeam) Seal(_ encrypt.AADDomain, _ []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	return append([]byte(nil), plain...), 0, nil
}

func (spoolAADAllocProbeSeam) SealTo(dst []byte, _ encrypt.AADDomain, _ []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	return append(dst, plain...), 0, nil
}

func (spoolAADAllocProbeSeam) SealAtGen(_ encrypt.AADDomain, _ []encrypt.AADField, plain []byte, _ uint32) ([]byte, error) {
	return append([]byte(nil), plain...), nil
}

func (spoolAADAllocProbeSeam) SealAtGenTo(dst []byte, _ encrypt.AADDomain, _ []encrypt.AADField, plain []byte, _ uint32) ([]byte, error) {
	return append(dst, plain...), nil
}

func (spoolAADAllocProbeSeam) Open(_ encrypt.AADDomain, _ []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	return append([]byte(nil), ct...), nil
}

func (spoolAADAllocProbeSeam) OpenTo(dst []byte, _ encrypt.AADDomain, _ []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	return append(dst, ct...), nil
}

type recordingAADFieldsSeam struct {
	spoolAADAllocProbeSeam
	fieldsBacking []*encrypt.AADField
}

func (s *recordingAADFieldsSeam) SealTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	s.fieldsBacking = append(s.fieldsBacking, unsafe.SliceData(fields[:cap(fields)]))
	return s.spoolAADAllocProbeSeam.SealTo(dst, domain, fields, plain)
}

func TestEncryptedSpoolWriter_ReusesAADFields(t *testing.T) {
	data := []byte("abcd")
	seam := &recordingAADFieldsSeam{}
	w := &encryptedSpoolRecordWriter{w: io.Discard, seam: seam, domain: "spool:aad-reuse"}
	_, err := w.Write(data)
	require.NoError(t, err)
	_, err = w.Write(data)
	require.NoError(t, err)
	require.Len(t, seam.fieldsBacking, 2)
	require.Equal(t, seam.fieldsBacking[0], seam.fieldsBacking[1], "writer must reuse AAD fields backing across records")
}

// spool unit tests exercise the real seam-backed write/read path. clusterID is
// fixed (16 bytes); the same seam instance seals and opens within a test.
func newClusterTestSeam(t *testing.T) storage.DataEncryptor {
	t.Helper()
	var clusterID [16]byte
	copy(clusterID[:], bytes.Repeat([]byte("c"), 16))
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x33}, encrypt.KEKSize), clusterID[:])
	require.NoError(t, err)
	return storage.NewDEKKeeperAdapter(keeper, clusterID[:])
}
