package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"testing"
	"unsafe"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

type failingReader struct {
	err error
}

func (r failingReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestSpoolObjectComputesSizeAndETag(t *testing.T) {
	data := []byte("hello")
	sp, err := spoolObject(context.Background(), t.TempDir(), bytes.NewReader(data), "__grainfs_test_internal")
	require.NoError(t, err)
	defer sp.Cleanup()
	require.Equal(t, int64(5), sp.Size)
	require.Equal(t, storage.InternalETag(data), sp.ETag) // xxhash3 for internal buckets

	rc, err := sp.Open()
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))
}

func TestSpoolObjectS3BucketUsesMD5ETag(t *testing.T) {
	data := []byte("hello s3 object")
	sp, err := spoolObject(context.Background(), t.TempDir(), bytes.NewReader(data), "user-bucket")
	require.NoError(t, err)
	defer sp.Cleanup()
	require.Equal(t, int64(len(data)), sp.Size)
	h := md5.Sum(data)
	require.Equal(t, hex.EncodeToString(h[:]), sp.ETag) // MD5 for S3 user buckets
}

func TestSpoolObjectNoBucketSkipsHashing(t *testing.T) {
	data := []byte("no etag needed")
	sp, err := spoolObject(context.Background(), t.TempDir(), bytes.NewReader(data), "")
	require.NoError(t, err)
	defer sp.Cleanup()
	require.Equal(t, int64(len(data)), sp.Size)
	require.Empty(t, sp.ETag) // no bucket → no etag computed
}

func TestSpoolObjectCleansTempOnReadError(t *testing.T) {
	dir := t.TempDir()
	_, err := spoolObject(context.Background(), dir, failingReader{err: errors.New("boom")}, "__grainfs_test_internal")
	require.ErrorContains(t, err, "spool object")
	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestEncryptedSpoolObjectHidesPlaintext(t *testing.T) {
	seam := newClusterTestSeam(t)
	payload := []byte("sensitive cluster spool payload")
	sp, err := spoolObjectEncrypted(context.Background(), t.TempDir(), bytes.NewReader(payload), "user-bucket", seam, "cluster-spool:test")
	require.NoError(t, err)
	defer sp.Cleanup()
	require.Equal(t, int64(len(payload)), sp.Size)
	require.Equal(t, "7d0467b8ee0ad76a1c41e37b3c2d3056", sp.ETag)

	raw, err := os.ReadFile(sp.Path)
	require.NoError(t, err)
	require.NotContains(t, string(raw), string(payload))

	rc, err := sp.Open()
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got)
}

func TestSpoolObjectEncryptedRoundTripsViaDEKSeam(t *testing.T) {
	clusterID := bytes.Repeat([]byte("c"), 16)
	kek := bytes.Repeat([]byte{0x77}, encrypt.KEKSize)
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err)
	seam := storage.NewDEKKeeperAdapter(keeper, clusterID)

	dir := t.TempDir()
	plain := bytes.Repeat([]byte("spool-bytes-"), 200_000) // > 1 MiB, multiple records
	sp, err := spoolObjectEncrypted(context.Background(), dir, bytes.NewReader(plain), "bkt", seam, "cluster-spool:test")
	require.NoError(t, err)
	defer sp.Cleanup()

	// On-disk spool file must be ciphertext, not the plaintext run.
	raw, err := os.ReadFile(sp.Path)
	require.NoError(t, err)
	require.False(t, bytes.Contains(raw, plain[:4096]), "spool file must not contain plaintext")

	// Reads back to the exact plaintext via the seam.
	rc, err := sp.Open()
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plain, got)
}

func TestEncryptedSpoolObjectOpenStreamsWithoutDecryptingFutureRecords(t *testing.T) {
	seam := newClusterTestSeam(t)
	payload := append(bytes.Repeat([]byte("a"), spoolCopyBufferSize), bytes.Repeat([]byte("b"), spoolCopyBufferSize)...)
	sp, err := spoolObjectEncrypted(context.Background(), t.TempDir(), bytes.NewReader(payload), "user-bucket", seam, "cluster-spool:stream")
	require.NoError(t, err)
	defer sp.Cleanup()

	f, err := os.OpenFile(sp.Path, os.O_RDWR, 0)
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

	rc, err := sp.Open()
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

func TestEncryptedSpoolObjectRejectsOversizedRecordHeader(t *testing.T) {
	seam := newClusterTestSeam(t)
	payload := []byte("sensitive cluster spool payload")
	sp, err := spoolObjectEncrypted(context.Background(), t.TempDir(), bytes.NewReader(payload), "user-bucket", seam, "cluster-spool:oversized")
	require.NoError(t, err)
	defer sp.Cleanup()

	f, err := os.OpenFile(sp.Path, os.O_RDWR, 0)
	require.NoError(t, err)
	var hdr [12]byte
	_, err = f.ReadAt(hdr[:], 0)
	require.NoError(t, err)
	binary.BigEndian.PutUint32(hdr[4:8], uint32(maxEncryptedSpoolBlobBytes+1))
	_, err = f.WriteAt(hdr[:], 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	rc, err := sp.Open()
	require.NoError(t, err)
	_, err = io.ReadAll(rc)
	require.Error(t, err)
	require.NoError(t, rc.Close())
}

func TestSpoolECShardsReconstructsOriginal(t *testing.T) {
	sp, err := spoolObject(context.Background(), t.TempDir(), bytes.NewReader([]byte("hello erasure coding")), "__grainfs_test_internal")
	require.NoError(t, err)
	defer sp.Cleanup()

	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	shards, err := spoolECShards(context.Background(), cfg, t.TempDir(), sp)
	require.NoError(t, err)
	defer shards.Cleanup()

	payloads := make([][]byte, cfg.NumShards())
	for i := range payloads {
		rc, err := shards.OpenShard(i)
		require.NoError(t, err)
		payloads[i], err = io.ReadAll(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
	}
	got, err := ECReconstruct(cfg, payloads)
	require.NoError(t, err)
	require.Equal(t, "hello erasure coding", string(got))
}

func TestSpoolECShardsReconstructsEmptyObject(t *testing.T) {
	sp, err := spoolObject(context.Background(), t.TempDir(), bytes.NewReader(nil), "__grainfs_test_internal")
	require.NoError(t, err)
	defer sp.Cleanup()

	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	shards, err := spoolECShards(context.Background(), cfg, t.TempDir(), sp)
	require.NoError(t, err)
	defer shards.Cleanup()

	payloads := make([][]byte, cfg.NumShards())
	for i := range payloads {
		rc, err := shards.OpenShard(i)
		require.NoError(t, err)
		payloads[i], err = io.ReadAll(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
	}
	got, err := ECReconstruct(cfg, payloads)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestEncryptedSpoolECShardsHidePlaintextAndReconstruct(t *testing.T) {
	seam := newClusterTestSeam(t)
	marker := []byte("sensitive-erasure-coding-block-")
	payload := bytes.Repeat(marker, 4096)
	sp, err := spoolObjectEncrypted(context.Background(), t.TempDir(), bytes.NewReader(payload), "user-bucket", seam, "cluster-spool:test-ec")
	require.NoError(t, err)
	defer sp.Cleanup()

	cfg := ECConfig{DataShards: 2, ParityShards: 1}
	shards, err := spoolECShards(context.Background(), cfg, t.TempDir(), sp)
	require.NoError(t, err)
	defer shards.Cleanup()

	payloads := make([][]byte, cfg.NumShards())
	for i := range payloads {
		raw, err := os.ReadFile(shards.paths[i])
		require.NoError(t, err)
		require.False(t, bytes.Contains(raw, marker), "raw EC shard %d contains plaintext marker", i)

		rc, err := shards.OpenShard(i)
		require.NoError(t, err)
		payloads[i], err = io.ReadAll(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
	}
	got, err := ECReconstruct(cfg, payloads)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestECStreamBlockSizeScalesWithObjectSize(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 1}

	require.Equal(t, 64<<10, ecStreamBlockSize(cfg, 64<<10))
	require.Equal(t, 1<<20, ecStreamBlockSize(cfg, 2<<20))
	require.Equal(t, 1<<20, ecStreamBlockSize(cfg, 64<<20))
}

// newClusterTestSeam returns a DataEncryptor over the static test encryptor so
// TestEncryptedSpoolReader_MultiRecordByteExact reconstructs a payload spanning
// several 1 MiB spool records (last one smaller) byte-for-byte. This is the
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
	sp, err := spoolObjectEncrypted(context.Background(), t.TempDir(), bytes.NewReader(payload), "user-bucket", seam, "cluster-spool:reuse")
	require.NoError(t, err)
	defer sp.Cleanup()

	rc, err := sp.Open()
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
	sp, err := spoolObjectEncrypted(context.Background(), t.TempDir(), bytes.NewReader(payload), "user-bucket", seam, "cluster-spool:zeroize")
	require.NoError(t, err)
	defer sp.Cleanup()

	rc, err := sp.Open()
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
	_, _, _, err := readSpoolEncryptedRecord(bytes.NewReader(frame), residueOpenSeam{}, "d", 0, plainDst, nil)
	require.Error(t, err)

	full := plainDst[:cap(plainDst)]
	for i, b := range full {
		require.Zerof(t, b, "plainDst[%d]=%#x not wiped on Open error", i, b)
	}
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
