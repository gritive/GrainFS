package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// encryptedObjectHeaderLen is the on-disk header size before the first
// record: magic + format_version(2) + dek_gen(4). Kept in the test file
// because production code addresses records via sequential reads, not offsets.
const encryptedObjectHeaderLen = len(encryptedObjectMagic) + 6

func testEncryptor(t *testing.T) *encrypt.Encryptor {
	t.Helper()
	key := bytes.Repeat([]byte{0x44}, 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)
	return enc
}

// testSegEnc wraps the static test Encryptor in the DataEncryptor seam (the
// EncryptorAdapter seals at gen 0), matching how LocalBackend builds segEnc.
func testSegEnc(t *testing.T) DataEncryptor {
	t.Helper()
	return NewEncryptorAdapter(testEncryptor(t), make([]byte, 16))
}

func TestEncryptedObjectFileRoundTripAndNoPlaintext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testSegEnc(t)
	plaintext := bytes.Repeat([]byte("sensitive-local-object-"), 4096)

	h, release := hashForBucket("")
	defer release()
	size, err := writeEncryptedObjectFile(path, enc, objectFileAADFields("b", "k"), bytes.NewReader(plaintext), h)
	require.NoError(t, err)
	require.Equal(t, int64(len(plaintext)), size)
	require.NotEmpty(t, etagFromHash(h))

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "sensitive-local-object")

	rc, err := openEncryptedObjectFile(path, enc, objectFileAADFields("b", "k"), size)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestEncryptedObjectFileWriteKeepsRecordsBoundedForReadAt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testSegEnc(t)
	plaintext := bytes.Repeat([]byte("x"), 2*(1<<20)+1)

	size, err := writeEncryptedObjectFile(path, enc, objectFileAADFields("b", "k"), bytes.NewReader(plaintext), io.Discard)
	require.NoError(t, err)
	require.Equal(t, int64(len(plaintext)), size)

	records := countEncryptedObjectRecords(t, path)
	require.Equal(t, 17, records)
}

func TestEncryptedObjectFileReadAtWriteAtAndTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testSegEnc(t)
	fields := objectFileAADFields("b", "k")

	size, err := writeEncryptedObjectFile(path, enc, fields, bytes.NewReader([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")), io.Discard)
	require.NoError(t, err)
	require.Equal(t, int64(26), size)

	size, etag, err := writeAtEncryptedObjectFile(path, enc, fields, 5, []byte("-----"), size)
	require.NoError(t, err)
	require.Equal(t, int64(26), size)
	require.NotEmpty(t, etag)

	buf := make([]byte, 10)
	n, err := readAtEncryptedObjectFile(path, enc, fields, size, 0, buf)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Equal(t, "ABCDE-----", string(buf))

	size, err = truncateEncryptedObjectFile(path, enc, fields, size, 8)
	require.NoError(t, err)
	require.Equal(t, int64(8), size)

	rc, err := openEncryptedObjectFile(path, enc, fields, size)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "ABCDE---", string(got))
}

func TestEncryptedObjectFileReadAtDoesNotDecryptUnneededChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testSegEnc(t)
	fields := objectFileAADFields("b", "k")
	plaintext := append(bytes.Repeat([]byte("a"), encryptedChunkSize), bytes.Repeat([]byte("b"), encryptedChunkSize)...)

	size, err := writeEncryptedObjectFile(path, enc, fields, bytes.NewReader(plaintext), io.Discard)
	require.NoError(t, err)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var hdr [8]byte
	_, err = f.ReadAt(hdr[:], int64(encryptedObjectHeaderLen))
	require.NoError(t, err)
	firstBlobLen := binary.BigEndian.Uint32(hdr[4:])
	// Corrupt the second encrypted record body. A ReadAt contained in the first
	// chunk should not need to authenticate or decrypt this record.
	secondBodyOffset := int64(encryptedObjectHeaderLen + 8 + int(firstBlobLen) + 8)
	_, err = f.Seek(secondBodyOffset, io.SeekStart)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x00})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	buf := make([]byte, 32)
	n, err := readAtEncryptedObjectFile(path, enc, fields, size, 0, buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, bytes.Repeat([]byte("a"), len(buf)), buf)
}

func TestEncryptedObjectFileReadAtRejectsSkippedChunkHeaderTamper(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testSegEnc(t)
	fields := objectFileAADFields("b", "k")
	plaintext := append(bytes.Repeat([]byte("a"), encryptedChunkSize), bytes.Repeat([]byte("b"), encryptedChunkSize)...)

	size, err := writeEncryptedObjectFile(path, enc, fields, bytes.NewReader(plaintext), io.Discard)
	require.NoError(t, err)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var hdr [8]byte
	_, err = f.ReadAt(hdr[:], int64(encryptedObjectHeaderLen))
	require.NoError(t, err)
	binary.BigEndian.PutUint32(hdr[:4], encryptedChunkSize-1)
	_, err = f.WriteAt(hdr[:], int64(encryptedObjectHeaderLen))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	buf := make([]byte, 32)
	_, err = readAtEncryptedObjectFile(path, enc, fields, size, int64(encryptedChunkSize), buf)
	require.Error(t, err)
}

func TestEncryptedObjectFileReadAtRejectsCorruptRequestedChunk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testSegEnc(t)
	fields := objectFileAADFields("b", "k")
	plaintext := append(bytes.Repeat([]byte("a"), encryptedChunkSize), bytes.Repeat([]byte("b"), encryptedChunkSize)...)

	size, err := writeEncryptedObjectFile(path, enc, fields, bytes.NewReader(plaintext), io.Discard)
	require.NoError(t, err)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	firstBodyOffset := int64(encryptedObjectHeaderLen + 8)
	_, err = f.WriteAt([]byte{0x00}, firstBodyOffset)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	buf := make([]byte, 32)
	_, err = readAtEncryptedObjectFile(path, enc, fields, size, 0, buf)
	require.Error(t, err)
}

func countEncryptedObjectRecords(t *testing.T, path string) int {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	_, err = readEncryptedObjectHeader(f)
	require.NoError(t, err)

	records := 0
	var hdr [8]byte
	for {
		_, err = io.ReadFull(f, hdr[:])
		if err == io.EOF {
			return records
		}
		require.NoError(t, err)

		blobLen := binary.BigEndian.Uint32(hdr[4:])
		_, err = f.Seek(int64(blobLen), io.SeekCurrent)
		require.NoError(t, err)
		records++
	}
}

func TestEncryptedObjectFileOpenStreamsWithoutDecryptingFutureChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testSegEnc(t)
	fields := objectFileAADFields("b", "k")
	plaintext := append(bytes.Repeat([]byte("a"), encryptedChunkSize), bytes.Repeat([]byte("b"), encryptedChunkSize)...)

	size, err := writeEncryptedObjectFile(path, enc, fields, bytes.NewReader(plaintext), io.Discard)
	require.NoError(t, err)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var hdr [8]byte
	_, err = f.ReadAt(hdr[:], int64(encryptedObjectHeaderLen))
	require.NoError(t, err)
	firstBlobLen := binary.BigEndian.Uint32(hdr[4:])
	secondBodyOffset := int64(encryptedObjectHeaderLen + 8 + int(firstBlobLen) + 8)
	_, err = f.Seek(secondBodyOffset, io.SeekStart)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x00})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	rc, err := openEncryptedObjectFile(path, enc, fields, size)
	require.NoError(t, err)
	defer rc.Close()
	buf := make([]byte, 32)
	n, err := rc.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, bytes.Repeat([]byte("a"), len(buf)), buf)
}

func TestEncryptedObjectFileOpenRejectsTruncatedExpectedObject(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testSegEnc(t)
	fields := objectFileAADFields("b", "k")

	size, err := writeEncryptedObjectFile(path, enc, fields, bytes.NewReader([]byte("payload")), io.Discard)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(path, int64(encryptedObjectHeaderLen)))

	rc, err := openEncryptedObjectFile(path, enc, fields, size)
	require.NoError(t, err)
	defer rc.Close()
	_, err = io.ReadAll(rc)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestEncryptedObjectFileWriteAtRejectsWholeRecordTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testSegEnc(t)
	fields := objectFileAADFields("b", "k")
	plaintext := append(bytes.Repeat([]byte("a"), encryptedChunkSize), bytes.Repeat([]byte("b"), 32)...)

	size, err := writeEncryptedObjectFile(path, enc, fields, bytes.NewReader(plaintext), io.Discard)
	require.NoError(t, err)

	f, err := os.Open(path)
	require.NoError(t, err)
	var hdr [8]byte
	_, err = f.ReadAt(hdr[:], int64(encryptedObjectHeaderLen))
	require.NoError(t, err)
	firstBlobLen := binary.BigEndian.Uint32(hdr[4:])
	require.NoError(t, f.Close())

	firstRecordEnd := int64(encryptedObjectHeaderLen + len(hdr) + int(firstBlobLen))
	require.NoError(t, os.Truncate(path, firstRecordEnd))

	_, _, err = writeAtEncryptedObjectFile(path, enc, fields, 0, []byte("z"), size)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestEncryptedObjectHeader_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := writeEncryptedObjectHeader(&buf, 7); err != nil {
		t.Fatalf("writeEncryptedObjectHeader: %v", err)
	}
	gen, err := readEncryptedObjectHeader(&buf)
	if err != nil {
		t.Fatalf("readEncryptedObjectHeader: %v", err)
	}
	if gen != 7 {
		t.Fatalf("dek_gen mismatch: want 7 got %d", gen)
	}
}

func TestEncryptedObjectHeader_RejectsLegacyMagic(t *testing.T) {
	legacy := append([]byte("GFOBJENC1"), 0x00, 0x00)
	_, err := readEncryptedObjectHeader(bytes.NewReader(legacy))
	if err == nil {
		t.Fatal("expected legacy magic to be rejected")
	}
}

// corruptByteAt flips one byte at the given offset in the file at path.
// Used to simulate on-disk ciphertext tampering for AEAD-verify regression
// tests.
func corruptByteAt(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err, "open for corrupt")
	defer f.Close()
	var b [1]byte
	_, err = f.ReadAt(b[:], off)
	require.NoError(t, err, "read at")
	b[0] ^= 0xFF
	_, err = f.WriteAt(b[:], off)
	require.NoError(t, err, "write at")
}

// TestEncryptedSegment_PerSegmentAADIsolation locks in the AAD-isolation
// contract documented on WriteSegmentBlob: each segment's encryption domain
// includes its unique blob_id, so corrupting one segment's ciphertext must
// NOT affect another segment's ability to decrypt. If a future refactor
// accidentally collapses the per-segment domain (e.g. drops blob_id and
// reuses object-level AAD), this test fails immediately.
func TestEncryptedSegment_PerSegmentAADIsolation(t *testing.T) {
	enc := testEncryptor(t)
	dir := t.TempDir()
	b, err := NewEncryptedLocalBackend(dir, enc)
	require.NoError(t, err, "NewEncryptedLocalBackend")
	t.Cleanup(func() { _ = b.Close() })

	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "test"), "CreateBucket")

	// 2 segments: exactly one full 16 MiB chunk + 100-byte tail.
	data := makePattern((16 << 20) + 100)
	obj, err := b.PutObject(ctx, "test", "k", bytes.NewReader(data), "application/octet-stream")
	require.NoError(t, err, "PutObject")
	require.Len(t, obj.Segments, 2, "expected 2 segments")

	// Corrupt one byte of segment[1]'s ciphertext on disk. Offset 64 is well
	// inside the first AEAD record's ciphertext for any reasonable header
	// layout, so AES-GCM verify must fail on segment[1] and only segment[1].
	path := b.segmentPath("test", "k", obj.Segments[1].BlobID)
	corruptByteAt(t, path, 64)

	// GetObject should successfully decrypt segment[0] (16 MiB of correct
	// plaintext), then fail on segment[1]. The first segment's bytes must
	// match the original exactly — proving the AAD does not leak across
	// segments.
	rc, _, err := b.GetObject(ctx, "test", "k")
	require.NoError(t, err, "GetObject")
	defer rc.Close()

	first := make([]byte, 16<<20)
	_, err = io.ReadFull(rc, first)
	require.NoError(t, err, "first segment must decrypt cleanly")
	require.True(t, bytes.Equal(first, data[:16<<20]),
		"first segment bytes differ — AAD isolation broken")

	// Reading further should error (segment 2 ciphertext corrupted).
	rest, err := io.ReadAll(rc)
	require.Error(t, err, "corrupted segment 2 must surface as an error")
	require.Empty(t, rest, "no partial plaintext from corrupted segment expected")
}

// fakeDataEncryptor records the gen it seals at and can be told to advance the
// gen mid-stream to exercise the writer's generation-pinning guard.
type fakeDataEncryptor struct {
	gen     uint32
	advance bool // when true, gen++ after each Seal
	enc     *encrypt.Encryptor
}

func newFakeDataEncryptor(t *testing.T) *fakeDataEncryptor {
	t.Helper()
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x07}, 32))
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	return &fakeDataEncryptor{enc: enc}
}

func (f *fakeDataEncryptor) aad(domain encrypt.AADDomain, fields []encrypt.AADField) []byte {
	return encrypt.BuildAAD(domain, make([]byte, 16), fields...)
}

func (f *fakeDataEncryptor) Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	ct, err := f.enc.SealValueAADTo(nil, f.aad(domain, fields), plain)
	if err != nil {
		return nil, 0, err
	}
	g := f.gen
	if f.advance {
		f.gen++
	}
	return ct, g, nil
}

func (f *fakeDataEncryptor) Open(domain encrypt.AADDomain, fields []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	return f.enc.OpenValueAADTo(nil, f.aad(domain, fields), ct)
}

func TestWriteEncryptedObjectFile_GenPinning_FailsOnMidStreamChange(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/obj"
	f := newFakeDataEncryptor(t)
	f.advance = true // gen changes after the first chunk
	// Two chunks worth of plaintext forces a second Seal at a different gen.
	plain := bytes.Repeat([]byte("x"), encryptedChunkSize+1)
	_, err := writeEncryptedObjectFile(path, f, objectFileAADFields("b", "k"), bytes.NewReader(plain), io.Discard)
	if err == nil {
		t.Fatal("expected gen-pinning to fail the write on mid-stream gen change")
	}
}

func TestWriteEncryptedObjectFile_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/obj"
	f := newFakeDataEncryptor(t) // constant gen 0
	plain := bytes.Repeat([]byte("y"), encryptedChunkSize+123)
	n, err := writeEncryptedObjectFile(path, f, objectFileAADFields("b", "k"), bytes.NewReader(plain), io.Discard)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if n != int64(len(plain)) {
		t.Fatalf("size mismatch: want %d got %d", len(plain), n)
	}
	got, err := readEncryptedObjectFile(path, f, objectFileAADFields("b", "k"), n)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatal("round-trip mismatch")
	}
}
