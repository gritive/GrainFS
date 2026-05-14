package storage

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func testEncryptor(t *testing.T) *encrypt.Encryptor {
	t.Helper()
	key := bytes.Repeat([]byte{0x44}, 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)
	return enc
}

func TestEncryptedObjectFileRoundTripAndNoPlaintext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testEncryptor(t)
	plaintext := bytes.Repeat([]byte("sensitive-local-object-"), 4096)

	size, etag, err := writeEncryptedObjectFile(path, enc, "local-object:physical-1", bytes.NewReader(plaintext))
	require.NoError(t, err)
	require.Equal(t, int64(len(plaintext)), size)
	require.NotEmpty(t, etag)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "sensitive-local-object")

	rc, err := openEncryptedObjectFile(path, enc, "local-object:physical-1", size)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)
}

func TestEncryptedObjectFileWriteKeepsRecordsBoundedForReadAt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testEncryptor(t)
	plaintext := bytes.Repeat([]byte("x"), 2*(1<<20)+1)

	size, _, err := writeEncryptedObjectFile(path, enc, "local-object:large-records", bytes.NewReader(plaintext))
	require.NoError(t, err)
	require.Equal(t, int64(len(plaintext)), size)

	records := countEncryptedObjectRecords(t, path)
	require.Equal(t, 17, records)
}

func TestEncryptedObjectFileReadAtWriteAtAndTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testEncryptor(t)
	domain := "local-object:physical-2"

	size, _, err := writeEncryptedObjectFile(path, enc, domain, bytes.NewReader([]byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")))
	require.NoError(t, err)
	require.Equal(t, int64(26), size)

	size, etag, err := writeAtEncryptedObjectFile(path, enc, domain, 5, []byte("-----"), size)
	require.NoError(t, err)
	require.Equal(t, int64(26), size)
	require.NotEmpty(t, etag)

	buf := make([]byte, 10)
	n, err := readAtEncryptedObjectFile(path, enc, domain, size, 0, buf)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Equal(t, "ABCDE-----", string(buf))

	size, err = truncateEncryptedObjectFile(path, enc, domain, size, 8)
	require.NoError(t, err)
	require.Equal(t, int64(8), size)

	rc, err := openEncryptedObjectFile(path, enc, domain, size)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "ABCDE---", string(got))
}

func TestEncryptedObjectFileReadAtDoesNotDecryptUnneededChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testEncryptor(t)
	domain := "local-object:physical-readat"
	plaintext := append(bytes.Repeat([]byte("a"), encryptedChunkSize), bytes.Repeat([]byte("b"), encryptedChunkSize)...)

	size, _, err := writeEncryptedObjectFile(path, enc, domain, bytes.NewReader(plaintext))
	require.NoError(t, err)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var hdr [8]byte
	_, err = f.ReadAt(hdr[:], int64(len(encryptedObjectMagic)))
	require.NoError(t, err)
	firstBlobLen := binary.BigEndian.Uint32(hdr[4:])
	// Corrupt the second encrypted record body. A ReadAt contained in the first
	// chunk should not need to authenticate or decrypt this record.
	secondBodyOffset := int64(len(encryptedObjectMagic) + 8 + int(firstBlobLen) + 8)
	_, err = f.Seek(secondBodyOffset, io.SeekStart)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x00})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	buf := make([]byte, 32)
	n, err := readAtEncryptedObjectFile(path, enc, domain, size, 0, buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, bytes.Repeat([]byte("a"), len(buf)), buf)
}

func TestEncryptedObjectFileReadAtRejectsSkippedChunkHeaderTamper(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testEncryptor(t)
	domain := "local-object:physical-readat-header-tamper"
	plaintext := append(bytes.Repeat([]byte("a"), encryptedChunkSize), bytes.Repeat([]byte("b"), encryptedChunkSize)...)

	size, _, err := writeEncryptedObjectFile(path, enc, domain, bytes.NewReader(plaintext))
	require.NoError(t, err)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var hdr [8]byte
	_, err = f.ReadAt(hdr[:], int64(len(encryptedObjectMagic)))
	require.NoError(t, err)
	binary.BigEndian.PutUint32(hdr[:4], encryptedChunkSize-1)
	_, err = f.WriteAt(hdr[:], int64(len(encryptedObjectMagic)))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	buf := make([]byte, 32)
	_, err = readAtEncryptedObjectFile(path, enc, domain, size, int64(encryptedChunkSize), buf)
	require.Error(t, err)
}

func TestEncryptedObjectFileReadAtRejectsCorruptRequestedChunk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testEncryptor(t)
	domain := "local-object:physical-readat-corrupt"
	plaintext := append(bytes.Repeat([]byte("a"), encryptedChunkSize), bytes.Repeat([]byte("b"), encryptedChunkSize)...)

	size, _, err := writeEncryptedObjectFile(path, enc, domain, bytes.NewReader(plaintext))
	require.NoError(t, err)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	firstBodyOffset := int64(len(encryptedObjectMagic) + 8)
	_, err = f.WriteAt([]byte{0x00}, firstBodyOffset)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	buf := make([]byte, 32)
	_, err = readAtEncryptedObjectFile(path, enc, domain, size, 0, buf)
	require.Error(t, err)
}

func countEncryptedObjectRecords(t *testing.T, path string) int {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	magic := make([]byte, len(encryptedObjectMagic))
	_, err = io.ReadFull(f, magic)
	require.NoError(t, err)
	require.Equal(t, encryptedObjectMagic, string(magic))

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
	enc := testEncryptor(t)
	domain := "local-object:physical-stream"
	plaintext := append(bytes.Repeat([]byte("a"), encryptedChunkSize), bytes.Repeat([]byte("b"), encryptedChunkSize)...)

	size, _, err := writeEncryptedObjectFile(path, enc, domain, bytes.NewReader(plaintext))
	require.NoError(t, err)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	var hdr [8]byte
	_, err = f.ReadAt(hdr[:], int64(len(encryptedObjectMagic)))
	require.NoError(t, err)
	firstBlobLen := binary.BigEndian.Uint32(hdr[4:])
	secondBodyOffset := int64(len(encryptedObjectMagic) + 8 + int(firstBlobLen) + 8)
	_, err = f.Seek(secondBodyOffset, io.SeekStart)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x00})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	rc, err := openEncryptedObjectFile(path, enc, domain, size)
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
	enc := testEncryptor(t)
	domain := "local-object:physical-truncated"

	size, _, err := writeEncryptedObjectFile(path, enc, domain, bytes.NewReader([]byte("payload")))
	require.NoError(t, err)
	require.NoError(t, os.Truncate(path, int64(len(encryptedObjectMagic))))

	rc, err := openEncryptedObjectFile(path, enc, domain, size)
	require.NoError(t, err)
	defer rc.Close()
	_, err = io.ReadAll(rc)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestEncryptedObjectFileWriteAtRejectsWholeRecordTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "object")
	enc := testEncryptor(t)
	domain := "local-object:physical-whole-record-truncated"
	plaintext := append(bytes.Repeat([]byte("a"), encryptedChunkSize), bytes.Repeat([]byte("b"), 32)...)

	size, _, err := writeEncryptedObjectFile(path, enc, domain, bytes.NewReader(plaintext))
	require.NoError(t, err)

	f, err := os.Open(path)
	require.NoError(t, err)
	var hdr [8]byte
	_, err = f.ReadAt(hdr[:], int64(len(encryptedObjectMagic)))
	require.NoError(t, err)
	firstBlobLen := binary.BigEndian.Uint32(hdr[4:])
	require.NoError(t, f.Close())

	firstRecordEnd := int64(len(encryptedObjectMagic) + len(hdr) + int(firstBlobLen))
	require.NoError(t, os.Truncate(path, firstRecordEnd))

	_, _, err = writeAtEncryptedObjectFile(path, enc, domain, 0, []byte("z"), size)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
