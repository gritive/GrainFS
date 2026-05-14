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
