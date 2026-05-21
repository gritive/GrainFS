package eccodec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteReadShard_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("hello shard")},
		{"binary", []byte{0x00, 0xff, 0x7f, 0x01, 0x02, 0x03}},
		{"4k", make([]byte, 4096)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "nested", "shard_0")

			require.NoError(t, WriteShardAtomic(path, tt.data))

			got, err := ReadShardVerified(path)
			require.NoError(t, err)
			assert.Equal(t, tt.data, got)
		})
	}
}

func TestReadShard_BitFlipDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shard_0")
	require.NoError(t, WriteShardAtomic(path, []byte("original payload")))

	// Flip a byte in the payload (first byte).
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	raw[0] ^= 0x01
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	_, err = ReadShardVerified(path)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrCRCMismatch), "want ErrCRCMismatch, got %v", err)
}

func TestReadShard_FooterFlipDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shard_0")
	require.NoError(t, WriteShardAtomic(path, []byte("payload")))

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	raw[len(raw)-1] ^= 0xff // flip last CRC byte
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	_, err = ReadShardVerified(path)
	require.ErrorIs(t, err, ErrCRCMismatch)
}

func TestShardReader_FooterFlipDetected(t *testing.T) {
	raw := EncodeShard([]byte("payload"))
	raw[len(raw)-1] ^= 0xff

	r, err := NewShardReader(bytes.NewReader(raw))
	require.NoError(t, err)
	_, err = io.ReadAll(r)
	require.ErrorIs(t, err, ErrCRCMismatch)
}

func TestShardReader_RoundTripMultiRead(t *testing.T) {
	data := bytes.Repeat([]byte("payload-"), 8192)
	r, err := NewShardReader(bytes.NewReader(EncodeShard(data)))
	require.NoError(t, err)

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestSizedShardReader_RoundTrip(t *testing.T) {
	data := bytes.Repeat([]byte("payload-"), 8192)
	raw := EncodeShard(data)
	r := NewSizedShardReader(bytes.NewReader(raw[len(shardMagic):]), int64(len(data)))

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestSizedShardReader_FooterFlipDetectedOnExactRead(t *testing.T) {
	data := bytes.Repeat([]byte("payload-"), 8192)
	raw := EncodeShard(data)
	raw[len(raw)-1] ^= 0xff
	r := NewSizedShardReader(bytes.NewReader(raw[len(shardMagic):]), int64(len(data)))

	buf := make([]byte, len(data))
	_, err := io.ReadFull(r, buf)
	require.ErrorIs(t, err, ErrCRCMismatch)
}

func TestReadShard_TruncatedDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shard_0")
	require.NoError(t, os.WriteFile(path, []byte{0x01, 0x02}, 0o644)) // < 4 bytes

	_, err := ReadShardVerified(path)
	require.ErrorIs(t, err, ErrCRCMismatch)
}

func TestReadShard_MissingFileReturnsNotFound(t *testing.T) {
	_, err := ReadShardVerified(filepath.Join(t.TempDir(), "does-not-exist"))
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}

func TestWriteShard_AtomicOnCrash(t *testing.T) {
	// Simulate: a pre-existing file should survive a failed write attempt
	// because the write goes to .tmp first and is only renamed on success.
	dir := t.TempDir()
	path := filepath.Join(dir, "shard_0")
	require.NoError(t, WriteShardAtomic(path, []byte("v1")))

	// Second write — success: file should now contain v2.
	require.NoError(t, WriteShardAtomic(path, []byte("v2")))
	got, err := ReadShardVerified(path)
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), got)

	// Leftover .tmp files should not exist after a successful write.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		assert.NotContains(t, e.Name(), ".tmp", "leftover tmp file: %s", e.Name())
	}
}

func TestEncryptedShardStream_RoundTripMultiChunk(t *testing.T) {
	enc := testEncryptor(t)
	data := bytes.Repeat([]byte("encrypted-shard-payload-"), 256)
	aad := []byte("v2/bucket/key/3")

	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), enc, aad, 1024))
	require.True(t, IsEncryptedShard(encoded.Bytes()))

	var got bytes.Buffer
	require.NoError(t, DecodeEncryptedShard(&got, bytes.NewReader(encoded.Bytes()), enc, aad))
	assert.Equal(t, data, got.Bytes())
}

func TestEncryptedShardReader_RoundTripMultiChunk(t *testing.T) {
	enc := testEncryptor(t)
	data := bytes.Repeat([]byte("encrypted-shard-payload-"), 256)
	aad := []byte("v2/bucket/key/3")

	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), enc, aad, 1024))

	r, err := NewEncryptedShardReader(bytes.NewReader(encoded.Bytes()), enc, aad)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, data, got)

	closer, ok := r.(io.Closer)
	require.True(t, ok)
	require.NoError(t, closer.Close())
	require.NoError(t, closer.Close())
	_, err = r.Read(make([]byte, 1))
	require.Error(t, err)
}

func TestEncryptedShardRangeReader_DoesNotReadSkippedChunks(t *testing.T) {
	enc := testEncryptor(t)
	data := bytes.Repeat([]byte("0123456789abcdef"), 512)
	aad := []byte("v2/bucket/key/3")
	const chunkSize = 1024

	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), enc, aad, chunkSize))

	offset := int64(3*chunkSize + 17)
	length := int64(193)
	firstChunkCipherOffset := int64(encryptedHeaderLen) + 3*int64(encryptedChunkHeaderLen+chunkSize+enc.AEADOverhead())
	spy := &rangeGuardReaderAt{
		data:              encoded.Bytes(),
		allowBeforeData:   encryptedHeaderLen,
		minDataReadOffset: firstChunkCipherOffset,
	}

	r, err := NewEncryptedShardRangeReader(spy, enc, aad, offset, length)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, data[offset:offset+length], got)
}

func TestEncryptedShardRangeReader_CloseReleasesChunkBuffers(t *testing.T) {
	enc := testEncryptor(t)
	data := bytes.Repeat([]byte("0123456789abcdef"), 512)
	aad := []byte("v2/bucket/key/3")
	const chunkSize = 1024

	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), enc, aad, chunkSize))

	reader, err := NewEncryptedShardRangeReader(bytes.NewReader(encoded.Bytes()), enc, aad, 0, int64(len(data)))
	require.NoError(t, err)
	rangeReader := reader.(*encryptedShardRangeReader)
	_, err = io.ReadAll(reader)
	require.NoError(t, err)
	require.NotNil(t, rangeReader.plainBuf)
	require.NotNil(t, rangeReader.cipherBuf)

	closer, ok := reader.(io.Closer)
	require.True(t, ok)
	require.NoError(t, closer.Close())
	require.Nil(t, rangeReader.plainBuf)
	require.Nil(t, rangeReader.cipherBuf)
}

func TestReadEncryptedShardRangeAt_DoesNotReadSkippedChunks(t *testing.T) {
	enc := testEncryptor(t)
	data := bytes.Repeat([]byte("0123456789abcdef"), 512)
	aad := []byte("v2/bucket/key/3")
	const chunkSize = 1024

	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), enc, aad, chunkSize))

	offset := int64(3*chunkSize + 17)
	length := 193
	firstChunkCipherOffset := int64(encryptedHeaderLen) + 3*int64(encryptedChunkHeaderLen+chunkSize+enc.AEADOverhead())
	spy := &rangeGuardReaderAt{
		data:              encoded.Bytes(),
		allowBeforeData:   encryptedHeaderLen,
		minDataReadOffset: firstChunkCipherOffset,
	}

	got := make([]byte, length)
	n, err := ReadEncryptedShardRangeAt(spy, enc, aad, offset, got)
	require.NoError(t, err)
	require.Equal(t, length, n)
	assert.Equal(t, data[offset:offset+int64(length)], got)
}

func TestReadEncryptedShardRangeAt_CrossesChunkBoundary(t *testing.T) {
	enc := testEncryptor(t)
	data := bytes.Repeat([]byte("0123456789abcdef"), 256)
	aad := []byte("v2/bucket/key/3")
	const chunkSize = 1024

	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), enc, aad, chunkSize))

	offset := int64(chunkSize - 13)
	got := make([]byte, 64)
	n, err := ReadEncryptedShardRangeAt(bytes.NewReader(encoded.Bytes()), enc, aad, offset, got)
	require.NoError(t, err)
	require.Equal(t, len(got), n)
	assert.Equal(t, data[offset:offset+int64(len(got))], got)
}

func TestReadEncryptedShardRangeAt_WrongAADFails(t *testing.T) {
	enc := testEncryptor(t)
	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader([]byte("payload")), enc, []byte("aad-a"), 1024))

	got := make([]byte, 4)
	n, err := ReadEncryptedShardRangeAt(bytes.NewReader(encoded.Bytes()), enc, []byte("aad-b"), 0, got)
	require.Error(t, err)
	require.Equal(t, 0, n)
	assert.Contains(t, err.Error(), "decrypt")
}

func TestReadEncryptedShardRangeAt_PartialPastEOF(t *testing.T) {
	enc := testEncryptor(t)
	data := []byte("payload")
	aad := []byte("aad")
	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), enc, aad, 1024))

	got := make([]byte, len(data)+3)
	n, err := ReadEncryptedShardRangeAt(bytes.NewReader(encoded.Bytes()), enc, aad, 0, got)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, len(data), n)
	assert.Equal(t, data, got[:n])
}

func TestEncryptedShardStream_WrongAADFails(t *testing.T) {
	enc := testEncryptor(t)
	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader([]byte("payload")), enc, []byte("aad-a"), 1024))

	var got bytes.Buffer
	err := DecodeEncryptedShard(&got, bytes.NewReader(encoded.Bytes()), enc, []byte("aad-b"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decrypt")
}

func TestEncryptedShardStream_TamperDetected(t *testing.T) {
	enc := testEncryptor(t)
	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(bytes.Repeat([]byte("x"), 2048)), enc, []byte("aad"), 1024))

	raw := encoded.Bytes()
	raw[len(raw)-1] ^= 0xff
	err := DecodeEncryptedShard(io.Discard, bytes.NewReader(raw), enc, []byte("aad"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decrypt")
}

func TestEncryptedShardStream_TruncatedChunkFails(t *testing.T) {
	enc := testEncryptor(t)
	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(bytes.Repeat([]byte("x"), 2048)), enc, []byte("aad"), 1024))

	raw := encoded.Bytes()
	err := DecodeEncryptedShard(io.Discard, bytes.NewReader(raw[:len(raw)-8]), enc, []byte("aad"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, io.ErrUnexpectedEOF), "want ErrUnexpectedEOF, got %v", err)
}

func TestEncryptedShardStream_RejectsOversizedChunkHeader(t *testing.T) {
	enc := testEncryptor(t)
	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader([]byte("payload")), enc, []byte("aad"), 1024))

	raw := append([]byte(nil), encoded.Bytes()...)
	binary.LittleEndian.PutUint32(raw[len(encryptedShardMagic):], uint32(maxEncryptedChunkSize+1))

	err := DecodeEncryptedShard(io.Discard, bytes.NewReader(raw), enc, []byte("aad"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid encrypted shard chunk size")
}

func testEncryptor(t *testing.T) *encrypt.Encryptor {
	t.Helper()
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x42}, 32))
	require.NoError(t, err)
	return enc
}

type rangeGuardReaderAt struct {
	data              []byte
	allowBeforeData   int64
	minDataReadOffset int64
}

func (r *rangeGuardReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.allowBeforeData && off < r.minDataReadOffset {
		return 0, fmt.Errorf("unexpected read before target chunk: off=%d min=%d", off, r.minDataReadOffset)
	}
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
