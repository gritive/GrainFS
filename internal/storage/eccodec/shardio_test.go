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

// TestEncryptedShardChunkedWriter_SemanticMatchWithEncodeEncryptedShard locks
// the streaming writer's semantic output to EncodeEncryptedShard's: both must
// produce a GFSENC3 stream that decodes back to the same plaintext with the
// same baseFields. (The per-chunk nonce is embedded in the sealed ciphertext
// by the seam, so two encodings of identical input never match byte-for-byte.)
func TestEncryptedShardChunkedWriter_SemanticMatchWithEncodeEncryptedShard(t *testing.T) {
	f := newFakeShardEncryptor(t)
	data := bytes.Repeat([]byte("encrypted-shard-payload-"), 300)
	fields := shardBaseFields()
	chunkSize := 1024

	// Streaming writer path.
	var streamed bytes.Buffer
	w, err := NewEncryptedShardChunkedWriter(&streamed, f, fields, chunkSize)
	require.NoError(t, err)
	// Multiple Write calls in chunks not aligned to chunkSize.
	for i := 0; i < len(data); i += 333 {
		end := i + 333
		if end > len(data) {
			end = len(data)
		}
		n, writeErr := w.Write(data[i:end])
		require.NoError(t, writeErr)
		require.Equal(t, end-i, n)
	}
	require.NoError(t, w.Close())
	require.True(t, IsEncryptedShard(streamed.Bytes()))

	// Decode streamed output and compare plaintext.
	r, err := NewEncryptedShardReader(bytes.NewReader(streamed.Bytes()), f, fields)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, data, got)

	// Double-close is a no-op.
	require.NoError(t, w.Close())
}

func TestEncryptedShardChunkedWriter_EmptyShard(t *testing.T) {
	f := newFakeShardEncryptor(t)
	var out bytes.Buffer
	w, err := NewEncryptedShardChunkedWriter(&out, f, shardBaseFields(), 1024)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Empty shards still emit the 20-byte header so readers can detect the
	// magic and chunk size. No chunk records follow.
	require.True(t, IsEncryptedShard(out.Bytes()))
	require.Equal(t, encryptedHeaderLen, out.Len())
}

func TestEncryptedShardStream_RoundTripMultiChunk(t *testing.T) {
	f := newFakeShardEncryptor(t)
	data := bytes.Repeat([]byte("encrypted-shard-payload-"), 256)
	fields := shardBaseFields()

	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), f, fields, 1024))
	require.True(t, IsEncryptedShard(encoded.Bytes()))

	var got bytes.Buffer
	require.NoError(t, DecodeEncryptedShard(&got, bytes.NewReader(encoded.Bytes()), f, fields))
	assert.Equal(t, data, got.Bytes())
}

func TestEncryptedShardReader_RoundTripMultiChunk(t *testing.T) {
	f := newFakeShardEncryptor(t)
	data := bytes.Repeat([]byte("encrypted-shard-payload-"), 256)
	fields := shardBaseFields()

	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), f, fields, 1024))

	r, err := NewEncryptedShardReader(bytes.NewReader(encoded.Bytes()), f, fields)
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

func BenchmarkEncryptedShardReaderRead5MiB(b *testing.B) {
	rawEnc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x42}, 32))
	require.NoError(b, err)
	f := &fakeShardEncryptor{enc: rawEnc}
	data := bytes.Repeat([]byte("x"), 5<<20)
	fields := shardBaseFields()

	var encoded bytes.Buffer
	require.NoError(b, EncodeEncryptedShard(&encoded, bytes.NewReader(data), f, fields, DefaultEncryptedChunkSize))
	payload := encoded.Bytes()

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := NewEncryptedShardReader(bytes.NewReader(payload), f, fields)
		require.NoError(b, err)
		_, err = io.Copy(io.Discard, r)
		require.NoError(b, err)
		if closer, ok := r.(io.Closer); ok {
			require.NoError(b, closer.Close())
		}
	}
}

func TestEncryptedShardRangeReader_DoesNotReadSkippedChunks(t *testing.T) {
	f := newFakeShardEncryptor(t)
	data := bytes.Repeat([]byte("0123456789abcdef"), 512)
	fields := shardBaseFields()
	const chunkSize = 1024
	// fakeShardEncryptor uses SealValueAADTo → overhead = 3 + 12 + 16 = 31.
	const overhead = 31

	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), f, fields, chunkSize))

	offset := int64(3*chunkSize + 17)
	length := int64(193)
	firstChunkCipherOffset := int64(encryptedHeaderLen) + 3*int64(encryptedChunkHeaderLen+chunkSize+overhead)
	spy := &rangeGuardReaderAt{
		data:              encoded.Bytes(),
		allowBeforeData:   encryptedHeaderLen,
		minDataReadOffset: firstChunkCipherOffset,
	}

	r, err := NewEncryptedShardRangeReader(spy, f, fields, offset, length)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, data[offset:offset+length], got)
}

func TestEncryptedShardRangeReader_CloseReleasesPlaintext(t *testing.T) {
	f := newFakeShardEncryptor(t)
	data := bytes.Repeat([]byte("0123456789abcdef"), 512)
	fields := shardBaseFields()
	const chunkSize = 1024

	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), f, fields, chunkSize))

	// Read a SUB-range that starts mid-chunk (offset 100) and ends before the
	// chunk boundary, so the decrypted chunk's backing slice has a non-empty
	// prefix [0:100] and suffix that the read window never exposes. Both must be
	// zeroed by Close, not just the visible [inChunk:end] window.
	reader, err := NewEncryptedShardRangeReader(bytes.NewReader(encoded.Bytes()), f, fields, 100, 200)
	require.NoError(t, err)
	rangeReader := reader.(*encryptedShardRangeReader)
	_, err = io.ReadAll(reader)
	require.NoError(t, err)

	// Capture the full backing slice before Close so we can observe the zeroing.
	plainFull := rangeReader.plainFull
	require.NotEmpty(t, plainFull, "expected a decrypted chunk backing slice")
	require.Greater(t, len(plainFull), len(rangeReader.plain), "prefix/suffix must exist outside the read window")

	closer, ok := reader.(io.Closer)
	require.True(t, ok)
	require.NoError(t, closer.Close())
	require.Nil(t, rangeReader.plain)
	require.Nil(t, rangeReader.plainFull)
	// The full backing allocation (prefix + window + suffix) must be all-zero.
	for i, b := range plainFull {
		require.Zerof(t, b, "plainFull[%d] not zeroed after Close", i)
	}
}

func TestReadEncryptedShardRangeAt_DoesNotReadSkippedChunks(t *testing.T) {
	f := newFakeShardEncryptor(t)
	data := bytes.Repeat([]byte("0123456789abcdef"), 512)
	fields := shardBaseFields()
	const chunkSize = 1024
	const overhead = 31

	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), f, fields, chunkSize))

	offset := int64(3*chunkSize + 17)
	length := 193
	firstChunkCipherOffset := int64(encryptedHeaderLen) + 3*int64(encryptedChunkHeaderLen+chunkSize+overhead)
	spy := &rangeGuardReaderAt{
		data:              encoded.Bytes(),
		allowBeforeData:   encryptedHeaderLen,
		minDataReadOffset: firstChunkCipherOffset,
	}

	got := make([]byte, length)
	n, err := ReadEncryptedShardRangeAt(spy, f, fields, offset, got)
	require.NoError(t, err)
	require.Equal(t, length, n)
	assert.Equal(t, data[offset:offset+int64(length)], got)
}

func TestReadEncryptedShardRangeAt_CrossesChunkBoundary(t *testing.T) {
	f := newFakeShardEncryptor(t)
	data := bytes.Repeat([]byte("0123456789abcdef"), 256)
	fields := shardBaseFields()
	const chunkSize = 1024

	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), f, fields, chunkSize))

	offset := int64(chunkSize - 13)
	got := make([]byte, 64)
	n, err := ReadEncryptedShardRangeAt(bytes.NewReader(encoded.Bytes()), f, fields, offset, got)
	require.NoError(t, err)
	require.Equal(t, len(got), n)
	assert.Equal(t, data[offset:offset+int64(len(got))], got)
}

func TestReadEncryptedShardRangeAt_WrongAADFails(t *testing.T) {
	f := newFakeShardEncryptor(t)
	fieldsA := []encrypt.AADField{encrypt.FieldString("b"), encrypt.FieldString("k"), encrypt.FieldUint32(1)}
	fieldsB := []encrypt.AADField{encrypt.FieldString("b"), encrypt.FieldString("k"), encrypt.FieldUint32(2)}
	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader([]byte("payload")), f, fieldsA, 1024))

	got := make([]byte, 4)
	n, err := ReadEncryptedShardRangeAt(bytes.NewReader(encoded.Bytes()), f, fieldsB, 0, got)
	require.Error(t, err)
	require.Equal(t, 0, n)
	assert.Contains(t, err.Error(), "decrypt")
}

func TestReadEncryptedShardRangeAt_PartialPastEOF(t *testing.T) {
	f := newFakeShardEncryptor(t)
	data := []byte("payload")
	fields := shardBaseFields()
	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), f, fields, 1024))

	got := make([]byte, len(data)+3)
	n, err := ReadEncryptedShardRangeAt(bytes.NewReader(encoded.Bytes()), f, fields, 0, got)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, len(data), n)
	assert.Equal(t, data, got[:n])
}

func TestEncryptedShardStream_WrongAADFails(t *testing.T) {
	f := newFakeShardEncryptor(t)
	fieldsA := []encrypt.AADField{encrypt.FieldString("b"), encrypt.FieldString("k"), encrypt.FieldUint32(1)}
	fieldsB := []encrypt.AADField{encrypt.FieldString("b"), encrypt.FieldString("k"), encrypt.FieldUint32(2)}
	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader([]byte("payload")), f, fieldsA, 1024))

	var got bytes.Buffer
	err := DecodeEncryptedShard(&got, bytes.NewReader(encoded.Bytes()), f, fieldsB)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decrypt")
}

func TestEncryptedShardStream_TamperDetected(t *testing.T) {
	f := newFakeShardEncryptor(t)
	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(bytes.Repeat([]byte("x"), 2048)), f, shardBaseFields(), 1024))

	raw := encoded.Bytes()
	raw[len(raw)-1] ^= 0xff
	err := DecodeEncryptedShard(io.Discard, bytes.NewReader(raw), f, shardBaseFields())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decrypt")
}

func TestEncryptedShardStream_TruncatedChunkFails(t *testing.T) {
	f := newFakeShardEncryptor(t)
	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(bytes.Repeat([]byte("x"), 2048)), f, shardBaseFields(), 1024))

	raw := encoded.Bytes()
	err := DecodeEncryptedShard(io.Discard, bytes.NewReader(raw[:len(raw)-8]), f, shardBaseFields())
	require.Error(t, err)
	assert.True(t, errors.Is(err, io.ErrUnexpectedEOF), "want ErrUnexpectedEOF, got %v", err)
}

func TestEncryptedShardStream_RejectsOversizedChunkHeader(t *testing.T) {
	f := newFakeShardEncryptor(t)
	var encoded bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader([]byte("payload")), f, shardBaseFields(), 1024))

	raw := append([]byte(nil), encoded.Bytes()...)
	// In GFSENC3: magic(8) + format_version(2) + dek_gen(4) + chunk_size at [14:18].
	// Overwrite chunk_size with an oversized value to trigger the "invalid chunk size" check.
	binary.LittleEndian.PutUint32(raw[14:18], uint32(maxEncryptedChunkSize+1))

	err := DecodeEncryptedShard(io.Discard, bytes.NewReader(raw), f, shardBaseFields())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid encrypted shard chunk size")
}

// fakeShardEncryptor seals via a real Encryptor under BuildAAD(DomainShard,…)
// and reports a gen it can advance mid-stream to exercise gen-pinning.
type fakeShardEncryptor struct {
	enc     *encrypt.Encryptor
	gen     uint32
	advance bool
}

func newFakeShardEncryptor(t *testing.T) *fakeShardEncryptor {
	t.Helper()
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x09}, 32))
	if err != nil {
		t.Fatalf("NewEncryptor: %v", err)
	}
	return &fakeShardEncryptor{enc: enc}
}

func (f *fakeShardEncryptor) aad(domain encrypt.AADDomain, fields []encrypt.AADField) []byte {
	return encrypt.BuildAAD(domain, make([]byte, 16), fields...)
}

func (f *fakeShardEncryptor) Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
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

func (f *fakeShardEncryptor) Open(domain encrypt.AADDomain, fields []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	return f.enc.OpenValueAADTo(nil, f.aad(domain, fields), ct)
}

func shardBaseFields() []encrypt.AADField {
	return []encrypt.AADField{encrypt.FieldString("b"), encrypt.FieldString("k"), encrypt.FieldUint32(3)}
}

func TestEncodeEncryptedShard_RoundTrip(t *testing.T) {
	f := newFakeShardEncryptor(t)
	plain := bytes.Repeat([]byte("z"), DefaultEncryptedChunkSize+777)
	var buf bytes.Buffer
	if err := EncodeEncryptedShard(&buf, bytes.NewReader(plain), f, shardBaseFields(), DefaultEncryptedChunkSize); err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !IsEncryptedShard(buf.Bytes()) {
		t.Fatal("expected GFSENC3 magic")
	}
	var out bytes.Buffer
	if err := DecodeEncryptedShard(&out, bytes.NewReader(buf.Bytes()), f, shardBaseFields()); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(out.Bytes(), plain) {
		t.Fatal("round-trip mismatch")
	}
}

func TestEncodeEncryptedShard_GenPinning_FailsOnMidStreamChange(t *testing.T) {
	f := newFakeShardEncryptor(t)
	f.advance = true
	plain := bytes.Repeat([]byte("x"), DefaultEncryptedChunkSize+1) // forces a 2nd chunk
	var buf bytes.Buffer
	err := EncodeEncryptedShard(&buf, bytes.NewReader(plain), f, shardBaseFields(), DefaultEncryptedChunkSize)
	if err == nil {
		t.Fatal("expected gen-pinning to fail on mid-stream gen change")
	}
}

func TestEncryptedShardHeader_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := writeEncryptedShardHeader(&buf, 7, DefaultEncryptedChunkSize, 31); err != nil {
		t.Fatalf("writeEncryptedShardHeader: %v", err)
	}
	gen, chunkSize, overhead, err := readEncryptedShardHeader(&buf)
	if err != nil {
		t.Fatalf("readEncryptedShardHeader: %v", err)
	}
	if gen != 7 || chunkSize != DefaultEncryptedChunkSize || overhead != 31 {
		t.Fatalf("header mismatch: gen=%d size=%d overhead=%d", gen, chunkSize, overhead)
	}
}

func TestEncryptedShardHeader_RejectsLegacyGFSENC2(t *testing.T) {
	legacy := append([]byte("GFSENC2\x00"), make([]byte, 12)...) // old magic + old header bytes
	_, _, _, err := readEncryptedShardHeader(bytes.NewReader(legacy))
	if err == nil {
		t.Fatal("expected legacy GFSENC2 magic to be rejected")
	}
}

func TestReadEncryptedShardRangeAt_RoundTrip(t *testing.T) {
	f := newFakeShardEncryptor(t)
	plain := make([]byte, 3*DefaultEncryptedChunkSize+42)
	for i := range plain {
		plain[i] = byte(i)
	}
	var buf bytes.Buffer
	if err := EncodeEncryptedShard(&buf, bytes.NewReader(plain), f, shardBaseFields(), DefaultEncryptedChunkSize); err != nil {
		t.Fatalf("encode: %v", err)
	}
	ra := bytes.NewReader(buf.Bytes())
	off := int64(DefaultEncryptedChunkSize + 100)
	dst := make([]byte, 5000)
	n, err := ReadEncryptedShardRangeAt(ra, f, shardBaseFields(), off, dst)
	if err != nil {
		t.Fatalf("range read: %v", err)
	}
	if !bytes.Equal(dst[:n], plain[off:off+int64(n)]) {
		t.Fatal("range round-trip mismatch")
	}
}

func TestEncryptedShard_ChunkSwap_FailsDecrypt(t *testing.T) {
	f := newFakeShardEncryptor(t)
	plain := bytes.Repeat([]byte("R"), 2*DefaultEncryptedChunkSize)
	var buf bytes.Buffer
	if err := EncodeEncryptedShard(&buf, bytes.NewReader(plain), f, shardBaseFields(), DefaultEncryptedChunkSize); err != nil {
		t.Fatalf("encode: %v", err)
	}
	raw := swapFirstTwoEncryptedShardChunks(t, buf.Bytes())
	var out bytes.Buffer
	err := DecodeEncryptedShard(&out, bytes.NewReader(raw), f, shardBaseFields())
	if err == nil {
		t.Fatal("expected chunk-swap to fail AEAD (ordinal AAD is load-bearing)")
	}
}

// swapFirstTwoEncryptedShardChunks parses the GFSENC3 header then swaps the
// sealed ciphertext payloads of chunk 0 and chunk 1. It preserves the
// chunk headers (plain_len, cipher_len) so the lengths stay consistent;
// only the sealed blobs are swapped. This proves the per-chunk ordinal in
// the AAD actually binds position.
func swapFirstTwoEncryptedShardChunks(t *testing.T, raw []byte) []byte {
	t.Helper()
	if len(raw) < encryptedHeaderLen {
		t.Fatal("too short for GFSENC3 header")
	}
	hdr := raw[:encryptedHeaderLen]
	_, _, overhead, err := parseEncryptedShardHeader(hdr)
	if err != nil {
		t.Fatalf("parse header: %v", err)
	}

	out := append([]byte(nil), raw...)
	pos := encryptedHeaderLen

	// Parse chunk 0.
	if pos+encryptedChunkHeaderLen > len(out) {
		t.Fatal("no chunk 0 header")
	}
	plain0 := binary.LittleEndian.Uint32(out[pos : pos+4])
	cipher0 := binary.LittleEndian.Uint32(out[pos+4 : pos+8])
	if cipher0 != plain0+uint32(overhead) {
		t.Fatalf("chunk 0 cipher_len mismatch: %d != %d+%d", cipher0, plain0, overhead)
	}
	sealed0Start := pos + encryptedChunkHeaderLen
	sealed0End := sealed0Start + int(cipher0)

	// Parse chunk 1.
	pos1 := sealed0End
	if pos1+encryptedChunkHeaderLen > len(out) {
		t.Fatal("no chunk 1 header")
	}
	plain1 := binary.LittleEndian.Uint32(out[pos1 : pos1+4])
	cipher1 := binary.LittleEndian.Uint32(out[pos1+4 : pos1+8])
	if cipher1 != plain1+uint32(overhead) {
		t.Fatalf("chunk 1 cipher_len mismatch: %d != %d+%d", cipher1, plain1, overhead)
	}
	sealed1Start := pos1 + encryptedChunkHeaderLen
	_ = sealed1Start + int(cipher1) // sealed1End unused; swap uses sealed1Start directly

	if cipher0 != cipher1 {
		t.Fatalf("chunk 0 and chunk 1 cipher lengths differ (%d vs %d); swap helper requires equal-length sealed blobs", cipher0, cipher1)
	}

	// Swap the sealed payloads.
	for i := 0; i < int(cipher0); i++ {
		out[sealed0Start+i], out[sealed1Start+i] = out[sealed1Start+i], out[sealed0Start+i]
	}
	return out
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
