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
	"unsafe"

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

// shardChunkOverhead derives the per-chunk ciphertext expansion the seam adds,
// cipher-agnostic: it seals a 1-byte plaintext and measures len(ct)-1. This
// tracks the underlying AEAD (AES-256-GCM nonce=12 → 31; XAES-256-GCM nonce=24
// → 43) so offset arithmetic in tests never hardcodes a cipher-specific value.
func shardChunkOverhead(t *testing.T, enc ShardEncryptor) int {
	t.Helper()
	ct, _, err := enc.Seal(encrypt.DomainShard, shardBaseFields(), []byte("x"))
	if err != nil {
		t.Fatalf("seal probe for overhead: %v", err)
	}
	return len(ct) - 1
}

func TestEncryptedShardRangeReader_DoesNotReadSkippedChunks(t *testing.T) {
	f := newFakeShardEncryptor(t)
	data := bytes.Repeat([]byte("0123456789abcdef"), 512)
	fields := shardBaseFields()
	const chunkSize = 1024
	overhead := shardChunkOverhead(t, f)

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
	overhead := shardChunkOverhead(t, f)

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
// and reports a gen it can advance mid-stream to simulate a DEK rotation racing
// an in-flight encode. sealAtGenGens records the gen argument of every SealAtGen
// call so tests can assert chunks 1+ pinned chunk 0's gen.
type fakeShardEncryptor struct {
	enc           *encrypt.Encryptor
	gen           uint32
	advance       bool
	sealAtGenGens []uint32
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

func (f *fakeShardEncryptor) SealAtGen(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte, gen uint32) ([]byte, error) {
	f.sealAtGenGens = append(f.sealAtGenGens, gen)
	return f.enc.SealValueAADTo(nil, f.aad(domain, fields), plain)
}

func (f *fakeShardEncryptor) Open(domain encrypt.AADDomain, fields []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	return f.enc.OpenValueAADTo(nil, f.aad(domain, fields), ct)
}

func (f *fakeShardEncryptor) OpenTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	return f.enc.OpenValueAADTo(dst, f.aad(domain, fields), ct)
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

// A DEK rotation racing the encode (gen drifts after chunk 0) must NOT fail the
// write (S4): chunks 1+ are sealed AT chunk 0's pinned gen, so the header's
// dek_gen describes every chunk and the shard round-trips.
func TestEncodeEncryptedShard_PinsGenAcrossMidStreamDrift(t *testing.T) {
	f := newFakeShardEncryptor(t)
	f.advance = true                                                // active gen drifts after chunk 0
	plain := bytes.Repeat([]byte("x"), DefaultEncryptedChunkSize+1) // forces a 2nd chunk
	var buf bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&buf, bytes.NewReader(plain), f, shardBaseFields(), DefaultEncryptedChunkSize),
		"mid-stream gen drift must not fail the encode")

	gen, _, _, err := readEncryptedShardHeader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, uint32(0), gen, "header pins chunk 0's gen")
	require.NotEmpty(t, f.sealAtGenGens, "chunk 1+ must go through SealAtGen")
	for i, g := range f.sealAtGenGens {
		require.Equalf(t, uint32(0), g, "chunk %d sealed at gen %d, want pinned 0", i+1, g)
	}

	var out bytes.Buffer
	require.NoError(t, DecodeEncryptedShard(&out, bytes.NewReader(buf.Bytes()), f, shardBaseFields()))
	require.Equal(t, plain, out.Bytes(), "round-trip mismatch")
}

// Same race, streaming writer (cpupool's encoder): it has already flushed the
// header + earlier chunks, so it must pin in a single pass.
func TestEncryptedShardChunkedWriter_PinsGenAcrossMidStreamDrift(t *testing.T) {
	f := newFakeShardEncryptor(t)
	f.advance = true
	var buf bytes.Buffer
	w, err := NewEncryptedShardChunkedWriter(&buf, f, shardBaseFields(), DefaultEncryptedChunkSize)
	require.NoError(t, err)
	plain := bytes.Repeat([]byte("s"), DefaultEncryptedChunkSize*2+5) // ≥3 chunks
	_, err = w.Write(plain)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	gen, _, _, err := readEncryptedShardHeader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, uint32(0), gen, "header pins chunk 0's gen")
	require.NotEmpty(t, f.sealAtGenGens, "chunks 1+ must go through SealAtGen")
	for i, g := range f.sealAtGenGens {
		require.Equalf(t, uint32(0), g, "streamed chunk %d sealed at gen %d, want pinned 0", i+1, g)
	}

	var out bytes.Buffer
	require.NoError(t, DecodeEncryptedShard(&out, bytes.NewReader(buf.Bytes()), f, shardBaseFields()))
	require.Equal(t, plain, out.Bytes(), "round-trip mismatch")
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

type errInjectShardEncryptor struct {
	inner   ShardEncryptor
	openErr error
}

func (e *errInjectShardEncryptor) Seal(d encrypt.AADDomain, f []encrypt.AADField, p []byte) ([]byte, uint32, error) {
	return e.inner.Seal(d, f, p)
}
func (e *errInjectShardEncryptor) SealAtGen(d encrypt.AADDomain, f []encrypt.AADField, p []byte, gen uint32) ([]byte, error) {
	return e.inner.SealAtGen(d, f, p, gen)
}
func (e *errInjectShardEncryptor) Open(_ encrypt.AADDomain, _ []encrypt.AADField, _ uint32, _ []byte) ([]byte, error) {
	return nil, e.openErr
}
func (e *errInjectShardEncryptor) OpenTo(_ []byte, _ encrypt.AADDomain, _ []encrypt.AADField, _ uint32, _ []byte) ([]byte, error) {
	return nil, e.openErr
}

func TestEncryptedShardReader_DEKGenUnknown_IsNotCorruption(t *testing.T) {
	real := newFakeShardEncryptor(t)
	plain := bytes.Repeat([]byte("g"), DefaultEncryptedChunkSize+5) // 2 chunks
	var buf bytes.Buffer
	if err := EncodeEncryptedShard(&buf, bytes.NewReader(plain), real, shardBaseFields(), DefaultEncryptedChunkSize); err != nil {
		t.Fatalf("encode: %v", err)
	}
	genUnknown := &errInjectShardEncryptor{inner: real, openErr: encrypt.ErrDEKGenUnknown}

	// Streaming reader path.
	var out bytes.Buffer
	err := DecodeEncryptedShard(&out, bytes.NewReader(buf.Bytes()), genUnknown, shardBaseFields())
	if err == nil || IsCorruption(err) || !errors.Is(err, encrypt.ErrDEKGenUnknown) {
		t.Fatalf("streaming: want transient+unwrapped, got %v (corrupt=%v)", err, IsCorruption(err))
	}

	// Range function path.
	dst := make([]byte, 100)
	_, rerr := ReadEncryptedShardRangeAt(bytes.NewReader(buf.Bytes()), genUnknown, shardBaseFields(), 0, dst)
	if rerr == nil || IsCorruption(rerr) || !errors.Is(rerr, encrypt.ErrDEKGenUnknown) {
		t.Fatalf("range func: got %v (corrupt=%v)", rerr, IsCorruption(rerr))
	}

	// Range reader struct path (readEncryptedShardChunkAt) via NewEncryptedShardRangeReader.
	rr, nerr := NewEncryptedShardRangeReader(bytes.NewReader(buf.Bytes()), genUnknown, shardBaseFields(), 0, int64(len(plain)))
	if nerr != nil {
		t.Fatalf("NewEncryptedShardRangeReader (header read should succeed): %v", nerr)
	}
	_, rrerr := io.ReadAll(rr)
	if rrerr == nil || IsCorruption(rrerr) || !errors.Is(rrerr, encrypt.ErrDEKGenUnknown) {
		t.Fatalf("range reader struct: got %v (corrupt=%v)", rrerr, IsCorruption(rrerr))
	}
}

func TestEncryptedShardReader_AEADFailure_IsCorruption(t *testing.T) {
	real := newFakeShardEncryptor(t)
	plain := bytes.Repeat([]byte("a"), DefaultEncryptedChunkSize+5)
	var buf bytes.Buffer
	if err := EncodeEncryptedShard(&buf, bytes.NewReader(plain), real, shardBaseFields(), DefaultEncryptedChunkSize); err != nil {
		t.Fatalf("encode: %v", err)
	}
	aeadFail := &errInjectShardEncryptor{inner: real, openErr: errors.New("cipher: message authentication failed")}
	var out bytes.Buffer
	err := DecodeEncryptedShard(&out, bytes.NewReader(buf.Bytes()), aeadFail, shardBaseFields())
	if err == nil || !IsCorruption(err) {
		t.Fatalf("AEAD failure must be corruption, got %v (corrupt=%v)", err, IsCorruption(err))
	}
}

// recordingShardEncryptor wraps a real fakeShardEncryptor and records, per
// OpenTo call, the backing-array pointer of the dst it received and of the
// slice it returned. It flags whether the plain Open path was ever taken. Used
// to prove the readers (a) never call Open and (b) reuse the same plaintext
// backing across chunks. No t.Fatal inside — record, assert after.
type recordingShardEncryptor struct {
	*fakeShardEncryptor
	openCalled bool
	dstPtrs    []uintptr // backing of dst passed to each OpenTo (0 for nil/empty)
	retPtrs    []uintptr // backing of slice returned by each OpenTo
}

func backingPtr(b []byte) uintptr {
	if cap(b) == 0 {
		return 0
	}
	return uintptr(unsafe.Pointer(unsafe.SliceData(b[:cap(b)])))
}

func (r *recordingShardEncryptor) Open(domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) ([]byte, error) {
	r.openCalled = true
	return r.fakeShardEncryptor.Open(domain, fields, gen, ct)
}

func (r *recordingShardEncryptor) OpenTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) ([]byte, error) {
	r.dstPtrs = append(r.dstPtrs, backingPtr(dst))
	out, err := r.fakeShardEncryptor.OpenTo(dst, domain, fields, gen, ct)
	r.retPtrs = append(r.retPtrs, backingPtr(out))
	return out, err
}

// assertReuse checks the readers used OpenTo (not Open) and, from the 2nd chunk
// onward, the dst backing handed to OpenTo equals the prior call's RETURNED
// backing — proving the plainFull/scratch buffer is reused, not freshly allocated.
func assertReuse(t *testing.T, rec *recordingShardEncryptor, wantChunks int) {
	t.Helper()
	assert.False(t, rec.openCalled, "reader must use OpenTo, never Open")
	require.GreaterOrEqual(t, len(rec.dstPtrs), wantChunks, "expected at least %d OpenTo calls", wantChunks)
	for i := 1; i < len(rec.dstPtrs); i++ {
		assert.Equalf(t, rec.retPtrs[i-1], rec.dstPtrs[i],
			"chunk %d: dst backing %#x != prior returned backing %#x (buffer not reused)",
			i, rec.dstPtrs[i], rec.retPtrs[i-1])
		assert.NotZerof(t, rec.dstPtrs[i], "chunk %d: dst backing must be a real (pre-sized) buffer", i)
	}
}

func TestEncryptedShardReaders_ReusePlaintextBufferAcrossChunks(t *testing.T) {
	const chunkSize = 1024
	// 4 full chunks (4096 bytes) so the reuse pattern is exercised multiple times.
	data := bytes.Repeat([]byte("0123456789abcdef"), 256)
	require.Equal(t, 4*chunkSize, len(data))
	fields := shardBaseFields()

	encode := func(t *testing.T) []byte {
		f := newFakeShardEncryptor(t)
		var buf bytes.Buffer
		require.NoError(t, EncodeEncryptedShard(&buf, bytes.NewReader(data), f, fields, chunkSize))
		return buf.Bytes()
	}

	t.Run("streaming_reader", func(t *testing.T) {
		payload := encode(t)
		rec := &recordingShardEncryptor{fakeShardEncryptor: newFakeShardEncryptor(t)}
		r, err := NewEncryptedShardReader(bytes.NewReader(payload), rec, fields)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		assert.Equal(t, data, got)
		if c, ok := r.(io.Closer); ok {
			require.NoError(t, c.Close())
		}
		assertReuse(t, rec, 4)
	})

	t.Run("range_reader", func(t *testing.T) {
		payload := encode(t)
		rec := &recordingShardEncryptor{fakeShardEncryptor: newFakeShardEncryptor(t)}
		r, err := NewEncryptedShardRangeReader(bytes.NewReader(payload), rec, fields, 0, int64(len(data)))
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		assert.Equal(t, data, got)
		if c, ok := r.(io.Closer); ok {
			require.NoError(t, c.Close())
		}
		assertReuse(t, rec, 4)
	})

	t.Run("read_range_at", func(t *testing.T) {
		payload := encode(t)
		rec := &recordingShardEncryptor{fakeShardEncryptor: newFakeShardEncryptor(t)}
		dst := make([]byte, len(data))
		n, err := ReadEncryptedShardRangeAt(bytes.NewReader(payload), rec, fields, 0, dst)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)
		assert.Equal(t, data, dst)
		assertReuse(t, rec, 4)
	})
}

// TestEncryptedShardReaders_PartialFinalChunkZeroesTail is the regression guard
// for the len→cap buffer-hygiene change. With a reused plaintext buffer, a FULL
// chunk fills the backing [0:chunkSize], then a PARTIAL final chunk decrypts only
// [0:plainLen] — the [plainLen:cap] tail must be zeroed (by the clear-before-OpenTo
// of the full capacity), NOT left holding the previous full chunk's plaintext. The
// uniform-full-chunk reuse test cannot catch a regression here (len==cap always),
// so this drives a non-chunk-aligned total and asserts the tail of the final
// (partial) chunk's backing is all-zero.
func TestEncryptedShardReaders_PartialFinalChunkZeroesTail(t *testing.T) {
	const chunkSize = 1024
	// 3 full chunks + a 200-byte partial final chunk, with distinctive per-byte
	// content so a residue from a prior full chunk would be a non-zero tail.
	data := make([]byte, 3*chunkSize+200)
	for i := range data {
		data[i] = byte('A' + i%26)
	}
	fields := shardBaseFields()

	encode := func(t *testing.T) []byte {
		f := newFakeShardEncryptor(t)
		var buf bytes.Buffer
		require.NoError(t, EncodeEncryptedShard(&buf, bytes.NewReader(data), f, fields, chunkSize))
		return buf.Bytes()
	}
	// assertTail checks the final chunk's plaintext window is exactly 200 bytes and
	// the backing's [200:cap] tail is fully zeroed (no prior full-chunk residue).
	assertTail := func(t *testing.T, plainFull []byte) {
		t.Helper()
		require.Equal(t, 200, len(plainFull), "final partial chunk plaintext len")
		require.GreaterOrEqual(t, cap(plainFull), chunkSize, "buffer pre-sized to chunkSize")
		tail := plainFull[len(plainFull):cap(plainFull)]
		for i, b := range tail {
			require.Zerof(t, b, "tail[%d] (backing index %d) not zeroed — prior chunk plaintext leaked", i, len(plainFull)+i)
		}
	}

	t.Run("streaming_reader", func(t *testing.T) {
		f := newFakeShardEncryptor(t)
		r, err := NewEncryptedShardReader(bytes.NewReader(encode(t)), f, fields)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, data, got)
		assertTail(t, r.(*encryptedShardReader).plainFull)
	})

	t.Run("range_reader", func(t *testing.T) {
		f := newFakeShardEncryptor(t)
		r, err := NewEncryptedShardRangeReader(bytes.NewReader(encode(t)), f, fields, 0, int64(len(data)))
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, data, got)
		assertTail(t, r.(*encryptedShardRangeReader).plainFull)
	})
}

func BenchmarkEncryptedShardRangeReaderRead5MiB(b *testing.B) {
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
		r, err := NewEncryptedShardRangeReader(bytes.NewReader(payload), f, fields, 0, int64(len(data)))
		require.NoError(b, err)
		_, err = io.Copy(io.Discard, r)
		require.NoError(b, err)
		if closer, ok := r.(io.Closer); ok {
			require.NoError(b, closer.Close())
		}
	}
}

func BenchmarkReadEncryptedShardRangeAt5MiB(b *testing.B) {
	rawEnc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x42}, 32))
	require.NoError(b, err)
	f := &fakeShardEncryptor{enc: rawEnc}
	data := bytes.Repeat([]byte("x"), 5<<20)
	fields := shardBaseFields()

	var encoded bytes.Buffer
	require.NoError(b, EncodeEncryptedShard(&encoded, bytes.NewReader(data), f, fields, DefaultEncryptedChunkSize))
	payload := bytes.NewReader(encoded.Bytes())
	dst := make([]byte, len(data))

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := ReadEncryptedShardRangeAt(payload, f, fields, 0, dst)
		require.NoError(b, err)
		require.Equal(b, len(data), n)
	}
}
