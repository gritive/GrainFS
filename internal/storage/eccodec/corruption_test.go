package eccodec

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errReader returns a fixed synthetic error on the first Read, simulating a
// live I/O fault (e.g. EIO) rather than a clean end-of-stream. It must surface
// the error from Read itself so io.ReadFull reports the underlying error, not
// io.ErrUnexpectedEOF.
type errReader struct{ err error }

func (r *errReader) Read([]byte) (int, error) { return 0, r.err }

func TestIsCorruption(t *testing.T) {
	enc := testEncryptor(t)

	tests := []struct {
		name string
		// build returns the error to classify.
		build func(t *testing.T) error
		want  bool
	}{
		{
			name: "nil error",
			build: func(t *testing.T) error {
				return nil
			},
			want: false,
		},
		{
			name: "generic error",
			build: func(t *testing.T) error {
				return errors.New("something else")
			},
			want: false,
		},
		{
			name: "os error stays transient",
			build: func(t *testing.T) error {
				_, err := ReadShardVerified("/nonexistent/shard/path")
				require.Error(t, err)
				return err
			},
			want: false,
		},
		{
			name: "CRC-tampered plain shard",
			build: func(t *testing.T) error {
				encoded := EncodeShard([]byte("healthy shard payload"))
				// flip a byte in the payload so the CRC no longer matches.
				encoded[len(shardMagic)+1] ^= 0xFF
				_, err := DecodeShard(encoded)
				require.Error(t, err)
				return err
			},
			want: true,
		},
		{
			name: "truncated plain shard (cut into footer)",
			build: func(t *testing.T) error {
				encoded := EncodeShard([]byte("payload that will be truncated"))
				// drop part of the trailing CRC footer.
				_, err := DecodeShard(encoded[:len(encoded)-2])
				require.Error(t, err)
				return err
			},
			want: true,
		},
		{
			name: "plain shard shorter than footer",
			build: func(t *testing.T) error {
				_, err := DecodeShard([]byte{0x01, 0x02})
				require.Error(t, err)
				return err
			},
			want: true,
		},
		{
			name: "bad magic encrypted shard",
			build: func(t *testing.T) error {
				bad := make([]byte, encryptedHeaderLen)
				copy(bad, []byte("NOTMAGIC"))
				_, err := NewEncryptedShardReader(bytes.NewReader(bad), enc, []byte("aad"))
				require.Error(t, err)
				return err
			},
			want: true,
		},
		{
			name: "truncated encrypted header",
			build: func(t *testing.T) error {
				// fewer bytes than the fixed header: io.ReadFull -> ErrUnexpectedEOF.
				_, err := NewEncryptedShardReader(bytes.NewReader(encryptedShardMagic), enc, []byte("aad"))
				require.Error(t, err)
				return err
			},
			want: true,
		},
		{
			name: "invalid encrypted chunk size in header",
			build: func(t *testing.T) error {
				var header [encryptedHeaderLen]byte
				copy(header[:], encryptedShardMagic)
				// chunkSize left as 0 -> invalid.
				_, err := NewEncryptedShardReader(bytes.NewReader(header[:]), enc, []byte("aad"))
				require.Error(t, err)
				return err
			},
			want: true,
		},
		{
			name: "bad chunk-length encrypted shard",
			build: func(t *testing.T) error {
				data := []byte("encrypted shard plaintext content")
				aad := []byte("v2/bucket/key/0")
				var encoded bytes.Buffer
				require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), enc, aad, DefaultEncryptedChunkSize))
				raw := encoded.Bytes()
				// corrupt the first chunk header's plaintext-length field so it
				// exceeds chunkSize (structural decode failure).
				raw[encryptedHeaderLen] = 0xFF
				raw[encryptedHeaderLen+1] = 0xFF
				raw[encryptedHeaderLen+2] = 0xFF
				raw[encryptedHeaderLen+3] = 0xFF
				err := DecodeEncryptedShard(io.Discard, bytes.NewReader(raw), enc, aad)
				require.Error(t, err)
				return err
			},
			want: true,
		},
		{
			name: "truncated encrypted payload (cut mid-ciphertext)",
			build: func(t *testing.T) error {
				data := []byte("encrypted shard plaintext content for truncation")
				aad := []byte("v2/bucket/key/0")
				var encoded bytes.Buffer
				require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), enc, aad, DefaultEncryptedChunkSize))
				raw := encoded.Bytes()
				// keep the full chunk header but cut the ciphertext short:
				// io.ReadFull on the payload -> ErrUnexpectedEOF (truncation).
				err := DecodeEncryptedShard(io.Discard, bytes.NewReader(raw[:len(raw)-4]), enc, aad)
				require.Error(t, err)
				return err
			},
			want: true,
		},
		{
			// AEAD-failure classification follows audit #2: key selection on the
			// shard read path is deterministic (single static --encryption-key-file,
			// no versioned data-shard keystore / rotation), so a tag failure on an
			// owned shard is tampered/corrupt bytes, not a key-version race.
			// Therefore: corruption.
			name: "byte-tampered encrypted shard (AEAD tag fails)",
			build: func(t *testing.T) error {
				data := []byte("encrypted shard plaintext content for tag tampering")
				aad := []byte("v2/bucket/key/0")
				var encoded bytes.Buffer
				require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), enc, aad, DefaultEncryptedChunkSize))
				raw := encoded.Bytes()
				// flip a byte inside the ciphertext (after header + chunk header)
				// so the GCM tag check fails without changing any length field.
				raw[encryptedHeaderLen+encryptedChunkHeaderLen+1] ^= 0xFF
				err := DecodeEncryptedShard(io.Discard, bytes.NewReader(raw), enc, aad)
				require.Error(t, err)
				return err
			},
			want: true,
		},
		{
			// Codec-boundary transient test: a real non-EOF read fault during
			// header parse must propagate unwrapped so the monitor treats it as
			// transient and does NOT quarantine.
			name: "transient read fault at codec boundary",
			build: func(t *testing.T) error {
				_, err := NewEncryptedShardReader(&errReader{err: errors.New("input/output error")}, enc, []byte("aad"))
				require.Error(t, err)
				return err
			},
			want: false,
		},
		{
			// Same transient fault while reading a chunk payload (past the
			// header) must also stay transient.
			name: "transient read fault during chunk read",
			build: func(t *testing.T) error {
				data := []byte("encrypted shard plaintext content")
				aad := []byte("v2/bucket/key/0")
				var encoded bytes.Buffer
				require.NoError(t, EncodeEncryptedShard(&encoded, bytes.NewReader(data), enc, aad, DefaultEncryptedChunkSize))
				raw := encoded.Bytes()
				// serve the header cleanly, then return a synthetic I/O fault
				// when the chunk header / payload is read.
				r := &faultAfterReader{data: raw, faultAt: encryptedHeaderLen, err: errors.New("input/output error")}
				err := DecodeEncryptedShard(io.Discard, r, enc, aad)
				require.Error(t, err)
				return err
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.build(t)
			assert.Equal(t, tt.want, IsCorruption(err), "err=%v", err)
		})
	}
}

// faultAfterReader serves data normally until faultAt bytes have been read,
// then returns a synthetic (non-EOF) error, simulating a live I/O fault that
// surfaces after a structurally valid prefix.
type faultAfterReader struct {
	data    []byte
	pos     int
	faultAt int
	err     error
}

func (r *faultAfterReader) Read(p []byte) (int, error) {
	if r.pos >= r.faultAt {
		return 0, r.err
	}
	n := copy(p, r.data[r.pos:r.faultAt])
	r.pos += n
	return n, nil
}

// TestIsCorruption_WPreservedThroughWrapping verifies that the corruption
// sentinel survives an extra fmt.Errorf("...: %w") layer, matching the wrap
// sites between eccodec and ShardService.ReadLocalShard ("decrypt shard: %w").
func TestIsCorruption_WPreservedThroughWrapping(t *testing.T) {
	base := fmt.Errorf("decrypt shard chunk %d: %w", 0, ErrShardCorrupt)
	wrapped := fmt.Errorf("decrypt shard: %w", base)
	assert.True(t, IsCorruption(wrapped))

	crc := fmt.Errorf("read shard: %w", ErrCRCMismatch)
	assert.True(t, IsCorruption(crc))

	transient := fmt.Errorf("decrypt shard: %w", errors.New("input/output error"))
	assert.False(t, IsCorruption(transient))
}

// faultReaderAt serves data normally for ReadAt below faultAt, then returns a
// synthetic non-EOF error, simulating a live I/O fault on the range path.
type faultReaderAt struct {
	data    []byte
	faultAt int64
	err     error
}

func (r *faultReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.faultAt {
		return 0, r.err
	}
	end := off + int64(len(p))
	if end > int64(len(r.data)) {
		end = int64(len(r.data))
	}
	n := copy(p, r.data[off:end])
	if n < len(p) {
		return n, r.err
	}
	return n, nil
}

// TestIsCorruption_RangePath mirrors TestIsCorruption for the range-read APIs
// (NewEncryptedShardRangeReader / ReadEncryptedShardRangeAt) so corruption
// classification is read-API-independent.
func TestIsCorruption_RangePath(t *testing.T) {
	enc := testEncryptor(t)
	aad := []byte("v2/bucket/key/0")
	plaintext := bytes.Repeat([]byte("range-read corruption probe "), 64)

	// encodeShard returns a valid GFSENC2 stream for plaintext.
	encodeShard := func(t *testing.T) []byte {
		t.Helper()
		var buf bytes.Buffer
		require.NoError(t, EncodeEncryptedShard(&buf, bytes.NewReader(plaintext), enc, aad, DefaultEncryptedChunkSize))
		return buf.Bytes()
	}

	// readRangeReader drains NewEncryptedShardRangeReader over [0, len).
	readRangeReader := func(t *testing.T, raw []byte) error {
		t.Helper()
		rr, err := NewEncryptedShardRangeReader(bytes.NewReader(raw), enc, aad, 0, int64(len(plaintext)))
		if err != nil {
			return err
		}
		_, err = io.Copy(io.Discard, rr)
		return err
	}

	// readRangeAt reads the whole plaintext through ReadEncryptedShardRangeAt.
	readRangeAt := func(t *testing.T, raw []byte) error {
		t.Helper()
		dst := make([]byte, len(plaintext))
		_, err := ReadEncryptedShardRangeAt(bytes.NewReader(raw), enc, aad, 0, dst)
		return err
	}

	tests := []struct {
		name  string
		build func(t *testing.T) error
		want  bool
	}{
		{
			name: "RangeReader bad magic",
			build: func(t *testing.T) error {
				raw := encodeShard(t)
				copy(raw, []byte("NOTMAGIC"))
				return readRangeReader(t, raw)
			},
			want: true,
		},
		{
			name: "RangeReader truncated payload",
			build: func(t *testing.T) error {
				raw := encodeShard(t)
				return readRangeReader(t, raw[:len(raw)-4])
			},
			want: true,
		},
		{
			name: "RangeReader AEAD tamper",
			build: func(t *testing.T) error {
				raw := encodeShard(t)
				raw[encryptedHeaderLen+encryptedChunkHeaderLen+1] ^= 0xFF
				return readRangeReader(t, raw)
			},
			want: true,
		},
		{
			name: "RangeReader transient I/O fault",
			build: func(t *testing.T) error {
				raw := encodeShard(t)
				// header reads cleanly; the chunk read hits a synthetic EIO.
				rr, err := NewEncryptedShardRangeReader(
					&faultReaderAt{data: raw, faultAt: encryptedHeaderLen, err: errors.New("input/output error")},
					enc, aad, 0, int64(len(plaintext)))
				require.NoError(t, err)
				_, err = io.Copy(io.Discard, rr)
				require.Error(t, err)
				return err
			},
			want: false,
		},
		{
			name: "RangeAt bad magic",
			build: func(t *testing.T) error {
				raw := encodeShard(t)
				copy(raw, []byte("NOTMAGIC"))
				return readRangeAt(t, raw)
			},
			want: true,
		},
		{
			name: "RangeAt truncated payload",
			build: func(t *testing.T) error {
				raw := encodeShard(t)
				return readRangeAt(t, raw[:len(raw)-4])
			},
			want: true,
		},
		{
			name: "RangeAt AEAD tamper",
			build: func(t *testing.T) error {
				raw := encodeShard(t)
				raw[encryptedHeaderLen+encryptedChunkHeaderLen+1] ^= 0xFF
				return readRangeAt(t, raw)
			},
			want: true,
		},
		{
			name: "RangeAt transient I/O fault",
			build: func(t *testing.T) error {
				raw := encodeShard(t)
				dst := make([]byte, len(plaintext))
				_, err := ReadEncryptedShardRangeAt(
					&faultReaderAt{data: raw, faultAt: encryptedHeaderLen, err: errors.New("input/output error")},
					enc, aad, 0, dst)
				require.Error(t, err)
				return err
			},
			want: false,
		},
		{
			// Multi-chunk shard truncated AFTER chunk 0 reads cleanly. chunk 0
			// succeeds (done > 0), then chunk 1's header/payload is past EOF. The
			// caller must preserve ErrShardCorrupt rather than remapping the
			// truncation to a bare io.ErrUnexpectedEOF (the single-chunk truncation
			// case never exercises that remap because done == 0).
			name: "RangeAt multi-chunk truncated after first chunk",
			build: func(t *testing.T) error {
				const chunkSize = 32
				var buf bytes.Buffer
				require.NoError(t, EncodeEncryptedShard(&buf, bytes.NewReader(plaintext), enc, aad, chunkSize))
				raw := buf.Bytes()
				// Cut a few bytes into chunk 1 so chunk 0 stays intact.
				fullCipherLen := chunkSize + enc.AEADOverhead()
				cut := encryptedHeaderLen + encryptedChunkHeaderLen + fullCipherLen + 2
				require.Less(t, cut, len(raw), "expected a multi-chunk shard")
				dst := make([]byte, len(plaintext))
				n, err := ReadEncryptedShardRangeAt(bytes.NewReader(raw[:cut]), enc, aad, 0, dst)
				require.Error(t, err)
				require.Greater(t, n, 0, "chunk 0 should have been read before truncation")
				return err
			},
			want: true,
		},
		{
			// Same layout via the streaming range reader, to confirm both range
			// paths agree on classification.
			name: "RangeReader multi-chunk truncated after first chunk",
			build: func(t *testing.T) error {
				const chunkSize = 32
				var buf bytes.Buffer
				require.NoError(t, EncodeEncryptedShard(&buf, bytes.NewReader(plaintext), enc, aad, chunkSize))
				raw := buf.Bytes()
				fullCipherLen := chunkSize + enc.AEADOverhead()
				cut := encryptedHeaderLen + encryptedChunkHeaderLen + fullCipherLen + 2
				require.Less(t, cut, len(raw), "expected a multi-chunk shard")
				rr, err := NewEncryptedShardRangeReader(bytes.NewReader(raw[:cut]), enc, aad, 0, int64(len(plaintext)))
				require.NoError(t, err)
				_, err = io.Copy(io.Discard, rr)
				require.Error(t, err)
				return err
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.build(t)
			assert.Equal(t, tt.want, IsCorruption(err), "err=%v", err)
		})
	}
}

// TestRangePath_HealthyExactMultiple_NotCorruption is the regression test for
// the P1 where a healthy shard whose plaintext is an exact multiple of
// chunkSize (no trailing partial chunk) was mis-classified as corruption when
// a read advanced past the last chunk and the next chunk-header ReadAt hit a
// clean (n==0) io.EOF. A clean read must return the data; an over-read by one
// byte must surface a non-IsCorruption error.
func TestRangePath_HealthyExactMultiple_NotCorruption(t *testing.T) {
	enc := testEncryptor(t)
	aad := []byte("v2/bucket/key/0")
	const chunkSize = 32
	// Exactly two full chunks, no trailing partial.
	plaintext := bytes.Repeat([]byte("Q"), 2*chunkSize)

	var buf bytes.Buffer
	require.NoError(t, EncodeEncryptedShard(&buf, bytes.NewReader(plaintext), enc, aad, chunkSize))
	raw := buf.Bytes()

	t.Run("ReadEncryptedShardRangeAt full read", func(t *testing.T) {
		dst := make([]byte, len(plaintext))
		n, err := ReadEncryptedShardRangeAt(bytes.NewReader(raw), enc, aad, 0, dst)
		require.NoError(t, err)
		assert.Equal(t, len(plaintext), n)
		assert.Equal(t, plaintext, dst)
	})

	t.Run("NewEncryptedShardRangeReader full stream", func(t *testing.T) {
		rr, err := NewEncryptedShardRangeReader(bytes.NewReader(raw), enc, aad, 0, int64(len(plaintext)))
		require.NoError(t, err)
		got, err := io.ReadAll(rr)
		require.NoError(t, err)
		assert.Equal(t, plaintext, got)
	})

	t.Run("ReadEncryptedShardRangeAt over-read by one byte", func(t *testing.T) {
		// Requesting one byte past the true end of a HEALTHY shard must NOT be
		// corruption: the next chunk-header ReadAt sees a clean n==0 EOF, which
		// ReadEncryptedShardRangeAt remaps to io.ErrUnexpectedEOF because some
		// bytes were already delivered.
		dst := make([]byte, len(plaintext)+1)
		n, err := ReadEncryptedShardRangeAt(bytes.NewReader(raw), enc, aad, 0, dst)
		require.Error(t, err)
		assert.False(t, IsCorruption(err), "over-read on a healthy shard must not be corruption, err=%v", err)
		assert.Equal(t, len(plaintext), n)
		assert.Equal(t, plaintext, dst[:n])
	})

	t.Run("NewEncryptedShardRangeReader over-range request", func(t *testing.T) {
		// Range length extends past the true plaintext end of a healthy shard.
		rr, err := NewEncryptedShardRangeReader(bytes.NewReader(raw), enc, aad, 0, int64(len(plaintext)+8))
		require.NoError(t, err)
		got, err := io.ReadAll(rr)
		// io.ReadAll swallows io.EOF; a clean end-of-stream yields no error and
		// the true bytes. Either way it must never be IsCorruption.
		assert.False(t, IsCorruption(err), "over-range on a healthy shard must not be corruption, err=%v", err)
		assert.Equal(t, plaintext, got)
	})
}
