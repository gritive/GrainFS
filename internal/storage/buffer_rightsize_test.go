package storage

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// lenReader mirrors a buffered request body (e.g. server.putObjectBodyReader):
// a non-bytes.Reader stream whose remaining length is authoritative via Len().
type lenReader struct {
	data []byte
	off  int
}

func (r *lenReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}

func (r *lenReader) Len() int { return len(r.data) - r.off }

// A1: firstChunkBufferSize must right-size the first chunk for ANY reader that
// reports an authoritative remaining length via `interface{ Len() int }`, not
// just the three concrete stdlib types. A buffered HTTP body reaches the
// SegmentWriter as a custom type (not *bytes.Reader), so without this it
// allocates a full DefaultChunkSize (16 MiB) buffer for a tiny object.
func TestFirstChunkBufferSize_HonorsLenInterface(t *testing.T) {
	t.Parallel()
	r := &lenReader{data: make([]byte, 4096)}
	n, exact := firstChunkBufferSize(r, DefaultChunkSize)
	require.Equal(t, 4096, n)
	require.True(t, exact)
}

// Regression: the concrete stdlib readers must keep working through the
// interface path.
func TestFirstChunkBufferSize_BytesReaderStillSniffed(t *testing.T) {
	t.Parallel()
	n, exact := firstChunkBufferSize(bytes.NewReader(make([]byte, 1024)), DefaultChunkSize)
	require.Equal(t, 1024, n)
	require.True(t, exact)
}

// An opaque stream (no Len) must fall back to the default size.
func TestFirstChunkBufferSize_OpaqueReaderFallsBack(t *testing.T) {
	t.Parallel()
	n, exact := firstChunkBufferSize(io.LimitReader(bytes.NewReader(make([]byte, 1024)), 1024), DefaultChunkSize)
	require.Equal(t, DefaultChunkSize, n)
	require.False(t, exact)
}

// A1 (SizeHint): an opaque streaming body (no Len) whose Content-Length is known
// must size the chunk buffers to the object, not DefaultChunkSize. patternReader
// fills a hint-sized buffer exactly without signaling EOF, so the EOF-confirming
// read happens on a later iteration — the chunker must use a small probe there
// instead of a fresh 16 MiB buffer, or the win evaporates.
func TestSegmentWriter_SizeHintRightSizesChunks(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("race instrumentation inflates TotalAlloc, making the byte threshold meaningless")
	}
	const objSize = 256 << 10
	w := NewSegmentWriter(&byteWriterBackend{})
	perOp := allocBytesPerRunForStorageTest(t, 20, func() error {
		_, err := w.WriteSized(context.Background(), "b", "k", "application/octet-stream", newPatternReader(objSize), int64(objSize))
		return err
	})
	// Without the hint: one 16 MiB chunk. With a naive hint (no EOF probe): a
	// hint-sized chunk plus a trailing 16 MiB chunk. Correct: ~objSize.
	require.Less(t, perOp, uint64(2<<20), "per-op alloc bytes too high: %d", perOp)
}
