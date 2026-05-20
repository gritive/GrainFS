package storage

import (
	"context"
	"io"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// makePattern returns a deterministic non-repeating byte pattern of length n.
// Identical chunks of the same size are unlikely to mask bugs because each
// byte depends on its index.
func makePattern(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = patternByte(i)
	}
	return out
}

func patternByte(i int) byte {
	return byte((i * 31) ^ (i >> 8))
}

type patternReader struct {
	pos int
	n   int
}

func newPatternReader(n int) *patternReader {
	return &patternReader{n: n}
}

func (r *patternReader) Read(p []byte) (int, error) {
	if r.pos >= r.n {
		return 0, io.EOF
	}
	remaining := r.n - r.pos
	if len(p) > remaining {
		p = p[:remaining]
	}
	for i := range p {
		p[i] = patternByte(r.pos + i)
	}
	r.pos += len(p)
	return len(p), nil
}

func allocBytesPerRunForStorageTest(t testing.TB, runs int, run func() error) uint64 {
	t.Helper()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		require.NoError(t, run())
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

// errAfterNReader returns n bytes of data then err on subsequent reads.
type errAfterNReader struct {
	n   int
	err error
	pos int
}

func (r *errAfterNReader) Read(p []byte) (int, error) {
	if r.pos >= r.n {
		return 0, r.err
	}
	remaining := r.n - r.pos
	if remaining > len(p) {
		remaining = len(p)
	}
	for i := 0; i < remaining; i++ {
		p[i] = byte(r.pos + i)
	}
	r.pos += remaining
	return remaining, nil
}

// writeViaSegmentWriter is the test-side entry point that runs the
// SegmentWriter pipeline against a LocalBackend and persists the resulting
// Object record.
func writeViaSegmentWriter(b *LocalBackend, bucket, key string, r io.Reader) (*Object, error) {
	w := NewSegmentWriter(localBackendAdapter{b})
	obj, err := w.Write(context.Background(), bucket, key, "application/octet-stream", r)
	if err != nil {
		return nil, err
	}
	if err := b.PutObjectRecord(context.Background(), bucket, key, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

type byteWriterBackend struct {
	readerCalls int
	byteCalls   int
}

func (b *byteWriterBackend) WriteSegment(ctx context.Context, bucket, key string, idx int, r io.Reader) (SegmentRef, error) {
	b.readerCalls++
	data, err := io.ReadAll(r)
	if err != nil {
		return SegmentRef{}, err
	}
	return SegmentRef{BlobID: key, Size: int64(len(data))}, nil
}

func (b *byteWriterBackend) WriteSegmentBytes(ctx context.Context, bucket, key string, idx int, data []byte) (SegmentRef, error) {
	b.byteCalls++
	return SegmentRef{BlobID: key, Size: int64(len(data))}, nil
}
