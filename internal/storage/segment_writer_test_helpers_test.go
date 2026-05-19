package storage

import (
	"context"
	"io"
)

// makePattern returns a deterministic non-repeating byte pattern of length n.
// Identical chunks of the same size are unlikely to mask bugs because each
// byte depends on its index.
func makePattern(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((i * 31) ^ (i >> 8))
	}
	return out
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
