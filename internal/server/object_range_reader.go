package server

import (
	"context"
	"io"
	"sync"

	"github.com/gritive/GrainFS/internal/storage"
)

type objectReadAtBackend interface {
	ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error)
}

const maxRangeReadAtChunk = 5 << 20

var readAtRangeBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, maxRangeReadAtChunk)
		return &buf
	},
}

type readAtRangeReader struct {
	ctx            context.Context
	backend        objectReadAtBackend
	obj            *storage.Object
	bucket, key    string
	offset, length int64
	pos            int64
	buf            []byte
	bufPos         int
	bufEnd         int
	pooled         bool

	// prepared, when non-nil, is a stateful per-GET ReaderAt that caches
	// per-read state (e.g. one decompressed EC segment) across refills. It is
	// built ONCE by newReadAtRangeReader; preparedCleanup releases it on Close.
	prepared        storage.ObjectRangeReaderAt
	preparedCleanup func()
}

// newReadAtRangeReader builds a range reader over [offset, offset+length) and,
// when the backend offers the stateful PreparedObjectReaderAt fast path, wires it
// once so refills reuse its per-GET cache instead of re-deriving on every call.
func newReadAtRangeReader(ctx context.Context, backend objectReadAtBackend, obj *storage.Object, bucket, key string, offset, length int64) *readAtRangeReader {
	r := &readAtRangeReader{
		ctx:     ctx,
		backend: backend,
		obj:     obj,
		bucket:  bucket,
		key:     key,
		offset:  offset,
		length:  length,
	}
	if obj != nil {
		if f, ok := backend.(storage.PreparedObjectReaderAt); ok {
			prepared, cleanup := f.PreparedObjectReaderAt(ctx, bucket, key, obj)
			r.prepared = prepared
			r.preparedCleanup = cleanup
		}
	}
	return r
}

func (r *readAtRangeReader) Read(p []byte) (int, error) {
	if r.pos >= r.length {
		return 0, io.EOF
	}
	if r.bufPos >= r.bufEnd {
		if r.buf == nil {
			size := r.length
			if size > maxRangeReadAtChunk {
				size = maxRangeReadAtChunk
			}
			if size == maxRangeReadAtChunk {
				bufp := readAtRangeBufferPool.Get().(*[]byte)
				r.buf = (*bufp)[:maxRangeReadAtChunk]
				r.pooled = true
			} else {
				r.buf = make([]byte, int(size))
			}
		}
		want := len(r.buf)
		if remaining := r.length - r.pos; int64(want) > remaining {
			want = int(remaining)
		}
		n, err := r.readAt(r.offset+r.pos, r.buf[:want])
		if n > 0 {
			r.bufPos = 0
			r.bufEnd = n
		}
		if err != nil && n == 0 {
			return 0, err
		}
		if n == 0 {
			return 0, io.EOF
		}
	}
	n := copy(p, r.buf[r.bufPos:r.bufEnd])
	r.bufPos += n
	r.pos += int64(n)
	return n, nil
}

func (r *readAtRangeReader) readAt(offset int64, buf []byte) (int, error) {
	if r.prepared != nil {
		return r.prepared.ReadAt(offset, buf)
	}
	if r.obj != nil {
		if prepared, ok := r.backend.(storage.PreparedReadAt); ok {
			return prepared.ReadAtObject(r.ctx, r.bucket, r.key, r.obj, offset, buf)
		}
	}
	return r.backend.ReadAt(r.ctx, r.bucket, r.key, offset, buf)
}

func (r *readAtRangeReader) Close() error {
	if r.pooled && r.buf != nil {
		buf := r.buf[:maxRangeReadAtChunk]
		readAtRangeBufferPool.Put(&buf)
	}
	r.buf = nil
	r.bufPos = 0
	r.bufEnd = 0
	r.pooled = false
	if r.preparedCleanup != nil {
		r.preparedCleanup()
		r.preparedCleanup = nil
		r.prepared = nil
	}
	return nil
}

// streamingRangeReader serves a byte range from a single sequential object
// stream: it skips `skip` bytes once, then yields the next `remaining` bytes.
//
// It exists for stripe-interleaved objects, whose ReadAt is O(offset) (the
// de-interleave reader has no random-access seek yet). The generic
// readAtRangeReader refills in maxRangeReadAtChunk-sized ReadAt calls at growing
// offsets; on a striped object each call re-opens a full de-interleave stream and
// re-discards the whole prefix, turning a single Range GET into O(N^2) decode
// work. Holding ONE stream and skipping once collapses that back to O(N) for a
// full-range GET (and O(start) for a tail range — a single pass, not quadratic).
type streamingRangeReader struct {
	rc        io.ReadCloser
	skip      int64
	remaining int64
	didSkip   bool
}

func (r *streamingRangeReader) Read(p []byte) (int, error) {
	if !r.didSkip {
		if r.skip > 0 {
			if _, err := io.CopyN(io.Discard, r.rc, r.skip); err != nil {
				return 0, err
			}
		}
		r.didSkip = true
	}
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.rc.Read(p)
	r.remaining -= int64(n)
	return n, err
}

func (r *streamingRangeReader) Close() error {
	if r.rc != nil {
		return r.rc.Close()
	}
	return nil
}
