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

type objectPreparedReadAtBackend interface {
	ReadAtObject(ctx context.Context, bucket, key string, obj *storage.Object, offset int64, buf []byte) (int, error)
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
	if r.obj != nil {
		if prepared, ok := r.backend.(objectPreparedReadAtBackend); ok {
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
	return nil
}
