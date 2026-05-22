package putpipeline

import (
	"bytes"
	"sync"
)

var bufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func newBuffer() *bytes.Buffer {
	return bufPool.Get().(*bytes.Buffer)
}

func releaseBuffer(b *bytes.Buffer) {
	b.Reset()
	bufPool.Put(b)
}
