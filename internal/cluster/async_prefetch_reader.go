package cluster

import (
	"errors"
	"io"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/pool"
)

// asyncPrefetchChunkSize sizes each prefetched chunk. Picked to match the
// encrypted shard chunk size so the underlying reader yields whole chunks per
// pull, amortizing channel send/receive cost.
const asyncPrefetchChunkSize = 1 << 20

// asyncPrefetchAhead bounds how many chunks each shard reader buffers in
// flight. With 2 chunks per shard × 4 data shards ≈ 8 MiB of prefetch buffer
// per EC read, kept tight to avoid RSS pressure.
const asyncPrefetchAhead = 2

var asyncPrefetchChunkPool = pool.New(func() *[]byte {
	b := make([]byte, asyncPrefetchChunkSize)
	return &b
})

func acquireAsyncPrefetchChunk() *[]byte {
	bp := asyncPrefetchChunkPool.Get()
	b := (*bp)[:cap(*bp)]
	*bp = b
	return bp
}

func releaseAsyncPrefetchChunk(bp *[]byte) {
	if bp == nil {
		return
	}
	*bp = (*bp)[:cap(*bp)]
	asyncPrefetchChunkPool.Put(bp)
}

// asyncPrefetchReader runs src in a background goroutine that fills a small
// bounded channel of chunks. The foreground Read drains chunks in order.
//
// The goal is to overlap disk reads + decrypt CPU across multiple parallel
// asyncPrefetchReaders (one per EC data shard) so that, while io.MultiReader
// is sequentially draining shard i, shards i+1..N are already pulling and
// decrypting chunks into their buffers.
type asyncPrefetchReader struct {
	ch     chan asyncPrefetchChunk
	cur    []byte
	curRef *[]byte
	srcErr error
	closed atomic.Bool
	stop   chan struct{}
	done   chan struct{}
}

type asyncPrefetchChunk struct {
	ref *[]byte
	n   int
	err error
}

// newAsyncPrefetchReader wraps src in a background prefetcher. closeSrc, if
// non-nil, is invoked once when the returned reader's Close is called. It
// should release resources owned by src (typically a *os.File close).
func newAsyncPrefetchReader(src io.Reader, closeSrc func() error) *asyncPrefetchReader {
	r := &asyncPrefetchReader{
		ch:   make(chan asyncPrefetchChunk, asyncPrefetchAhead),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go r.run(src, closeSrc)
	return r
}

func (r *asyncPrefetchReader) run(src io.Reader, closeSrc func() error) {
	defer close(r.done)
	defer close(r.ch)
	defer func() {
		if closeSrc != nil {
			_ = closeSrc()
		}
	}()
	for {
		ref := acquireAsyncPrefetchChunk()
		buf := *ref
		n, err := io.ReadFull(src, buf)
		if n == 0 && err != nil {
			releaseAsyncPrefetchChunk(ref)
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return
			}
			select {
			case r.ch <- asyncPrefetchChunk{err: err}:
			case <-r.stop:
			}
			return
		}
		msg := asyncPrefetchChunk{ref: ref, n: n}
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			msg.err = err
		}
		select {
		case r.ch <- msg:
		case <-r.stop:
			releaseAsyncPrefetchChunk(ref)
			return
		}
		if err != nil {
			return
		}
	}
}

func (r *asyncPrefetchReader) Read(p []byte) (int, error) {
	if len(r.cur) == 0 {
		if r.curRef != nil {
			releaseAsyncPrefetchChunk(r.curRef)
			r.curRef = nil
		}
		msg, ok := <-r.ch
		if !ok {
			if r.srcErr != nil {
				return 0, r.srcErr
			}
			return 0, io.EOF
		}
		if msg.err != nil {
			r.srcErr = msg.err
			if msg.ref != nil {
				releaseAsyncPrefetchChunk(msg.ref)
			}
			return 0, msg.err
		}
		r.curRef = msg.ref
		r.cur = (*msg.ref)[:msg.n]
	}
	n := copy(p, r.cur)
	r.cur = r.cur[n:]
	return n, nil
}

func (r *asyncPrefetchReader) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(r.stop)
	// Drain any in-flight chunks so the producer can finish.
	for msg := range r.ch {
		if msg.ref != nil {
			releaseAsyncPrefetchChunk(msg.ref)
		}
	}
	<-r.done
	if r.curRef != nil {
		releaseAsyncPrefetchChunk(r.curRef)
		r.curRef = nil
	}
	return nil
}
