// Package storage — SegmentReader streams an object's segments in original
// index order, while fetching them in parallel with a bounded worker pool.
//
// Memory bound: chunkSize × workers (~128 MiB for 8 × 16 MiB).
package storage

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// DefaultGetWorkers is the per-request fetch concurrency for segment reads.
const DefaultGetWorkers = 8

// readExactlySizedObject reads exactly size bytes from r into a freshly
// allocated buffer. Avoids io.ReadAll's geometric grow on known-size reads.
func readExactlySizedObject(r io.Reader, size int64) ([]byte, error) {
	if size < 0 {
		return nil, fmt.Errorf("negative object size %d", size)
	}
	if size == 0 {
		return nil, nil
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// segmentStore abstracts the source of segment bytes (LocalBackend in single
// node, ECStreamingReader-backed adapter in cluster).
type segmentStore interface {
	OpenSegment(ctx context.Context, ref SegmentRef) (io.ReadCloser, error)
}

// SegmentReader implements io.Reader by fetching N segments in parallel and
// emitting them in original index order.
type SegmentReader struct {
	store   segmentStore
	refs    []SegmentRef
	workers int
	ctx     context.Context
	cancel  context.CancelFunc

	// pending is pre-populated for every ref at construction so Read() can
	// always look up the slot it needs without a nil-deref race against
	// the not-yet-scheduled worker. pending[i] itself is never re-assigned
	// after NewSegmentReader returns; each slot's ready channel synchronizes
	// the fetcher (producer) with Read (consumer). No mutex needed in Read.
	pending []*pendingSegment

	nextIdx int
	err     error
}

type pendingSegment struct {
	buf   []byte
	err   error
	ready chan struct{}
}

type segmentBytesProvider interface {
	SegmentBytes() []byte
}

// NewSegmentReader builds a reader that streams len(refs) segments in order.
// Fetching starts immediately in the background.
func NewSegmentReader(store segmentStore, refs []SegmentRef) *SegmentReader {
	return NewSegmentReaderCtx(context.Background(), store, refs)
}

// NewSegmentReaderCtx builds a reader that streams len(refs) segments in
// order, canceling background fetches when ctx is canceled or Close is called.
func NewSegmentReaderCtx(ctx context.Context, store segmentStore, refs []SegmentRef) *SegmentReader {
	cctx, cancel := context.WithCancel(ctx)
	pending := make([]*pendingSegment, len(refs))
	for i := range refs {
		pending[i] = &pendingSegment{ready: make(chan struct{})}
	}
	r := &SegmentReader{
		store:   store,
		refs:    refs,
		workers: DefaultGetWorkers,
		pending: pending,
		ctx:     cctx,
		cancel:  cancel,
	}
	go r.fetchAll(cctx)
	return r
}

// NewStreamingSegmentReader builds a reader that opens one segment at a time and
// streams it directly to the caller. It avoids SegmentReader's parallel
// materialization path for full-body GETs where lower layers already stream.
func NewStreamingSegmentReader(store segmentStore, refs []SegmentRef) io.ReadCloser {
	return NewStreamingSegmentReaderCtx(context.Background(), store, refs)
}

// NewStreamingSegmentReaderCtx builds a sequential streaming segment reader.
func NewStreamingSegmentReaderCtx(ctx context.Context, store segmentStore, refs []SegmentRef) io.ReadCloser {
	cctx, cancel := context.WithCancel(ctx)
	return &streamingSegmentReader{
		store:  store,
		refs:   refs,
		ctx:    cctx,
		cancel: cancel,
	}
}

// Close cancels background fetch workers. It does not discard bytes already
// fetched into pending buffers.
func (r *SegmentReader) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}

type streamingSegmentReader struct {
	store  segmentStore
	refs   []SegmentRef
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	cur    io.ReadCloser
	idx    int
	closed bool
	next   *prefetchSlot // lookahead-1: at most one in-flight/parked next-segment open
}

// prefetchSlot holds one background OpenSegment result. ready is closed by the
// fetch goroutine after rc/err are set; Close() waits on ready to reap rc.
type prefetchSlot struct {
	idx   int
	rc    io.ReadCloser
	err   error
	ready chan struct{}
}

func (r *streamingSegmentReader) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	cur := r.cur
	r.cur = nil
	slot := r.next
	r.next = nil
	r.mu.Unlock()
	var err error
	if cur != nil {
		err = cur.Close()
	}
	if slot != nil {
		<-slot.ready // prompt: ctx canceled above unblocks the open (#1008 contract)
		if slot.rc != nil {
			_ = slot.rc.Close()
		}
	}
	return err
}

// nextNonEmpty returns the first index >= from with a non-zero Size, or len(refs).
func (r *streamingSegmentReader) nextNonEmpty(from int) int {
	for from < len(r.refs) && r.refs[from].Size == 0 {
		from++
	}
	return from
}

// startPrefetchLocked spawns the lookahead-1 open for the first non-empty ref
// after index `after`. Caller holds r.mu. No-op when closed, a slot already
// exists, or nothing remains.
//
// Memory bound: lookahead depth is exactly 1 — one extra open reader; for a
// zstd-stored segment one extra ≤16MiB plaintext buffer coexists with the
// current segment's buffer during the drain (peak ~2× segment size per GET,
// transient). This is bounded, per-request, and far below the 8-worker
// SegmentReader's accepted ~128MiB budget.
func (r *streamingSegmentReader) startPrefetchLocked(after int) {
	if r.closed || r.next != nil {
		return
	}
	target := r.nextNonEmpty(after + 1)
	if target >= len(r.refs) {
		return
	}
	slot := &prefetchSlot{idx: target, ready: make(chan struct{})}
	r.next = slot
	go func() {
		// Concurrent-with-drain OpenSegment is safe: the cluster store reads only
		// immutable request state on this path (cachedSeg* belongs to the
		// ranged-GET ReadAtSegment path, never full-body streaming).
		rc, err := r.store.OpenSegment(r.ctx, r.refs[slot.idx])
		slot.rc, slot.err = rc, err
		close(slot.ready)
	}()
}

func (r *streamingSegmentReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	total := 0
	for {
		cur, err := r.current()
		if err != nil {
			if total > 0 {
				if err == io.EOF {
					return total, nil
				}
				return total, err
			}
			return 0, err
		}
		n, err := cur.Read(p[total:])
		total += n
		if err == io.EOF {
			_ = r.closeCurrent(cur)
			if total == len(p) {
				return total, nil
			}
			continue
		}
		if err != nil {
			return total, err
		}
		if total == len(p) {
			return total, nil
		}
		if n == 0 {
			return total, nil
		}
	}
}

func (r *streamingSegmentReader) current() (io.ReadCloser, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, context.Canceled
	}
	if r.cur != nil {
		cur := r.cur
		r.mu.Unlock()
		return cur, nil
	}
	r.idx = r.nextNonEmpty(r.idx)
	if r.idx >= len(r.refs) {
		r.mu.Unlock()
		return nil, io.EOF
	}
	idx := r.idx
	if slot := r.next; slot != nil && slot.idx == idx {
		r.next = nil
		r.mu.Unlock()
		<-slot.ready
		if slot.err != nil {
			return nil, slot.err
		}
		r.mu.Lock()
		if r.closed || r.ctx.Err() != nil {
			cerr := r.ctx.Err()
			r.mu.Unlock()
			_ = slot.rc.Close()
			if cerr != nil {
				return nil, cerr
			}
			return nil, context.Canceled
		}
		if r.idx != idx {
			r.mu.Unlock()
			_ = slot.rc.Close()
			return nil, context.Canceled
		}
		r.cur = slot.rc
		r.startPrefetchLocked(idx)
		r.mu.Unlock()
		return slot.rc, nil
	}
	r.mu.Unlock()

	rc, err := r.store.OpenSegment(r.ctx, r.refs[idx])
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		_ = rc.Close()
		return nil, context.Canceled
	}
	if err := r.ctx.Err(); err != nil {
		_ = rc.Close()
		return nil, err
	}
	if r.idx != idx {
		_ = rc.Close()
		if r.cur != nil {
			return r.cur, nil
		}
		return nil, context.Canceled
	}
	r.cur = rc
	r.startPrefetchLocked(idx)
	return rc, nil
}

func (r *streamingSegmentReader) closeCurrent(cur io.ReadCloser) error {
	r.mu.Lock()
	if r.cur == cur {
		r.cur = nil
		r.idx++
	}
	r.mu.Unlock()
	return cur.Close()
}

func (r *SegmentReader) fetchAll(ctx context.Context) {
	sem := make(chan struct{}, r.workers)
	var wg sync.WaitGroup
	for i := range r.refs {
		i := i
		select {
		case <-ctx.Done():
			r.cancelUnscheduled(i, ctx.Err())
			wg.Wait()
			return
		default:
		}
		select {
		case <-ctx.Done():
			r.cancelUnscheduled(i, ctx.Err())
			wg.Wait()
			return
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			r.fetchOne(ctx, i)
		}()
	}
	wg.Wait()
}

func (r *SegmentReader) cancelUnscheduled(start int, err error) {
	for i := start; i < len(r.pending); i++ {
		p := r.pending[i]
		if p == nil {
			continue
		}
		p.err = err
		close(p.ready)
	}
}

func (r *SegmentReader) fetchOne(ctx context.Context, idx int) {
	p := r.pending[idx]
	defer close(p.ready)

	rc, err := r.store.OpenSegment(ctx, r.refs[idx])
	if err != nil {
		p.err = err
		return
	}
	defer rc.Close()
	if provider, ok := rc.(segmentBytesProvider); ok {
		p.buf = provider.SegmentBytes()
		return
	}
	buf, err := readExactlySizedObject(rc, r.refs[idx].Size)
	if err != nil {
		p.err = err
		return
	}
	p.buf = buf
}

// Read implements io.Reader. Returns bytes in original segment order.
func (r *SegmentReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.nextIdx >= len(r.refs) {
		return 0, io.EOF
	}

	p0 := r.pending[r.nextIdx] // guaranteed non-nil by NewSegmentReader
	<-p0.ready

	if p0.err != nil {
		r.err = p0.err
		return 0, p0.err
	}

	n := copy(p, p0.buf)
	p0.buf = p0.buf[n:]
	if len(p0.buf) == 0 {
		r.pending[r.nextIdx] = nil // release backing array for GC
		r.nextIdx++
	}
	return n, nil
}
