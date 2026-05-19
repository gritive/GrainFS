// Package storage — SegmentReader streams an object's segments in original
// index order, while fetching them in parallel with a bounded worker pool.
//
// Memory bound: chunkSize × workers (~128 MiB for 8 × 16 MiB).
package storage

import (
	"context"
	"io"
	"sync"
)

// DefaultGetWorkers is the per-request fetch concurrency for segment reads.
const DefaultGetWorkers = 8

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

// NewSegmentReader builds a reader that streams len(refs) segments in order.
// Fetching starts immediately in the background.
func NewSegmentReader(store segmentStore, refs []SegmentRef) *SegmentReader {
	pending := make([]*pendingSegment, len(refs))
	for i := range refs {
		pending[i] = &pendingSegment{ready: make(chan struct{})}
	}
	r := &SegmentReader{
		store:   store,
		refs:    refs,
		workers: DefaultGetWorkers,
		pending: pending,
	}
	// TODO(phase-2): accept ctx from caller so cluster GET can cancel on
	// client disconnect or reshard event. v1 uses background context — workers
	// always run to completion.
	go r.fetchAll(context.Background())
	return r
}

func (r *SegmentReader) fetchAll(ctx context.Context) {
	sem := make(chan struct{}, r.workers)
	var wg sync.WaitGroup
	for i := range r.refs {
		i := i
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			r.fetchOne(ctx, i)
		}()
	}
	wg.Wait()
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
	buf, err := io.ReadAll(rc)
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
