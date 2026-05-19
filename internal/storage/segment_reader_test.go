package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

func TestSegmentReader_ReassemblesInOrder(t *testing.T) {
	t.Parallel()
	segs := makeTestSegments(t, []int{16 << 20, 16 << 20, 1024})
	fakeStore := newFakeSegmentStore(segs)
	r := NewSegmentReader(fakeStore, segs.refs)

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, segs.flat) {
		t.Fatalf("reassembled bytes differ at offset %d", firstDiff(got, segs.flat))
	}
}

func TestSegmentReader_OutOfOrderArrivalStillInOrderOutput(t *testing.T) {
	t.Parallel()
	segs := makeTestSegments(t, []int{16 << 20, 16 << 20, 16 << 20, 16 << 20})
	store := newFakeSegmentStore(segs)
	// Force segments to complete in REVERSE order — idx 3 first, idx 0 last.
	store.delayByIdx = map[int]time.Duration{
		0: 100 * time.Millisecond,
		1: 75 * time.Millisecond,
		2: 50 * time.Millisecond,
		3: 0,
	}

	r := NewSegmentReader(store, segs.refs)
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, segs.flat) {
		t.Fatal("output not in order")
	}
}

func TestSegmentReader_OneSegmentFailsAbortsCleanly(t *testing.T) {
	t.Parallel()
	segs := makeTestSegments(t, []int{16 << 20, 16 << 20})
	store := newFakeSegmentStore(segs)
	store.errIdx = map[int]error{1: io.ErrUnexpectedEOF}

	r := NewSegmentReader(store, segs.refs)
	_, err := io.ReadAll(r)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}
}

func TestSegmentReader_ReleasesBackingArrayAfterConsumption(t *testing.T) {
	t.Parallel()
	segs := makeTestSegments(t, []int{16 << 20, 16 << 20, 16 << 20, 16 << 20})
	store := newFakeSegmentStore(segs)
	r := NewSegmentReader(store, segs.refs)

	// Consume in chunks; after each segment is fully drained, the pending
	// slot must be nil so its 16 MiB backing array is GC-eligible.
	buf := make([]byte, 1<<20) // 1 MiB read buffer
	consumed := 0
	for consumed < len(segs.flat) {
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("read: %v", err)
		}
		consumed += n
		if n == 0 {
			break
		}
	}
	// After full drain, every pending slot must be nil.
	for i, p := range r.pending {
		if p != nil {
			t.Fatalf("pending[%d] not released after consumption", i)
		}
	}
}

func TestSegmentReader_CloseCancelsWorkers(t *testing.T) {
	t.Parallel()
	store := &blockingSegmentStore{
		entered: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	r := NewSegmentReaderCtx(context.Background(), store, []SegmentRef{{BlobID: "blocked", Size: 1}})

	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not enter OpenSegment")
	}

	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case <-store.exited:
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after Close")
	}
}

func TestSegmentReader_CloseDoesNotScheduleRemainingSegments(t *testing.T) {
	t.Parallel()
	const refsN = DefaultGetWorkers + 4
	refs := make([]SegmentRef, refsN)
	for i := range refs {
		refs[i] = SegmentRef{BlobID: fmt.Sprintf("blocked-%d", i), Size: 1}
	}
	store := &countingBlockingSegmentStore{
		entered: make(chan string, refsN),
		exited:  make(chan string, refsN),
	}
	r := NewSegmentReaderCtx(context.Background(), store, refs)

	for i := 0; i < DefaultGetWorkers; i++ {
		select {
		case <-store.entered:
		case <-time.After(time.Second):
			t.Fatalf("worker %d did not enter OpenSegment", i)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for i := 0; i < DefaultGetWorkers; i++ {
		select {
		case <-store.exited:
		case <-time.After(time.Second):
			t.Fatalf("worker %d did not exit after Close", i)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if got := len(store.entered); got != 0 {
		t.Fatalf("Close scheduled %d extra segments after cancellation", got)
	}
}

func TestSegmentReader_LegacyConstructorStillWorks(t *testing.T) {
	t.Parallel()
	segs := makeTestSegments(t, []int{1024, 2048, 512})
	store := newFakeSegmentStore(segs)
	r := NewSegmentReader(store, segs.refs)

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, segs.flat) {
		t.Fatalf("reassembled bytes differ at offset %d", firstDiff(got, segs.flat))
	}
}

// --- helpers ---

type testSegments struct {
	refs []SegmentRef
	data [][]byte // raw plaintext per segment
	flat []byte   // concatenation of all segments
}

func makeTestSegments(t *testing.T, sizes []int) *testSegments {
	t.Helper()
	out := &testSegments{
		refs: make([]SegmentRef, len(sizes)),
		data: make([][]byte, len(sizes)),
	}
	for i, sz := range sizes {
		out.data[i] = makePattern(sz)
		out.refs[i] = SegmentRef{BlobID: fmt.Sprintf("blob-%d", i), Size: int64(sz)}
		out.flat = append(out.flat, out.data[i]...)
	}
	return out
}

func firstDiff(a, b []byte) int {
	min := len(a)
	if len(b) < min {
		min = len(b)
	}
	for i := 0; i < min; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	if len(a) != len(b) {
		return min
	}
	return -1
}

type fakeSegmentStore struct {
	mu         sync.Mutex
	data       map[string][]byte
	delayByIdx map[int]time.Duration // delay per ref idx (by BlobID match)
	errIdx     map[int]error         // injected error per ref idx
	refIndex   map[string]int        // BlobID → idx for lookup
}

func newFakeSegmentStore(segs *testSegments) *fakeSegmentStore {
	s := &fakeSegmentStore{
		data:     make(map[string][]byte),
		refIndex: make(map[string]int),
	}
	for i, ref := range segs.refs {
		s.data[ref.BlobID] = segs.data[i]
		s.refIndex[ref.BlobID] = i
	}
	return s
}

func (s *fakeSegmentStore) OpenSegment(ctx context.Context, ref SegmentRef) (io.ReadCloser, error) {
	s.mu.Lock()
	idx := s.refIndex[ref.BlobID]
	delay := s.delayByIdx[idx]
	injErr := s.errIdx[idx]
	buf := s.data[ref.BlobID]
	s.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if injErr != nil {
		return nil, injErr
	}
	return io.NopCloser(bytes.NewReader(buf)), nil
}

type blockingSegmentStore struct {
	entered chan struct{}
	exited  chan struct{}
}

func (s *blockingSegmentStore) OpenSegment(ctx context.Context, ref SegmentRef) (io.ReadCloser, error) {
	close(s.entered)
	<-ctx.Done()
	close(s.exited)
	return nil, ctx.Err()
}

type countingBlockingSegmentStore struct {
	entered chan string
	exited  chan string
}

func (s *countingBlockingSegmentStore) OpenSegment(ctx context.Context, ref SegmentRef) (io.ReadCloser, error) {
	s.entered <- ref.BlobID
	<-ctx.Done()
	s.exited <- ref.BlobID
	return nil, ctx.Err()
}
