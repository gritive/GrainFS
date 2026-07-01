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

	"github.com/stretchr/testify/require"
)

func TestSegmentReader_ReassemblesInOrder(t *testing.T) {
	t.Parallel()
	segs := makeTestSegments(t, []int{16 << 20, 16 << 20, 1024})
	fakeStore := newFakeSegmentStore(segs)
	r := NewSegmentReader(fakeStore, segs.refs)

	got, err := io.ReadAll(r)
	require.NoError(t, err, "read")
	require.True(t, bytes.Equal(got, segs.flat), "reassembled bytes differ at offset %d", firstDiff(got, segs.flat))
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
	require.NoError(t, err, "read")
	require.True(t, bytes.Equal(got, segs.flat), "output not in order")
}

func TestSegmentReader_OneSegmentFailsAbortsCleanly(t *testing.T) {
	t.Parallel()
	segs := makeTestSegments(t, []int{16 << 20, 16 << 20})
	store := newFakeSegmentStore(segs)
	store.errIdx = map[int]error{1: io.ErrUnexpectedEOF}

	r := NewSegmentReader(store, segs.refs)
	_, err := io.ReadAll(r)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
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
		require.True(t, err == nil || err == io.EOF, "read: %v", err)
		consumed += n
		if n == 0 {
			break
		}
	}
	// After full drain, every pending slot must be nil.
	for i, p := range r.pending {
		require.Nil(t, p, "pending[%d] not released after consumption", i)
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
		require.Fail(t, "worker did not enter OpenSegment")
	}

	require.NoError(t, r.Close(), "close")

	select {
	case <-store.exited:
	case <-time.After(time.Second):
		require.Fail(t, "worker did not exit after Close")
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
			require.Failf(t, "worker did not enter OpenSegment", "worker %d did not enter OpenSegment", i)
		}
	}
	require.NoError(t, r.Close(), "close")
	for i := 0; i < DefaultGetWorkers; i++ {
		select {
		case <-store.exited:
		case <-time.After(time.Second):
			require.Failf(t, "worker did not exit after Close", "worker %d did not exit after Close", i)
		}
	}
	time.Sleep(100 * time.Millisecond)
	require.Zero(t, len(store.entered), "Close scheduled extra segments after cancellation")
}

func TestSegmentReader_LegacyConstructorStillWorks(t *testing.T) {
	t.Parallel()
	segs := makeTestSegments(t, []int{1024, 2048, 512})
	store := newFakeSegmentStore(segs)
	r := NewSegmentReader(store, segs.refs)

	got, err := io.ReadAll(r)
	require.NoError(t, err, "read")
	require.True(t, bytes.Equal(got, segs.flat), "reassembled bytes differ at offset %d", firstDiff(got, segs.flat))
}

func TestSegmentReader_UsesProvidedSegmentBytesWithoutSecondReadAll(t *testing.T) {
	t.Parallel()
	want := []byte("materialized segment")
	store := &materializedSegmentStore{data: want}
	r := NewSegmentReader(store, []SegmentRef{{BlobID: "materialized", Size: int64(len(want))}})

	got, err := io.ReadAll(r)
	require.NoError(t, err, "read")
	require.Equal(t, want, got)
	require.True(t, store.closed, "materialized reader was not closed")
}

func TestStreamingSegmentReader_StreamsOneSegmentAtATime(t *testing.T) {
	t.Parallel()
	segs := makeTestSegments(t, []int{1024, 2048, 512})
	store := newTrackingStreamingSegmentStore(segs)
	r := NewStreamingSegmentReaderCtx(context.Background(), store, segs.refs)

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, segs.flat, got)
	require.Equal(t, 1, store.maxOpen)
	require.Equal(t, []string{"blob-0", "blob-1", "blob-2"}, store.openOrder)
	require.Equal(t, []string{"blob-0", "blob-1", "blob-2"}, store.closeOrder)
}

func TestStreamingSegmentReader_ContinuesAfterSegmentShortReadEOF(t *testing.T) {
	t.Parallel()
	store := &shortEOFSegmentStore{
		segments: map[string][]byte{
			"blob-0": []byte("abc"),
			"blob-1": []byte("def"),
		},
	}
	refs := []SegmentRef{
		{BlobID: "blob-0", Size: 3},
		{BlobID: "blob-1", Size: 3},
	}
	r := NewStreamingSegmentReaderCtx(context.Background(), store, refs)

	buf := make([]byte, 3)
	n, err := r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "abc", string(buf[:n]))

	n, err = r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "def", string(buf[:n]))

	n, err = r.Read(buf)
	require.Equal(t, io.EOF, err)
	require.Zero(t, n)
}

func TestStreamingSegmentReader_CloseWhileOpeningClosesReturnedReader(t *testing.T) {
	t.Parallel()
	store := &closeDuringOpenSegmentStore{
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
		closed:   make(chan struct{}),
		openDone: make(chan struct{}),
	}
	refs := []SegmentRef{{BlobID: "blocked", Size: 1}, {BlobID: "not-opened", Size: 1}}
	r := NewStreamingSegmentReaderCtx(context.Background(), store, refs)
	readDone := make(chan error, 1)
	go func() {
		_, err := r.Read(make([]byte, 1))
		readDone <- err
	}()

	select {
	case <-store.entered:
	case <-time.After(time.Second):
		require.Fail(t, "OpenSegment was not entered")
	}

	require.NoError(t, r.Close())
	close(store.release)

	select {
	case <-store.openDone:
	case <-time.After(time.Second):
		require.Fail(t, "OpenSegment did not return")
	}
	select {
	case <-store.closed:
	case <-time.After(time.Second):
		require.Fail(t, "reader returned after Close was not closed")
	}
	select {
	case err := <-readDone:
		require.Error(t, err)
	case <-time.After(time.Second):
		require.Fail(t, "Read did not unblock after Close")
	}
	require.Equal(t, []string{"blocked"}, store.openOrder)
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

type materializedSegmentStore struct {
	data   []byte
	closed bool
}

func (s *materializedSegmentStore) OpenSegment(context.Context, SegmentRef) (io.ReadCloser, error) {
	return &materializedSegmentReadCloser{store: s, data: s.data}, nil
}

type materializedSegmentReadCloser struct {
	store *materializedSegmentStore
	data  []byte
}

func (r *materializedSegmentReadCloser) SegmentBytes() []byte { return r.data }

func (r *materializedSegmentReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("Read should not be called for materialized segments")
}

func (r *materializedSegmentReadCloser) Close() error {
	r.store.closed = true
	return nil
}

type trackingStreamingSegmentStore struct {
	mu         sync.Mutex
	data       map[string][]byte
	open       int
	maxOpen    int
	openOrder  []string
	closeOrder []string
}

func newTrackingStreamingSegmentStore(segs *testSegments) *trackingStreamingSegmentStore {
	s := &trackingStreamingSegmentStore{data: make(map[string][]byte)}
	for i, ref := range segs.refs {
		s.data[ref.BlobID] = segs.data[i]
	}
	return s
}

func (s *trackingStreamingSegmentStore) OpenSegment(_ context.Context, ref SegmentRef) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.data[ref.BlobID]
	if !ok {
		return nil, fmt.Errorf("missing ref %s", ref.BlobID)
	}
	s.open++
	if s.open > s.maxOpen {
		s.maxOpen = s.open
	}
	s.openOrder = append(s.openOrder, ref.BlobID)
	return &trackingSegmentReadCloser{
		Reader: bytes.NewReader(data),
		store:  s,
		blobID: ref.BlobID,
	}, nil
}

type trackingSegmentReadCloser struct {
	*bytes.Reader
	store  *trackingStreamingSegmentStore
	blobID string
}

func (r *trackingSegmentReadCloser) Close() error {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	r.store.open--
	r.store.closeOrder = append(r.store.closeOrder, r.blobID)
	return nil
}

type shortEOFSegmentStore struct {
	segments map[string][]byte
}

func (s *shortEOFSegmentStore) OpenSegment(_ context.Context, ref SegmentRef) (io.ReadCloser, error) {
	data, ok := s.segments[ref.BlobID]
	if !ok {
		return nil, fmt.Errorf("missing ref %s", ref.BlobID)
	}
	return &shortEOFReadCloser{data: data}, nil
}

type shortEOFReadCloser struct {
	data []byte
	done bool
}

func (r *shortEOFReadCloser) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	return copy(p, r.data), io.EOF
}

func (r *shortEOFReadCloser) Close() error { return nil }

type closeDuringOpenSegmentStore struct {
	entered   chan struct{}
	release   chan struct{}
	closed    chan struct{}
	openDone  chan struct{}
	openOrder []string
}

func (s *closeDuringOpenSegmentStore) OpenSegment(ctx context.Context, ref SegmentRef) (io.ReadCloser, error) {
	s.openOrder = append(s.openOrder, ref.BlobID)
	close(s.entered)
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	close(s.openDone)
	return &closeSignalReadCloser{closed: s.closed}, nil
}

type closeSignalReadCloser struct {
	closed chan struct{}
	once   sync.Once
}

func (r *closeSignalReadCloser) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (r *closeSignalReadCloser) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
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
