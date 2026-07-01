package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// prefetchProbeStore records OpenSegment invocation order and can gate opens
// so tests observe overlap deterministically.
type prefetchProbeStore struct {
	mu       sync.Mutex
	opened   []int
	openGate map[int]chan struct{}
	data     map[int][]byte
	openErr  map[int]error
	closeCnt atomic.Int32
	refIndex map[string]int
}

func newPrefetchProbeStore(segs ...[]byte) (*prefetchProbeStore, []SegmentRef) {
	st := &prefetchProbeStore{
		openGate: map[int]chan struct{}{},
		data:     map[int][]byte{},
		openErr:  map[int]error{},
		refIndex: map[string]int{},
	}
	refs := make([]SegmentRef, len(segs))
	for i, b := range segs {
		id := fmt.Sprintf("seg-%d", i)
		refs[i] = SegmentRef{BlobID: id, Size: int64(len(b))}
		st.data[i] = b
		st.refIndex[id] = i
	}
	return st, refs
}

type probeReadCloser struct {
	*bytes.Reader
	onClose func()
}

func (p *probeReadCloser) Close() error { p.onClose(); return nil }

func (s *prefetchProbeStore) OpenSegment(ctx context.Context, ref SegmentRef) (io.ReadCloser, error) {
	idx := s.refIndex[ref.BlobID]
	s.mu.Lock()
	gate := s.openGate[idx]
	s.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	s.mu.Lock()
	s.opened = append(s.opened, idx)
	err := s.openErr[idx]
	data := s.data[idx]
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &probeReadCloser{Reader: bytes.NewReader(data), onClose: func() { s.closeCnt.Add(1) }}, nil
}

func (s *prefetchProbeStore) openedSnapshot() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.opened...)
}

func TestStreamingSegmentReaderPrefetchesNextDuringDrain(t *testing.T) {
	st, refs := newPrefetchProbeStore(bytes.Repeat([]byte{'a'}, 1024), bytes.Repeat([]byte{'b'}, 1024))
	r := NewStreamingSegmentReaderCtx(context.Background(), st, refs)
	defer r.Close()

	one := make([]byte, 1)
	_, err := io.ReadFull(r, one)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		for _, idx := range st.openedSnapshot() {
			if idx == 1 {
				return true
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond, "segment 1 was not prefetched during segment 0 drain")

	rest, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, append(bytes.Repeat([]byte{'a'}, 1023), bytes.Repeat([]byte{'b'}, 1024)...), rest)
}

func TestStreamingSegmentReaderCloseCancelsInflightPrefetch(t *testing.T) {
	st, refs := newPrefetchProbeStore(bytes.Repeat([]byte{'a'}, 8), bytes.Repeat([]byte{'b'}, 8))
	gate := make(chan struct{})
	st.openGate[1] = gate

	r := NewStreamingSegmentReaderCtx(context.Background(), st, refs)
	one := make([]byte, 1)
	_, err := io.ReadFull(r, one)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- r.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung waiting for in-flight prefetch")
	}
	close(gate)
}

func TestStreamingSegmentReaderCloseClosesPrefetchedReader(t *testing.T) {
	st, refs := newPrefetchProbeStore(bytes.Repeat([]byte{'a'}, 8), bytes.Repeat([]byte{'b'}, 8))
	r := NewStreamingSegmentReaderCtx(context.Background(), st, refs)
	one := make([]byte, 1)
	_, err := io.ReadFull(r, one)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		for _, idx := range st.openedSnapshot() {
			if idx == 1 {
				return true
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond)
	require.NoError(t, r.Close())
	require.Eventually(t, func() bool { return st.closeCnt.Load() == 2 }, 2*time.Second, 5*time.Millisecond,
		"prefetched reader leaked on Close")
}

func TestStreamingSegmentReaderPrefetchErrorSurfacesAtBoundary(t *testing.T) {
	st, refs := newPrefetchProbeStore(bytes.Repeat([]byte{'a'}, 16), bytes.Repeat([]byte{'b'}, 16))
	st.openErr[1] = fmt.Errorf("shard open boom")
	r := NewStreamingSegmentReaderCtx(context.Background(), st, refs)
	defer r.Close()

	buf := make([]byte, 16)
	_, err := io.ReadFull(r, buf)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{'a'}, 16), buf)

	_, err = r.Read(buf)
	require.ErrorContains(t, err, "shard open boom")
}

func TestStreamingSegmentReaderPrefetchSkipsEmptyRefs(t *testing.T) {
	st, refs := newPrefetchProbeStore(bytes.Repeat([]byte{'a'}, 8), nil, bytes.Repeat([]byte{'c'}, 8))
	r := NewStreamingSegmentReaderCtx(context.Background(), st, refs)
	defer r.Close()
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, append(bytes.Repeat([]byte{'a'}, 8), bytes.Repeat([]byte{'c'}, 8)...), out)
	for _, idx := range st.openedSnapshot() {
		require.NotEqual(t, 1, idx, "zero-size ref must not be opened")
	}
}

func TestStreamingSegmentReaderSingleSegmentNoOverfetch(t *testing.T) {
	st, refs := newPrefetchProbeStore(bytes.Repeat([]byte{'a'}, 8))
	r := NewStreamingSegmentReaderCtx(context.Background(), st, refs)
	defer r.Close()
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{'a'}, 8), out)
	require.Equal(t, []int{0}, st.openedSnapshot())
}
