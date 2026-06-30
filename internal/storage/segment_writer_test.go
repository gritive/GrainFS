package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestSegmentWriter_Boundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		size     int
		wantSegs int
	}{
		{"zero", 0, 1},
		{"one_byte", 1, 1},
		{"under_chunk", 15 << 20, 1},
		{"exact_chunk", 16 << 20, 1},
		{"chunk_plus_one", (16 << 20) + 1, 2},
		{"two_full_chunks", 32 << 20, 2},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newTestLocalBackend(t)
			data := makePattern(tc.size)
			obj, err := writeViaSegmentWriter(b, "test", "k-"+tc.name, bytes.NewReader(data))
			require.NoError(t, err, "write")
			require.Len(t, obj.Segments, tc.wantSegs)
			require.Equal(t, int64(tc.size), obj.Size)
			// Simple-PUT ETag = MD5 of full plaintext.
			h := md5.New()
			h.Write(data)
			wantEtag := hex.EncodeToString(h.Sum(nil))
			require.Equal(t, wantEtag, obj.ETag)
		})
	}
}

func TestSegmentWriter_S3FastETagSkipsMD5WhenOptedIn(t *testing.T) {
	t.Setenv(s3ETagMD5Env, "0")
	b := newTestLocalBackend(t)
	data := makePattern(4096 + 7)

	obj, err := writeViaSegmentWriter(b, "test", "fast-etag", bytes.NewReader(data))
	require.NoError(t, err, "write")

	h := md5.New()
	h.Write(data)
	wantMD5 := hex.EncodeToString(h.Sum(nil))
	require.NotEqual(t, wantMD5, obj.ETag)
	require.Equal(t, InternalETag(data), obj.ETag)
	require.Len(t, obj.ETag, 16)
}

func TestSegmentWriter_UnknownContentLength(t *testing.T) {
	t.Parallel()
	b := newTestLocalBackend(t)
	data := makePattern(4096 + 7)
	r := iotest.OneByteReader(bytes.NewReader(data))
	w := NewSegmentWriterWithChunkSize(localBackendAdapter{b}, 1024)
	obj, err := w.Write(context.Background(), "test", "drip", "application/octet-stream", r)
	require.NoError(t, err, "write")
	require.Len(t, obj.Segments, 5)
	require.Equal(t, int64(7), obj.Segments[4].Size)
}

func TestSegmentWriter_CustomChunkSize(t *testing.T) {
	t.Parallel()
	b := newTestLocalBackend(t)
	data := makePattern(4096 + 7)

	w := NewSegmentWriterWithChunkSize(localBackendAdapter{b}, 1024)
	obj, err := w.Write(context.Background(), "test", "small-chunks", "application/octet-stream", bytes.NewReader(data))
	require.NoError(t, err, "write")
	require.Len(t, obj.Segments, 5)
	require.Equal(t, int64(7), obj.Segments[4].Size)
}

func TestSegmentWriter_UsesByteWriterFastPath(t *testing.T) {
	t.Parallel()
	b := &byteWriterBackend{}
	data := makePattern(2048)

	w := NewSegmentWriterWithChunkSize(b, 1024)
	obj, err := w.Write(context.Background(), "test", "fast-path", "application/octet-stream", bytes.NewReader(data))
	require.NoError(t, err, "write")
	require.Zero(t, b.readerCalls, "reader path calls")
	require.Equal(t, 2, b.byteCalls, "byte path calls")
	require.Len(t, obj.Segments, 2)
}

func TestSegmentWriter_CustomWorkersBoundsConcurrentWrites(t *testing.T) {
	b := &countingConcurrentBytesBackend{}
	data := makePattern(4 << 10)

	w := NewSegmentWriterWithChunkSizeAndWorkers(b, 1024, 1)
	obj, err := w.Write(context.Background(), "test", "serial", "application/octet-stream", bytes.NewReader(data))
	require.NoError(t, err, "write")
	require.Equal(t, int64(len(data)), obj.Size)
	require.Equal(t, int32(1), b.maxActive.Load(), "max concurrent writes")
}

func TestSegmentWriter_StreamErrorMidChunk(t *testing.T) {
	t.Parallel()
	b := newTestLocalBackend(t)
	r := &errAfterNReader{n: 1024 + 100, err: io.ErrUnexpectedEOF}
	w := NewSegmentWriterWithChunkSize(localBackendAdapter{b}, 1024)
	_, err := w.Write(context.Background(), "test", "boom", "application/octet-stream", r)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	_, getErr := b.HeadObject(context.Background(), "test", "boom")
	require.ErrorIs(t, getErr, ErrObjectNotFound, "partial PUT must not appear in meta")
}

type bytesOnlySegmentBackend struct {
	calls int
}

func (b *bytesOnlySegmentBackend) WriteSegment(context.Context, string, string, int, io.Reader) (SegmentRef, error) {
	return SegmentRef{}, errors.New("reader path should not be used")
}

func (b *bytesOnlySegmentBackend) WriteSegmentBytes(_ context.Context, _ string, _ string, idx int, data []byte) (SegmentRef, error) {
	b.calls++
	return SegmentRef{
		BlobID:   string(rune('a' + idx)),
		Size:     int64(len(data)),
		Checksum: ChecksumOf(data),
	}, nil
}

type countingConcurrentBytesBackend struct {
	active    atomic.Int32
	maxActive atomic.Int32
}

func (b *countingConcurrentBytesBackend) WriteSegment(context.Context, string, string, int, io.Reader) (SegmentRef, error) {
	return SegmentRef{}, errors.New("reader path should not be used")
}

func (b *countingConcurrentBytesBackend) WriteSegmentBytes(_ context.Context, _ string, _ string, idx int, data []byte) (SegmentRef, error) {
	active := b.active.Add(1)
	for {
		max := b.maxActive.Load()
		if active <= max || b.maxActive.CompareAndSwap(max, active) {
			break
		}
	}
	time.Sleep(time.Millisecond)
	b.active.Add(-1)
	return SegmentRef{
		BlobID:   string(rune('a' + idx)),
		Size:     int64(len(data)),
		Checksum: ChecksumOf(data),
	}, nil
}

func TestSegmentWriter_UsesByteFastPathWhenAvailable(t *testing.T) {
	t.Parallel()
	b := &bytesOnlySegmentBackend{}
	data := makePattern((16 << 20) + 7)

	obj, err := NewSegmentWriter(b).Write(context.Background(), "test", "fast", "application/octet-stream", bytes.NewReader(data))
	require.NoError(t, err, "write")
	require.Equal(t, 2, b.calls, "byte fast path calls")
	require.Equal(t, int64(len(data)), obj.Size)
}

func TestSegmentWriter_ByteFastPathAllocBytesBounded(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("race instrumentation inflates TotalAlloc, making the byte threshold meaningless")
	}
	payload := makePattern(DefaultChunkSize)

	run := func(t testing.TB) error {
		t.Helper()
		b := &bytesOnlySegmentBackend{}
		obj, err := NewSegmentWriter(b).Write(context.Background(), "test", "fast", "application/octet-stream", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		require.Equal(t, int64(len(payload)), obj.Size)
		return nil
	}

	require.NoError(t, run(t)) // warm chunk pool
	allocedBytes := int64(allocBytesPerRunForStorageTest(t, 10, func() error {
		return run(t)
	}))
	t.Logf("SegmentWriter byte fast path alloc bytes: %d", allocedBytes)
	// 12 MiB threshold: pool reuse keeps hot-path alloc well below DefaultChunkSize
	// (16 MiB). Extra headroom over the observed ~7 MiB absorbs background-goroutine
	// TotalAlloc noise when running inside the full test suite.
	require.LessOrEqual(t, allocedBytes, int64(12*1024*1024))
}

type blockingBytesBackend struct {
	entered chan struct{}
	release chan struct{}
	ptrs    chan uintptr
}

func (b *blockingBytesBackend) WriteSegment(context.Context, string, string, int, io.Reader) (SegmentRef, error) {
	return SegmentRef{}, errors.New("reader path should not be used")
}

func (b *blockingBytesBackend) WriteSegmentBytes(_ context.Context, _ string, _ string, idx int, data []byte) (SegmentRef, error) {
	if len(data) > 0 {
		b.ptrs <- uintptr(unsafe.Pointer(&data[0]))
	}
	if idx == 0 {
		close(b.entered)
		<-b.release
	}
	return SegmentRef{
		BlobID:   string(rune('a' + idx)),
		Size:     int64(len(data)),
		Checksum: ChecksumOf(data),
	}, nil
}

func TestSegmentWriter_PooledChunkReleasedAfterBackendReturns(t *testing.T) {
	backend := &blockingBytesBackend{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		ptrs:    make(chan uintptr, 2),
	}
	payload := makePattern((DefaultChunkSize * 2) + 1)
	done := make(chan error, 1)
	go func() {
		_, err := NewSegmentWriterWithChunkSizeAndWorkers(backend, DefaultChunkSize, 2).
			Write(context.Background(), "test", "pooled", "application/octet-stream", bytes.NewReader(payload))
		done <- err
	}()

	<-backend.entered
	firstPtr := <-backend.ptrs
	secondPtr := <-backend.ptrs
	require.NotEqual(t, firstPtr, secondPtr, "second chunk reused the first chunk buffer before backend returned")
	close(backend.release)
	require.NoError(t, <-done)
}
