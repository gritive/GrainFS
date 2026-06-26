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
	payload := makePattern(5 << 20)

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

	require.NoError(t, run(t))
	allocedBytes := int64(allocBytesPerRunForStorageTest(t, 3, func() error {
		return run(t)
	}))
	t.Logf("SegmentWriter byte fast path alloc bytes: %d", allocedBytes)
	require.LessOrEqual(t, allocedBytes, int64(18*1024*1024))
}
