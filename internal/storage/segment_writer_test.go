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
		{"four_chunks", 64 << 20, 4},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newTestLocalBackend(t)
			data := makePattern(tc.size)
			obj, err := writeViaSegmentWriter(b, "test", "k-"+tc.name, bytes.NewReader(data))
			if err != nil {
				t.Fatalf("write: %v", err)
			}
			if len(obj.Segments) != tc.wantSegs {
				t.Fatalf("segments: want %d, got %d", tc.wantSegs, len(obj.Segments))
			}
			if obj.Size != int64(tc.size) {
				t.Fatalf("size: want %d, got %d", tc.size, obj.Size)
			}
			// Simple-PUT ETag = MD5 of full plaintext.
			h := md5.New()
			h.Write(data)
			wantEtag := hex.EncodeToString(h.Sum(nil))
			if obj.ETag != wantEtag {
				t.Fatalf("etag: want %s, got %s", wantEtag, obj.ETag)
			}
		})
	}
}

func TestSegmentWriter_UnknownContentLength(t *testing.T) {
	t.Parallel()
	b := newTestLocalBackend(t)
	data := makePattern((16 << 20) + 5000)
	r := iotest.OneByteReader(bytes.NewReader(data))
	obj, err := writeViaSegmentWriter(b, "test", "drip", r)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(obj.Segments) != 2 {
		t.Fatalf("segments: want 2, got %d", len(obj.Segments))
	}
	if obj.Segments[1].Size != 5000 {
		t.Fatalf("trailing segment size: want 5000, got %d", obj.Segments[1].Size)
	}
}

func TestSegmentWriter_CustomChunkSize(t *testing.T) {
	t.Parallel()
	b := newTestLocalBackend(t)
	data := makePattern(4096 + 7)

	w := NewSegmentWriterWithChunkSize(localBackendAdapter{b}, 1024)
	obj, err := w.Write(context.Background(), "test", "small-chunks", "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(obj.Segments) != 5 {
		t.Fatalf("segments: want 5, got %d", len(obj.Segments))
	}
	if obj.Segments[4].Size != 7 {
		t.Fatalf("trailing segment size: want 7, got %d", obj.Segments[4].Size)
	}
}

func TestSegmentWriter_UsesByteWriterFastPath(t *testing.T) {
	t.Parallel()
	b := &byteWriterBackend{}
	data := makePattern(2048)

	w := NewSegmentWriterWithChunkSize(b, 1024)
	obj, err := w.Write(context.Background(), "test", "fast-path", "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if b.readerCalls != 0 {
		t.Fatalf("reader path calls: want 0, got %d", b.readerCalls)
	}
	if b.byteCalls != 2 {
		t.Fatalf("byte path calls: want 2, got %d", b.byteCalls)
	}
	if len(obj.Segments) != 2 {
		t.Fatalf("segments: want 2, got %d", len(obj.Segments))
	}
}

func TestSegmentWriter_CustomWorkersBoundsConcurrentWrites(t *testing.T) {
	b := &countingConcurrentBytesBackend{}
	data := makePattern(4 << 10)

	w := NewSegmentWriterWithChunkSizeAndWorkers(b, 1024, 1)
	obj, err := w.Write(context.Background(), "test", "serial", "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if obj.Size != int64(len(data)) {
		t.Fatalf("size: want %d, got %d", len(data), obj.Size)
	}
	if got := b.maxActive.Load(); got != 1 {
		t.Fatalf("max concurrent writes: want 1, got %d", got)
	}
}

func TestSegmentWriter_StreamErrorMidChunk(t *testing.T) {
	t.Parallel()
	b := newTestLocalBackend(t)
	r := &errAfterNReader{n: (16 << 20) + 100, err: io.ErrUnexpectedEOF}
	_, err := writeViaSegmentWriter(b, "test", "boom", r)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("want ErrUnexpectedEOF, got %v", err)
	}
	if _, getErr := b.HeadObject(context.Background(), "test", "boom"); !errors.Is(getErr, ErrObjectNotFound) {
		t.Fatalf("partial PUT must not appear in meta: %v", getErr)
	}
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
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if b.calls != 2 {
		t.Fatalf("byte fast path calls: want 2, got %d", b.calls)
	}
	if obj.Size != int64(len(data)) {
		t.Fatalf("size: want %d, got %d", len(data), obj.Size)
	}
}

func TestSegmentWriter_ByteFastPathAllocBytesBounded(t *testing.T) {
	payload := makePattern(5 << 20)

	run := func(t testing.TB) {
		t.Helper()
		b := &bytesOnlySegmentBackend{}
		obj, err := NewSegmentWriter(b).Write(context.Background(), "test", "fast", "application/octet-stream", bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if obj.Size != int64(len(payload)) {
			t.Fatalf("size: want %d, got %d", len(payload), obj.Size)
		}
	}

	run(t)
	res := testing.Benchmark(func(b *testing.B) {
		for b.Loop() {
			run(b)
		}
	})
	allocedBytes := res.AllocedBytesPerOp()
	t.Logf("SegmentWriter byte fast path alloc bytes: %d", allocedBytes)
	if allocedBytes > 18*1024*1024 {
		t.Fatalf("alloc bytes: got %d, want <= %d", allocedBytes, 18*1024*1024)
	}
}
