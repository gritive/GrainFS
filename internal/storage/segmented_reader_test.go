package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
)

func setupThreeSegmentObject(t *testing.T) (*LocalBackend, *Object) {
	t.Helper()
	b := newTestLocalBackend(t)
	body := bytes.Repeat([]byte("S"), 10<<20)
	var obj *Object
	off := int64(0)
	for i := 0; i < 3; i++ {
		o, err := b.AppendObject(context.Background(), "test", "k", off, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		obj = o
		off = o.Size
	}
	return b, obj
}

func TestSegmentedReaderFullStitch(t *testing.T) {
	b, obj := setupThreeSegmentObject(t)
	r, err := b.OpenSegmentedReader("test", "k", obj, 0, obj.Size-1)
	if err != nil {
		t.Fatalf("OpenSegmentedReader: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if int64(len(got)) != obj.Size {
		t.Fatalf("read %d, want %d", len(got), obj.Size)
	}
	for i, c := range got {
		if c != 'S' {
			t.Fatalf("byte %d = %c", i, c)
		}
	}
}

func TestSegmentedReaderRangeWithinSingleSegment(t *testing.T) {
	b, obj := setupThreeSegmentObject(t)
	// Range: bytes=100-200 (within seg1)
	r, err := b.OpenSegmentedReader("test", "k", obj, 100, 200)
	if err != nil {
		t.Fatalf("OpenSegmentedReader: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 101 {
		t.Fatalf("read %d bytes, want 101", len(got))
	}
}

func TestSegmentedReaderRangeAcrossSegments(t *testing.T) {
	b, obj := setupThreeSegmentObject(t)
	// Range: bytes=5MiB - 15MiB → seg1[5MiB..10MiB) + seg2[0..5MiB]
	start := int64(5 << 20)
	end := int64(15<<20) - 1
	r, err := b.OpenSegmentedReader("test", "k", obj, start, end)
	if err != nil {
		t.Fatalf("OpenSegmentedReader: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	want := end - start + 1
	if int64(len(got)) != want {
		t.Fatalf("read %d, want %d", len(got), want)
	}
}

func TestSegmentedReaderRangeAtBoundary(t *testing.T) {
	b, obj := setupThreeSegmentObject(t)
	// 정확히 segment 경계: 1 byte from seg1 + 1 byte from seg2
	start := int64(10<<20) - 1
	end := int64(10 << 20)
	r, err := b.OpenSegmentedReader("test", "k", obj, start, end)
	if err != nil {
		t.Fatalf("OpenSegmentedReader: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("read %d, want 2", len(got))
	}
	if got[0] != 'S' || got[1] != 'S' {
		t.Fatalf("bytes=%v, want [S S]", got)
	}
}

func TestSegmentedReaderEncryptedTamperRejected(t *testing.T) {
	enc := testEncryptor(t)
	b, err := NewEncryptedLocalBackend(t.TempDir(), enc)
	if err != nil {
		t.Fatalf("NewEncryptedLocalBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	ctx := context.Background()
	if err := b.CreateBucket(ctx, "test"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	body := bytes.Repeat([]byte("Z"), 1<<20)
	obj, err := b.AppendObject(ctx, "test", "k", 0, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// segment blob 1바이트 tamper → AES-GCM tag mismatch
	path := b.segmentPath("test", "k", obj.Segments[0].BlobID)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.WriteAt([]byte{0xff}, 100); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := b.OpenSegmentedReader("test", "k", obj, 0, obj.Size-1)
	if err != nil {
		t.Fatalf("OpenSegmentedReader: %v", err)
	}
	defer r.Close()
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("expected decrypt error, got nil")
	}
}
