package storage

import (
	"bytes"
	"context"
	"io"
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
