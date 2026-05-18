package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func newTestLocalBackend(t *testing.T) *LocalBackend {
	t.Helper()
	dir := t.TempDir()
	b, err := NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.CreateBucket(context.Background(), "test"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	return b
}

func TestAppendObjectRejectsMismatchedOffset(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	if _, err := b.AppendObject(ctx, "test", "k", 0, strings.NewReader("0123456789")); err != nil {
		t.Fatalf("initial: %v", err)
	}
	_, err := b.AppendObject(ctx, "test", "k", 5, bytes.NewReader([]byte("xxx")))
	if !errors.Is(err, ErrAppendOffsetMismatch) {
		t.Fatalf("expected ErrAppendOffsetMismatch, got %v", err)
	}
}

func TestAppendObjectInitialCreates10MiBSegment(t *testing.T) {
	b := newTestLocalBackend(t)
	body := bytes.Repeat([]byte("A"), 10<<20) // 10 MiB
	obj, err := b.AppendObject(context.Background(), "test", "k", 0, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if obj.Size != int64(10<<20) {
		t.Fatalf("size=%d", obj.Size)
	}
	if len(obj.Segments) != 1 {
		t.Fatalf("segments=%d", len(obj.Segments))
	}
	if !obj.IsAppendable {
		t.Fatal("IsAppendable=false")
	}
	if !strings.HasSuffix(obj.ETag, "-1") {
		t.Fatalf("etag=%q", obj.ETag)
	}
}

func TestAppendObjectSequentialThreeSegments(t *testing.T) {
	b := newTestLocalBackend(t)
	body := bytes.Repeat([]byte("X"), 10<<20) // 10 MiB

	off := int64(0)
	var obj *Object
	for i := 0; i < 3; i++ {
		var err error
		obj, err = b.AppendObject(context.Background(), "test", "k", off, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		off = obj.Size
	}
	if obj.Size != int64(30<<20) {
		t.Fatalf("size=%d, want %d", obj.Size, 30<<20)
	}
	if len(obj.Segments) != 3 {
		t.Fatalf("segments=%d, want 3", len(obj.Segments))
	}
	if !strings.HasSuffix(obj.ETag, "-3") {
		t.Fatalf("etag=%q, want suffix -3", obj.ETag)
	}
}
