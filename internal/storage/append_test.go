package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/chunkref"
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
	obj, err := b.AppendObject(context.Background(), "test", "k", 0, newRepeatByteReader('A', 10<<20)) // 10 MiB
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
	// Until Task 3.1 wires real per-call MD5s, the prefix is an MD5 of segment-checksum bytes — assert structure only.
	if !strings.HasSuffix(obj.ETag, "-1") {
		t.Fatalf("etag=%q, want suffix -1", obj.ETag)
	}
	if idx := strings.IndexByte(obj.ETag, '-'); idx != 32 {
		t.Fatalf("etag=%q, want 32 hex chars before '-'", obj.ETag)
	}
}

func TestAppendObjectSequentialThreeSegments(t *testing.T) {
	b := newTestLocalBackend(t)

	off := int64(0)
	var obj *Object
	for i := 0; i < 3; i++ {
		var err error
		obj, err = b.AppendObject(context.Background(), "test", "k", off, newRepeatByteReader('X', 10<<20)) // 10 MiB
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
	// Until Task 3.1 wires real per-call MD5s, the prefix is an MD5 of segment-checksum bytes — assert structure only.
	if !strings.HasSuffix(obj.ETag, "-3") {
		t.Fatalf("etag=%q, want suffix -3", obj.ETag)
	}
	if idx := strings.IndexByte(obj.ETag, '-'); idx != 32 {
		t.Fatalf("etag=%q, want 32 hex chars before '-'", obj.ETag)
	}
}

type repeatByteReader struct {
	b byte
	n int64
}

func newRepeatByteReader(b byte, n int64) *repeatByteReader {
	return &repeatByteReader{b: b, n: n}
}

func (r *repeatByteReader) Read(p []byte) (int, error) {
	if r.n == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.n {
		p = p[:r.n]
	}
	for i := range p {
		p[i] = r.b
	}
	r.n -= int64(len(p))
	return len(p), nil
}

func TestAppendObjectRejectsAtCap(t *testing.T) {
	// Save and restore cap for fast test
	orig := MaxAppendSegments
	t.Cleanup(func() { MaxAppendSegments = orig })
	MaxAppendSegments = 4 // local override

	b := newTestLocalBackend(t)
	ctx := context.Background()
	body := []byte("ABC")

	off := int64(0)
	for i := 0; i < 4; i++ {
		obj, err := b.AppendObject(ctx, "test", "k", off, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		off = obj.Size
	}
	_, err := b.AppendObject(ctx, "test", "k", off, bytes.NewReader(body))
	if !errors.Is(err, ErrAppendCapExceeded) {
		t.Fatalf("expected ErrAppendCapExceeded, got %v", err)
	}
}

func TestAppendObjectConvertsPlainPutAtCurrentOffset(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()

	if _, err := b.PutObject(ctx, "test", "k", strings.NewReader("hello"), "text/plain"); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	obj, err := b.AppendObject(ctx, "test", "k", 5, bytes.NewReader([]byte("world")))
	if err != nil {
		t.Fatalf("AppendObject: %v", err)
	}
	if !obj.IsAppendable {
		t.Fatal("IsAppendable=false")
	}
	if obj.Size != 10 {
		t.Fatalf("size=%d, want 10", obj.Size)
	}

	rc, _, err := b.GetObject(ctx, "test", "k")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "helloworld" {
		t.Fatalf("body=%q, want helloworld", string(got))
	}
}

func TestAppendObjectConvertsPlainPutAddsBaseAndAppendRefs(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()

	if _, err := b.PutObject(ctx, "test", "k", strings.NewReader("hello"), "text/plain"); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	obj, err := b.AppendObject(ctx, "test", "k", 5, bytes.NewReader([]byte("world")))
	if err != nil {
		t.Fatalf("AppendObject: %v", err)
	}
	if len(obj.Segments) != 2 {
		t.Fatalf("segments=%d, want 2", len(obj.Segments))
	}
	m := chunkref.ObjectVersionID("test", "k", obj.VersionID)
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		for _, seg := range obj.Segments {
			c := chunkref.ChunkID(ParseLocator(seg.BlobID).String())
			if n, _ := s.RefCount(c); n != 1 {
				t.Fatalf("RefCount(%s) = %d, want 1", c, n)
			}
			if _, err := txn.Get(refMembershipKey(m, c)); err != nil {
				t.Fatalf("missing ref (%v, %s): %v", m, c, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestWriteSegmentBlob_PopulatesChecksum(t *testing.T) {
	b := newTestLocalBackend(t)

	data := []byte("hello segment world")
	ref, err := b.WriteSegmentBlob("test", "key-a", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(ref.Checksum) != ChecksumLen {
		t.Fatalf("checksum length: want %d, got %d", ChecksumLen, len(ref.Checksum))
	}
	want := ChecksumOf(data)
	if !bytes.Equal(ref.Checksum, want) {
		t.Fatalf("checksum mismatch: want %x, got %x", want, ref.Checksum)
	}
}

func TestErrAppendObjectTooLargeSentinel(t *testing.T) {
	if !errors.Is(ErrAppendObjectTooLarge, ErrAppendObjectTooLarge) {
		t.Fatalf("sentinel must be self-equal under errors.Is")
	}
	if errors.Is(ErrAppendObjectTooLarge, ErrAppendCapExceeded) {
		t.Fatalf("ErrAppendObjectTooLarge must not alias ErrAppendCapExceeded")
	}
}
