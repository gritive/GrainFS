package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/chunkref"
)

func TestPutObjectRecordAddsChunkRefs(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	if err := b.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	obj := &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}, {BlobID: "chunk-B"}}}
	if err := b.PutObjectRecord(ctx, "bkt", "k", obj); err != nil {
		t.Fatalf("put: %v", err)
	}
	m := chunkref.ObjectVersionID("bkt", "k", "")
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		for _, c := range []chunkref.ChunkID{"chunk-A", "chunk-B"} {
			n, err := s.RefCount(c)
			if err != nil {
				return err
			}
			if n != 1 {
				t.Fatalf("RefCount(%s) = %d, want 1", c, n)
			}
			if _, gerr := txn.Get(refMembershipKey(m, c)); gerr != nil {
				t.Fatalf("missing ref (%v, %s): %v", m, c, gerr)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestOverwriteRemovesStaleChunkRefs(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	_ = b.CreateBucket(ctx, "bkt")
	if err := b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "old-chunk"}}}); err != nil {
		t.Fatal(err)
	}
	if err := b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "new-chunk"}}}); err != nil {
		t.Fatal(err)
	}
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		if n, _ := s.RefCount("old-chunk"); n != 0 {
			t.Fatalf("old-chunk RefCount = %d, want 0 (stale ref removed)", n)
		}
		if n, _ := s.RefCount("new-chunk"); n != 1 {
			t.Fatalf("new-chunk RefCount = %d, want 1", n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOverwritePartialOverlapChunkRefs(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	_ = b.CreateBucket(ctx, "bkt")
	if err := b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}, {BlobID: "chunk-B"}}}); err != nil {
		t.Fatal(err)
	}
	if err := b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}, {BlobID: "chunk-C"}}}); err != nil {
		t.Fatal(err)
	}
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		if n, _ := s.RefCount("chunk-A"); n != 1 {
			t.Fatalf("chunk-A RefCount = %d, want 1 (shared, survives)", n)
		}
		if n, _ := s.RefCount("chunk-B"); n != 0 {
			t.Fatalf("chunk-B RefCount = %d, want 0 (dropped)", n)
		}
		if _, ok, _ := s.TombstoneTime("chunk-B"); !ok {
			t.Fatalf("expected tombstone for dropped chunk-B")
		}
		if n, _ := s.RefCount("chunk-C"); n != 1 {
			t.Fatalf("chunk-C RefCount = %d, want 1 (new)", n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteObjectRemovesChunkRefs(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	_ = b.CreateBucket(ctx, "bkt")
	if err := b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}}}); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteObject(ctx, "bkt", "k"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		if n, _ := s.RefCount("chunk-A"); n != 0 {
			t.Fatalf("RefCount = %d, want 0 after delete", n)
		}
		if _, ok, _ := s.TombstoneTime("chunk-A"); !ok {
			t.Fatalf("expected tombstone after delete-to-zero")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestPutObjectRecordTombstoneEvictedOnRewrite verifies that re-adding a chunk
// (same chunk-A, overwrite with same content) clears a tombstone that was set
// when a prior overwrite removed it. This exercises the AddRef tombstone eviction path.
func TestPutObjectRecordTombstoneEvictedOnRewrite(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	_ = b.CreateBucket(ctx, "bkt")

	// First write: chunk-A referenced.
	if err := b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}}}); err != nil {
		t.Fatal(err)
	}
	// Overwrite with chunk-B: chunk-A should get tombstoned.
	if err := b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-B"}}}); err != nil {
		t.Fatal(err)
	}
	// Overwrite again re-adding chunk-A: tombstone must be cleared.
	if err := b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}}}); err != nil {
		t.Fatal(err)
	}
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		if _, ok, _ := s.TombstoneTime("chunk-A"); ok {
			t.Fatalf("chunk-A tombstone should have been cleared after re-add")
		}
		if n, _ := s.RefCount("chunk-A"); n != 1 {
			t.Fatalf("chunk-A RefCount = %d, want 1", n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure tombstone timestamp is roughly now (not zero).
func TestDeleteObjectTombstoneTimestamp(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	_ = b.CreateBucket(ctx, "bkt")
	before := time.Now()
	if err := b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-X"}}}); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteObject(ctx, "bkt", "k"); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		ts, ok, err := s.TombstoneTime("chunk-X")
		if err != nil {
			return err
		}
		if !ok {
			t.Fatal("expected tombstone")
		}
		if ts.Before(before) || ts.After(after) {
			t.Fatalf("tombstone ts %v outside [%v, %v]", ts, before, after)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
