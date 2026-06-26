package storage

import (
	"context"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/chunkref"
	"github.com/stretchr/testify/require"
)

func TestPutObjectRecordAddsChunkRefs(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bkt"), "create bucket")
	obj := &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}, {BlobID: "chunk-B"}}}
	require.NoError(t, b.PutObjectRecord(ctx, "bkt", "k", obj), "put")
	m := chunkref.ObjectVersionID("bkt", "k", "")
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		for _, c := range []chunkref.ChunkID{"chunk-A", "chunk-B"} {
			n, err := s.RefCount(c)
			if err != nil {
				return err
			}
			require.Equal(t, 1, n, "RefCount(%s)", c)
			_, err = txn.Get(refMembershipKey(m, c))
			require.NoError(t, err, "missing ref (%v, %s)", m, c)
		}
		return nil
	}); err != nil {
		require.NoError(t, err, "view")
	}
}

func TestOverwriteRemovesStaleChunkRefs(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	_ = b.CreateBucket(ctx, "bkt")
	require.NoError(t, b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "old-chunk"}}}))
	require.NoError(t, b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "new-chunk"}}}))
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		n, err := s.RefCount("old-chunk")
		require.NoError(t, err)
		require.Zero(t, n, "old-chunk RefCount")
		n, err = s.RefCount("new-chunk")
		require.NoError(t, err)
		require.Equal(t, 1, n, "new-chunk RefCount")
		return nil
	}); err != nil {
		require.NoError(t, err)
	}
}

func TestOverwritePartialOverlapChunkRefs(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	_ = b.CreateBucket(ctx, "bkt")
	require.NoError(t, b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}, {BlobID: "chunk-B"}}}))
	require.NoError(t, b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}, {BlobID: "chunk-C"}}}))
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		n, err := s.RefCount("chunk-A")
		require.NoError(t, err)
		require.Equal(t, 1, n, "chunk-A RefCount")
		_, ok, err := s.TombstoneTime("chunk-A")
		require.NoError(t, err)
		require.False(t, ok, "chunk-A has a tombstone after surviving overwrite")
		n, err = s.RefCount("chunk-B")
		require.NoError(t, err)
		require.Zero(t, n, "chunk-B RefCount")
		_, ok, err = s.TombstoneTime("chunk-B")
		require.NoError(t, err)
		require.True(t, ok, "expected tombstone for dropped chunk-B")
		n, err = s.RefCount("chunk-C")
		require.NoError(t, err)
		require.Equal(t, 1, n, "chunk-C RefCount")
		return nil
	}); err != nil {
		require.NoError(t, err)
	}
}

func TestAppendObjectRecordAddsOnlyNewChunkRef(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	_ = b.CreateBucket(ctx, "bkt")
	require.NoError(t, b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}}}))
	next := &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}, {BlobID: "chunk-B"}}}
	require.NoError(t, b.putObjectRecordAppend(ctx, "bkt", "k", next, []string{"chunk-B"}))
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		n, err := s.RefCount("chunk-A")
		require.NoError(t, err)
		require.Equal(t, 1, n, "chunk-A RefCount")
		_, ok, err := s.TombstoneTime("chunk-A")
		require.NoError(t, err)
		require.False(t, ok, "chunk-A has a tombstone after append-only manifest update")
		n, err = s.RefCount("chunk-B")
		require.NoError(t, err)
		require.Equal(t, 1, n, "chunk-B RefCount")
		return nil
	}); err != nil {
		require.NoError(t, err)
	}
}

func TestDeleteObjectRemovesChunkRefs(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	_ = b.CreateBucket(ctx, "bkt")
	require.NoError(t, b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}}}))
	require.NoError(t, b.DeleteObject(ctx, "bkt", "k"), "delete")
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		n, err := s.RefCount("chunk-A")
		require.NoError(t, err)
		require.Zero(t, n, "RefCount after delete")
		_, ok, err := s.TombstoneTime("chunk-A")
		require.NoError(t, err)
		require.True(t, ok, "expected tombstone after delete-to-zero")
		return nil
	}); err != nil {
		require.NoError(t, err)
	}
}

func TestDeleteObjectRemovesAppendSideRecordChunkRefs(t *testing.T) {
	b, obj := seedAppendSideRecordObject(t, []string{"hello", "world"})
	ctx := context.Background()
	require.NoError(t, b.DeleteObject(ctx, "test", "k"))
	err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		for i, seg := range obj.Segments {
			c := chunkref.ChunkID(ParseLocator(seg.BlobID).String())
			n, err := s.RefCount(c)
			require.NoError(t, err)
			require.Zero(t, n, "segment %d RefCount(%s)", i, c)
			_, ok, err := s.TombstoneTime(c)
			require.NoError(t, err)
			require.True(t, ok, "segment %d expected tombstone for %s", i, c)
			if _, err := txn.Get(appendSegmentKey("test", "k", obj.VersionID, i+1)); err == nil {
				require.Failf(t, "side segment record still present", "seq=%d", i)
			} else if err != badger.ErrKeyNotFound {
				return err
			}
		}
		if _, err := txn.Get(appendSummaryKey("test", "k", obj.VersionID)); err == nil {
			require.Fail(t, "append summary record still present")
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		return nil
	})
	require.NoError(t, err, "view")
}

// TestPutObjectRecordTombstoneEvictedOnRewrite verifies that re-adding a chunk
// (same chunk-A, overwrite with same content) clears a tombstone that was set
// when a prior overwrite removed it. This exercises the AddRef tombstone eviction path.
func TestPutObjectRecordTombstoneEvictedOnRewrite(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	_ = b.CreateBucket(ctx, "bkt")

	// First write: chunk-A referenced.
	require.NoError(t, b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}}}))
	// Overwrite with chunk-B: chunk-A should get tombstoned.
	require.NoError(t, b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-B"}}}))
	// Overwrite again re-adding chunk-A: tombstone must be cleared.
	require.NoError(t, b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-A"}}}))
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		_, ok, err := s.TombstoneTime("chunk-A")
		require.NoError(t, err)
		require.False(t, ok, "chunk-A tombstone should have been cleared after re-add")
		n, err := s.RefCount("chunk-A")
		require.NoError(t, err)
		require.Equal(t, 1, n, "chunk-A RefCount")
		return nil
	}); err != nil {
		require.NoError(t, err)
	}
}

// Ensure tombstone timestamp is roughly now (not zero).
func TestDeleteObjectTombstoneTimestamp(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	_ = b.CreateBucket(ctx, "bkt")
	before := time.Now()
	require.NoError(t, b.PutObjectRecord(ctx, "bkt", "k", &Object{Key: "k", Segments: []SegmentRef{{BlobID: "chunk-X"}}}))
	require.NoError(t, b.DeleteObject(ctx, "bkt", "k"))
	after := time.Now()
	if err := b.db.View(func(txn *badger.Txn) error {
		s := NewChunkRefStore(txn)
		ts, ok, err := s.TombstoneTime("chunk-X")
		if err != nil {
			return err
		}
		require.True(t, ok, "expected tombstone")
		require.False(t, ts.Before(before) || ts.After(after), "tombstone ts %v outside [%v, %v]", ts, before, after)
		return nil
	}); err != nil {
		require.NoError(t, err)
	}
}
