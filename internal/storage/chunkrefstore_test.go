package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/chunkref"
	"github.com/stretchr/testify/require"
)

func openTestRefDB(t *testing.T) *badger.DB {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err, "open badger")
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestChunkRefStoreRemoveStillReferencedNoTombstone(t *testing.T) {
	// Two manifests reference the same chunk; removing one leaves refcount 1 and
	// must NOT write a tombstone (the chunk is still pinned — e.g. by a snapshot
	// after the live object is deleted). This is the false-eviction guard.
	db := openTestRefDB(t)
	c := chunkref.ChunkID("shared")
	m1 := chunkref.ObjectVersionID("bkt", "k", "v1")
	m2 := chunkref.SnapshotID(7)
	err := db.Update(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		if err := s.AddRef(m1, c); err != nil {
			return err
		}
		if err := s.AddRef(m2, c); err != nil {
			return err
		}
		return s.RemoveRef(m1, c, time.Unix(1000, 0))
	})
	require.NoError(t, err, "update")
	err = db.View(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		got, err := s.RefCount(c)
		require.NoError(t, err)
		require.Equal(t, 1, got, "RefCount still referenced by m2")
		_, ok, err := s.TombstoneTime(c)
		require.NoError(t, err)
		require.False(t, ok, "tombstone written while chunk still referenced")
		return nil
	})
	require.NoError(t, err, "view")
}

func TestChunkRefStoreAddIsIdempotent(t *testing.T) {
	db := openTestRefDB(t)
	m := chunkref.ObjectVersionID("bkt", "k", "v1")
	c := chunkref.ChunkID("chunk-1")
	err := db.Update(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		if err := s.AddRef(m, c); err != nil {
			return err
		}
		return s.AddRef(m, c)
	})
	require.NoError(t, err, "update")
	err = db.View(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		got, err := s.RefCount(c)
		require.NoError(t, err)
		require.Equal(t, 1, got, "RefCount")
		return nil
	})
	require.NoError(t, err, "view")
}

func TestChunkRefStoreRemoveToZeroWritesTombstone(t *testing.T) {
	db := openTestRefDB(t)
	m := chunkref.ObjectVersionID("bkt", "k", "v1")
	c := chunkref.ChunkID("chunk-1")
	tZero := time.Unix(1000, 0)
	err := db.Update(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		if err := s.AddRef(m, c); err != nil {
			return err
		}
		return s.RemoveRef(m, c, tZero)
	})
	require.NoError(t, err, "update")
	err = db.View(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		got, err := s.RefCount(c)
		require.NoError(t, err)
		require.Zero(t, got, "RefCount")
		ts, ok, err := s.TombstoneTime(c)
		require.NoError(t, err)
		require.True(t, ok, "TombstoneTime present")
		require.True(t, ts.Equal(tZero), "TombstoneTime = %v, want %v", ts, tZero)
		return nil
	})
	require.NoError(t, err, "view")
}

func TestChunkRefStoreReAddEvictsTombstone(t *testing.T) {
	db := openTestRefDB(t)
	m1 := chunkref.ObjectVersionID("bkt", "k", "v1")
	m2 := chunkref.SnapshotID(7)
	c := chunkref.ChunkID("chunk-1")
	err := db.Update(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		if err := s.AddRef(m1, c); err != nil {
			return err
		}
		if err := s.RemoveRef(m1, c, time.Unix(1000, 0)); err != nil {
			return err
		}
		return s.AddRef(m2, c)
	})
	require.NoError(t, err, "update")
	err = db.View(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		_, ok, err := s.TombstoneTime(c)
		require.NoError(t, err)
		require.False(t, ok, "tombstone present after re-add")
		got, err := s.RefCount(c)
		require.NoError(t, err)
		require.Equal(t, 1, got, "RefCount")
		return nil
	})
	require.NoError(t, err, "view")
}

func TestChunkRefStoreRemoveAbsentIsNoop(t *testing.T) {
	db := openTestRefDB(t)
	m := chunkref.ObjectVersionID("bkt", "k", "v1")
	c := chunkref.ChunkID("chunk-1")
	err := db.Update(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		return s.RemoveRef(m, c, time.Unix(1, 0)) // never added
	})
	require.NoError(t, err, "remove absent should be no-op")
	err = db.View(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		_, ok, err := s.TombstoneTime(c)
		require.NoError(t, err)
		require.False(t, ok, "no-op remove must not write tombstone")
		return nil
	})
	require.NoError(t, err, "view")
}
