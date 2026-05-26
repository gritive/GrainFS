package storage

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/chunkref"
)

func openTestRefDB(t *testing.T) *badger.DB {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
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
	if err := db.Update(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		if err := s.AddRef(m1, c); err != nil {
			return err
		}
		if err := s.AddRef(m2, c); err != nil {
			return err
		}
		return s.RemoveRef(m1, c, time.Unix(1000, 0))
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := db.View(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		if got, _ := s.RefCount(c); got != 1 {
			t.Fatalf("RefCount = %d, want 1 (still referenced by m2)", got)
		}
		if _, ok, _ := s.TombstoneTime(c); ok {
			t.Fatalf("tombstone written while chunk still referenced — false GC candidate")
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestChunkRefStoreAddIsIdempotent(t *testing.T) {
	db := openTestRefDB(t)
	m := chunkref.ObjectVersionID("bkt", "k", "v1")
	c := chunkref.ChunkID("chunk-1")
	if err := db.Update(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		if err := s.AddRef(m, c); err != nil {
			return err
		}
		return s.AddRef(m, c)
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := db.View(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		if got, _ := s.RefCount(c); got != 1 {
			t.Fatalf("RefCount = %d, want 1", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestChunkRefStoreRemoveToZeroWritesTombstone(t *testing.T) {
	db := openTestRefDB(t)
	m := chunkref.ObjectVersionID("bkt", "k", "v1")
	c := chunkref.ChunkID("chunk-1")
	tZero := time.Unix(1000, 0)
	if err := db.Update(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		if err := s.AddRef(m, c); err != nil {
			return err
		}
		return s.RemoveRef(m, c, tZero)
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := db.View(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		if got, _ := s.RefCount(c); got != 0 {
			t.Fatalf("RefCount = %d, want 0", got)
		}
		ts, ok, _ := s.TombstoneTime(c)
		if !ok || !ts.Equal(tZero) {
			t.Fatalf("TombstoneTime = (%v,%v), want (%v,true)", ts, ok, tZero)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestChunkRefStoreReAddEvictsTombstone(t *testing.T) {
	db := openTestRefDB(t)
	m1 := chunkref.ObjectVersionID("bkt", "k", "v1")
	m2 := chunkref.SnapshotID(7)
	c := chunkref.ChunkID("chunk-1")
	if err := db.Update(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		if err := s.AddRef(m1, c); err != nil {
			return err
		}
		if err := s.RemoveRef(m1, c, time.Unix(1000, 0)); err != nil {
			return err
		}
		return s.AddRef(m2, c)
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := db.View(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		if _, ok, _ := s.TombstoneTime(c); ok {
			t.Fatalf("tombstone present after re-add, want evicted")
		}
		if got, _ := s.RefCount(c); got != 1 {
			t.Fatalf("RefCount = %d, want 1", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestChunkRefStoreRemoveAbsentIsNoop(t *testing.T) {
	db := openTestRefDB(t)
	m := chunkref.ObjectVersionID("bkt", "k", "v1")
	c := chunkref.ChunkID("chunk-1")
	if err := db.Update(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		return s.RemoveRef(m, c, time.Unix(1, 0)) // never added
	}); err != nil {
		t.Fatalf("remove absent should be no-op, got %v", err)
	}
	if err := db.View(func(txn *badger.Txn) error {
		s := ChunkRefStore{txn: txn}
		if _, ok, _ := s.TombstoneTime(c); ok {
			t.Fatalf("no-op remove must not write tombstone")
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}
