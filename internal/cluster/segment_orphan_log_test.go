package cluster

import (
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/chunkref"
)

func newTestBadger(t *testing.T) *badger.DB {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLoggingLevel(badger.ERROR))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSegmentOrphanLog_ObserveFirstWins(t *testing.T) {
	log := NewSegmentOrphanLog(newTestBadger(t), "group-0")
	c := chunkref.ChunkID("blob-1")
	t0 := time.Unix(1000, 0)
	if err := log.Observe(c, t0); err != nil {
		t.Fatal(err)
	}
	if err := log.Observe(c, t0.Add(time.Hour)); err != nil { // must NOT overwrite
		t.Fatal(err)
	}
	got, ok, err := log.TombstoneTime(c)
	if err != nil || !ok || !got.Equal(t0) {
		t.Fatalf("got %v ok=%v err=%v want %v", got, ok, err, t0)
	}
}

func TestSegmentOrphanLog_Forget(t *testing.T) {
	log := NewSegmentOrphanLog(newTestBadger(t), "group-0")
	c := chunkref.ChunkID("blob-2")
	_ = log.Observe(c, time.Unix(1000, 0))
	if err := log.Forget(c); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := log.TombstoneTime(c); err != nil || ok {
		t.Fatalf("after Forget ok=%v err=%v want ok=false", ok, err)
	}
	if err := log.Forget(c); err != nil {
		t.Fatalf("Forget must be idempotent: %v", err)
	}
}

func TestSegmentOrphanLog_AbsentIsNotTombstoned(t *testing.T) {
	log := NewSegmentOrphanLog(newTestBadger(t), "group-0")
	if _, ok, err := log.TombstoneTime(chunkref.ChunkID("never")); err != nil || ok {
		t.Fatalf("ok=%v err=%v want ok=false err=nil", ok, err)
	}
}

// TestSegmentOrphanLog_SurvivesFSMRestore proves that sgc: keys survive the
// exact DropPrefix call that FSM.Restore makes on the group prefix. This is the
// critical safety property: orphan-log entries must not be wiped by a Raft
// snapshot restore.
func TestSegmentOrphanLog_SurvivesFSMRestore(t *testing.T) {
	db := newTestBadger(t)

	// Build a real group keyspace, matching what FSM.Restore uses.
	ks, err := newStateKeyspace("group-0")
	if err != nil {
		t.Fatal(err)
	}

	// Write one group-prefixed FSM key.
	groupKey := ks.Key([]byte("objmeta"))
	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Set(groupKey, []byte("value"))
	}); err != nil {
		t.Fatalf("set group key: %v", err)
	}

	// Write one sgc: orphan-log entry.
	orphanLog := NewSegmentOrphanLog(db, "group-0")
	c := chunkref.ChunkID("seg-survive-test")
	t0 := time.Unix(9999, 0)
	if err := orphanLog.Observe(c, t0); err != nil {
		t.Fatalf("observe: %v", err)
	}

	// Simulate FSM.Restore: DropPrefix on the group prefix (the exact call in apply.go:951).
	if err := db.DropPrefix(ks.Prefix(nil)); err != nil {
		t.Fatalf("DropPrefix: %v", err)
	}

	// Assert the group-prefixed key is GONE.
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(groupKey)
		return err
	})
	if err != badger.ErrKeyNotFound {
		t.Fatalf("group key must be gone after DropPrefix, got err=%v", err)
	}

	// Assert the sgc: orphan-log entry SURVIVES.
	got, ok, err := orphanLog.TombstoneTime(c)
	if err != nil {
		t.Fatalf("TombstoneTime after DropPrefix: %v", err)
	}
	if !ok {
		t.Fatal("orphan log entry must survive FSM DropPrefix, but TombstoneTime returned ok=false")
	}
	if !got.Equal(t0) {
		t.Fatalf("orphan log t_zero mismatch: got %v want %v", got, t0)
	}
}
