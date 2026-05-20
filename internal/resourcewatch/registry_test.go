package resourcewatch

import (
	"sync"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/badgerutil"
)

func openTestDB(t *testing.T) *badger.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := badger.Open(badgerutil.SmallOptions(dir))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestRegistry_RegisterDeregister_RoundTrip(t *testing.T) {
	r := NewRegistry()
	db := openTestDB(t)
	e := r.Register(DBCategoryMeta, db)
	if e == nil || e.Category != DBCategoryMeta || e.DB != db {
		t.Fatalf("register returned %+v", e)
	}
	if got := len(r.Snapshot()); got != 1 {
		t.Fatalf("Snapshot len=%d want 1", got)
	}
	r.Deregister(e)
	if got := len(r.Snapshot()); got != 0 {
		t.Fatalf("after deregister Snapshot len=%d want 0", got)
	}
}

func TestRegistry_Snapshot_StableCopy(t *testing.T) {
	r := NewRegistry()
	r.Register(DBCategoryMeta, openTestDB(t))
	snap := r.Snapshot()
	r.Register(DBCategoryDedup, openTestDB(t))
	if len(snap) != 1 {
		t.Fatalf("snapshot mutated by later Register: len=%d want 1", len(snap))
	}
}

func TestRegistry_Reset_ClearsState(t *testing.T) {
	r := NewRegistry()
	r.Register(DBCategoryMeta, openTestDB(t))
	r.Reset()
	if got := len(r.Snapshot()); got != 0 {
		t.Fatalf("after Reset len=%d want 0", got)
	}
}

func TestRegistry_ConcurrentRegister_NoDataRace(t *testing.T) {
	r := NewRegistry()
	db := openTestDB(t)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := r.Register(DBCategoryMeta, db)
			_ = r.Snapshot()
			r.Deregister(e)
		}()
	}
	wg.Wait()
}

func TestRegistry_RegisterNilDB_Panics(t *testing.T) {
	r := NewRegistry()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on nil DB")
		}
	}()
	r.Register(DBCategoryMeta, nil)
}

func TestRegistry_RegisterDuplicate_AppendsBoth(t *testing.T) {
	r := NewRegistry()
	db := openTestDB(t)
	r.Register(DBCategoryMeta, db)
	r.Register(DBCategoryMeta, db)
	if got := len(r.Snapshot()); got != 2 {
		t.Fatalf("duplicate register len=%d want 2 (caller responsibility)", got)
	}
}

func TestRegistry_DeregisterNonExistent_Noop(t *testing.T) {
	r := NewRegistry()
	stale := &RegisteredDB{Category: DBCategoryMeta}
	r.Deregister(stale)
	r.Deregister(nil)
	if got := len(r.Snapshot()); got != 0 {
		t.Fatalf("unexpected entries after stale deregister: %d", got)
	}
}

func TestRegistry_NewInstance_IsolatedFromDefault(t *testing.T) {
	defer Default.Reset()
	r := NewRegistry()
	r.Register(DBCategoryMeta, openTestDB(t))
	if got := len(Default.Snapshot()); got != 0 {
		t.Fatalf("Default leaked from instance: len=%d", got)
	}
	if got := len(r.Snapshot()); got != 1 {
		t.Fatalf("instance lost entry: len=%d", got)
	}
}
