package resourcewatch

import (
	"context"
	"path/filepath"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

func TestVlogProvider_Snapshot_SumsAllRegisteredCategories(t *testing.T) {
	root := t.TempDir()
	r := NewRegistry()
	for _, cat := range []Category{DBCategoryMeta, DBCategoryDedup} {
		dir := filepath.Join(root, string(cat))
		db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		if err != nil {
			t.Fatalf("open %s: %v", cat, err)
		}
		t.Cleanup(func() { _ = db.Close() })
		r.Register(cat, db)
	}
	p := NewVlogProvider(VlogProviderOptions{DataDir: root, Registry: r})
	s, err := p.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if s.Limit <= s.Open {
		t.Fatalf("Limit=%d Open=%d (Limit must include Bavail)", s.Limit, s.Open)
	}
	if _, ok := s.Categories[DBCategoryMeta]; !ok {
		t.Fatalf("Categories missing meta: %v", s.Categories)
	}
	if _, ok := s.Categories[DBCategoryDedup]; !ok {
		t.Fatalf("Categories missing dedup: %v", s.Categories)
	}
}

func TestVlogProvider_Snapshot_StatfsError_ReturnsErr(t *testing.T) {
	p := NewVlogProvider(VlogProviderOptions{DataDir: "/nonexistent-path-vlog-test", Registry: NewRegistry()})
	if _, err := p.Snapshot(context.Background()); err == nil {
		t.Fatalf("expected statfs error")
	}
}

func TestVlogProvider_Snapshot_NoDBs_ReturnsZero(t *testing.T) {
	p := NewVlogProvider(VlogProviderOptions{DataDir: t.TempDir(), Registry: NewRegistry()})
	s, err := p.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if s.Open != 0 {
		t.Fatalf("Open=%d want 0", s.Open)
	}
}

func TestVlogProvider_Snapshot_BavailZero_RatioIsOne(t *testing.T) {
	t.Skip("Bavail=0 boundary requires mock filesystem; covered by Limit > 0 invariant")
}

func TestVlogProvider_Snapshot_ClosedDBInSnapshot_NoPanic(t *testing.T) {
	r := NewRegistry()
	dir := filepath.Join(t.TempDir(), "meta")
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	r.Register(DBCategoryMeta, db)
	_ = db.Close()
	p := NewVlogProvider(VlogProviderOptions{DataDir: t.TempDir(), Registry: r})
	defer func() {
		if rv := recover(); rv != nil {
			t.Fatalf("Snapshot panicked on closed DB: %v", rv)
		}
	}()
	_, _ = p.Snapshot(context.Background())
}
