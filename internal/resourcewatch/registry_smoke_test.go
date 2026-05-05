package resourcewatch

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

func touchVlog(t *testing.T, dir string, mtime time.Time) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	p := filepath.Join(dir, "000000.vlog")
	f, err := os.Create(p)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = f.Close()
	if err := os.Chtimes(p, mtime, mtime); err != nil {
		t.Fatalf("chtimes: %v", err)
	}
}

func TestVerifyVlogRegistry_DetectsMissing(t *testing.T) {
	root := t.TempDir()
	touchVlog(t, filepath.Join(root, "orphan"), time.Now())
	report, err := VerifyVlogRegistry(root, NewRegistry(), false)
	if err != nil {
		t.Fatalf("VerifyVlogRegistry: %v", err)
	}
	if len(report.Live) != 1 {
		t.Fatalf("Live=%v want 1", report.Live)
	}
}

func TestVerifyVlogRegistry_ExcludesNonBadgerDirs(t *testing.T) {
	root := t.TempDir()
	for _, p := range []string{"blobs", "snapshots", ".recovery", "wal", "shards"} {
		touchVlog(t, filepath.Join(root, p, "sub"), time.Now())
	}
	report, _ := VerifyVlogRegistry(root, NewRegistry(), false)
	if len(report.Live)+len(report.Stale) != 0 {
		t.Fatalf("excluded dirs surfaced: live=%v stale=%v", report.Live, report.Stale)
	}
}

func TestVerifyVlogRegistry_StrictReturnsError(t *testing.T) {
	root := t.TempDir()
	touchVlog(t, filepath.Join(root, "orphan"), time.Now())
	_, err := VerifyVlogRegistry(root, NewRegistry(), true)
	if err == nil || !strings.Contains(err.Error(), "under-populated") {
		t.Fatalf("strict: err=%v", err)
	}
}

func TestVerifyVlogRegistry_NoMissing_AllRegistered(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "meta")
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	r := NewRegistry()
	r.Register(DBCategoryMeta, db)
	report, err := VerifyVlogRegistry(root, r, false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(report.Live)+len(report.Stale) != 0 {
		t.Fatalf("registered but flagged: live=%v stale=%v", report.Live, report.Stale)
	}
}

func TestVerifyVlogRegistry_StaleOrphan_LogOnly(t *testing.T) {
	root := t.TempDir()
	stale := time.Now().Add(-2 * time.Hour)
	touchVlog(t, filepath.Join(root, "old-removed-group"), stale)
	report, err := VerifyVlogRegistry(root, NewRegistry(), false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(report.Stale) != 1 {
		t.Fatalf("Stale=%v want 1 (old mtime → stale)", report.Stale)
	}
	if len(report.Live) != 0 {
		t.Fatalf("Live=%v want 0", report.Live)
	}
}

func TestVerifyVlogRegistry_LiveUnregistered_Incident(t *testing.T) {
	root := t.TempDir()
	touchVlog(t, filepath.Join(root, "fresh-unregistered"), time.Now())
	report, _ := VerifyVlogRegistry(root, NewRegistry(), false)
	if len(report.Live) != 1 {
		t.Fatalf("Live=%v want 1 (recent mtime → live)", report.Live)
	}
}

func TestWalkVlogDirs_SkipDirOnExcluded(t *testing.T) {
	root := t.TempDir()
	deep := filepath.Join(root, "blobs", "a", "b", "c", "d")
	touchVlog(t, deep, time.Now())
	dirs := walkVlogDirs(root)
	for d := range dirs {
		if strings.Contains(d, "/blobs/") || strings.HasSuffix(d, "/blobs") {
			t.Fatalf("walked into excluded dir: %s", d)
		}
	}
}
