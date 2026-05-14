package nfsexport

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func openTestStore(t *testing.T, dir string) (*badger.DB, *Store) {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	store, err := OpenStore(db)
	require.NoError(t, err)
	return db, store
}

func TestStorePutGetListOrdering(t *testing.T) {
	db, store := openTestStore(t, t.TempDir())
	defer db.Close()

	require.NoError(t, store.Put("zeta", Config{FsidMinor: 2}))
	require.NoError(t, store.Put("alpha", Config{FsidMinor: 1, ReadOnly: true}))

	cfg, ok := store.Get("alpha")
	require.True(t, ok)
	require.True(t, cfg.ReadOnly)
	require.Equal(t, []string{"alpha", "zeta"}, store.Snapshot().SortedNames())
}

func TestStoreDeleteIdempotent(t *testing.T) {
	db, store := openTestStore(t, t.TempDir())
	defer db.Close()

	require.NoError(t, store.Put("bucket", Config{FsidMinor: 1}))
	require.NoError(t, store.Delete("bucket"))
	require.NoError(t, store.Delete("bucket"))
	_, ok := store.Get("bucket")
	require.False(t, ok)
}

func TestStorePutAdvancesAllocator(t *testing.T) {
	db, store := openTestStore(t, t.TempDir())
	defer db.Close()

	require.NoError(t, store.Put("seed", Config{FsidMajor: 1, FsidMinor: 7, Generation: 1}))
	got, err := store.GetAllocator()
	require.NoError(t, err)
	require.Equal(t, uint64(7), got)

	cfg, err := store.ApplyUpsert("next", false, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(8), cfg.FsidMinor)
}

func TestStoreApplyCreateRejectsExistingExport(t *testing.T) {
	db, store := openTestStore(t, t.TempDir())
	defer db.Close()

	created, err := store.ApplyCreate("bucket", false, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), created.Generation)

	_, err = store.ApplyCreate("bucket", true, 1)
	require.True(t, errors.Is(err, ErrExportExists), "err = %v", err)

	got, ok := store.Get("bucket")
	require.True(t, ok)
	require.Equal(t, created, got)
}

func TestStoreReopenRehydratesSnapshot(t *testing.T) {
	dir := t.TempDir()
	db, store := openTestStore(t, dir)
	require.NoError(t, store.Put("bucket", Config{FsidMajor: 1, FsidMinor: 2, Generation: 3}))
	require.NoError(t, db.Close())

	db, store = openTestStore(t, dir)
	defer db.Close()
	cfg, ok := store.Get("bucket")
	require.True(t, ok)
	require.Equal(t, Config{FsidMajor: 1, FsidMinor: 2, Generation: 3}, cfg)
}

func TestStoreConcurrentPutAndSnapshotReaders(t *testing.T) {
	db, store := openTestStore(t, t.TempDir())
	defer db.Close()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				require.NoError(t, store.Put(fmt.Sprintf("bucket-%d-%d", id, j), Config{FsidMinor: uint64(j + 1)}))
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = store.Snapshot().SortedNames()
			}
		}()
	}
	wg.Wait()
}
