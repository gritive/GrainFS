package badgermeta_test

import (
	"errors"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/badgermeta"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/metastore"
	"github.com/gritive/GrainFS/internal/metastore/storetest"
	"github.com/stretchr/testify/require"
)

func openBadgerStore(t *testing.T) *badgermeta.Store {
	t.Helper()
	db, err := badger.Open(badgerutil.SmallOptions(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return badgermeta.Wrap(db)
}

func TestBadgerStoreConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) metastore.Store {
		return openBadgerStore(t)
	})
}

func TestIteratorItemZeroAlloc(t *testing.T) {
	s := openBadgerStore(t)
	require.NoError(t, s.Update(func(txn metastore.Txn) error {
		for i := 0; i < 10; i++ {
			if err := txn.Set([]byte{'k', byte('0' + i)}, []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, s.View(func(txn metastore.Txn) error {
		it := txn.NewIterator(metastore.IteratorOptions{Prefix: []byte("k"), PrefetchValues: false})
		defer it.Close()
		it.Seek([]byte("k"))
		require.True(t, it.Valid())
		allocs := testing.AllocsPerRun(100, func() {
			_ = it.Item()
		})
		require.Zero(t, allocs, "iterator Item() must not allocate per element")
		return nil
	}))
}

func TestErrTxnTooBigMapped(t *testing.T) {
	// Direct unit test of the translation: forcing a real ErrTxnTooBig needs
	// thousands of writes (slow/flaky); the mapping is what matters.
	require.ErrorIs(t, badgermeta.MapErrForTest(badger.ErrTxnTooBig), metastore.ErrTxnTooBig)
	require.ErrorIs(t, badgermeta.MapErrForTest(badger.ErrKeyNotFound), metastore.ErrKeyNotFound)
	boom := errors.New("boom")
	require.ErrorIs(t, badgermeta.MapErrForTest(boom), boom)
	require.NoError(t, badgermeta.MapErrForTest(nil))
}
