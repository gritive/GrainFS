// Package storetest provides the conformance suite every metastore.Store
// implementation must pass. It pins exactly the semantics internal/cluster
// relies on — implementations run it from their own _test.go.
package storetest

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gritive/GrainFS/internal/metastore"
	"github.com/stretchr/testify/require"
)

// Run executes the conformance suite. opener must return a fresh, empty
// store; cleanup is the caller's responsibility (t.Cleanup inside opener).
func Run(t *testing.T, opener func(t *testing.T) metastore.Store) {
	t.Helper()

	t.Run("GetSetRoundtrip", func(t *testing.T) {
		s := opener(t)
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			return txn.Set([]byte("k1"), []byte("v1"))
		}))
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			item, err := txn.Get([]byte("k1"))
			require.NoError(t, err)
			val, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, []byte("v1"), val)
			require.Equal(t, []byte("k1"), item.KeyCopy(nil))
			return nil
		}))
	})

	t.Run("GetMissingIsErrKeyNotFound", func(t *testing.T) {
		s := opener(t)
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			_, err := txn.Get([]byte("absent"))
			require.ErrorIs(t, err, metastore.ErrKeyNotFound)
			return nil
		}))
	})

	t.Run("DeleteThenGetMissing", func(t *testing.T) {
		s := opener(t)
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			return txn.Set([]byte("k"), []byte("v"))
		}))
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			return txn.Delete([]byte("k"))
		}))
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			_, err := txn.Get([]byte("k"))
			require.ErrorIs(t, err, metastore.ErrKeyNotFound)
			return nil
		}))
	})

	t.Run("DeleteMissingSucceeds", func(t *testing.T) {
		s := opener(t)
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			return txn.Delete([]byte("never-existed"))
		}))
	})

	t.Run("ValueZeroCopyCallback", func(t *testing.T) {
		s := opener(t)
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			return txn.Set([]byte("k"), []byte("payload"))
		}))
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			item, err := txn.Get([]byte("k"))
			require.NoError(t, err)
			var got []byte
			require.NoError(t, item.Value(func(v []byte) error {
				got = append(got, v...) // must copy to retain
				return nil
			}))
			require.Equal(t, []byte("payload"), got)
			return nil
		}))
	})

	t.Run("ValueCallbackErrorPropagates", func(t *testing.T) {
		s := opener(t)
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			return txn.Set([]byte("k"), []byte("v"))
		}))
		sentinel := errors.New("decode failed")
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			item, err := txn.Get([]byte("k"))
			require.NoError(t, err)
			require.ErrorIs(t, item.Value(func([]byte) error { return sentinel }), sentinel)
			return nil
		}))
	})

	t.Run("PrefixScanAscendingByteOrder", func(t *testing.T) {
		s := opener(t)
		// Interleave prefixed and non-prefixed keys; insertion order != byte order.
		puts := [][2]string{
			{"p:b", "2"}, {"q:x", "X"}, {"p:a", "1"}, {"p:c", "3"}, {"o:z", "Z"},
		}
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			for _, kv := range puts {
				if err := txn.Set([]byte(kv[0]), []byte(kv[1])); err != nil {
					return err
				}
			}
			return nil
		}))
		var keys, vals []string
		prefix := []byte("p:")
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			it := txn.NewIterator(metastore.IteratorOptions{Prefix: prefix, PrefetchValues: true})
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				// Item.Key() is the zero-copy accessor keyspace.scanGroupPrefix
				// strips prefixes from (plan-gate F2) — it must agree with
				// KeyCopy and carry the FULL key (prefix included).
				require.Equal(t, item.KeyCopy(nil), append([]byte(nil), item.Key()...))
				keys = append(keys, string(item.KeyCopy(nil)))
				v, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				vals = append(vals, string(v))
			}
			return nil
		}))
		require.Equal(t, []string{"p:a", "p:b", "p:c"}, keys)
		require.Equal(t, []string{"1", "2", "3"}, vals)
	})

	t.Run("RewindAndValid", func(t *testing.T) {
		s := opener(t)
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			for _, k := range []string{"sgc:g1:a", "sgc:g1:b", "zzz"} {
				if err := txn.Set([]byte(k), []byte("v")); err != nil {
					return err
				}
			}
			return nil
		}))
		// segment_orphan_log.go pattern: Prefix option + Rewind + Valid.
		var got []string
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			it := txn.NewIterator(metastore.IteratorOptions{Prefix: []byte("sgc:g1:"), PrefetchValues: false})
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				got = append(got, string(it.Item().KeyCopy(nil)))
			}
			return nil
		}))
		require.Equal(t, []string{"sgc:g1:a", "sgc:g1:b"}, got)
	})

	t.Run("SeekMidRange", func(t *testing.T) {
		s := opener(t)
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			for _, k := range []string{"p:a", "p:b", "p:d"} {
				if err := txn.Set([]byte(k), []byte("v")); err != nil {
					return err
				}
			}
			return nil
		}))
		var got []string
		prefix := []byte("p:")
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			it := txn.NewIterator(metastore.IteratorOptions{Prefix: prefix})
			defer it.Close()
			// Seek to a key that does not exist: lands on the next key in order.
			for it.Seek([]byte("p:c")); it.ValidForPrefix(prefix); it.Next() {
				got = append(got, string(it.Item().KeyCopy(nil)))
			}
			return nil
		}))
		require.Equal(t, []string{"p:d"}, got)
	})

	t.Run("TwoGetsItemsIndependent", func(t *testing.T) {
		// badger contract: items returned by txn.Get stay valid until the txn
		// ends — a second Get must NOT invalidate or alias the first item.
		// (plan-gate F1: kills wrapper-reuse implementations.)
		s := opener(t)
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			if err := txn.Set([]byte("k1"), []byte("v1")); err != nil {
				return err
			}
			return txn.Set([]byte("k2"), []byte("v2"))
		}))
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			item1, err := txn.Get([]byte("k1"))
			require.NoError(t, err)
			item2, err := txn.Get([]byte("k2"))
			require.NoError(t, err)
			v1, err := item1.ValueCopy(nil)
			require.NoError(t, err)
			v2, err := item2.ValueCopy(nil)
			require.NoError(t, err)
			require.Equal(t, []byte("v1"), v1, "first item must survive a second Get")
			require.Equal(t, []byte("v2"), v2)
			require.Equal(t, []byte("k1"), item1.KeyCopy(nil))
			return nil
		}))
	})

	t.Run("ReadYourOwnWrites", func(t *testing.T) {
		s := opener(t)
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			if err := txn.Set([]byte("k"), []byte("new")); err != nil {
				return err
			}
			item, err := txn.Get([]byte("k"))
			if err != nil {
				return err
			}
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			require.Equal(t, []byte("new"), v)
			// Delete inside the same txn is also visible.
			if err := txn.Delete([]byte("k")); err != nil {
				return err
			}
			_, err = txn.Get([]byte("k"))
			require.ErrorIs(t, err, metastore.ErrKeyNotFound)
			return nil
		}))
	})

	t.Run("UpdateErrorDiscards", func(t *testing.T) {
		s := opener(t)
		boom := errors.New("boom")
		err := s.Update(func(txn metastore.Txn) error {
			if err := txn.Set([]byte("k"), []byte("v")); err != nil {
				return err
			}
			return boom
		})
		require.ErrorIs(t, err, boom)
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			_, gerr := txn.Get([]byte("k"))
			require.ErrorIs(t, gerr, metastore.ErrKeyNotFound)
			return nil
		}))
	})

	t.Run("ManualTxnCommit", func(t *testing.T) {
		s := opener(t)
		txn := s.NewTransaction(true)
		require.NoError(t, txn.Set([]byte("k"), []byte("v")))
		require.NoError(t, txn.Commit())
		txn.Discard() // no-op after commit (apply_actor defer pattern)
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			_, err := txn.Get([]byte("k"))
			return err
		}))
	})

	t.Run("ManualTxnDiscard", func(t *testing.T) {
		s := opener(t)
		txn := s.NewTransaction(true)
		require.NoError(t, txn.Set([]byte("k"), []byte("v")))
		txn.Discard()
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			_, err := txn.Get([]byte("k"))
			require.ErrorIs(t, err, metastore.ErrKeyNotFound)
			return nil
		}))
	})

	t.Run("SnapshotIsolationAcrossCommit", func(t *testing.T) {
		// badger transactions read from a snapshot taken at creation —
		// commits that land after the txn started must not become visible
		// mid-transaction. Cluster read paths (latest-pointer then object
		// meta) depend on this coherence. (code-gate finding 2.)
		s := opener(t)
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			return txn.Set([]byte("stable"), []byte("old"))
		}))
		ro := s.NewTransaction(false)
		defer ro.Discard()
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			if err := txn.Set([]byte("fresh"), []byte("new")); err != nil {
				return err
			}
			return txn.Set([]byte("stable"), []byte("mutated"))
		}))
		// The pre-existing txn still sees the world as of its creation.
		_, err := ro.Get([]byte("fresh"))
		require.ErrorIs(t, err, metastore.ErrKeyNotFound, "commit after txn start must be invisible")
		item, err := ro.Get([]byte("stable"))
		require.NoError(t, err)
		v, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("old"), v, "txn must read its snapshot, not the live state")
		it := ro.NewIterator(metastore.IteratorOptions{Prefix: []byte("fresh")})
		defer it.Close()
		it.Rewind()
		require.False(t, it.Valid(), "iterator must also be snapshot-scoped")
		// A transaction created after the commit sees the new state.
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			_, err := txn.Get([]byte("fresh"))
			return err
		}))
	})

	t.Run("FinishedTxnRejected", func(t *testing.T) {
		// badger semantics (verified against txn.go/iterator.go): a finished
		// txn returns ErrDiscardedTxn from Get/Set/Delete, a plain error from
		// Commit, and panics from NewIterator. MemStore must fail identically
		// so migrated tests cannot hide lifecycle bugs. (code-gate finding 3.)
		s := opener(t)
		txn := s.NewTransaction(true)
		// A pending write BEFORE Discard: badger's Commit short-circuits to
		// nil on an empty write set even when discarded, so only a non-empty
		// finished txn exercises the rejection path.
		require.NoError(t, txn.Set([]byte("pre"), []byte("v")))
		txn.Discard()
		_, err := txn.Get([]byte("k"))
		require.ErrorIs(t, err, metastore.ErrDiscardedTxn)
		require.ErrorIs(t, txn.Set([]byte("k"), []byte("v")), metastore.ErrDiscardedTxn)
		require.ErrorIs(t, txn.Delete([]byte("k")), metastore.ErrDiscardedTxn)
		require.Error(t, txn.Commit())
		require.Panics(t, func() { txn.NewIterator(metastore.IteratorOptions{}) })
		// The discarded write must never become visible.
		require.NoError(t, s.View(func(v metastore.Txn) error {
			_, gerr := v.Get([]byte("pre"))
			require.ErrorIs(t, gerr, metastore.ErrKeyNotFound)
			return nil
		}))
	})

	t.Run("DiscardIdempotent", func(t *testing.T) {
		// apply_actor's ErrTxnTooBig fallback discards, and tests discard
		// inside the commit seam — double Discard must be a no-op (badger is
		// idempotent; the contract now promises it). (S6.5-2 plan-gate C6.)
		s := opener(t)
		txn := s.NewTransaction(true)
		require.NoError(t, txn.Set([]byte("k"), []byte("v")))
		txn.Discard()
		txn.Discard() // second discard: no panic
		ro := s.NewTransaction(false)
		ro.Discard()
		ro.Discard()
	})

	t.Run("ReadOnlyTxnRejectsWrites", func(t *testing.T) {
		s := opener(t)
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			require.Error(t, txn.Set([]byte("k"), []byte("v")))
			require.Error(t, txn.Delete([]byte("k")))
			return nil
		}))
	})

	t.Run("DropPrefix", func(t *testing.T) {
		s := opener(t)
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			for _, k := range []string{"g1:a", "g1:b", "g2:a"} {
				if err := txn.Set([]byte(k), []byte("v")); err != nil {
					return err
				}
			}
			return nil
		}))
		require.NoError(t, s.DropPrefix([]byte("g1:")))
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			_, err := txn.Get([]byte("g1:a"))
			require.ErrorIs(t, err, metastore.ErrKeyNotFound)
			_, err = txn.Get([]byte("g1:b"))
			require.ErrorIs(t, err, metastore.ErrKeyNotFound)
			_, err = txn.Get([]byte("g2:a"))
			require.NoError(t, err)
			return nil
		}))
	})

	t.Run("ManyKeysScanComplete", func(t *testing.T) {
		s := opener(t)
		const n = 500
		require.NoError(t, s.Update(func(txn metastore.Txn) error {
			for i := 0; i < n; i++ {
				if err := txn.Set([]byte(fmt.Sprintf("m:%05d", i)), []byte{byte(i)}); err != nil {
					return err
				}
			}
			return nil
		}))
		count := 0
		prefix := []byte("m:")
		require.NoError(t, s.View(func(txn metastore.Txn) error {
			it := txn.NewIterator(metastore.IteratorOptions{Prefix: prefix})
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				count++
			}
			return nil
		}))
		require.Equal(t, n, count)
	})
}
