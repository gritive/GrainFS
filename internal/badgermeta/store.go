// Package badgermeta adapts a BadgerDB handle to the metastore.Store
// contract (Phase 6.5). It is a thin forwarding layer: zero-copy access
// patterns (Item.Value, iterator Item reuse) are preserved so the adapter
// adds interface dispatch only on the cluster metadata hot path.
package badgermeta

import (
	"errors"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/metastore"
)

// Store wraps a *badger.DB as a metastore.Store.
type Store struct {
	db *badger.DB
}

// Wrap adapts an already-open BadgerDB handle. The composition root that
// opened the DB keeps the raw handle for consumers outside the metastore
// contract (lifecycle/migration stores); this adapter deliberately exposes
// no escape hatch back to *badger.DB.
func Wrap(db *badger.DB) *Store { return &Store{db: db} }

func (s *Store) View(fn func(metastore.Txn) error) error {
	return s.db.View(func(btxn *badger.Txn) error {
		txn := txnWrapper{txn: btxn}
		return fn(&txn)
	})
}

func (s *Store) Update(fn func(metastore.Txn) error) error {
	return s.db.Update(func(btxn *badger.Txn) error {
		txn := txnWrapper{txn: btxn}
		return fn(&txn)
	})
}

func (s *Store) NewTransaction(update bool) metastore.Txn {
	return &txnWrapper{txn: s.db.NewTransaction(update)}
}

func (s *Store) DropPrefix(prefix []byte) error { return s.db.DropPrefix(prefix) }

func (s *Store) Close() error { return s.db.Close() }

// mapErr translates badger sentinels to metastore sentinels. Other errors
// pass through unchanged.
func mapErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, badger.ErrKeyNotFound):
		return metastore.ErrKeyNotFound
	case errors.Is(err, badger.ErrTxnTooBig):
		return metastore.ErrTxnTooBig
	default:
		return err
	}
}

type txnWrapper struct {
	txn *badger.Txn
}

// Get allocates a fresh itemWrapper per call. This is deliberate, NOT an
// optimization target: badger's txn.Get items are each independently valid
// until the txn ends (unlike iterator items, which rebind on advance), so a
// reused wrapper would silently alias an earlier Item to a later Get. badger
// itself allocates an Item per Get, so this adds no allocation beyond the
// underlying store. Pinned by the TwoGetsItemsIndependent conformance test.
func (t *txnWrapper) Get(key []byte) (metastore.Item, error) {
	bitem, err := t.txn.Get(key)
	if err != nil {
		return nil, mapErr(err)
	}
	return &itemWrapper{item: bitem}, nil
}

func (t *txnWrapper) Set(key, val []byte) error { return mapErr(t.txn.Set(key, val)) }

func (t *txnWrapper) Delete(key []byte) error { return mapErr(t.txn.Delete(key)) }

func (t *txnWrapper) NewIterator(opts metastore.IteratorOptions) metastore.Iterator {
	bopts := badger.DefaultIteratorOptions
	bopts.Prefix = opts.Prefix
	bopts.PrefetchValues = opts.PrefetchValues
	return &iteratorWrapper{it: t.txn.NewIterator(bopts)}
}

func (t *txnWrapper) Commit() error { return mapErr(t.txn.Commit()) }

func (t *txnWrapper) Discard() { t.txn.Discard() }

type itemWrapper struct {
	item *badger.Item
}

func (i *itemWrapper) Key() []byte { return i.item.Key() }

func (i *itemWrapper) KeyCopy(dst []byte) []byte { return i.item.KeyCopy(dst) }

func (i *itemWrapper) Value(fn func(val []byte) error) error { return i.item.Value(fn) }

func (i *itemWrapper) ValueCopy(dst []byte) ([]byte, error) { return i.item.ValueCopy(dst) }

// iteratorWrapper reuses a single itemWrapper across positions so Item()
// allocates nothing per element (hot-path guardrail; pinned by
// TestIteratorItemZeroAlloc). This reuse is safe ONLY for iterators: the
// badger iterator contract already rebinds its Item on advance.
type iteratorWrapper struct {
	it   *badger.Iterator
	item itemWrapper
}

func (w *iteratorWrapper) Rewind() { w.it.Rewind() }

func (w *iteratorWrapper) Seek(key []byte) { w.it.Seek(key) }

func (w *iteratorWrapper) Valid() bool { return w.it.Valid() }

func (w *iteratorWrapper) ValidForPrefix(prefix []byte) bool { return w.it.ValidForPrefix(prefix) }

func (w *iteratorWrapper) Next() { w.it.Next() }

func (w *iteratorWrapper) Item() metastore.Item {
	w.item.item = w.it.Item()
	return &w.item
}

func (w *iteratorWrapper) Close() { w.it.Close() }
