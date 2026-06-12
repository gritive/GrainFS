// Package metastore defines the metadata key-value store contract consumed by
// internal/cluster (Phase 6.5). The interface mirrors the subset of the
// BadgerDB transaction API that cluster actually uses, preserving its
// zero-copy access patterns (Item.Value(fn), ValueCopy(dst)) so that the
// abstraction adds interface dispatch only — no extra allocation or copying
// on the PUT/GET hot path.
//
// Implementations: internal/badgermeta (BadgerDB, production) and MemStore
// (in-memory, unit-test injection). Both must satisfy the semantics pinned by
// storetest.Run.
package metastore

import "errors"

// Sentinel errors. Implementations must return errors matching these via
// errors.Is. They live here (not in internal/cluster) so that adapters can
// return them without importing cluster — see
// docs/adr/2026-06-13-metastore-contract-package.md.
var (
	// ErrKeyNotFound is returned by Txn.Get when the key does not exist.
	ErrKeyNotFound = errors.New("metastore: key not found")
	// ErrTxnTooBig is returned by Txn.Set/Delete when the transaction has
	// grown past the store's batch limit. Callers split the batch and retry
	// (see cluster/apply_actor.go).
	ErrTxnTooBig = errors.New("metastore: txn too big")
)

// Store is the metadata store handle. View/Update run a function inside a
// read-only / read-write transaction respectively (commit on nil return,
// discard on error). NewTransaction hands out a manual transaction for
// callers that need ErrTxnTooBig batch-splitting (apply_actor).
type Store interface {
	View(fn func(Txn) error) error
	Update(fn func(Txn) error) error
	NewTransaction(update bool) Txn
	// DropPrefix deletes all keys with the given prefix (FSM.Restore).
	DropPrefix(prefix []byte) error
	Close() error
}

// Txn is a transaction. Get returns ErrKeyNotFound for missing keys.
// Delete of a missing key succeeds. Set/Delete on a read-only transaction
// return a non-nil error. Commit/Discard are for manual transactions
// (NewTransaction); Discard after Commit is a no-op.
//
// Items returned by Get are each independently valid until the transaction
// ends — a later Get must not invalidate an earlier Item (badger contract,
// pinned by the TwoGetsItemsIndependent conformance test).
type Txn interface {
	Get(key []byte) (Item, error)
	Set(key, val []byte) error
	Delete(key []byte) error
	NewIterator(opts IteratorOptions) Iterator
	Commit() error
	Discard()
}

// Item is a key-value pair handle. Key and the slice passed to Value's fn
// are only valid until the iterator advances or the transaction ends —
// callers must copy (KeyCopy/ValueCopy) to retain.
type Item interface {
	Key() []byte
	KeyCopy(dst []byte) []byte
	Value(fn func(val []byte) error) error
	ValueCopy(dst []byte) ([]byte, error)
}

// IteratorOptions controls iteration. Prefix restricts the key range;
// PrefetchValues hints bulk value loading (maps to badger's option; the
// in-memory store ignores it).
type IteratorOptions struct {
	Prefix         []byte
	PrefetchValues bool
}

// Iterator walks keys in ascending byte order. Item() is only valid while
// Valid() is true. Implementations may reuse the returned Item across
// positions (zero-alloc); callers must not retain it across Next().
type Iterator interface {
	Rewind()
	Seek(key []byte)
	Valid() bool
	ValidForPrefix(prefix []byte) bool
	Next()
	Item() Item
	Close()
}
