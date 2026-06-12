package metastore

import (
	"bytes"
	"errors"
	"sort"
	"strings"
	"sync"
)

// MemStore is an in-memory Store for unit tests, built on copy-on-write
// snapshots: every transaction captures an immutable map reference at
// creation (snapshot isolation, matching badger), and Commit publishes a
// fresh copy under a short lock. No lock is ever held for a transaction's
// lifetime, so nested transactions (View inside Update etc.) cannot
// deadlock. Unlike badger there is no conflict detection: concurrent write
// transactions are last-writer-wins per key — unobservable under the
// single-writer FSM apply actor the cluster uses.
type MemStore struct {
	mu   sync.RWMutex
	data map[string][]byte // immutable once published; replaced wholesale on commit
}

// NewMemStore returns an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{data: make(map[string][]byte)}
}

var errMemReadOnlyTxn = errors.New("metastore: read-only transaction")

// safeCopy mirrors badger's y.SafeCopy byte-for-byte: overwrite dst from the
// start (reusing capacity) and normalize a nil result to an empty slice.
func safeCopy(dst, src []byte) []byte {
	b := append(dst[:0], src...)
	if b == nil {
		return []byte{}
	}
	return b
}

func (s *MemStore) View(fn func(Txn) error) error {
	txn := s.NewTransaction(false)
	defer txn.Discard()
	return fn(txn)
}

func (s *MemStore) Update(fn func(Txn) error) error {
	txn := s.NewTransaction(true)
	defer txn.Discard()
	if err := fn(txn); err != nil {
		return err
	}
	return txn.Commit()
}

func (s *MemStore) NewTransaction(update bool) Txn {
	t := &memTxn{store: s, update: update, snap: s.snapshot()}
	if update {
		t.pending = make(map[string]memPending)
	}
	return t
}

// snapshot returns the current committed map. Published maps are never
// mutated in place, so holding the reference is a stable point-in-time view.
func (s *MemStore) snapshot() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data
}

// publish replaces the committed map with a copy transformed by apply.
func (s *MemStore) publish(apply func(next map[string][]byte)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		next[k] = v
	}
	apply(next)
	s.data = next
}

func (s *MemStore) DropPrefix(prefix []byte) error {
	s.publish(func(next map[string][]byte) {
		for k := range next {
			if strings.HasPrefix(k, string(prefix)) {
				delete(next, k)
			}
		}
	})
	return nil
}

func (s *MemStore) Close() error { return nil }

// memPending is a buffered write: value for Set, deleted=true for Delete.
type memPending struct {
	value   []byte
	deleted bool
}

type txnState uint8

const (
	txnActive txnState = iota
	txnCommitted
	txnDiscarded
)

type memTxn struct {
	store   *MemStore
	update  bool
	snap    map[string][]byte // immutable snapshot captured at creation
	pending map[string]memPending
	state   txnState
}

func (t *memTxn) Get(key []byte) (Item, error) {
	if t.state != txnActive {
		return nil, ErrDiscardedTxn
	}
	k := string(key)
	if t.update {
		if p, ok := t.pending[k]; ok {
			if p.deleted {
				return nil, ErrKeyNotFound
			}
			return &memItem{key: append([]byte(nil), key...), val: p.value}, nil
		}
	}
	v, ok := t.snap[k]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return &memItem{key: append([]byte(nil), key...), val: v}, nil
}

func (t *memTxn) Set(key, val []byte) error {
	switch {
	case !t.update:
		return errMemReadOnlyTxn // badger checks read-only before discarded
	case t.state != txnActive:
		return ErrDiscardedTxn
	}
	t.pending[string(key)] = memPending{value: append([]byte(nil), val...)}
	return nil
}

func (t *memTxn) Delete(key []byte) error {
	switch {
	case !t.update:
		return errMemReadOnlyTxn
	case t.state != txnActive:
		return ErrDiscardedTxn
	}
	t.pending[string(key)] = memPending{deleted: true}
	return nil
}

func (t *memTxn) NewIterator(opts IteratorOptions) Iterator {
	if t.state != txnActive {
		panic(ErrDiscardedTxn) // badger panics here, not an error return
	}
	// Merge the immutable snapshot with this txn's pending overlay; no store
	// lock needed (the snapshot is never mutated after publication).
	merged := make(map[string][]byte, len(t.snap))
	for k, v := range t.snap {
		merged[k] = v
	}
	if t.update {
		for k, p := range t.pending {
			if p.deleted {
				delete(merged, k)
			} else {
				merged[k] = p.value
			}
		}
	}
	prefix := string(opts.Prefix)
	keys := make([]string, 0, len(merged))
	for k := range merged {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return &memIterator{keys: keys, data: merged}
}

func (t *memTxn) Commit() error {
	// badger: an empty commit short-circuits to Discard()+nil BEFORE the
	// discarded check; only a commit with pending writes on a finished txn
	// errors.
	if len(t.pending) == 0 {
		t.Discard()
		return nil
	}
	if t.state != txnActive {
		return errors.New("metastore: trying to commit a discarded txn")
	}
	t.state = txnCommitted
	// Applied against the LIVE map (not the snapshot): concurrent committers
	// are last-writer-wins per key — no conflict detection by contract.
	t.store.publish(func(next map[string][]byte) {
		for k, p := range t.pending {
			if p.deleted {
				delete(next, k)
			} else {
				next[k] = p.value
			}
		}
	})
	return nil
}

func (t *memTxn) Discard() {
	if t.state == txnActive {
		t.state = txnDiscarded
	}
}

type memItem struct {
	key []byte
	val []byte
}

func (i *memItem) Key() []byte { return i.key }

func (i *memItem) KeyCopy(dst []byte) []byte { return safeCopy(dst, i.key) }

func (i *memItem) Value(fn func(val []byte) error) error { return fn(i.val) }

func (i *memItem) ValueCopy(dst []byte) ([]byte, error) { return safeCopy(dst, i.val), nil }

type memIterator struct {
	keys []string
	data map[string][]byte
	idx  int
	item memItem
}

func (it *memIterator) Rewind() { it.idx = 0 }

func (it *memIterator) Seek(key []byte) {
	it.idx = sort.SearchStrings(it.keys, string(key))
}

func (it *memIterator) Valid() bool { return it.idx < len(it.keys) }

func (it *memIterator) ValidForPrefix(prefix []byte) bool {
	return it.Valid() && bytes.HasPrefix([]byte(it.keys[it.idx]), prefix)
}

func (it *memIterator) Next() { it.idx++ }

func (it *memIterator) Item() Item {
	k := it.keys[it.idx]
	it.item.key = []byte(k)
	it.item.val = it.data[k]
	return &it.item
}

func (it *memIterator) Close() {}
