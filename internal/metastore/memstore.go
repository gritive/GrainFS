package metastore

import (
	"bytes"
	"errors"
	"sort"
	"strings"
	"sync"
)

// MemStore is an in-memory Store for unit tests. Locks are held per
// operation, never for a transaction's lifetime, so nested transactions
// (View inside Update etc.) cannot deadlock. Write transactions buffer into
// a pending overlay and apply on Commit under a short write lock — other
// transactions never observe uncommitted writes (matching badger). The one
// semantic this gives up is cross-operation snapshot isolation inside a
// read transaction, which the conformance suite does not require and the
// single-writer FSM apply actor cannot observe.
type MemStore struct {
	mu   sync.RWMutex
	data map[string][]byte
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
	t := &memTxn{store: s, update: update}
	if update {
		t.pending = make(map[string]memPending)
	}
	return t
}

func (s *MemStore) DropPrefix(prefix []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.data {
		if strings.HasPrefix(k, string(prefix)) {
			delete(s.data, k)
		}
	}
	return nil
}

func (s *MemStore) Close() error { return nil }

// memPending is a buffered write: value for Set, deleted=true for Delete.
type memPending struct {
	value   []byte
	deleted bool
}

type memTxn struct {
	store   *MemStore
	update  bool
	pending map[string]memPending
	done    bool
}

func (t *memTxn) Get(key []byte) (Item, error) {
	k := string(key)
	if t.update {
		if p, ok := t.pending[k]; ok {
			if p.deleted {
				return nil, ErrKeyNotFound
			}
			return &memItem{key: append([]byte(nil), key...), val: p.value}, nil
		}
	}
	t.store.mu.RLock()
	v, ok := t.store.data[k]
	t.store.mu.RUnlock()
	if !ok {
		return nil, ErrKeyNotFound
	}
	// v is safe to hold after RUnlock: committed value slices are never
	// mutated in place (Set stores a copy, Commit swaps map entries).
	return &memItem{key: append([]byte(nil), key...), val: v}, nil
}

func (t *memTxn) Set(key, val []byte) error {
	if !t.update {
		return errMemReadOnlyTxn
	}
	t.pending[string(key)] = memPending{value: append([]byte(nil), val...)}
	return nil
}

func (t *memTxn) Delete(key []byte) error {
	if !t.update {
		return errMemReadOnlyTxn
	}
	t.pending[string(key)] = memPending{deleted: true}
	return nil
}

func (t *memTxn) NewIterator(opts IteratorOptions) Iterator {
	// Snapshot the merged (committed ∪ pending) view at iterator creation,
	// under a short read lock.
	t.store.mu.RLock()
	merged := make(map[string][]byte, len(t.store.data))
	for k, v := range t.store.data {
		merged[k] = v
	}
	t.store.mu.RUnlock()
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
	if t.done {
		return nil
	}
	t.done = true
	if !t.update {
		return nil
	}
	t.store.mu.Lock()
	for k, p := range t.pending {
		if p.deleted {
			delete(t.store.data, k)
		} else {
			t.store.data[k] = p.value
		}
	}
	t.store.mu.Unlock()
	return nil
}

func (t *memTxn) Discard() { t.done = true }

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
