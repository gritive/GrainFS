package nfsexport

import (
	"sort"
	"sync"
	"sync/atomic"

	badger "github.com/dgraph-io/badger/v4"
)

const KeyPrefix = "__nfsexport_/"

type Snapshot struct {
	byBucket map[string]Config
	names    []string
}

func (s *Snapshot) Get(bucket string) (Config, bool) {
	if s == nil {
		return Config{}, false
	}
	cfg, ok := s.byBucket[bucket]
	return cfg, ok
}

func (s *Snapshot) SortedNames() []string {
	if s == nil {
		return nil
	}
	return append([]string(nil), s.names...)
}

type Store struct {
	db   *badger.DB
	mu   sync.Mutex
	snap atomic.Pointer[Snapshot]
}

func OpenStore(db *badger.DB) (*Store, error) {
	s := &Store{db: db}
	byBucket := make(map[string]Config)
	err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(KeyPrefix)})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if err := item.Value(func(v []byte) error {
				bucket, cfg, err := DecodeUpsertPayload(v)
				if err != nil {
					return err
				}
				byBucket[bucket] = cfg
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.snap.Store(newSnapshot(byBucket))
	return s, nil
}

func (s *Store) Snapshot() *Snapshot {
	return s.snap.Load()
}

func (s *Store) Get(bucket string) (Config, bool) {
	return s.Snapshot().Get(bucket)
}

func (s *Store) Put(bucket string, cfg Config) error {
	payload, err := EncodeUpsertPayload(bucket, cfg)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(KeyPrefix+bucket), payload)
	}); err != nil {
		return err
	}
	next := cloneSnapshotMap(s.Snapshot())
	next[bucket] = cfg
	s.snap.Store(newSnapshot(next))
	return nil
}

func (s *Store) Delete(bucket string) error {
	if bucket == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(KeyPrefix + bucket))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	}); err != nil {
		return err
	}
	next := cloneSnapshotMap(s.Snapshot())
	delete(next, bucket)
	s.snap.Store(newSnapshot(next))
	return nil
}

func newSnapshot(byBucket map[string]Config) *Snapshot {
	names := make([]string, 0, len(byBucket))
	for name := range byBucket {
		names = append(names, name)
	}
	sort.Strings(names)
	copied := make(map[string]Config, len(byBucket))
	for name, cfg := range byBucket {
		copied[name] = cfg
	}
	return &Snapshot{byBucket: copied, names: names}
}

func cloneSnapshotMap(s *Snapshot) map[string]Config {
	out := make(map[string]Config)
	if s == nil {
		return out
	}
	for name, cfg := range s.byBucket {
		out[name] = cfg
	}
	return out
}
