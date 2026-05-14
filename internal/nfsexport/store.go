package nfsexport

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	badger "github.com/dgraph-io/badger/v4"
)

const KeyPrefix = "__nfsexport_/"
const cleanupKeyPrefix = "__nfsexport_cleanup_/"
const allocatorKey = "__nfsexport_meta_/next_minor"

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

func (s *Snapshot) Entries() map[string]Config {
	if s == nil {
		return nil
	}
	return cloneSnapshotMap(s)
}

type Store struct {
	db   *badger.DB
	mu   sync.Mutex
	snap atomic.Pointer[Snapshot]
}

func OpenStore(db *badger.DB) (*Store, error) {
	s := &Store{db: db}
	byBucket := make(map[string]Config)
	var maxMinor uint64
	err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(KeyPrefix)})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			keyBucket := strings.TrimPrefix(string(item.Key()), KeyPrefix)
			if err := item.Value(func(v []byte) error {
				bucket, cfg, err := DecodeUpsertPayload(v)
				if err != nil {
					return err
				}
				if bucket != keyBucket {
					return fmt.Errorf("nfsexport: key/payload bucket mismatch: key=%q payload=%q", keyBucket, bucket)
				}
				if cfg.FsidMinor > maxMinor {
					maxMinor = cfg.FsidMinor
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
	if err := s.ensureAllocatorAtLeast(maxMinor); err != nil {
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
		if cfg.FsidMinor > 0 {
			cur, err := getAllocatorTxn(txn)
			if err != nil {
				return err
			}
			if cur < cfg.FsidMinor {
				if err := setAllocatorTxn(txn, cfg.FsidMinor); err != nil {
					return err
				}
			}
		}
		return txn.Set([]byte(KeyPrefix+bucket), payload)
	}); err != nil {
		return err
	}
	next := cloneSnapshotMap(s.Snapshot())
	next[bucket] = cfg
	s.snap.Store(newSnapshot(next))
	return nil
}

func (s *Store) ApplyUpsert(bucket string, readOnly bool, fsidMajor uint64) (Config, error) {
	if bucket == "" {
		return Config{}, fmt.Errorf("bucket is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	var cfg Config
	if err := s.db.Update(func(txn *badger.Txn) error {
		prev, exists, err := getConfigTxn(txn, bucket)
		if err != nil {
			return err
		}
		cfg = Config{
			ReadOnly:   readOnly,
			FsidMajor:  fsidMajor,
			FsidMinor:  prev.FsidMinor,
			Generation: prev.Generation + 1,
		}
		if !exists || cfg.FsidMinor == 0 {
			next, err := getAllocatorTxn(txn)
			if err != nil {
				return err
			}
			next++
			cfg.FsidMinor = next
			if err := setAllocatorTxn(txn, next); err != nil {
				return err
			}
		}
		payload, err := EncodeUpsertPayload(bucket, cfg)
		if err != nil {
			return err
		}
		return txn.Set([]byte(KeyPrefix+bucket), payload)
	}); err != nil {
		return Config{}, err
	}
	next := cloneSnapshotMap(s.Snapshot())
	next[bucket] = cfg
	s.snap.Store(newSnapshot(next))
	return cfg, nil
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

func (s *Store) GetAllocator() (uint64, error) {
	var out uint64
	err := s.db.View(func(txn *badger.Txn) error {
		v, err := getAllocatorTxn(txn)
		out = v
		return err
	})
	return out, err
}

func (s *Store) SetAllocator(v uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return setAllocatorTxn(txn, v)
	})
}

func (s *Store) ReplaceAll(rows map[string]Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := cloneSnapshotMap((*Snapshot)(nil))
	var maxMinor uint64
	if err := s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(KeyPrefix)})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		for bucket, cfg := range rows {
			payload, err := EncodeUpsertPayload(bucket, cfg)
			if err != nil {
				return err
			}
			if cfg.FsidMinor > maxMinor {
				maxMinor = cfg.FsidMinor
			}
			if err := txn.Set([]byte(KeyPrefix+bucket), payload); err != nil {
				return err
			}
			next[bucket] = cfg
		}
		return setAllocatorTxn(txn, maxMinor)
	}); err != nil {
		return err
	}
	s.snap.Store(newSnapshot(next))
	return nil
}

func (s *Store) MarkBucketDeleteCleanup(bucket string) error {
	if bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(cleanupKeyPrefix+bucket), []byte{1})
	})
}

func (s *Store) ClearBucketDeleteCleanup(bucket string) error {
	if bucket == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(cleanupKeyPrefix + bucket))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

func (s *Store) PendingBucketDeleteCleanups() ([]string, error) {
	var out []string
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Prefix: []byte(cleanupKeyPrefix)})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			bucket := strings.TrimPrefix(string(it.Item().Key()), cleanupKeyPrefix)
			if bucket != "" {
				out = append(out, bucket)
			}
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

func (s *Store) ensureAllocatorAtLeast(min uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Update(func(txn *badger.Txn) error {
		cur, err := getAllocatorTxn(txn)
		if err != nil {
			return err
		}
		if cur >= min {
			return nil
		}
		return setAllocatorTxn(txn, min)
	})
}

func getConfigTxn(txn *badger.Txn, bucket string) (Config, bool, error) {
	item, err := txn.Get([]byte(KeyPrefix + bucket))
	if err == badger.ErrKeyNotFound {
		return Config{}, false, nil
	}
	if err != nil {
		return Config{}, false, err
	}
	var cfg Config
	err = item.Value(func(v []byte) error {
		payloadBucket, decoded, err := DecodeUpsertPayload(v)
		if err != nil {
			return err
		}
		if payloadBucket != bucket {
			return fmt.Errorf("nfsexport: key/payload bucket mismatch: key=%q payload=%q", bucket, payloadBucket)
		}
		cfg = decoded
		return nil
	})
	return cfg, true, err
}

func getAllocatorTxn(txn *badger.Txn) (uint64, error) {
	item, err := txn.Get([]byte(allocatorKey))
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var out uint64
	err = item.Value(func(v []byte) error {
		if len(v) != 8 {
			return fmt.Errorf("nfsexport: invalid allocator length %d", len(v))
		}
		out = binary.BigEndian.Uint64(v)
		return nil
	})
	return out, err
}

func setAllocatorTxn(txn *badger.Txn, v uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return txn.Set([]byte(allocatorKey), buf[:])
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
