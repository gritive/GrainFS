package lifecycle

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

var lifecyclePrefix = []byte("lifecycle:")

// Store persists lifecycle configurations in BadgerDB.
// Key format: "lifecycle:{bucket}"
type Store struct {
	db *badger.DB
}

// NewStore creates a Store backed by the given BadgerDB instance.
func NewStore(db *badger.DB) *Store {
	return &Store{db: db}
}

func (s *Store) key(bucket string) []byte {
	return []byte("lifecycle:" + bucket)
}

// Get returns the lifecycle configuration for bucket, or nil if not set.
func (s *Store) Get(bucket string) (*LifecycleConfiguration, error) {
	var cfg LifecycleConfiguration
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.key(bucket))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return xml.Unmarshal(val, &cfg)
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// put stores cfg as the lifecycle configuration for bucket.
// Test-only: production writes go through PutRaw (FSM apply path).
func (s *Store) put(bucket string, cfg *LifecycleConfiguration) error {
	data, err := xml.Marshal(cfg)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.key(bucket), data)
	})
}

// PutRaw stores raw S3 wire XML bytes as the lifecycle configuration for
// bucket. Used by the meta-Raft FSM apply path so the operator's GET round-
// trip remains byte-for-byte. Callers must have validated the XML upstream.
func (s *Store) PutRaw(bucket string, raw []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.key(bucket), append([]byte(nil), raw...))
	})
}

// GetRaw returns the raw S3 wire XML bytes for bucket, or (nil, nil) if not
// set. Used by the S3 GET handler so the operator round-trip is byte-for-byte
// (ADR 0011).
func (s *Store) GetRaw(bucket string) ([]byte, error) {
	var out []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.key(bucket))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			out = append([]byte(nil), val...)
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Delete removes the lifecycle configuration for bucket (no-op if not set).
func (s *Store) Delete(bucket string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(s.key(bucket))
	})
}

// ListBuckets returns the names of all buckets that have a lifecycle
// configuration persisted locally, in Badger key order (lexical). Callers
// must not assume the list matches FSM-applied state on a follower.
func (s *Store) ListBuckets() ([]string, error) {
	var out []string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = lifecyclePrefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(lifecyclePrefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			out = append(out, string(bytes.TrimPrefix(key, lifecyclePrefix)))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("lifecycle: list buckets: %w", err)
	}
	return out, nil
}
