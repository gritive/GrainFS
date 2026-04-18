package lifecycle

import (
	"encoding/xml"
	"errors"

	badger "github.com/dgraph-io/badger/v4"
)

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

// Put stores cfg as the lifecycle configuration for bucket.
func (s *Store) Put(bucket string, cfg *LifecycleConfiguration) error {
	data, err := xml.Marshal(cfg)
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.key(bucket), data)
	})
}

// Delete removes the lifecycle configuration for bucket (no-op if not set).
func (s *Store) Delete(bucket string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(s.key(bucket))
	})
}
