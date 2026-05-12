package migration

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

var (
	jobPrefix    = []byte("migration:job:")
	cursorPrefix = []byte("migration:cursor:")
)

// JobStore persists migration job state and per-bucket cursors in BadgerDB.
type JobStore struct {
	db *badger.DB
}

// NewJobStore creates a JobStore backed by the given BadgerDB instance.
func NewJobStore(db *badger.DB) *JobStore {
	return &JobStore{db: db}
}

func (s *JobStore) jobKey(bucket string) []byte {
	return append(append([]byte{}, jobPrefix...), []byte(bucket)...)
}

func (s *JobStore) cursorKey(bucket string) []byte {
	return append(append([]byte{}, cursorPrefix...), []byte(bucket)...)
}

// GetCursor returns the last saved S3 pagination cursor for bucket, or "" if none.
func (s *JobStore) GetCursor(bucket string) (string, error) {
	var out string
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.cursorKey(bucket))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			out = string(val)
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return "", nil
	}
	return out, err
}

// SaveCursor writes cursor directly to BadgerDB (bypasses Raft).
func (s *JobStore) SaveCursor(bucket, cursor string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.cursorKey(bucket), []byte(cursor))
	})
}

// GetJob returns the JobState for bucket, or (nil, nil) if not found.
func (s *JobStore) GetJob(bucket string) (*JobState, error) {
	var state JobState
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.jobKey(bucket))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &state)
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &state, nil
}

// SaveJob writes state to BadgerDB.
func (s *JobStore) SaveJob(state *JobState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("migration: marshal job state: %w", err)
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.jobKey(state.Bucket), data)
	})
}

// ListJobs returns all job records with the given status.
func (s *JobStore) ListJobs(status JobStatus) ([]*JobState, error) {
	var out []*JobState
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = jobPrefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(jobPrefix); it.Next() {
			var state JobState
			if err := it.Item().Value(func(val []byte) error {
				return json.Unmarshal(val, &state)
			}); err != nil {
				return err
			}
			if state.Status == status {
				cp := state
				out = append(out, &cp)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("migration: list jobs: %w", err)
	}
	return out, nil
}

// DeleteJob removes the job state and cursor for bucket. No-op if not found.
func (s *JobStore) DeleteJob(bucket string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		_ = txn.Delete(s.jobKey(bucket))
		_ = txn.Delete(s.cursorKey(bucket))
		return nil
	})
}

// ListBuckets returns all bucket names that have a job record.
func (s *JobStore) ListBuckets() ([]string, error) {
	var out []string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = jobPrefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(jobPrefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			out = append(out, string(bytes.TrimPrefix(key, jobPrefix)))
		}
		return nil
	})
	return out, err
}
