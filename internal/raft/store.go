package raft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// LogStore provides durable storage for Raft log entries and state.
type LogStore interface {
	// AppendEntries persists log entries starting at the given index.
	AppendEntries(entries []LogEntry) error

	// GetEntry returns a single log entry by index.
	GetEntry(index uint64) (*LogEntry, error)

	// GetEntries returns entries in [lo, hi] inclusive.
	GetEntries(lo, hi uint64) ([]LogEntry, error)

	// LastIndex returns the index of the last log entry (0 if empty).
	LastIndex() (uint64, error)

	// TruncateAfter removes all entries with index > afterIndex.
	TruncateAfter(afterIndex uint64) error

	// SaveState persists currentTerm and votedFor atomically.
	SaveState(term uint64, votedFor string) error

	// LoadState loads the last persisted term and votedFor.
	LoadState() (term uint64, votedFor string, err error)

	// SaveSnapshot stores a snapshot at the given index/term.
	SaveSnapshot(index, term uint64, data []byte) error

	// LoadSnapshot loads the latest snapshot.
	LoadSnapshot() (index, term uint64, data []byte, err error)

	// Close closes the store.
	Close() error
}

// key prefixes for BadgerDB storage.
var (
	prefixLog      = []byte("raft:log:")
	keyState       = []byte("raft:state")
	keySnapshot    = []byte("raft:snapshot")
	keySnapshotMeta = []byte("raft:snapshot:meta")
)

// BadgerLogStore implements LogStore using BadgerDB.
type BadgerLogStore struct {
	db *badger.DB
}

// NewBadgerLogStore creates a new log store backed by BadgerDB.
func NewBadgerLogStore(path string) (*BadgerLogStore, error) {
	opts := badger.DefaultOptions(path).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger log store: %w", err)
	}
	return &BadgerLogStore{db: db}, nil
}

func logKey(index uint64) []byte {
	key := make([]byte, len(prefixLog)+8)
	copy(key, prefixLog)
	binary.BigEndian.PutUint64(key[len(prefixLog):], index)
	return key
}

func (s *BadgerLogStore) AppendEntries(entries []LogEntry) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, entry := range entries {
			data, err := json.Marshal(entry)
			if err != nil {
				return fmt.Errorf("marshal entry %d: %w", entry.Index, err)
			}
			if err := txn.Set(logKey(entry.Index), data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BadgerLogStore) GetEntry(index uint64) (*LogEntry, error) {
	var entry LogEntry
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(logKey(index))
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("entry %d not found", index)
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &entry)
		})
	})
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

func (s *BadgerLogStore) GetEntries(lo, hi uint64) ([]LogEntry, error) {
	var entries []LogEntry
	err := s.db.View(func(txn *badger.Txn) error {
		for idx := lo; idx <= hi; idx++ {
			item, err := txn.Get(logKey(idx))
			if err == badger.ErrKeyNotFound {
				break
			}
			if err != nil {
				return err
			}
			var entry LogEntry
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &entry)
			}); err != nil {
				return err
			}
			entries = append(entries, entry)
		}
		return nil
	})
	return entries, err
}

func (s *BadgerLogStore) LastIndex() (uint64, error) {
	var lastIdx uint64
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		opts.Prefix = prefixLog
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek to a key past the prefix range
		seekKey := make([]byte, len(prefixLog)+8)
		copy(seekKey, prefixLog)
		for i := len(prefixLog); i < len(seekKey); i++ {
			seekKey[i] = 0xFF
		}
		it.Seek(seekKey)

		if it.ValidForPrefix(prefixLog) {
			key := it.Item().Key()
			lastIdx = binary.BigEndian.Uint64(key[len(prefixLog):])
		}
		return nil
	})
	return lastIdx, err
}

func (s *BadgerLogStore) TruncateAfter(afterIndex uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixLog
		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := logKey(afterIndex + 1)
		for it.Seek(startKey); it.ValidForPrefix(prefixLog); it.Next() {
			if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
				return err
			}
		}
		return nil
	})
}

type raftState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"votedFor"`
}

func (s *BadgerLogStore) SaveState(term uint64, votedFor string) error {
	data, err := json.Marshal(raftState{Term: term, VotedFor: votedFor})
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyState, data)
	})
}

func (s *BadgerLogStore) LoadState() (uint64, string, error) {
	var st raftState
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyState)
		if err == badger.ErrKeyNotFound {
			return nil // fresh node
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &st)
		})
	})
	return st.Term, st.VotedFor, err
}

type snapshotMeta struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
}

func (s *BadgerLogStore) SaveSnapshot(index, term uint64, data []byte) error {
	meta, err := json.Marshal(snapshotMeta{Index: index, Term: term})
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(keySnapshotMeta, meta); err != nil {
			return err
		}
		return txn.Set(keySnapshot, data)
	})
}

func (s *BadgerLogStore) LoadSnapshot() (uint64, uint64, []byte, error) {
	var meta snapshotMeta
	var data []byte

	err := s.db.View(func(txn *badger.Txn) error {
		// Load meta
		metaItem, err := txn.Get(keySnapshotMeta)
		if err == badger.ErrKeyNotFound {
			return nil // no snapshot
		}
		if err != nil {
			return err
		}
		if err := metaItem.Value(func(val []byte) error {
			return json.Unmarshal(val, &meta)
		}); err != nil {
			return err
		}

		// Load data
		dataItem, err := txn.Get(keySnapshot)
		if err != nil {
			return err
		}
		return dataItem.Value(func(val []byte) error {
			data = make([]byte, len(val))
			copy(data, val)
			return nil
		})
	})
	return meta.Index, meta.Term, data, err
}

func (s *BadgerLogStore) Close() error {
	return s.db.Close()
}
