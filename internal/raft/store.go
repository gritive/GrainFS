package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dgraph-io/badger/v4"

	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
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

	// TruncateBefore removes all entries with index < beforeIndex.
	// Used for Raft log GC: callers must only pass a quorum-safe watermark.
	TruncateBefore(beforeIndex uint64) error

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
	prefixLog       = []byte("raft:log:")
	keyState        = []byte("raft:state")
	keySnapshot     = []byte("raft:snapshot")
	keySnapshotMeta = []byte("raft:snapshot:meta")
	keyManagedMode  = []byte("raft:meta:managed")
)

// BadgerLogStoreOption configures a BadgerLogStore.
type BadgerLogStoreOption func(*BadgerLogStore)

// WithManagedMode enables Raft log GC mode. The managed-mode flag is
// persisted in the DB; reopening with a different setting returns an error.
func WithManagedMode() BadgerLogStoreOption {
	return func(s *BadgerLogStore) { s.managedMode = true }
}

// BadgerLogStore implements LogStore using BadgerDB.
type BadgerLogStore struct {
	db          *badger.DB
	managedMode bool
}

// IsManagedMode reports whether this store was opened with managed mode.
func (s *BadgerLogStore) IsManagedMode() bool { return s.managedMode }

// NewBadgerLogStore creates a new log store backed by BadgerDB.
func NewBadgerLogStore(path string, opts ...BadgerLogStoreOption) (*BadgerLogStore, error) {
	s := &BadgerLogStore{}
	for _, opt := range opts {
		opt(s)
	}
	dbOpts := badger.DefaultOptions(path).WithLogger(nil).WithSyncWrites(true)
	db, err := badger.Open(dbOpts)
	if err != nil {
		return nil, fmt.Errorf("open badger log store: %w", err)
	}
	s.db = db
	if err := s.checkManagedMode(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// checkManagedMode writes (first open) or verifies (subsequent opens) the
// managed-mode flag persisted in the DB. Mismatch returns a clear error.
func (s *BadgerLogStore) checkManagedMode() error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(keyManagedMode)
		if err != nil {
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			// First open: record the chosen mode.
			val := "false"
			if s.managedMode {
				val = "true"
			}
			return txn.Set(keyManagedMode, []byte(val))
		}
		return item.Value(func(val []byte) error {
			stored := string(val) == "true"
			if stored == s.managedMode {
				return nil
			}
			if stored {
				return fmt.Errorf("data dir opened in managed=true; " +
					"use --badger-managed-mode or start fresh")
			}
			return fmt.Errorf("data dir opened in non-managed mode; " +
				"remove --badger-managed-mode to continue non-managed, " +
				"or wipe data/raft/ and restart to enable managed mode")
		})
	})
}

func logKey(index uint64) []byte {
	key := make([]byte, len(prefixLog)+8)
	copy(key, prefixLog)
	binary.BigEndian.PutUint64(key[len(prefixLog):], index)
	return key
}

func marshalLogEntry(entry LogEntry) []byte {
	b := flatbuffers.NewBuilder(64)
	var cmdOff flatbuffers.UOffsetT
	if len(entry.Command) > 0 {
		cmdOff = b.CreateByteVector(entry.Command)
	}
	pb.LogEntryStart(b)
	pb.LogEntryAddTerm(b, entry.Term)
	pb.LogEntryAddIndex(b, entry.Index)
	if len(entry.Command) > 0 {
		pb.LogEntryAddCommand(b, cmdOff)
	}
	root := pb.LogEntryEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

func unmarshalLogEntry(data []byte) LogEntry {
	e := pb.GetRootAsLogEntry(data, 0)
	return LogEntry{Term: e.Term(), Index: e.Index(), Command: e.CommandBytes()}
}

func (s *BadgerLogStore) AppendEntries(entries []LogEntry) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, entry := range entries {
			data := marshalLogEntry(entry)
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
			entry = unmarshalLogEntry(val)
			return nil
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
				entry = unmarshalLogEntry(val)
				return nil
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

func (s *BadgerLogStore) TruncateBefore(beforeIndex uint64) error {
	if beforeIndex == 0 {
		return nil
	}
	const batchSize = 1000
	endKey := logKey(beforeIndex)
	for {
		done := false
		err := s.db.Update(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefixLog
			it := txn.NewIterator(opts)
			defer it.Close()

			count := 0
			for it.Seek(prefixLog); it.ValidForPrefix(prefixLog); it.Next() {
				key := it.Item().Key()
				if bytes.Compare(key, endKey) >= 0 {
					done = true
					break
				}
				if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
					return err
				}
				count++
				if count >= batchSize {
					break
				}
			}
			if count < batchSize {
				done = true
			}
			return nil
		})
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func (s *BadgerLogStore) SaveState(term uint64, votedFor string) error {
	b := flatbuffers.NewBuilder(32)
	votedForOff := b.CreateString(votedFor)
	pb.RaftStateStart(b)
	pb.RaftStateAddTerm(b, term)
	pb.RaftStateAddVotedFor(b, votedForOff)
	root := pb.RaftStateEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	data := make([]byte, len(raw))
	copy(data, raw)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyState, data)
	})
}

func (s *BadgerLogStore) LoadState() (uint64, string, error) {
	var term uint64
	var votedFor string
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyState)
		if err == badger.ErrKeyNotFound {
			return nil // fresh node
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			st := pb.GetRootAsRaftState(val, 0)
			term = st.Term()
			votedFor = string(st.VotedFor())
			return nil
		})
	})
	return term, votedFor, err
}

func (s *BadgerLogStore) SaveSnapshot(index, term uint64, data []byte) error {
	b := flatbuffers.NewBuilder(16)
	pb.SnapshotMetaStart(b)
	pb.SnapshotMetaAddIndex(b, index)
	pb.SnapshotMetaAddTerm(b, term)
	root := pb.SnapshotMetaEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	meta := make([]byte, len(raw))
	copy(meta, raw)
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(keySnapshotMeta, meta); err != nil {
			return err
		}
		return txn.Set(keySnapshot, data)
	})
}

func (s *BadgerLogStore) LoadSnapshot() (uint64, uint64, []byte, error) {
	var index, term uint64
	var data []byte

	err := s.db.View(func(txn *badger.Txn) error {
		metaItem, err := txn.Get(keySnapshotMeta)
		if err == badger.ErrKeyNotFound {
			return nil // no snapshot
		}
		if err != nil {
			return err
		}
		if err := metaItem.Value(func(val []byte) error {
			m := pb.GetRootAsSnapshotMeta(val, 0)
			index = m.Index()
			term = m.Term()
			return nil
		}); err != nil {
			return err
		}

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
	return index, term, data, err
}

func (s *BadgerLogStore) Close() error {
	return s.db.Close()
}
