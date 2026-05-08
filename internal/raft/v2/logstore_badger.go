package raftv2

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"

	badger "github.com/dgraph-io/badger/v4"
)

// badgerLogStore is a durable LogStore backed by BadgerDB. It is owned
// exclusively by the actor goroutine; all methods must be called from that
// goroutine. The one exception is LastIndex(), which reads a cached atomic.Uint64
// and is safe to call from any goroutine — this is defense-in-depth for
// observers that want the latest index without a DB round-trip.
//
// Key format: keyPrefix || be64(idx), where be64 is big-endian uint64.
// Big-endian preserves numeric order under Badger's lexicographic iteration.
//
// Value format (all fields fixed-width, big-endian):
//
//	[Term: 8 bytes][Index: 8 bytes][Type: 1 byte][CommandLen: 4 bytes][Command: CommandLen bytes]
//
// Total fixed overhead: 21 bytes. This format is internal-only and versioned
// implicitly by the package; no on-disk migration support is required in PR 10
// (ephemeral M2 testing environment).
type badgerLogStore struct {
	db        *badger.DB
	keyPrefix []byte // copied on construction; immutable after that
	lastIdx   atomic.Uint64
}

const entryHeaderSize = 8 + 8 + 1 + 4 // Term + Index + Type + CommandLen

// newBadgerLogStore opens a LogStore view into db under the given prefix.
// Multiple Raft groups can coexist in one Badger DB by using distinct prefixes
// (e.g. []byte("raft/v2/log/groupA/")).
//
// On open, the store scans the prefix with a reverse iterator to find the
// maximum stored index in O(1) key reads and populates lastIdx.
func newBadgerLogStore(db *badger.DB, prefix []byte) (*badgerLogStore, error) {
	// Copy prefix so callers cannot corrupt keys by mutating their slice later.
	p := make([]byte, len(prefix))
	copy(p, prefix)

	s := &badgerLogStore{db: db, keyPrefix: p}

	// Find the last index by seeking to the end of the prefix range and
	// validate that the highest key's value decodes — defense against
	// prefix collision with foreign keys (different LogStore prefix that
	// happens to share a leading subsequence, or non-LogStore data sharing
	// the keyspace). A decode failure on the highest key means the prefix
	// is unsafe; fail loudly rather than seed lastIdx with garbage.
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek past the prefix's last possible key: prefix || 0xFF...0xFF.
		seekKey := make([]byte, len(p)+8)
		copy(seekKey, p)
		for i := len(p); i < len(seekKey); i++ {
			seekKey[i] = 0xFF
		}
		it.Seek(seekKey)
		if !it.ValidForPrefix(p) {
			return nil
		}
		key := it.Item().Key()
		if len(key) < len(p)+8 {
			return fmt.Errorf("highest key under prefix is shorter than expected (%d bytes)", len(key))
		}
		idx := binary.BigEndian.Uint64(key[len(p):])
		// Validate the value decodes and the encoded Index matches the key.
		if err := it.Item().Value(func(val []byte) error {
			e, err := decodeEntry(val)
			if err != nil {
				return fmt.Errorf("decode highest entry: %w", err)
			}
			if e.Index != idx {
				return fmt.Errorf("encoded Index (%d) disagrees with key (%d) — possible prefix collision", e.Index, idx)
			}
			return nil
		}); err != nil {
			return err
		}
		s.lastIdx.Store(idx)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("badgerLogStore: open scan: %w", err)
	}
	return s, nil
}

// makeKey encodes an index into a key under the store's prefix.
func (s *badgerLogStore) makeKey(idx uint64) []byte {
	key := make([]byte, len(s.keyPrefix)+8)
	copy(key, s.keyPrefix)
	binary.BigEndian.PutUint64(key[len(s.keyPrefix):], idx)
	return key
}

// maxCommandSize is the largest LogEntry.Command this encoding can express.
// CommandLen is a 4-byte field; values >= 2^32 would silently truncate.
const maxCommandSize = math.MaxUint32

// errCommandTooLarge is returned when an entry's Command exceeds the encoding
// limit. Surfaced through Append rather than panicking — the proposer is the
// caller of last resort and should be free to reject oversized payloads
// without crashing the actor.
var errCommandTooLarge = fmt.Errorf("badgerLogStore: Command exceeds %d bytes", maxCommandSize)

// encodeEntry serialises a LogEntry into the compact binary format.
// Returns errCommandTooLarge if Command exceeds the 4-byte length field's range.
func encodeEntry(e LogEntry) ([]byte, error) {
	cmdLen := len(e.Command)
	if cmdLen > maxCommandSize {
		return nil, fmt.Errorf("%w (got %d)", errCommandTooLarge, cmdLen)
	}
	buf := make([]byte, entryHeaderSize+cmdLen)
	binary.BigEndian.PutUint64(buf[0:], e.Term)
	binary.BigEndian.PutUint64(buf[8:], e.Index)
	buf[16] = byte(e.Type)
	binary.BigEndian.PutUint32(buf[17:], uint32(cmdLen))
	if cmdLen > 0 {
		copy(buf[entryHeaderSize:], e.Command)
	}
	return buf, nil
}

// decodeEntry deserialises a LogEntry from the compact binary format.
// It always allocates a fresh Command slice so callers may retain the
// entry beyond the Badger item callback lifetime.
func decodeEntry(val []byte) (LogEntry, error) {
	if len(val) < entryHeaderSize {
		return LogEntry{}, fmt.Errorf("badgerLogStore: short value (%d bytes)", len(val))
	}
	term := binary.BigEndian.Uint64(val[0:])
	idx := binary.BigEndian.Uint64(val[8:])
	typ := LogEntryType(val[16])
	cmdLen := int(binary.BigEndian.Uint32(val[17:]))
	if len(val) < entryHeaderSize+cmdLen {
		return LogEntry{}, fmt.Errorf("badgerLogStore: value too short for command (%d < %d)", len(val), entryHeaderSize+cmdLen)
	}
	var cmd []byte
	if cmdLen > 0 {
		cmd = make([]byte, cmdLen)
		copy(cmd, val[entryHeaderSize:entryHeaderSize+cmdLen])
	}
	return LogEntry{Term: term, Index: idx, Type: typ, Command: cmd}, nil
}

// FirstIndex returns 1. Log compaction (snapshots) is not implemented until
// PR 12+; the first valid index is always 1.
func (s *badgerLogStore) FirstIndex() uint64 { return 1 }

// LastIndex returns the index of the most recently appended entry.
// Returns 0 on an empty store.
// Safe to call from any goroutine (reads a cached atomic).
func (s *badgerLogStore) LastIndex() uint64 { return s.lastIdx.Load() }

// Entry returns the log entry at 1-based logical index idx.
// Returns ErrLogIndexOutOfRange if idx == 0 or idx > LastIndex().
func (s *badgerLogStore) Entry(idx uint64) (LogEntry, error) {
	if idx == 0 || idx > s.lastIdx.Load() {
		return LogEntry{}, ErrLogIndexOutOfRange
	}
	var entry LogEntry
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.makeKey(idx))
		if err == badger.ErrKeyNotFound {
			// Key within [1, lastIdx] missing — storage corruption.
			return fmt.Errorf("%w: idx %d missing", ErrLogIndexOutOfRange, idx)
		}
		if err != nil {
			return fmt.Errorf("badgerLogStore: Entry: %w", err)
		}
		return item.Value(func(val []byte) error {
			var e LogEntry
			e, err = decodeEntry(val)
			if err != nil {
				return err
			}
			entry = e
			return nil
		})
	})
	if err != nil {
		return LogEntry{}, err
	}
	return entry, nil
}

// TermAt returns the term of the entry at 1-based logical index idx.
// Returns (0, nil) for idx==0 (Raft sentinel for "no previous entry").
// Returns ErrLogIndexOutOfRange if idx > LastIndex().
//
// TODO(perf): TermAt currently delegates to Entry, which decodes the full
// value (including a Command deep-copy). For PR 13+ when this store is
// wired into the live actor, applyConflictHint's binary search calls TermAt
// O(log N) times per AE-reject, each with one Badger View txn + one alloc.
// Add a fast-path that reads only the first 8 bytes of the value (the Term
// field is at offset 0). Unblocked, but not yet a bottleneck since
// memLogStore is the default in NewNode through PR 11.
func (s *badgerLogStore) TermAt(idx uint64) (uint64, error) {
	if idx == 0 {
		return 0, nil
	}
	e, err := s.Entry(idx)
	if err != nil {
		return 0, err
	}
	return e.Term, nil
}

// Append appends entries to the log in a single atomic Badger transaction.
// Panics if entries[0].Index != LastIndex()+1 (non-contiguous append).
// After a successful commit, lastIdx is updated to the last entry's index.
func (s *badgerLogStore) Append(entries []LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	if entries[0].Index != s.lastIdx.Load()+1 {
		panic("raftv2: Append: non-contiguous index")
	}
	// Pre-encode all entries before opening the txn so an oversized Command
	// fails fast (and atomically) without leaving a half-written batch.
	encoded := make([][]byte, len(entries))
	for i, e := range entries {
		buf, err := encodeEntry(e)
		if err != nil {
			return err
		}
		encoded[i] = buf
	}
	err := s.db.Update(func(txn *badger.Txn) error {
		for i, e := range entries {
			if err := txn.Set(s.makeKey(e.Index), encoded[i]); err != nil {
				return fmt.Errorf("badgerLogStore: Append: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.lastIdx.Store(entries[len(entries)-1].Index)
	return nil
}

// TruncateAfter removes all entries with index > idx.
// After this call, LastIndex() == idx.
// No-op if idx >= LastIndex().
// Collects keys to delete first (before committing) to avoid iterator+delete
// interaction within the same transaction.
func (s *badgerLogStore) TruncateAfter(idx uint64) error {
	if idx >= s.lastIdx.Load() {
		return nil
	}

	// Collect all keys with index > idx before mutating.
	var toDelete [][]byte
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := s.makeKey(idx + 1)
		for it.Seek(startKey); it.ValidForPrefix(s.keyPrefix); it.Next() {
			toDelete = append(toDelete, it.Item().KeyCopy(nil))
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("badgerLogStore: TruncateAfter scan: %w", err)
	}

	if len(toDelete) == 0 {
		s.lastIdx.Store(idx)
		return nil
	}

	// Chunk the deletes — Badger txns are bounded (default ~100K ops or
	// MaxBatchSize bytes via ErrTxnTooBig). A long divergent suffix on a
	// follower partition-heal can produce tens of thousands of deletes.
	// Update lastIdx only after the final chunk commits so a mid-truncate
	// crash leaves lastIdx pointing at the still-durable old high; the
	// next AppendEntries will re-fail PrevLogTerm and re-issue the truncate.
	const truncateChunkSize = 1024
	for offset := 0; offset < len(toDelete); offset += truncateChunkSize {
		end := offset + truncateChunkSize
		if end > len(toDelete) {
			end = len(toDelete)
		}
		chunk := toDelete[offset:end]
		err := s.db.Update(func(txn *badger.Txn) error {
			for _, key := range chunk {
				if err := txn.Delete(key); err != nil {
					return fmt.Errorf("badgerLogStore: TruncateAfter delete: %w", err)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	s.lastIdx.Store(idx)
	return nil
}

// EntriesFrom returns entries starting at startIdx, capped at maxEntries.
// maxEntries == 0 means unlimited.
// Returns ErrLogIndexOutOfRange if startIdx > LastIndex()+1.
// Each returned entry has its Command slice freshly allocated (deep copy);
// callers may retain entries across subsequent store mutations.
func (s *badgerLogStore) EntriesFrom(startIdx uint64, maxEntries int) ([]LogEntry, error) {
	if startIdx == 0 {
		return nil, ErrLogIndexOutOfRange
	}
	last := s.lastIdx.Load()
	if startIdx > last+1 {
		return nil, ErrLogIndexOutOfRange
	}
	if startIdx > last {
		return nil, nil
	}

	var result []LogEntry
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := s.makeKey(startIdx)
		for it.Seek(startKey); it.ValidForPrefix(s.keyPrefix); it.Next() {
			if maxEntries > 0 && len(result) >= maxEntries {
				break
			}
			var entry LogEntry
			if err := it.Item().Value(func(val []byte) error {
				e, err := decodeEntry(val)
				if err != nil {
					return err
				}
				entry = e
				return nil
			}); err != nil {
				return fmt.Errorf("badgerLogStore: EntriesFrom: %w", err)
			}
			result = append(result, entry)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
