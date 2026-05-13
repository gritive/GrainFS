package raft

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
	// firstIdx / prevTerm mirror memLogStore's compaction state. They are
	// persisted under dedicated meta keys (see metaFirstIndexKey / metaPrevTermKey)
	// and updated atomically with the prefix-key range deletion in CompactBefore.
	// Loaded on open (newBadgerLogStore) and updated only by CompactBefore.
	// Read on FirstIndex/TermAt/EntriesFrom, written only inside the actor goroutine.
	firstIdx atomic.Uint64
	prevTerm atomic.Uint64
}

const entryHeaderSize = 8 + 8 + 1 + 4 // Term + Index + Type + CommandLen

// Meta keys live under keyPrefix + 0xFF + suffix. The 0xFF marker after the
// caller's prefix sorts AFTER all be64-encoded entry keys (be64(2^64-1) has
// every byte 0xFF, so meta keys with non-zero suffixes still sort strictly
// after any conceivable index key — and indices in practice are tiny).
// More importantly, meta keys have length != len(keyPrefix)+8, so the entry-
// scan loop (which validates that exact length) skips them cleanly.
const (
	metaMarker            = 0xFF
	metaFirstIndexSuffix  = "meta/firstIndex"
	metaPrevTermSuffix    = "meta/prevTerm"
	metaSchemaVersionSuff = "meta/schemaVersion"
)

// raftV2SchemaVersion is the on-disk schema version of a v2 log store.
// Bumped to 1 in M6.0 (Path B) because single-phase ConfChange entries
// gained an Op tag + learner snapshot. An older store decoded by the
// post-M6.0 codec would mis-parse those bytes — open-time refusal forces
// operators to clean-restart per CHANGELOG procedure.
const raftV2SchemaVersion uint8 = 1

// makeMetaKey returns prefix || 0xFF || suffix.
func (s *badgerLogStore) makeMetaKey(suffix string) []byte {
	key := make([]byte, len(s.keyPrefix)+1+len(suffix))
	copy(key, s.keyPrefix)
	key[len(s.keyPrefix)] = metaMarker
	copy(key[len(s.keyPrefix)+1:], suffix)
	return key
}

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
	// Default firstIdx == 1 (no compaction). prevTerm starts at 0 (no
	// snapshot covers any entry yet). Loaded from meta keys below if present.
	s.firstIdx.Store(1)

	// Schema-version gate (M6.0). Refuse to open a pre-M6.0 store because
	// the single-phase ConfChange wire format changed (Op tag + learner
	// snapshot). Empty stores stamp the current version on first open.
	// Detection rules:
	//   - schema key present, value == raftV2SchemaVersion: OK.
	//   - schema key present, value != raftV2SchemaVersion: refuse loudly.
	//   - schema key absent + no entry keys exist + no compaction meta:
	//     fresh store, stamp current version.
	//   - schema key absent + entries OR compaction meta exist: pre-M6.0
	//     store; refuse loudly.
	stamped, sawEntries, sawCompaction, err := s.openSchemaCheck(db)
	if err != nil {
		return nil, err
	}
	if !stamped {
		if sawEntries || sawCompaction {
			return nil, fmt.Errorf("raftv2: log store schema version absent but entries/compaction present — pre-M6.0 store, wipe and re-bootstrap (see CHANGELOG)")
		}
		// Fresh store: stamp current version.
		if err := db.Update(func(txn *badger.Txn) error {
			return txn.Set(s.makeMetaKey(metaSchemaVersionSuff), []byte{raftV2SchemaVersion})
		}); err != nil {
			return nil, fmt.Errorf("raftv2: log store stamp schema version: %w", err)
		}
	}

	// Load compaction meta first — if a previous run left the log compacted,
	// FirstIndex must reflect that before any caller queries.
	err = db.View(func(txn *badger.Txn) error {
		if v, err := readMetaUint64(txn, s.makeMetaKey(metaFirstIndexSuffix)); err != nil {
			return fmt.Errorf("badgerLogStore: load firstIndex: %w", err)
		} else if v > 0 {
			s.firstIdx.Store(v)
		}
		if v, err := readMetaUint64(txn, s.makeMetaKey(metaPrevTermSuffix)); err != nil {
			return fmt.Errorf("badgerLogStore: load prevTerm: %w", err)
		} else {
			s.prevTerm.Store(v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Find the last index by seeking to the end of the prefix range and
	// validate that the highest key's value decodes — defense against
	// prefix collision with foreign keys (different LogStore prefix that
	// happens to share a leading subsequence, or non-LogStore data sharing
	// the keyspace). A decode failure on the highest key means the prefix
	// is unsafe; fail loudly rather than seed lastIdx with garbage.
	//
	// Meta keys also live under this prefix (with length != len(p)+8). The
	// reverse seek walks past them by length-validating each candidate key.
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek past the prefix's last possible entry key: prefix || 0xFF...0xFF (8 bytes).
		// Meta keys (prefix || 0xFF || suffix) sort after this seek key in some
		// cases — we step the iterator past them by checking key length.
		seekKey := make([]byte, len(p)+8)
		copy(seekKey, p)
		for i := len(p); i < len(seekKey); i++ {
			seekKey[i] = 0xFF
		}
		for it.Seek(seekKey); it.ValidForPrefix(p); it.Next() {
			key := it.Item().Key()
			if len(key) != len(p)+8 {
				// Meta key (or future variant) — skip; we want only entry keys.
				continue
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
		}
		// No entry keys found — empty log. lastIdx stays 0; if firstIdx was
		// loaded > 1 from meta, LastIndex() returns firstIdx-1 (compacted but
		// no entries appended after).
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("badgerLogStore: open scan: %w", err)
	}
	return s, nil
}

// openSchemaCheck inspects the store for the schema-version stamp and
// reports whether the version is acceptable. Returns:
//
//	stamped       — true if the schema-version key exists and matches.
//	sawEntries    — true if any entry-shaped key exists under keyPrefix.
//	sawCompaction — true if either compaction meta key exists.
//
// If the stamp exists with the wrong value, the function returns an
// error directly so callers can surface a clear migration message.
func (s *badgerLogStore) openSchemaCheck(db *badger.DB) (stamped, sawEntries, sawCompaction bool, err error) {
	err = db.View(func(txn *badger.Txn) error {
		// Schema version key.
		schemaKey := s.makeMetaKey(metaSchemaVersionSuff)
		item, gerr := txn.Get(schemaKey)
		if gerr == nil {
			verr := item.Value(func(val []byte) error {
				if len(val) != 1 {
					return fmt.Errorf("raftv2: log store schema version: bad length %d", len(val))
				}
				if val[0] != raftV2SchemaVersion {
					return fmt.Errorf("raftv2: log store schema version %d unsupported (need %d) — wipe and re-bootstrap (see CHANGELOG)", val[0], raftV2SchemaVersion)
				}
				return nil
			})
			if verr != nil {
				return verr
			}
			stamped = true
		} else if gerr != badger.ErrKeyNotFound {
			return fmt.Errorf("raftv2: log store read schema version: %w", gerr)
		}
		// Compaction meta? (firstIndex / prevTerm)
		if _, cerr := txn.Get(s.makeMetaKey(metaFirstIndexSuffix)); cerr == nil {
			sawCompaction = true
		} else if cerr != badger.ErrKeyNotFound {
			return cerr
		}
		if !sawCompaction {
			if _, cerr := txn.Get(s.makeMetaKey(metaPrevTermSuffix)); cerr == nil {
				sawCompaction = true
			} else if cerr != badger.ErrKeyNotFound {
				return cerr
			}
		}
		// Any entry key? (length == prefix+8) — scan the first one.
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		wantLen := len(s.keyPrefix) + 8
		for it.Seek(s.keyPrefix); it.ValidForPrefix(s.keyPrefix); it.Next() {
			if len(it.Item().Key()) == wantLen {
				sawEntries = true
				return nil
			}
		}
		return nil
	})
	return stamped, sawEntries, sawCompaction, err
}

// readMetaUint64 reads an 8-byte big-endian uint64 from the given key. Returns
// (0, nil) when the key is not found (default state).
func readMetaUint64(txn *badger.Txn, key []byte) (uint64, error) {
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var v uint64
	err = item.Value(func(val []byte) error {
		if len(val) != 8 {
			return fmt.Errorf("meta value must be 8 bytes, got %d", len(val))
		}
		v = binary.BigEndian.Uint64(val)
		return nil
	})
	return v, err
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

// FirstIndex returns the index of the first non-compacted entry. Equals 1
// before any CompactBefore call; equals (snapshot's LastIncludedIndex)+1
// after compaction.
func (s *badgerLogStore) FirstIndex() uint64 {
	v := s.firstIdx.Load()
	if v == 0 {
		return 1
	}
	return v
}

// LastIndex returns the index of the most recently appended entry.
// Returns 0 on a fresh empty store. After compaction with no subsequent
// appends, returns FirstIndex()-1 so the next Append lands at FirstIndex().
// Safe to call from any goroutine (reads cached atomics).
func (s *badgerLogStore) LastIndex() uint64 {
	last := s.lastIdx.Load()
	if last == 0 {
		// No entries appended yet — but compaction may have advanced the
		// boundary. LastIndex must equal FirstIndex-1 in that case so the
		// next Append lands at FirstIndex.
		return s.FirstIndex() - 1
	}
	return last
}

// Entry returns the log entry at 1-based logical index idx.
// Returns ErrLogIndexOutOfRange if idx == 0, idx < FirstIndex() (compacted),
// or idx > LastIndex().
func (s *badgerLogStore) Entry(idx uint64) (LogEntry, error) {
	if idx == 0 || idx < s.FirstIndex() || idx > s.LastIndex() {
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
	// Snapshot-boundary fast path: TermAt(firstIndex-1) returns prevTerm so
	// the leader's PrevLogTerm at the boundary still resolves after compaction.
	first := s.FirstIndex()
	if idx == first-1 {
		return s.prevTerm.Load(), nil
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
	if entries[0].Index != s.LastIndex()+1 {
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
	// Power-loss durability — see Raft §5.3. The actor reports AE Success to
	// the leader only after Append returns; if the entry isn't on disk, the
	// leader's matchIndex would advance against a follower whose log regresses
	// on restart. badger.DefaultOptions has SyncWrites=false; explicit Sync()
	// guarantees durability regardless of caller's DB options.
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("badgerLogStore: Append sync: %w", err)
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

	// Collect all keys with index > idx before mutating. Filter to entry-key
	// length to skip the meta keys (firstIndex/prevTerm) that share the prefix.
	var toDelete [][]byte
	wantLen := len(s.keyPrefix) + 8
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := s.makeKey(idx + 1)
		for it.Seek(startKey); it.ValidForPrefix(s.keyPrefix); it.Next() {
			key := it.Item().Key()
			if len(key) != wantLen {
				continue // meta key — leave alone
			}
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
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("badgerLogStore: TruncateAfter sync: %w", err)
	}
	s.lastIdx.Store(idx)
	return nil
}

// EntriesFrom returns entries starting at startIdx, capped at maxEntries.
// maxEntries == 0 means unlimited.
// Returns ErrLogIndexOutOfRange if startIdx == 0, startIdx < FirstIndex()
// (compacted), or startIdx > LastIndex()+1.
// Each returned entry has its Command slice freshly allocated (deep copy);
// callers may retain entries across subsequent store mutations.
func (s *badgerLogStore) EntriesFrom(startIdx uint64, maxEntries int) ([]LogEntry, error) {
	if startIdx == 0 {
		return nil, ErrLogIndexOutOfRange
	}
	if startIdx < s.FirstIndex() {
		return nil, ErrLogIndexOutOfRange
	}
	last := s.LastIndex()
	if startIdx > last+1 {
		return nil, ErrLogIndexOutOfRange
	}
	if startIdx > last {
		return nil, nil
	}

	limit := last - startIdx + 1
	if maxEntries > 0 && uint64(maxEntries) < limit {
		limit = uint64(maxEntries)
	}
	resultCap := int(limit)
	if uint64(resultCap) != limit {
		resultCap = math.MaxInt
	}
	result := make([]LogEntry, 0, resultCap)
	err := s.db.View(func(txn *badger.Txn) error {
		key := make([]byte, len(s.keyPrefix)+8)
		copy(key, s.keyPrefix)
		for idx := startIdx; idx < startIdx+limit; idx++ {
			binary.BigEndian.PutUint64(key[len(s.keyPrefix):], idx)
			item, err := txn.Get(key)
			if err != nil {
				return fmt.Errorf("missing log entry %d: %w", idx, err)
			}
			var entry LogEntry
			if err := item.Value(func(val []byte) error {
				e, err := decodeEntry(val)
				if err != nil {
					return err
				}
				if e.Index != idx {
					return fmt.Errorf("encoded Index (%d) disagrees with requested index (%d)", e.Index, idx)
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

// InstallSnapshotBoundary seeds firstIndex / prevTerm meta keys directly
// without requiring entries to exist at the boundary. Used by InstallSnapshot
// when the leader's snapshot covers indices the follower has never seen.
// The log MUST be empty at call time (caller ensures via TruncateAfter(0)).
// Persists durably (db.Sync).
func (s *badgerLogStore) InstallSnapshotBoundary(lastIncludedIndex, lastIncludedTerm uint64) error {
	if s.LastIndex() != s.FirstIndex()-1 {
		// The log must be empty above the current snapshot boundary.
		// InstallSnapshot must TruncateAfter(FirstIndex()-1) first.
		return ErrLogIndexOutOfRange
	}
	firstBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(firstBuf, lastIncludedIndex+1)
	prevTermBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(prevTermBuf, lastIncludedTerm)
	err := s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(s.makeMetaKey(metaFirstIndexSuffix), firstBuf); err != nil {
			return err
		}
		return txn.Set(s.makeMetaKey(metaPrevTermSuffix), prevTermBuf)
	})
	if err != nil {
		return fmt.Errorf("badgerLogStore: InstallSnapshotBoundary: %w", err)
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("badgerLogStore: InstallSnapshotBoundary sync: %w", err)
	}
	s.firstIdx.Store(lastIncludedIndex + 1)
	s.prevTerm.Store(lastIncludedTerm)
	s.lastIdx.Store(0)
	return nil
}

// CompactBefore deletes all entry keys with index <= boundary and persists
// the new firstIndex (= boundary+1) and prevTerm (= term of entry at boundary)
// in a single Badger txn (chunked when the delete count is large), then
// fsyncs. After return, FirstIndex() == boundary+1 and TermAt(boundary)
// returns prevTerm.
//
// No-op if boundary < FirstIndex(). Returns ErrLogIndexOutOfRange if
// boundary > LastIndex().
func (s *badgerLogStore) CompactBefore(boundary uint64) error {
	if boundary < s.FirstIndex() {
		return nil
	}
	if boundary > s.LastIndex() {
		return ErrLogIndexOutOfRange
	}
	// Capture term at boundary BEFORE deleting.
	t, err := s.TermAt(boundary)
	if err != nil {
		return err
	}

	// Collect entry keys for indices [FirstIndex, boundary] to delete.
	first := s.FirstIndex()
	wantLen := len(s.keyPrefix) + 8
	var toDelete [][]byte
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		startKey := s.makeKey(first)
		for it.Seek(startKey); it.ValidForPrefix(s.keyPrefix); it.Next() {
			key := it.Item().Key()
			if len(key) != wantLen {
				continue // meta key
			}
			idx := binary.BigEndian.Uint64(key[len(s.keyPrefix):])
			if idx > boundary {
				break
			}
			toDelete = append(toDelete, it.Item().KeyCopy(nil))
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("badgerLogStore: CompactBefore scan: %w", err)
	}

	// Encode new meta values.
	firstBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(firstBuf, boundary+1)
	prevTermBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(prevTermBuf, t)

	// Chunk deletes (mirror TruncateAfter). The final chunk's txn also
	// updates the meta keys so a mid-compaction crash leaves either the
	// pre-compaction state (no meta update) OR the fully compacted state
	// (meta update + all deletes durable). The intermediate states (some
	// deletes done, meta not updated) are recoverable: a re-run of
	// CompactBefore at the same boundary is idempotent (no-ops on already-
	// deleted keys, sets meta to the same values).
	const chunkSize = 1024
	for offset := 0; offset < len(toDelete) || offset == 0; offset += chunkSize {
		end := offset + chunkSize
		if end > len(toDelete) {
			end = len(toDelete)
		}
		isLast := end == len(toDelete)
		chunk := toDelete[offset:end]
		err := s.db.Update(func(txn *badger.Txn) error {
			for _, key := range chunk {
				if err := txn.Delete(key); err != nil {
					return fmt.Errorf("badgerLogStore: CompactBefore delete: %w", err)
				}
			}
			if isLast {
				if err := txn.Set(s.makeMetaKey(metaFirstIndexSuffix), firstBuf); err != nil {
					return fmt.Errorf("badgerLogStore: CompactBefore set firstIndex: %w", err)
				}
				if err := txn.Set(s.makeMetaKey(metaPrevTermSuffix), prevTermBuf); err != nil {
					return fmt.Errorf("badgerLogStore: CompactBefore set prevTerm: %w", err)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if isLast {
			break
		}
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("badgerLogStore: CompactBefore sync: %w", err)
	}
	s.firstIdx.Store(boundary + 1)
	s.prevTerm.Store(t)
	return nil
}
