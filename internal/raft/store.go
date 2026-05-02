package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/badgerutil"
	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
)

// Snapshot is a point-in-time FSM state capture including cluster membership.
// Servers == nil means a legacy snapshot (no membership saved).
type Snapshot struct {
	Index   uint64
	Term    uint64
	Data    []byte
	Servers []Server // cluster config at snapshot time; nil = legacy

	// §4.3 joint consensus state at snapshot point. JointPhase=JointNone
	// (zero value) means the snapshot was taken outside any joint cycle
	// — the other fields are unused. When restoring during JointEntering,
	// the leader's heartbeat watcher (checkJointAdvance) auto-resumes the
	// transition once C_new majority is reachable.
	JointPhase           jointPhase
	JointOldVoters       []string
	JointNewVoters       []string
	JointEnterIndex      uint64
	JointManagedLearners []string // PR-K3: learners added by ChangeMembership
}

// SnapshotMeta describes a persisted snapshot without loading the snapshot
// payload. It is used by offline recovery planning where dry-run memory use and
// source immutability matter.
type SnapshotMeta struct {
	Index                uint64
	Term                 uint64
	DataSize             int64
	Servers              []Server
	JointPhase           JointPhase
	JointOldVoters       []string
	JointNewVoters       []string
	JointEnterIndex      uint64
	JointManagedLearners []string
}

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

	// SaveSnapshot stores a snapshot (index, term, data, and cluster servers).
	SaveSnapshot(snap Snapshot) error

	// LoadSnapshot loads the latest snapshot.
	LoadSnapshot() (Snapshot, error)

	// IsBootstrapped reports whether Bootstrap has been called on this store.
	IsBootstrapped() (bool, error)

	// SaveBootstrapMarker marks this store as bootstrapped. Idempotent.
	SaveBootstrapMarker() error

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
	keyBootstrapped = []byte("raft:meta:bootstrapped")
)

// BadgerLogStoreOption configures a BadgerLogStore.
type BadgerLogStoreOption func(*BadgerLogStore)

// WithManagedMode enables Raft log GC mode. The managed-mode flag is
// persisted in the DB; reopening with a different setting returns an error.
func WithManagedMode() BadgerLogStoreOption {
	return func(s *BadgerLogStore) { s.managedMode = true }
}

// BadgerLogStore implements LogStore using BadgerDB.
//
// When `prefix` is non-empty, the store operates in *shared* mode: it views
// an externally-managed *badger.DB and prefixes every key with `prefix`. This
// is the C2 P0b prototype path — multiple groups share one process-level DB.
// Close() is a no-op for shared stores; the caller closes the DB.
type BadgerLogStore struct {
	db          *badger.DB
	managedMode bool
	prefix      []byte // non-nil → shared mode; prepended to every key
	shared      bool   // true → Close() does not close db
}

// IsManagedMode reports whether this store was opened with managed mode.
func (s *BadgerLogStore) IsManagedMode() bool { return s.managedMode }

// withPrefix returns prefix||key (or key as-is when prefix is empty).
func (s *BadgerLogStore) withPrefix(key []byte) []byte {
	if len(s.prefix) == 0 {
		return key
	}
	out := make([]byte, 0, len(s.prefix)+len(key))
	out = append(out, s.prefix...)
	out = append(out, key...)
	return out
}

func (s *BadgerLogStore) keyState() []byte        { return s.withPrefix(keyState) }
func (s *BadgerLogStore) keySnapshot() []byte     { return s.withPrefix(keySnapshot) }
func (s *BadgerLogStore) keySnapshotMeta() []byte { return s.withPrefix(keySnapshotMeta) }
func (s *BadgerLogStore) keyManagedMode() []byte  { return s.withPrefix(keyManagedMode) }
func (s *BadgerLogStore) keyBootstrapped() []byte { return s.withPrefix(keyBootstrapped) }
func (s *BadgerLogStore) logPrefix() []byte       { return s.withPrefix(prefixLog) }

func (s *BadgerLogStore) logKey(index uint64) []byte {
	pfx := s.logPrefix()
	key := make([]byte, len(pfx)+8)
	copy(key, pfx)
	binary.BigEndian.PutUint64(key[len(pfx):], index)
	return key
}

// NewBadgerLogStore creates a new log store backed by BadgerDB.
func NewBadgerLogStore(path string, opts ...BadgerLogStoreOption) (*BadgerLogStore, error) {
	s := &BadgerLogStore{}
	for _, opt := range opts {
		opt(s)
	}
	dbOpts := badgerutil.RaftLogOptions(path, true) // raft index 키가 곧 unique — badger MVCC 불필요
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

// OpenSharedLogStore returns a BadgerLogStore that views an externally-managed
// *badger.DB, scoped to keys prefixed by groupID. The caller owns db.Close()
// — this store's Close() is a no-op. Used by C2 P0b shared raft-log prototype.
//
// Prefix encoding: 4-byte big-endian length || groupID. The fixed-width length
// avoids the wraparound that would occur with a 1-byte length when
// len(groupID) >= 256, which can cause cross-group prefix collisions and
// allow LastIndex/TruncateBefore/TruncateAfter on one group to corrupt
// another. Trailing ':' from the prior 1-byte encoding is dropped — the
// length prefix is sufficient for unambiguous parsing.
func OpenSharedLogStore(db *badger.DB, groupID string, opts ...BadgerLogStoreOption) (*BadgerLogStore, error) {
	if db == nil {
		return nil, fmt.Errorf("OpenSharedLogStore: nil db")
	}
	if groupID == "" {
		return nil, fmt.Errorf("OpenSharedLogStore: empty groupID")
	}
	if len(groupID) > 0xFFFFFFFF {
		return nil, fmt.Errorf("OpenSharedLogStore: groupID length %d exceeds uint32 max", len(groupID))
	}
	prefix := make([]byte, 4+len(groupID))
	binary.BigEndian.PutUint32(prefix[:4], uint32(len(groupID)))
	copy(prefix[4:], groupID)
	s := &BadgerLogStore{db: db, prefix: prefix, shared: true}
	for _, opt := range opts {
		opt(s)
	}
	if err := s.checkManagedMode(); err != nil {
		return nil, err
	}
	return s, nil
}

// checkManagedMode writes (first open) or verifies (subsequent opens) the
// managed-mode flag persisted in the DB. Mismatch returns a clear error.
//
// On the shared-DB path (C2 P0b) two goroutines opening the same groupID
// concurrently can race on the get-then-set, producing badger.ErrConflict.
// The op is idempotent, so we retry transparently up to a small bound.
func (s *BadgerLogStore) checkManagedMode() error {
	const maxRetries = 8
	for i := 0; i < maxRetries; i++ {
		err := s.checkManagedModeOnce()
		if err == nil || !errors.Is(err, badger.ErrConflict) {
			return err
		}
	}
	return fmt.Errorf("checkManagedMode: badger transaction conflict after %d retries", maxRetries)
}

func (s *BadgerLogStore) checkManagedModeOnce() error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(s.keyManagedMode())
		if err != nil {
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			// First open: record the chosen mode.
			val := "false"
			if s.managedMode {
				val = "true"
			}
			return txn.Set(s.keyManagedMode(), []byte(val))
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

func marshalLogEntry(entry LogEntry) []byte {
	b := raftBuilderPool.Get()
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
	pb.LogEntryAddEntryType(b, pb.LogEntryType(entry.Type))
	root := pb.LogEntryEnd(b)
	return fbFinishRPC(b, root)
}

func unmarshalLogEntry(data []byte) (entry LogEntry, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unmarshal log entry: invalid flatbuffer: %v", r)
		}
	}()
	e := pb.GetRootAsLogEntry(data, 0)
	return LogEntry{Term: e.Term(), Index: e.Index(), Command: e.CommandBytes(), Type: LogEntryType(e.EntryType())}, nil
}

func (s *BadgerLogStore) AppendEntries(entries []LogEntry) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, entry := range entries {
			data := marshalLogEntry(entry)
			if err := txn.Set(s.logKey(entry.Index), data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BadgerLogStore) GetEntry(index uint64) (*LogEntry, error) {
	var entry LogEntry
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.logKey(index))
		if err == badger.ErrKeyNotFound {
			return fmt.Errorf("entry %d not found", index)
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var e error
			entry, e = unmarshalLogEntry(val)
			return e
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
			item, err := txn.Get(s.logKey(idx))
			if err == badger.ErrKeyNotFound {
				break
			}
			if err != nil {
				return err
			}
			var entry LogEntry
			if err := item.Value(func(val []byte) error {
				var e error
				entry, e = unmarshalLogEntry(val)
				return e
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
	logPfx := s.logPrefix()
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		opts.Prefix = logPfx
		it := txn.NewIterator(opts)
		defer it.Close()

		// Seek to a key past the prefix range
		seekKey := make([]byte, len(logPfx)+8)
		copy(seekKey, logPfx)
		for i := len(logPfx); i < len(seekKey); i++ {
			seekKey[i] = 0xFF
		}
		it.Seek(seekKey)

		if it.ValidForPrefix(logPfx) {
			key := it.Item().Key()
			lastIdx = binary.BigEndian.Uint64(key[len(logPfx):])
		}
		return nil
	})
	return lastIdx, err
}

func (s *BadgerLogStore) TruncateAfter(afterIndex uint64) error {
	logPfx := s.logPrefix()
	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = logPfx
		it := txn.NewIterator(opts)
		defer it.Close()

		startKey := s.logKey(afterIndex + 1)
		for it.Seek(startKey); it.ValidForPrefix(logPfx); it.Next() {
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
	logPfx := s.logPrefix()
	endKey := s.logKey(beforeIndex)
	for {
		done := false
		err := s.db.Update(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = logPfx
			it := txn.NewIterator(opts)
			defer it.Close()

			count := 0
			for it.Seek(logPfx); it.ValidForPrefix(logPfx); it.Next() {
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
	b := raftBuilderPool.Get()
	votedForOff := b.CreateString(votedFor)
	pb.RaftStateStart(b)
	pb.RaftStateAddTerm(b, term)
	pb.RaftStateAddVotedFor(b, votedForOff)
	root := pb.RaftStateEnd(b)
	data := fbFinishRPC(b, root)
	stateKey := s.keyState()
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(stateKey, data)
	})
}

func (s *BadgerLogStore) LoadState() (uint64, string, error) {
	var term uint64
	var votedFor string
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.keyState())
		if err == badger.ErrKeyNotFound {
			return nil // fresh node
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("unmarshal raft state: invalid flatbuffer: %v", r)
				}
			}()
			st := pb.GetRootAsRaftState(val, 0)
			term = st.Term()
			votedFor = string(st.VotedFor())
			return nil
		})
	})
	return term, votedFor, err
}

func (s *BadgerLogStore) SaveSnapshot(snap Snapshot) error {
	b := raftBuilderPool.Get()

	// Build ServerEntry objects BEFORE SnapshotMetaStart (FlatBuffers rule: create nested objects before parent).
	var serversVec flatbuffers.UOffsetT
	if len(snap.Servers) > 0 {
		serverOffs := make([]flatbuffers.UOffsetT, len(snap.Servers))
		for i := len(snap.Servers) - 1; i >= 0; i-- {
			sv := snap.Servers[i]
			idOff := b.CreateString(sv.ID)
			pb.ServerEntryStart(b)
			pb.ServerEntryAddId(b, idOff)
			pb.ServerEntryAddSuffrage(b, int8(sv.Suffrage))
			serverOffs[i] = pb.ServerEntryEnd(b)
		}
		pb.SnapshotMetaStartServersVector(b, len(serverOffs))
		for i := len(serverOffs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(serverOffs[i])
		}
		serversVec = b.EndVector(len(serverOffs))
	}

	// §4.3 joint voter sets serialize as plain string vectors. JointPhase=JointNone
	// (zero) leaves the vectors empty so legacy snapshots remain readable.
	stringVec := func(ss []string) flatbuffers.UOffsetT {
		if len(ss) == 0 {
			return 0
		}
		offs := make([]flatbuffers.UOffsetT, len(ss))
		for i, s := range ss {
			offs[i] = b.CreateString(s)
		}
		b.StartVector(4, len(ss), 4)
		for i := len(offs) - 1; i >= 0; i-- {
			b.PrependUOffsetT(offs[i])
		}
		return b.EndVector(len(ss))
	}
	jointOldVec := stringVec(snap.JointOldVoters)
	jointNewVec := stringVec(snap.JointNewVoters)
	jointManagedVec := stringVec(snap.JointManagedLearners)

	pb.SnapshotMetaStart(b)
	pb.SnapshotMetaAddIndex(b, snap.Index)
	pb.SnapshotMetaAddTerm(b, snap.Term)
	if len(snap.Servers) > 0 {
		pb.SnapshotMetaAddServers(b, serversVec)
	}
	if snap.JointPhase != JointNone {
		pb.SnapshotMetaAddJointPhase(b, int8(snap.JointPhase))
	}
	if jointOldVec != 0 {
		pb.SnapshotMetaAddJointOldVoters(b, jointOldVec)
	}
	if jointNewVec != 0 {
		pb.SnapshotMetaAddJointNewVoters(b, jointNewVec)
	}
	if snap.JointEnterIndex != 0 {
		pb.SnapshotMetaAddJointEnterIndex(b, snap.JointEnterIndex)
	}
	if jointManagedVec != 0 {
		pb.SnapshotMetaAddJointManagedLearners(b, jointManagedVec)
	}
	root := pb.SnapshotMetaEnd(b)
	meta := fbFinishRPC(b, root)

	metaKey := s.keySnapshotMeta()
	dataKey := s.keySnapshot()
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(metaKey, meta); err != nil {
			return err
		}
		return txn.Set(dataKey, snap.Data)
	})
}

func (s *BadgerLogStore) LoadSnapshot() (Snapshot, error) {
	var snap Snapshot
	metaKey := s.keySnapshotMeta()
	dataKey := s.keySnapshot()
	err := s.db.View(func(txn *badger.Txn) error {
		metaItem, err := txn.Get(metaKey)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		if err := metaItem.Value(func(val []byte) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("unmarshal snapshot meta: invalid flatbuffer: %v", r)
				}
			}()
			m := pb.GetRootAsSnapshotMeta(val, 0)
			snap.Index = m.Index()
			snap.Term = m.Term()
			var se pb.ServerEntry
			for i := 0; i < m.ServersLength(); i++ {
				if m.Servers(&se, i) {
					snap.Servers = append(snap.Servers, Server{
						ID:       string(se.Id()),
						Suffrage: ServerSuffrage(se.Suffrage()),
					})
				}
			}
			snap.JointPhase = jointPhase(m.JointPhase())
			if oldLen := m.JointOldVotersLength(); oldLen > 0 {
				snap.JointOldVoters = make([]string, oldLen)
				for i := 0; i < oldLen; i++ {
					snap.JointOldVoters[i] = string(m.JointOldVoters(i))
				}
			}
			if newLen := m.JointNewVotersLength(); newLen > 0 {
				snap.JointNewVoters = make([]string, newLen)
				for i := 0; i < newLen; i++ {
					snap.JointNewVoters[i] = string(m.JointNewVoters(i))
				}
			}
			snap.JointEnterIndex = m.JointEnterIndex()
			if mLen := m.JointManagedLearnersLength(); mLen > 0 {
				snap.JointManagedLearners = make([]string, mLen)
				for i := 0; i < mLen; i++ {
					snap.JointManagedLearners[i] = string(m.JointManagedLearners(i))
				}
			}
			return nil
		}); err != nil {
			return err
		}
		dataItem, err := txn.Get(dataKey)
		if err != nil {
			return err
		}
		return dataItem.Value(func(val []byte) error {
			snap.Data = make([]byte, len(val))
			copy(snap.Data, val)
			return nil
		})
	})
	return snap, err
}

// InspectManagedModeReadOnly reads the persisted managed-mode bit without
// running NewBadgerLogStore's first-open write path.
func InspectManagedModeReadOnly(path string) (managed bool, present bool, err error) {
	db, err := badger.Open(badger.DefaultOptions(path).WithLogger(nil).WithReadOnly(true))
	if err != nil {
		return false, false, fmt.Errorf("open raft store read-only: %w", err)
	}
	defer db.Close()
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyManagedMode)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		present = true
		return item.Value(func(val []byte) error {
			managed = string(val) == "true"
			return nil
		})
	})
	return managed, present, err
}

// InspectSnapshotMetaReadOnly returns snapshot metadata and payload size without
// reading the payload bytes.
func InspectSnapshotMetaReadOnly(path string) (SnapshotMeta, error) {
	db, err := badger.Open(badger.DefaultOptions(path).WithLogger(nil).WithReadOnly(true))
	if err != nil {
		return SnapshotMeta{}, fmt.Errorf("open raft store read-only: %w", err)
	}
	defer db.Close()

	var meta SnapshotMeta
	err = db.View(func(txn *badger.Txn) error {
		metaItem, err := txn.Get(keySnapshotMeta)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := metaItem.Value(func(val []byte) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("unmarshal snapshot meta: invalid flatbuffer: %v", r)
				}
			}()
			m := pb.GetRootAsSnapshotMeta(val, 0)
			meta.Index = m.Index()
			meta.Term = m.Term()
			var se pb.ServerEntry
			for i := 0; i < m.ServersLength(); i++ {
				if m.Servers(&se, i) {
					meta.Servers = append(meta.Servers, Server{
						ID:       string(se.Id()),
						Suffrage: ServerSuffrage(se.Suffrage()),
					})
				}
			}
			meta.JointPhase = JointPhase(m.JointPhase())
			if oldLen := m.JointOldVotersLength(); oldLen > 0 {
				meta.JointOldVoters = make([]string, oldLen)
				for i := 0; i < oldLen; i++ {
					meta.JointOldVoters[i] = string(m.JointOldVoters(i))
				}
			}
			if newLen := m.JointNewVotersLength(); newLen > 0 {
				meta.JointNewVoters = make([]string, newLen)
				for i := 0; i < newLen; i++ {
					meta.JointNewVoters[i] = string(m.JointNewVoters(i))
				}
			}
			meta.JointEnterIndex = m.JointEnterIndex()
			if managedLen := m.JointManagedLearnersLength(); managedLen > 0 {
				meta.JointManagedLearners = make([]string, managedLen)
				for i := 0; i < managedLen; i++ {
					meta.JointManagedLearners[i] = string(m.JointManagedLearners(i))
				}
			}
			return nil
		}); err != nil {
			return err
		}
		dataItem, err := txn.Get(keySnapshot)
		if err != nil {
			return err
		}
		meta.DataSize = dataItem.ValueSize()
		return nil
	})
	return meta, err
}

// LoadSnapshotReadOnly loads the full snapshot payload for offline recovery
// execution without mutating source metadata.
func LoadSnapshotReadOnly(path string) (Snapshot, error) {
	db, err := badger.Open(badger.DefaultOptions(path).WithLogger(nil).WithReadOnly(true))
	if err != nil {
		return Snapshot{}, fmt.Errorf("open raft store read-only: %w", err)
	}
	defer db.Close()
	return loadSnapshotFromDB(db)
}

func loadSnapshotFromDB(db *badger.DB) (Snapshot, error) {
	var snap Snapshot
	err := db.View(func(txn *badger.Txn) error {
		metaItem, err := txn.Get(keySnapshotMeta)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		if err := metaItem.Value(func(val []byte) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("unmarshal snapshot meta: invalid flatbuffer: %v", r)
				}
			}()
			m := pb.GetRootAsSnapshotMeta(val, 0)
			snap.Index = m.Index()
			snap.Term = m.Term()
			var se pb.ServerEntry
			for i := 0; i < m.ServersLength(); i++ {
				if m.Servers(&se, i) {
					snap.Servers = append(snap.Servers, Server{
						ID:       string(se.Id()),
						Suffrage: ServerSuffrage(se.Suffrage()),
					})
				}
			}
			snap.JointPhase = jointPhase(m.JointPhase())
			if oldLen := m.JointOldVotersLength(); oldLen > 0 {
				snap.JointOldVoters = make([]string, oldLen)
				for i := 0; i < oldLen; i++ {
					snap.JointOldVoters[i] = string(m.JointOldVoters(i))
				}
			}
			if newLen := m.JointNewVotersLength(); newLen > 0 {
				snap.JointNewVoters = make([]string, newLen)
				for i := 0; i < newLen; i++ {
					snap.JointNewVoters[i] = string(m.JointNewVoters(i))
				}
			}
			snap.JointEnterIndex = m.JointEnterIndex()
			if mLen := m.JointManagedLearnersLength(); mLen > 0 {
				snap.JointManagedLearners = make([]string, mLen)
				for i := 0; i < mLen; i++ {
					snap.JointManagedLearners[i] = string(m.JointManagedLearners(i))
				}
			}
			return nil
		}); err != nil {
			return err
		}
		dataItem, err := txn.Get(keySnapshot)
		if err != nil {
			return err
		}
		return dataItem.Value(func(val []byte) error {
			snap.Data = make([]byte, len(val))
			copy(snap.Data, val)
			return nil
		})
	})
	return snap, err
}

// IsBootstrapped reports whether Bootstrap has been called on this store.
func (s *BadgerLogStore) IsBootstrapped() (bool, error) {
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(s.keyBootstrapped())
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// SaveBootstrapMarker marks this store as bootstrapped.
func (s *BadgerLogStore) SaveBootstrapMarker() error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.keyBootstrapped(), []byte{1})
	})
}

func (s *BadgerLogStore) Close() error {
	if s.shared {
		// Shared mode: the caller owns *badger.DB lifecycle.
		return nil
	}
	return s.db.Close()
}
