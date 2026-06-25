package cluster

import (
	"fmt"
	"sync"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
)

// restoreCrashAfterDrop, set only by tests, fires inside FSM.Restore right after DropPrefix
// and before the re-write — simulates a process crash mid-Restore.
var restoreCrashAfterDrop func()

// FSM owns snapshot/restore access to the BadgerDB metadata store.
type FSM struct {
	db        MetadataStore
	keys      *stateKeyspace
	clusterID [16]byte
	dekKeeper *encrypt.DEKKeeper
	mu        sync.RWMutex
}

// NewFSM creates a new finite state machine backed by BadgerDB.
// If keys is nil, newStateKeyspaceEmpty() is used (single-group identity mode).
func NewFSM(db MetadataStore, keys *stateKeyspace) *FSM {
	if keys == nil {
		keys = newStateKeyspaceEmpty()
	}
	return &FSM{db: db, keys: keys}
}

// deleteMarkerETag is the sentinel ETag we store on a tombstone (soft-delete).
// Used by callers to distinguish a real object version from a delete marker.
const deleteMarkerETag = "DEL"

func appendBaseCoalescedRef(key, versionID string, existing *objectMeta) CoalescedShardRef {
	coalescedID := "base"
	if versionID != "" {
		coalescedID = "base-" + versionID
	}
	return CoalescedShardRef{
		CoalescedID: coalescedID,
		Size:        existing.Size,
		ETag:        existing.ETag,
		ShardKey:    ecObjectShardKey(key, versionID),
		ECData:      existing.ECData,
		ECParity:    existing.ECParity,
		StripeBytes: existing.StripeBytes,
		NodeIDs:     append([]string(nil), existing.NodeIDs...),
	}
}

// Snapshot serializes this group's metadata state for Raft snapshots. Keys in
// the snapshot blob are GROUP-RELATIVE (the group prefix is stripped); Restore
// re-applies the prefix on write. For the empty keyspace the group prefix is nil,
// so this is a whole-DB scan exactly as before.
func (f *FSM) Snapshot() ([]byte, error) {
	state := make(map[string][]byte)
	err := f.db.View(func(txn MetadataTxn) error {
		return f.keys.scanGroupPrefix(txn, nil, func(rawKey []byte, item MetaItem) error {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			state[string(rawKey)] = val
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return marshalSnapshotState(state)
}

// dropAllKeys deletes every key in the underlying DB. Used by Restore's
// empty-keyspace branch (single-group / pre-shared mode), where a snapshot owns
// the whole DB. The shared branch uses DropPrefix on the group prefix instead.
func (f *FSM) dropAllKeys() error {
	return f.db.Update(func(txn MetadataTxn) error {
		it := txn.NewIterator(MetaIteratorOptions{PrefetchValues: true})
		var keysToDelete [][]byte
		for it.Rewind(); it.Valid(); it.Next() {
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}
		it.Close()
		for _, k := range keysToDelete {
			if err := txn.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// Restore replaces this group's metadata state from a snapshot, refusing any
// snapshot whose store-meta record does not carry FormatVersion 2 (satisfies
// raft.Snapshotter; SnapshotManager.Restore supplies the store-meta record
// loaded alongside the payload). Snapshot blob keys must be GROUP-RELATIVE; the
// group prefix is applied on write. Validation runs BEFORE any mutation so a
// rejected restore leaves existing state intact.
func (f *FSM) Restore(meta raft.SnapshotMeta, data []byte) error {
	if meta.FormatVersion != raft.FSMSnapshotFormatVersion {
		return fmt.Errorf("FSM.Restore: unsupported snapshot FormatVersion %d (want %d)", meta.FormatVersion, raft.FSMSnapshotFormatVersion)
	}
	state, err := unmarshalSnapshotState(data)
	if err != nil {
		return fmt.Errorf("FSM.Restore: decode snapshot: %w", err)
	}
	// Keys in the blob must be group-RELATIVE. Reject any that already carry this
	// group's prefix (a mis-encode). Only meaningful when we actually have a prefix.
	if f.keys.isShared() {
		for k := range state {
			if f.keys.HasPrefix([]byte(k)) {
				return fmt.Errorf("FSM.Restore: snapshot key %q already carries a group prefix — refusing", k)
			}
		}
	}
	// Drop only this group's keys; for the empty keyspace fall back to the legacy
	// whole-DB delete loop (the snapshot owns the entire DB in that mode).
	if f.keys.isShared() {
		if err := f.db.DropPrefix(f.keys.Prefix(nil)); err != nil {
			return fmt.Errorf("FSM.Restore: DropPrefix: %w", err)
		}
	} else {
		if err := f.dropAllKeys(); err != nil {
			return fmt.Errorf("FSM.Restore: drop existing keys: %w", err)
		}
	}
	if restoreCrashAfterDrop != nil {
		restoreCrashAfterDrop()
	}
	// Re-encode keys with the group prefix on write.
	return f.db.Update(func(txn MetadataTxn) error {
		for k, v := range state {
			if err := txn.Set(f.keys.Key([]byte(k)), v); err != nil {
				return err
			}
		}
		return nil
	})
}
