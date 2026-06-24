package cluster

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
)

// restoreCrashAfterDrop, set only by tests, fires inside FSM.Restore right after DropPrefix
// and before the re-write — simulates a process crash mid-Restore.
var restoreCrashAfterDrop func()

// FSM applies committed Raft log entries to BadgerDB metadata store.
type FSM struct {
	db        MetadataStore
	keys      *stateKeyspace
	clusterID [16]byte
	dekKeeper *encrypt.DEKKeeper

	// Guards onMigrateShard and commitNotifier against concurrent Set* calls + Apply.
	mu sync.RWMutex

	// Optional hooks for Phase 13 balancer. Nil when no peers configured/non-balancer mode.
	// onMigrateShard is a buffered channel; Apply sends non-blocking to avoid blocking the Raft apply loop.
	onMigrateShard chan<- MigrationTask
	commitNotifier interface {
		NotifyCommit(bucket, key, versionID string)
	}
	balancerNotifier interface {
		NotifyMigrationDone(bucket, key, versionID string)
	}
}

// SetMigrationHooks wires the FSM to the balancer/migration subsystem.
// ch must be a buffered channel; Apply drops tasks if the channel is full.
// bn (balancerNotifier) is called on CmdMigrationDone to release the inflight slot.
func (f *FSM) SetMigrationHooks(ch chan<- MigrationTask, notifier interface {
	NotifyCommit(bucket, key, versionID string)
}, bn interface {
	NotifyMigrationDone(bucket, key, versionID string)
}) {
	f.mu.Lock()
	f.onMigrateShard = ch
	f.commitNotifier = notifier
	f.balancerNotifier = bn
	f.mu.Unlock()
}

// NewFSM creates a new finite state machine backed by BadgerDB.
// If keys is nil, newStateKeyspaceEmpty() is used (single-group identity mode).
func NewFSM(db MetadataStore, keys *stateKeyspace) *FSM {
	if keys == nil {
		keys = newStateKeyspaceEmpty()
	}
	return &FSM{db: db, keys: keys}
}

// ApplyTxn decodes a committed command and applies it to the supplied
// transaction. The caller owns the transaction lifecycle (commit/discard);
// this lets the apply actor batch many entries into one transaction.
func (f *FSM) ApplyTxn(txn MetadataTxn, raw []byte) error {
	cmd, err := DecodeCommand(raw)
	if err != nil {
		return fmt.Errorf("decode command: %w", err)
	}

	switch cmd.Type {
	case CmdNoOp:
		return nil
	case CmdCreateBucket, CmdDeleteBucket, CmdSetBucketPolicy, CmdDeleteBucketPolicy:
		// Bucket control-plane moved to meta-raft (MetaBucketStore). These group-0
		// slots are retired: a greenfield cluster never proposes them; old-log replay
		// is a harmless no-op. Enum values kept reserved so wire format is stable.
		return nil
	case CmdMigrateShard:
		return f.applyMigrateShard(txn, cmd.Data)
	case CmdMigrationDone:
		return f.applyMigrationDone(txn, cmd.Data)
	case CmdSetRing:
		return f.applySetRing(txn, cmd.Data)
	case CmdSetBucketVersioning:
		// Bucket versioning moved to meta-raft (MetaBucketStore). Retired no-op;
		// enum value kept reserved so old-log replay is safe.
		return nil
	case CmdResealFSMValues:
		return f.applyResealFSMValues(txn, cmd.Data)
	case CmdFSMValueResealDone:
		// Ordering fence: the marker mutates no state; its only effect is the
		// per-node post-apply hook (notifyOnApply) firing a re-Kick after all
		// preceding CmdResealFSMValues batches in raft order. Gen is carried
		// for logging/observability only — the re-Kick is gen-agnostic. S7-1a-2.
		return nil
	default:
		// Unknown / retired command types (the data plane moved off-raft; the
		// retired per-object/multipart/append/placement slots are no longer named).
		// A greenfield cluster never proposes these; any stale replay is a no-op.
		log.Warn().Uint8("type", uint8(cmd.Type)).Msg("fsm: unknown command type")
		return nil
	}
}

// Apply processes a committed command in its own transaction. Retained for
// offline replay (migrate.go) and tests; the apply actor uses ApplyTxn directly.
func (f *FSM) Apply(raw []byte) error {
	return f.db.Update(func(txn MetadataTxn) error {
		return f.ApplyTxn(txn, raw)
	})
}

// applyResealFSMValues handles CmdResealFSMValues in the serialized apply loop.
// For each key in the command, it reads the CURRENT value from the badger txn
// and reseals it at the KEEPER-CURRENT active gen (via setValue). The skip
// decision compares the value's frame gen to the keeper-current gen — NOT to
// cmd.ActiveGen, which is only a hint (used for the metric label). Tracking
// keeper-current is what prevents a back-to-back-rotation livelock: if a 2nd
// rotation lands mid-drain, this node reseals onto the new current gen and the
// drain's next scan sees the values as already-current.
//
// We deliberately do NOT SealAtGen(cmd.ActiveGen): a follower may have applied
// this data-group command before installing cmd.ActiveGen via meta-raft, so
// SealAtGen would fail closed (ErrDEKGenUnknown). setValue seals at the gen the
// keeper actually holds, which is always resident. Cross-node gen-determinism
// is not required: FSM-value seals are already per-node non-deterministic
// (random nonce), any node can open any node's value while the gen is resident,
// and S7-0 blocks prune so a value never sits at a pruned gen.
//
// The reseal is race-free because it runs in the single-threaded apply loop:
// a concurrent SetBucketPolicy is Raft-ordered before or after, never clobbered.
func (f *FSM) applyResealFSMValues(txn MetadataTxn, data []byte) error {
	c, err := decodeResealFSMValuesCmd(data)
	if err != nil {
		return err
	}
	de := f.dataEncryptor()
	if de == nil {
		return nil // encryption disabled — nothing sealed
	}
	current, ok := f.activeDEKGen()
	if !ok {
		return nil // encryption disabled — nothing sealed
	}
	for _, k := range c.Keys {
		key := []byte(k)
		item, err := txn.Get(key)
		if err == ErrMetaKeyNotFound {
			continue // key deleted since the leader scanned it — fine
		}
		if err != nil {
			return err
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		gen, ct, frameOK, derr := decodeFSMValueFrameV2(raw)
		if derr != nil {
			// Deliberate fail-closed-and-break: any non-NotFound error (decode,
			// Open, seal) returns, failing the apply → ProposeResealFSMValues
			// errors → DrainFSMValueRewrap returns and the single-flight guard
			// releases via its defer. We do NOT skip-and-continue: that would
			// re-livelock the drain on a persistently-stale key.
			return derr
		}
		if !frameOK || gen == current {
			continue // plaintext or already keeper-current — idempotent skip
		}
		plain, err := de.Open(encrypt.DomainFSMValue, []encrypt.AADField{encrypt.FieldBytes(key)}, gen, ct)
		if err != nil {
			return err // fail-closed-and-break (see above)
		}
		if err := f.setValue(txn, key, plain); err != nil { // reseals at keeper-current gen
			return err // fail-closed-and-break (see above)
		}
		// Label with the actual reseal target (keeper-current), not cmd.ActiveGen
		// (which is only a hint of the rotation that drove this batch and can lag
		// keeper-current under back-to-back rotations).
		RewrapFSMValuesTotal.WithLabelValues(strconv.FormatUint(uint64(current), 10)).Inc()
	}
	return nil
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

// pendingMigrationKey returns the BadgerDB key for a not-yet-executed migration task.
// Note: this uses a bare "pending-migration:" prefix (no group prefix); used only by
// apply_test.go to verify key format. The production writer uses f.keys.PendingMigrationKey.
//
//nolint:unused // referenced by apply_test.go
func pendingMigrationKey(bucket, key, versionID string) []byte {
	return []byte("pending-migration:" + bucket + "/" + key + "/" + versionID)
}

// persistPendingMigration writes a migration task to the supplied transaction
// so it survives a crash. Shares the apply actor's batch transaction.
func (f *FSM) persistPendingMigration(txn MetadataTxn, task MigrationTask) error {
	val, err := encodeMigrateShardCmd(MigrateShardFSMCmd(task))
	if err != nil {
		return fmt.Errorf("fsm: marshal pending migration: %w", err)
	}
	return txn.Set(f.keys.PendingMigrationKey(task.Bucket, task.Key, task.VersionID), val)
}

// RecoverPending reads all pending-migration keys from BadgerDB and enqueues them
// to ch for re-execution. Call this at startup AFTER starting the executor goroutine
// so the channel consumer is running before tasks are sent.
func (f *FSM) RecoverPending(ctx context.Context, ch chan<- MigrationTask) error {
	return f.db.View(func(txn MetadataTxn) error {
		return f.keys.scanGroupPrefix(txn, []byte("pending-migration:"), func(_ []byte, item MetaItem) error {
			var taskData []byte
			if err := item.Value(func(val []byte) error {
				taskData = make([]byte, len(val))
				copy(taskData, val)
				return nil
			}); err != nil {
				log.Error().Err(err).Msg("fsm: recover pending migration: read failed")
				return nil
			}
			cmd, err := decodeMigrateShardCmd(taskData)
			if err != nil {
				log.Error().Err(err).Msg("fsm: recover pending migration: decode failed")
				return nil
			}
			task := MigrationTask(cmd)
			select {
			case ch <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	})
}

func (f *FSM) applyMigrateShard(txn MetadataTxn, data []byte) error {
	cmd, err := decodeMigrateShardCmd(data)
	if err != nil {
		return fmt.Errorf("decode MigrateShardCmd: %w", err)
	}
	// Guard: balancer proposals without object identity are placeholders; skip them.
	if cmd.Bucket == "" || cmd.Key == "" {
		return nil
	}
	task := MigrationTask(cmd)
	f.mu.RLock()
	ch := f.onMigrateShard
	f.mu.RUnlock()
	if ch != nil {
		select {
		case ch <- task:
		default:
			// Channel full: persist to BadgerDB so the task survives a crash.
			if err := f.persistPendingMigration(txn, task); err != nil {
				log.Error().Str("bucket", task.Bucket).Str("key", task.Key).Err(err).Msg("fsm: migration queue full and persist failed — task lost")
			} else {
				log.Warn().Str("bucket", task.Bucket).Str("key", task.Key).Msg("fsm: migration queue full, persisted to BadgerDB")
			}
		}
	}
	return nil
}

func (f *FSM) applyMigrationDone(txn MetadataTxn, data []byte) error {
	cmd, err := decodeMigrationDoneCmd(data)
	if err != nil {
		return fmt.Errorf("decode MigrationDoneCmd: %w", err)
	}
	// Clean up any persisted pending-migration entry for this task using the
	// shared batch transaction.
	if derr := txn.Delete(f.keys.PendingMigrationKey(cmd.Bucket, cmd.Key, cmd.VersionID)); derr != nil && derr != ErrMetaKeyNotFound {
		log.Warn().Str("bucket", cmd.Bucket).Str("key", cmd.Key).Err(derr).Msg("fsm: delete pending-migration key failed")
	}
	f.mu.RLock()
	notifier := f.commitNotifier
	bn := f.balancerNotifier
	f.mu.RUnlock()
	if notifier != nil {
		notifier.NotifyCommit(cmd.Bucket, cmd.Key, cmd.VersionID)
	}
	if bn != nil {
		bn.NotifyMigrationDone(cmd.Bucket, cmd.Key, cmd.VersionID)
	}
	return nil
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

// applySetRing is a no-op stub; ring-based placement has been removed.
// The switch case and this function will be deleted in Task 7 along with ring.go.
func (f *FSM) applySetRing(_ MetadataTxn, _ []byte) error { return nil }
