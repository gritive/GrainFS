package cluster

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/reservedname"
	"github.com/gritive/GrainFS/internal/storage"
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

	// Guards onMigrateShard, commitNotifier, and coalesceCfg against concurrent
	// Set* calls + Apply.
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

	// coalesceCfg is the FSM's own copy of the coalesce configuration.
	// Protected by mu. Updated via SetCoalesceCfg (called from
	// DistributedBackend.SetCoalesceConfig so the apply loop sees fresh caps).
	coalesceCfg CoalesceConfig
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

// SetCoalesceCfg updates the FSM's coalesce configuration. Safe to call
// concurrently with Apply; protected by mu.
func (f *FSM) SetCoalesceCfg(cfg CoalesceConfig) {
	f.mu.Lock()
	f.coalesceCfg = cfg
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
	case CmdCreateBucket:
		return f.applyCreateBucket(txn, cmd.Data)
	case CmdDeleteBucket:
		return f.applyDeleteBucket(txn, cmd.Data)
	case CmdPutObjectMeta:
		// reserved, removed in data-plane raft-free Slice 2; blob CAS-quorum is
		// authoritative. No production proposer; greenfield — no raft-log replay of
		// these commands. No-op on any stale entries.
		return nil
	case CmdDeleteObject:
		return f.applyDeleteObject(txn, cmd.Data)
	case CmdCreateMultipartUpload, CmdCompleteMultipart, CmdAbortMultipart:
		// reserved, removed in multipart-off-raft epic (M4): no production proposer,
		// greenfield — no raft-log replay of these commands. No-op on stale entries.
		return nil
	case CmdSetBucketPolicy:
		return f.applySetBucketPolicy(txn, cmd.Data)
	case CmdDeleteBucketPolicy:
		return f.applyDeleteBucketPolicy(txn, cmd.Data)
	case CmdMigrateShard:
		return f.applyMigrateShard(txn, cmd.Data)
	case CmdMigrationDone:
		return f.applyMigrationDone(txn, cmd.Data)
	case CmdPutShardPlacement, CmdDeleteShardPlacement:
		// reserved, removed: ring-derived placement. No proposer; greenfield. No-op.
		return nil
	case CmdSetRing:
		return f.applySetRing(txn, cmd.Data)
	case CmdDeleteObjectVersion:
		return f.applyDeleteObjectVersion(txn, cmd.Data)
	case CmdSetBucketVersioning:
		return f.applySetBucketVersioning(txn, cmd.Data)
	case CmdSetObjectACL, CmdSetObjectTags:
		// reserved, removed in data-plane raft-free Slice 2; blob RMW is
		// authoritative. No production proposer; no raft-log replay of these
		// commands in a greenfield cluster. No-op on any stale entries.
		return nil
	case CmdAppendObject, CmdCoalesceSegments:
		// reserved, removed in append/coalesce-off-raft Slice 1; no production
		// proposer; greenfield — no raft-log replay of these commands. No-op on stale entries.
		return nil
	case CmdPutObjectQuarantine:
		// reserved, removed in data-plane raft-free Slice 2; quarantine is now
		// stored in the quorum-meta blob (IsQuarantined/QuarantineCause). No
		// production proposer; no raft-log replay in a greenfield cluster.
		return nil
	case CmdResealFSMValues:
		return f.applyResealFSMValues(txn, cmd.Data)
	case CmdFSMValueResealDone:
		// Ordering fence: the marker mutates no state; its only effect is the
		// per-node post-apply hook (notifyOnApply) firing a re-Kick after all
		// preceding CmdResealFSMValues batches in raft order. Gen is carried
		// for logging/observability only — the re-Kick is gen-agnostic. S7-1a-2.
		return nil
	case CmdDeleteMultipartDone:
		// reserved, removed in multipart-off-raft epic (M4): no-op on stale entries.
		return nil
	default:
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

func (f *FSM) applyCreateBucket(txn MetadataTxn, data []byte) error {
	c, err := decodeCreateBucketCmd(data)
	if err != nil {
		return err
	}
	if !c.BypassReserved && reservedname.IsReservedBucketName(c.Bucket) {
		return fmt.Errorf("bucket name %q is reserved and cannot be created via public API", c.Bucket)
	}
	return txn.Set(f.keys.BucketKey(c.Bucket), []byte(`{}`))
}

func (f *FSM) applyDeleteBucket(txn MetadataTxn, data []byte) error {
	c, err := decodeDeleteBucketCmd(data)
	if err != nil {
		return err
	}
	if reservedname.IsReservedBucketName(c.Bucket) {
		return fmt.Errorf("bucket name %q is reserved and cannot be deleted via public API", c.Bucket)
	}
	// Clear the per-bucket state so a recreated same-name bucket starts fresh (S3
	// semantics — a new bucket inherits nothing from a prior incarnation): the
	// existence record, the bucket policy, and the versioning state. Tolerate
	// absent keys (most are unset on a plain bucket), mirroring applyDeleteObject.
	// (Per-bucket state in other subsystems — lifecycle config, IAM bucket-upstream
	// — lives outside this FSM keyspace and is tracked separately; object obj:/lat:
	// records are GC'd by the orphan scrubber and DeleteBucket requires an empty
	// bucket.)
	for _, key := range [][]byte{
		f.keys.BucketKey(c.Bucket),
		f.keys.BucketPolicyKey(c.Bucket),
		f.keys.BucketVerKey(c.Bucket),
	} {
		if err := txn.Delete(key); err != nil && err != ErrMetaKeyNotFound {
			return err
		}
	}
	return nil
}

func (f *FSM) applyDeleteObject(txn MetadataTxn, data []byte) error {
	c, err := decodeDeleteObjectCmd(data)
	if err != nil {
		return err
	}
	// VersionID == "" is a legacy DeleteObjectCmd (no version ID attached by the
	// proposer). Treat it as a hard delete of the legacy latest-only records —
	// preserves prior semantics for pre-versioning log replay.
	if c.VersionID == "" {
		// Validate (decrypt) the meta before deleting; surfaces corruption errors.
		if item, gerr := txn.Get(f.keys.ObjectMetaKey(c.Bucket, c.Key)); gerr == nil {
			if verr := item.Value(func(raw []byte) error {
				_, err := f.openValue(item.Key(), raw)
				return err
			}); verr != nil {
				return verr
			}
		}
		if err := txn.Delete(f.keys.ObjectMetaKey(c.Bucket, c.Key)); err != nil && err != ErrMetaKeyNotFound {
			return err
		}
		if err := txn.Delete(f.keys.ShardPlacementKey(c.Bucket, c.Key)); err != nil && err != ErrMetaKeyNotFound {
			return err
		}
		return nil
	}

	// Tombstone (soft-delete): write a delete marker as a new version and
	// flip the latest pointer at it. Prior versions remain addressable via
	// GetObjectVersion.
	markerMeta, err := marshalObjectMeta(objectMeta{
		Key:          c.Key,
		Size:         0,
		ContentType:  "",
		ETag:         deleteMarkerETag,
		LastModified: 0,
	})
	if err != nil {
		return fmt.Errorf("marshal delete marker: %w", err)
	}
	// Read the current latest versionID before overwriting it, so we can
	// delete the versioned placement record that was stored under
	// shardPlacementKey(bucket, key+"/"+prevVersionID).
	prevVersionID := ""
	if latItem, gerr := txn.Get(f.keys.LatestKey(c.Bucket, c.Key)); gerr == nil {
		_ = latItem.Value(func(v []byte) error { prevVersionID = string(v); return nil })
	}

	if err := f.setValue(txn, f.keys.ObjectMetaKeyV(c.Bucket, c.Key, c.VersionID), markerMeta); err != nil {
		return err
	}
	if err := txn.Set(f.keys.LatestKey(c.Bucket, c.Key), []byte(c.VersionID)); err != nil {
		return err
	}
	// Legacy latest-only key is removed so HeadObject returns 404 while prior
	// versions remain queryable via GetObjectVersion.
	if err := txn.Delete(f.keys.ObjectMetaKey(c.Bucket, c.Key)); err != nil && err != ErrMetaKeyNotFound {
		return err
	}
	// Remove the versioned placement record for the previous latest version.
	// Bare-key placement (legacy pre-versioned objects) is also cleaned up.
	if prevVersionID != "" {
		placementKey := f.keys.ShardPlacementKey(c.Bucket, c.Key+"/"+prevVersionID)
		if err := txn.Delete(placementKey); err != nil && err != ErrMetaKeyNotFound {
			return err
		}
	}
	if err := txn.Delete(f.keys.ShardPlacementKey(c.Bucket, c.Key)); err != nil && err != ErrMetaKeyNotFound {
		return err
	}
	return nil
}

// applyDeleteObjectVersion hard-deletes one specific version and, if it was the
// latest, recomputes the latest pointer from the remaining versions (ULIDs sort
// lexicographically by time, so the max-sorted key wins).
func (f *FSM) applyDeleteObjectVersion(txn MetadataTxn, data []byte) error {
	c, err := decodeDeleteObjectVersionCmd(data)
	if err != nil {
		return err
	}
	metaKey := f.keys.ObjectMetaKeyV(c.Bucket, c.Key, c.VersionID)
	latKey := f.keys.LatestKey(c.Bucket, c.Key)

	if _, gerr := txn.Get(metaKey); gerr == ErrMetaKeyNotFound {
		return nil // idempotent
	} else if gerr != nil {
		return gerr
	}
	if derr := txn.Delete(metaKey); derr != nil {
		return derr
	}
	// Remove the versioned placement record stored under key+"/"+versionID.
	placementKey := f.keys.ShardPlacementKey(c.Bucket, c.Key+"/"+c.VersionID)
	if derr := txn.Delete(placementKey); derr != nil && derr != ErrMetaKeyNotFound {
		return derr
	}

	// If the deleted version was the latest, find the new latest by scanning
	// remaining versioned keys and picking the max (ULID sorts by time ASC).
	if latItem, gerr := txn.Get(latKey); gerr == nil {
		var current string
		_ = latItem.Value(func(v []byte) error { current = string(v); return nil })
		if current == c.VersionID {
			newLatest := ""
			rawPrefix := []byte("obj:" + c.Bucket + "/" + c.Key + "/")
			if serr := f.keys.scanGroupPrefix(txn, rawPrefix, func(raw []byte, _ MetaItem) error {
				vid := string(raw[len(rawPrefix):])
				if vid == "" || vid == c.VersionID {
					return nil
				}
				if vid > newLatest {
					newLatest = vid
				}
				return nil
			}); serr != nil {
				return serr
			}
			if newLatest == "" {
				if derr := txn.Delete(latKey); derr != nil && derr != ErrMetaKeyNotFound {
					return derr
				}
				// No versions left — drop legacy latest-only key as well.
				if derr := txn.Delete(f.keys.ObjectMetaKey(c.Bucket, c.Key)); derr != nil && derr != ErrMetaKeyNotFound {
					return derr
				}
			} else {
				if derr := txn.Set(latKey, []byte(newLatest)); derr != nil {
					return derr
				}
			}
		}
	} else if gerr != ErrMetaKeyNotFound {
		return gerr
	}
	return nil
}

func (f *FSM) applySetBucketPolicy(txn MetadataTxn, data []byte) error {
	c, err := decodeSetBucketPolicyCmd(data)
	if err != nil {
		return err
	}
	policyJSON := append([]byte(nil), c.PolicyJSON...)
	return f.setValue(txn, f.keys.BucketPolicyKey(c.Bucket), policyJSON)
}

func (f *FSM) applyDeleteBucketPolicy(txn MetadataTxn, data []byte) error {
	c, err := decodeDeleteBucketPolicyCmd(data)
	if err != nil {
		return err
	}
	err = txn.Delete(f.keys.BucketPolicyKey(c.Bucket))
	if err == ErrMetaKeyNotFound {
		return nil
	}
	return err
}

func (f *FSM) applySetBucketVersioning(txn MetadataTxn, data []byte) error {
	c, err := decodeSetBucketVersioningCmd(data)
	if err != nil {
		return err
	}
	if _, err := txn.Get(f.keys.BucketKey(c.Bucket)); err == ErrMetaKeyNotFound {
		return storage.ErrBucketNotFound
	} else if err != nil {
		return err
	}
	return txn.Set(f.keys.BucketVerKey(c.Bucket), []byte(c.State))
}

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
