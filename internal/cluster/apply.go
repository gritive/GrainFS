package cluster

import (
	"context"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/storage"
)

// FSM applies committed Raft log entries to BadgerDB metadata store.
type FSM struct {
	db *badger.DB

	// Guards onMigrateShard and commitNotifier against concurrent SetMigrationHooks + Apply.
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

	rings *ringStore
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
func NewFSM(db *badger.DB) *FSM {
	return &FSM{db: db, rings: newRingStore()}
}

// Apply processes a committed command and updates the metadata store.
func (f *FSM) Apply(raw []byte) error {
	cmd, err := DecodeCommand(raw)
	if err != nil {
		return fmt.Errorf("decode command: %w", err)
	}

	switch cmd.Type {
	case CmdNoOp:
		return nil
	case CmdCreateBucket:
		return f.applyCreateBucket(cmd.Data)
	case CmdDeleteBucket:
		return f.applyDeleteBucket(cmd.Data)
	case CmdPutObjectMeta:
		return f.applyPutObjectMeta(cmd.Data)
	case CmdDeleteObject:
		return f.applyDeleteObject(cmd.Data)
	case CmdCreateMultipartUpload:
		return f.applyCreateMultipartUpload(cmd.Data)
	case CmdCompleteMultipart:
		return f.applyCompleteMultipart(cmd.Data)
	case CmdAbortMultipart:
		return f.applyAbortMultipart(cmd.Data)
	case CmdSetBucketPolicy:
		return f.applySetBucketPolicy(cmd.Data)
	case CmdDeleteBucketPolicy:
		return f.applyDeleteBucketPolicy(cmd.Data)
	case CmdMigrateShard:
		return f.applyMigrateShard(cmd.Data)
	case CmdMigrationDone:
		return f.applyMigrationDone(cmd.Data)
	case CmdPutShardPlacement:
		// No-op: shard placement is now derived deterministically from the ring.
		// Old log entries are replayed harmlessly.
		return nil
	case CmdDeleteShardPlacement:
		// No-op: see CmdPutShardPlacement.
		return nil
	case CmdSetRing:
		return f.applySetRing(cmd.Data)
	case CmdDeleteObjectVersion:
		return f.applyDeleteObjectVersion(cmd.Data)
	case CmdSetBucketVersioning:
		return f.applySetBucketVersioning(cmd.Data)
	case CmdSetObjectACL:
		return f.applySetObjectACL(cmd.Data)
	default:
		log.Warn().Uint8("type", uint8(cmd.Type)).Msg("fsm: unknown command type")
		return nil
	}
}

func bucketKey(bucket string) []byte          { return []byte("bucket:" + bucket) }
func bucketPolicyKey(bucket string) []byte    { return []byte("policy:" + bucket) }
func bucketVerKey(bucket string) []byte       { return []byte("bucketver:" + bucket) }
func objectMetaKey(bucket, key string) []byte { return []byte("obj:" + bucket + "/" + key) }

// objectMetaKeyV returns the per-version metadata key:
//
//	obj:{bucket}/{key}/{versionID}
//
// Coexists with the legacy latest-only objectMetaKey during the transition.
func objectMetaKeyV(bucket, key, versionID string) []byte {
	return []byte("obj:" + bucket + "/" + key + "/" + versionID)
}

// latestKey points to the current latest version id for an object, or the
// literal "DEL" marker when the object has been tombstoned (soft-deleted).
func latestKey(bucket, key string) []byte {
	return []byte("lat:" + bucket + "/" + key)
}

func multipartKey(uploadID string) []byte { return []byte("mpu:" + uploadID) }
func shardPlacementKey(bucket, key string) []byte {
	return []byte("placement:" + bucket + "/" + key)
}

// deleteMarkerETag is the sentinel ETag we store on a tombstone (soft-delete).
// Used by callers to distinguish a real object version from a delete marker.
const deleteMarkerETag = "DEL"

func (f *FSM) applyCreateBucket(data []byte) error {
	c, err := decodeCreateBucketCmd(data)
	if err != nil {
		return err
	}
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(bucketKey(c.Bucket), []byte(`{}`))
	})
}

func (f *FSM) applyDeleteBucket(data []byte) error {
	c, err := decodeDeleteBucketCmd(data)
	if err != nil {
		return err
	}
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(bucketKey(c.Bucket))
	})
}

func (f *FSM) applyPutObjectMeta(data []byte) error {
	c, err := decodePutObjectMetaCmd(data)
	if err != nil {
		return err
	}
	etag := c.ETag
	if c.IsDeleteMarker {
		etag = deleteMarkerETag
	}
	meta, err := marshalObjectMeta(objectMeta{
		Key:          c.Key,
		Size:         c.Size,
		ContentType:  c.ContentType,
		ETag:         etag,
		LastModified: c.ModTime,
		RingVersion:  uint64(c.RingVersion),
		ECData:       c.ECData,
		ECParity:     c.ECParity,
		NodeIDs:      c.NodeIDs,
	})
	if err != nil {
		return fmt.Errorf("marshal object meta: %w", err)
	}
	if err := f.db.Update(func(txn *badger.Txn) error {
		// Versioned entries are only written when a VersionID is supplied. Legacy
		// replay (empty VersionID) gets the single legacy key only.
		if c.VersionID != "" {
			if err := txn.Set(objectMetaKeyV(c.Bucket, c.Key, c.VersionID), meta); err != nil {
				return err
			}
			if c.PreserveLatest {
				return nil
			}
		}
		if c.IsDeleteMarker {
			if c.VersionID != "" {
				if err := txn.Set(latestKey(c.Bucket, c.Key), []byte(c.VersionID)); err != nil {
					return err
				}
			}
			if err := txn.Delete(objectMetaKey(c.Bucket, c.Key)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			return nil
		}
		// Dual-write: keep the legacy latest-only key in sync during the transition
		// so readers that haven't been ported yet still see the object.
		if err := txn.Set(objectMetaKey(c.Bucket, c.Key), meta); err != nil {
			return err
		}
		if c.VersionID != "" {
			if err := txn.Set(latestKey(c.Bucket, c.Key), []byte(c.VersionID)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if c.RingVersion != 0 {
		f.rings.incRef(c.RingVersion)
	}
	return nil
}

func (f *FSM) applyDeleteObject(data []byte) error {
	c, err := decodeDeleteObjectCmd(data)
	if err != nil {
		return err
	}
	// VersionID == "" is a legacy DeleteObjectCmd (no version ID attached by the
	// proposer). Treat it as a hard delete of the legacy latest-only records —
	// preserves prior semantics for pre-versioning log replay.
	if c.VersionID == "" {
		var rv RingVersion
		if err := f.db.Update(func(txn *badger.Txn) error {
			if item, gerr := txn.Get(objectMetaKey(c.Bucket, c.Key)); gerr == nil {
				_ = item.Value(func(v []byte) error {
					if m, derr := unmarshalObjectMeta(v); derr == nil {
						rv = RingVersion(m.RingVersion)
					}
					return nil
				})
			}
			if err := txn.Delete(objectMetaKey(c.Bucket, c.Key)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if err := txn.Delete(shardPlacementKey(c.Bucket, c.Key)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		if rv != 0 {
			f.rings.decRef(rv)
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
	return f.db.Update(func(txn *badger.Txn) error {
		// Read the current latest versionID before overwriting it, so we can
		// delete the versioned placement record that was stored under
		// shardPlacementKey(bucket, key+"/"+prevVersionID).
		prevVersionID := ""
		if latItem, gerr := txn.Get(latestKey(c.Bucket, c.Key)); gerr == nil {
			_ = latItem.Value(func(v []byte) error { prevVersionID = string(v); return nil })
		}

		if err := txn.Set(objectMetaKeyV(c.Bucket, c.Key, c.VersionID), markerMeta); err != nil {
			return err
		}
		if err := txn.Set(latestKey(c.Bucket, c.Key), []byte(c.VersionID)); err != nil {
			return err
		}
		// Legacy latest-only key is removed so HeadObject returns 404 while prior
		// versions remain queryable via GetObjectVersion.
		if err := txn.Delete(objectMetaKey(c.Bucket, c.Key)); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		// Remove the versioned placement record for the previous latest version.
		// Bare-key placement (legacy pre-versioned objects) is also cleaned up.
		if prevVersionID != "" {
			placementKey := shardPlacementKey(c.Bucket, c.Key+"/"+prevVersionID)
			if err := txn.Delete(placementKey); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		if err := txn.Delete(shardPlacementKey(c.Bucket, c.Key)); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		return nil
	})
}

// applyDeleteObjectVersion hard-deletes one specific version and, if it was the
// latest, recomputes the latest pointer from the remaining versions (ULIDs sort
// lexicographically by time, so the max-sorted key wins).
func (f *FSM) applyDeleteObjectVersion(data []byte) error {
	c, err := decodeDeleteObjectVersionCmd(data)
	if err != nil {
		return err
	}
	metaKey := objectMetaKeyV(c.Bucket, c.Key, c.VersionID)
	latKey := latestKey(c.Bucket, c.Key)

	var rv RingVersion
	if err := f.db.View(func(txn *badger.Txn) error {
		item, gerr := txn.Get(metaKey)
		if gerr != nil {
			return gerr
		}
		return item.Value(func(v []byte) error {
			if m, derr := unmarshalObjectMeta(v); derr == nil {
				rv = RingVersion(m.RingVersion)
			}
			return nil
		})
	}); err != nil && err != badger.ErrKeyNotFound {
		return err
	}

	if err := f.db.Update(func(txn *badger.Txn) error {
		if _, gerr := txn.Get(metaKey); gerr == badger.ErrKeyNotFound {
			return nil // idempotent
		} else if gerr != nil {
			return gerr
		}
		if derr := txn.Delete(metaKey); derr != nil {
			return derr
		}
		// Remove the versioned placement record stored under key+"/"+versionID.
		placementKey := shardPlacementKey(c.Bucket, c.Key+"/"+c.VersionID)
		if derr := txn.Delete(placementKey); derr != nil && derr != badger.ErrKeyNotFound {
			return derr
		}

		// If the deleted version was the latest, find the new latest by scanning
		// remaining versioned keys and picking the max (ULID sorts by time ASC).
		if latItem, gerr := txn.Get(latKey); gerr == nil {
			var current string
			_ = latItem.Value(func(v []byte) error { current = string(v); return nil })
			if current == c.VersionID {
				newLatest := ""
				prefix := []byte("obj:" + c.Bucket + "/" + c.Key + "/")
				it := txn.NewIterator(badger.DefaultIteratorOptions)
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					k := string(it.Item().Key())
					vid := k[len(prefix):]
					if vid == "" || vid == c.VersionID {
						continue
					}
					if vid > newLatest {
						newLatest = vid
					}
				}
				it.Close()
				if newLatest == "" {
					if derr := txn.Delete(latKey); derr != nil && derr != badger.ErrKeyNotFound {
						return derr
					}
					// No versions left — drop legacy latest-only key as well.
					if derr := txn.Delete(objectMetaKey(c.Bucket, c.Key)); derr != nil && derr != badger.ErrKeyNotFound {
						return derr
					}
				} else {
					if derr := txn.Set(latKey, []byte(newLatest)); derr != nil {
						return derr
					}
				}
			}
		} else if gerr != badger.ErrKeyNotFound {
			return gerr
		}
		return nil
	}); err != nil {
		return err
	}
	if rv != 0 {
		f.rings.decRef(rv)
	}
	return nil
}

func (f *FSM) applyCreateMultipartUpload(data []byte) error {
	c, err := decodeCreateMultipartUploadCmd(data)
	if err != nil {
		return err
	}
	meta, err := marshalClusterMultipartMeta(clusterMultipartMeta{
		ContentType: c.ContentType,
	})
	if err != nil {
		return fmt.Errorf("marshal multipart meta: %w", err)
	}
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(multipartKey(c.UploadID), meta)
	})
}

func (f *FSM) applyCompleteMultipart(data []byte) error {
	c, err := decodeCompleteMultipartCmd(data)
	if err != nil {
		return err
	}
	objMeta, err := marshalObjectMeta(objectMeta{
		Key:          c.Key,
		Size:         c.Size,
		ContentType:  c.ContentType,
		ETag:         c.ETag,
		LastModified: c.ModTime,
	})
	if err != nil {
		return fmt.Errorf("marshal object meta: %w", err)
	}
	return f.db.Update(func(txn *badger.Txn) error {
		// Dual-write legacy + versioned, same pattern as applyPutObjectMeta.
		if err := txn.Set(objectMetaKey(c.Bucket, c.Key), objMeta); err != nil {
			return err
		}
		if c.VersionID != "" {
			if err := txn.Set(objectMetaKeyV(c.Bucket, c.Key, c.VersionID), objMeta); err != nil {
				return err
			}
			if err := txn.Set(latestKey(c.Bucket, c.Key), []byte(c.VersionID)); err != nil {
				return err
			}
		}
		return txn.Delete(multipartKey(c.UploadID))
	})
}

func (f *FSM) applyAbortMultipart(data []byte) error {
	c, err := decodeAbortMultipartCmd(data)
	if err != nil {
		return err
	}
	return f.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(multipartKey(c.UploadID))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

func (f *FSM) applySetBucketPolicy(data []byte) error {
	c, err := decodeSetBucketPolicyCmd(data)
	if err != nil {
		return err
	}
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(bucketPolicyKey(c.Bucket), c.PolicyJSON)
	})
}

func (f *FSM) applyDeleteBucketPolicy(data []byte) error {
	c, err := decodeDeleteBucketPolicyCmd(data)
	if err != nil {
		return err
	}
	return f.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(bucketPolicyKey(c.Bucket))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	})
}

func (f *FSM) applySetBucketVersioning(data []byte) error {
	c, err := decodeSetBucketVersioningCmd(data)
	if err != nil {
		return err
	}
	return f.db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get(bucketKey(c.Bucket)); err == badger.ErrKeyNotFound {
			return storage.ErrBucketNotFound
		} else if err != nil {
			return err
		}
		return txn.Set(bucketVerKey(c.Bucket), []byte(c.State))
	})
}

func (f *FSM) applySetObjectACL(data []byte) error {
	c, err := decodeSetObjectACLCmd(data)
	if err != nil {
		return err
	}
	legacyKey := objectMetaKey(c.Bucket, c.Key)
	return f.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(legacyKey)
		if err == badger.ErrKeyNotFound {
			return storage.ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			m, merr := unmarshalObjectMeta(val)
			if merr != nil {
				return merr
			}
			m.ACL = c.ACL
			newVal, merr := marshalObjectMeta(m)
			if merr != nil {
				return merr
			}
			if err := txn.Set(legacyKey, newVal); err != nil {
				return err
			}
			// Also update the versioned record if this bucket has versioning enabled.
			latItem, lerr := txn.Get(latestKey(c.Bucket, c.Key))
			if lerr != nil {
				return nil //nolint:nilerr // no versioned key, nothing more to do
			}
			var versionID string
			_ = latItem.Value(func(v []byte) error { versionID = string(v); return nil })
			if versionID == "" {
				return nil
			}
			vKey := objectMetaKeyV(c.Bucket, c.Key, versionID)
			vItem, verr := txn.Get(vKey)
			if verr != nil {
				return nil //nolint:nilerr // versioned record missing, skip
			}
			return vItem.Value(func(vval []byte) error {
				vm, merr := unmarshalObjectMeta(vval)
				if merr != nil {
					return merr
				}
				vm.ACL = c.ACL
				vNewVal, merr := marshalObjectMeta(vm)
				if merr != nil {
					return merr
				}
				return txn.Set(vKey, vNewVal)
			})
		})
	})
}

// pendingMigrationKey returns the BadgerDB key for a not-yet-executed migration task.
func pendingMigrationKey(bucket, key, versionID string) []byte {
	return []byte("pending-migration:" + bucket + "/" + key + "/" + versionID)
}

// persistPendingMigration writes a migration task to BadgerDB so it survives a crash.
func (f *FSM) persistPendingMigration(task MigrationTask) error {
	val, err := encodeMigrateShardCmd(MigrateShardFSMCmd{
		Bucket:    task.Bucket,
		Key:       task.Key,
		VersionID: task.VersionID,
		SrcNode:   task.SrcNode,
		DstNode:   task.DstNode,
	})
	if err != nil {
		return fmt.Errorf("fsm: marshal pending migration: %w", err)
	}
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(pendingMigrationKey(task.Bucket, task.Key, task.VersionID), val)
	})
}

// RecoverPending reads all pending-migration keys from BadgerDB and enqueues them
// to ch for re-execution. Call this at startup AFTER starting the executor goroutine
// so the channel consumer is running before tasks are sent.
func (f *FSM) RecoverPending(ctx context.Context, ch chan<- MigrationTask) error {
	prefix := []byte("pending-migration:")
	return f.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var taskData []byte
			if err := item.Value(func(val []byte) error {
				taskData = make([]byte, len(val))
				copy(taskData, val)
				return nil
			}); err != nil {
				log.Error().Err(err).Msg("fsm: recover pending migration: read failed")
				continue
			}
			cmd, err := decodeMigrateShardCmd(taskData)
			if err != nil {
				log.Error().Err(err).Msg("fsm: recover pending migration: decode failed")
				continue
			}
			task := MigrationTask{
				Bucket:    cmd.Bucket,
				Key:       cmd.Key,
				VersionID: cmd.VersionID,
				SrcNode:   cmd.SrcNode,
				DstNode:   cmd.DstNode,
			}
			select {
			case ch <- task:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
}

func (f *FSM) applyMigrateShard(data []byte) error {
	cmd, err := decodeMigrateShardCmd(data)
	if err != nil {
		return fmt.Errorf("decode MigrateShardCmd: %w", err)
	}
	// Guard: balancer proposals without object identity are placeholders; skip them.
	if cmd.Bucket == "" || cmd.Key == "" {
		return nil
	}
	task := MigrationTask{
		Bucket:    cmd.Bucket,
		Key:       cmd.Key,
		VersionID: cmd.VersionID,
		SrcNode:   cmd.SrcNode,
		DstNode:   cmd.DstNode,
	}
	f.mu.RLock()
	ch := f.onMigrateShard
	f.mu.RUnlock()
	if ch != nil {
		select {
		case ch <- task:
		default:
			// Channel full: persist to BadgerDB so the task survives a crash.
			if err := f.persistPendingMigration(task); err != nil {
				log.Error().Str("bucket", task.Bucket).Str("key", task.Key).Err(err).Msg("fsm: migration queue full and persist failed — task lost")
			} else {
				log.Warn().Str("bucket", task.Bucket).Str("key", task.Key).Msg("fsm: migration queue full, persisted to BadgerDB")
			}
		}
	}
	return nil
}

func (f *FSM) applyMigrationDone(data []byte) error {
	cmd, err := decodeMigrationDoneCmd(data)
	if err != nil {
		return fmt.Errorf("decode MigrationDoneCmd: %w", err)
	}
	// Clean up any persisted pending-migration entry for this task.
	if err := f.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(pendingMigrationKey(cmd.Bucket, cmd.Key, cmd.VersionID))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
	}); err != nil {
		log.Warn().Str("bucket", cmd.Bucket).Str("key", cmd.Key).Err(err).Msg("fsm: delete pending-migration key failed")
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

// Snapshot serializes the full metadata state for Raft snapshots.
func (f *FSM) Snapshot() ([]byte, error) {
	state := make(map[string][]byte)
	err := f.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			state[key] = val
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return marshalSnapshotState(state)
}

// Restore replaces the metadata state from a snapshot.
func (f *FSM) Restore(data []byte) error {
	state, err := unmarshalSnapshotState(data)
	if err != nil {
		return err
	}

	// Drop all existing keys and write snapshot state
	return f.db.Update(func(txn *badger.Txn) error {
		// Delete existing
		it := txn.NewIterator(badger.DefaultIteratorOptions)
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

		// Write snapshot state
		for k, v := range state {
			if err := txn.Set([]byte(k), v); err != nil {
				return err
			}
		}
		return nil
	})
}

// applySetRing commits a new ring snapshot to the in-memory ring store.
// Called when cluster membership changes; version must be monotonically increasing.
func (f *FSM) applySetRing(data []byte) error {
	c, err := decodeSetRingCmd(data)
	if err != nil {
		return fmt.Errorf("decode SetRingCmd: %w", err)
	}
	f.rings.putRing(&Ring{
		Version:  c.Version,
		VNodes:   c.VNodes,
		VPerNode: c.VPerNode,
	})
	return nil
}

// GetRingStore returns the FSM's ring store for use by the backend.
func (f *FSM) GetRingStore() *ringStore { return f.rings }
