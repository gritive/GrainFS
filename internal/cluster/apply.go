package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

// restoreCrashAfterDrop, set only by tests, fires inside FSM.Restore right after DropPrefix
// and before the re-write — simulates a process crash mid-Restore.
var restoreCrashAfterDrop func()

// FSM applies committed Raft log entries to BadgerDB metadata store.
type FSM struct {
	db   *badger.DB
	keys *stateKeyspace
	enc  *encrypt.Encryptor

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
// If keys is nil, newStateKeyspaceEmpty() is used (single-group identity mode).
func NewFSM(db *badger.DB, keys *stateKeyspace) *FSM {
	if keys == nil {
		keys = newStateKeyspaceEmpty()
	}
	return &FSM{db: db, keys: keys, rings: newRingStore()}
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
	case CmdPutObjectQuarantine:
		return f.applyPutObjectQuarantine(cmd.Data)
	default:
		log.Warn().Uint8("type", uint8(cmd.Type)).Msg("fsm: unknown command type")
		return nil
	}
}

func (f *FSM) applyPutObjectQuarantine(data []byte) error {
	c, err := decodePutObjectQuarantineCmd(data)
	if err != nil {
		return err
	}
	if c.Bucket == "" || c.Key == "" {
		return errors.New("quarantine: bucket and key are required")
	}
	value, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(f.keys.QuarantineKey(c.Bucket, c.Key, c.VersionID), value)
	})
}

// Badger key builders. Production code uses the FSM's KeySpace; these
// package-level helpers exist so unit tests can build the same keys without
// instantiating a full FSM. golangci-lint's unused analyzer doesn't traverse
// _test.go callers (run.tests=false), hence the nolint directives.

//nolint:unused // referenced by apply_test.go.
func bucketKey(bucket string) []byte { return []byte("bucket:" + bucket) }

//nolint:unused // referenced by apply_test.go.
func bucketPolicyKey(bucket string) []byte { return []byte("policy:" + bucket) }

//nolint:unused // referenced by apply_test.go.
func bucketVerKey(bucket string) []byte { return []byte("bucketver:" + bucket) }

//nolint:unused // referenced by apply_test.go.
func objectMetaKey(bucket, key string) []byte { return []byte("obj:" + bucket + "/" + key) }

// objectMetaKeyV returns the per-version metadata key:
//
//	obj:{bucket}/{key}/{versionID}
//
// Coexists with the legacy latest-only objectMetaKey during the transition.
//
//nolint:unused // referenced by apply_test.go and scrubbable_test.go.
func objectMetaKeyV(bucket, key, versionID string) []byte {
	return []byte("obj:" + bucket + "/" + key + "/" + versionID)
}

// latestKey points to the current latest version id for an object, or the
// literal "DEL" marker when the object has been tombstoned (soft-deleted).
//
//nolint:unused // referenced by scrubbable_test.go.
func latestKey(bucket, key string) []byte {
	return []byte("lat:" + bucket + "/" + key)
}

//nolint:unused // referenced by apply_test.go.
func multipartKey(uploadID string) []byte { return []byte("mpu:" + uploadID) }

//nolint:unused // referenced by ec_metadata_test.go, scrubber_wiring_test.go, shard_placement_test.go.
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
		return txn.Set(f.keys.BucketKey(c.Bucket), []byte(`{}`))
	})
}

func (f *FSM) applyDeleteBucket(data []byte) error {
	c, err := decodeDeleteBucketCmd(data)
	if err != nil {
		return err
	}
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(f.keys.BucketKey(c.Bucket))
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
		Key:              c.Key,
		Size:             c.Size,
		ContentType:      c.ContentType,
		ETag:             etag,
		LastModified:     c.ModTime,
		RingVersion:      uint64(c.RingVersion),
		ECData:           c.ECData,
		ECParity:         c.ECParity,
		NodeIDs:          c.NodeIDs,
		PlacementGroupID: c.PlacementGroupID,
		UserMetadata:     c.UserMetadata,
		SSEAlgorithm:     c.SSEAlgorithm,
	})
	if err != nil {
		return fmt.Errorf("marshal object meta: %w", err)
	}
	if err := f.db.Update(func(txn *badger.Txn) error {
		// Versioned entries are only written when a VersionID is supplied. Legacy
		// replay (empty VersionID) gets the single legacy key only.
		if c.VersionID != "" {
			if err := f.setValue(txn, f.keys.ObjectMetaKeyV(c.Bucket, c.Key, c.VersionID), meta); err != nil {
				return err
			}
			if c.PreserveLatest {
				return nil
			}
		}
		if c.IsDeleteMarker {
			if c.VersionID != "" {
				if err := txn.Set(f.keys.LatestKey(c.Bucket, c.Key), []byte(c.VersionID)); err != nil {
					return err
				}
			}
			if err := txn.Delete(f.keys.ObjectMetaKey(c.Bucket, c.Key)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			return nil
		}
		// Dual-write: keep the legacy latest-only key in sync during the transition
		// so readers that haven't been ported yet still see the object.
		if err := f.setValue(txn, f.keys.ObjectMetaKey(c.Bucket, c.Key), meta); err != nil {
			return err
		}
		if c.VersionID != "" {
			if err := txn.Set(f.keys.LatestKey(c.Bucket, c.Key), []byte(c.VersionID)); err != nil {
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
			if item, gerr := txn.Get(f.keys.ObjectMetaKey(c.Bucket, c.Key)); gerr == nil {
				if err := item.Value(func(raw []byte) error {
					v, err := f.openValue(item.Key(), raw)
					if err != nil {
						return err
					}
					if m, derr := unmarshalObjectMeta(v); derr == nil {
						rv = RingVersion(m.RingVersion)
					}
					return nil
				}); err != nil {
					return err
				}
			}
			if err := txn.Delete(f.keys.ObjectMetaKey(c.Bucket, c.Key)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			if err := txn.Delete(f.keys.ShardPlacementKey(c.Bucket, c.Key)); err != nil && err != badger.ErrKeyNotFound {
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
		if err := txn.Delete(f.keys.ObjectMetaKey(c.Bucket, c.Key)); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		// Remove the versioned placement record for the previous latest version.
		// Bare-key placement (legacy pre-versioned objects) is also cleaned up.
		if prevVersionID != "" {
			placementKey := f.keys.ShardPlacementKey(c.Bucket, c.Key+"/"+prevVersionID)
			if err := txn.Delete(placementKey); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		if err := txn.Delete(f.keys.ShardPlacementKey(c.Bucket, c.Key)); err != nil && err != badger.ErrKeyNotFound {
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
	metaKey := f.keys.ObjectMetaKeyV(c.Bucket, c.Key, c.VersionID)
	latKey := f.keys.LatestKey(c.Bucket, c.Key)

	var rv RingVersion
	if err := f.db.View(func(txn *badger.Txn) error {
		item, gerr := txn.Get(metaKey)
		if gerr != nil {
			return gerr
		}
		return item.Value(func(raw []byte) error {
			v, err := f.openValue(item.Key(), raw)
			if err != nil {
				return err
			}
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
		placementKey := f.keys.ShardPlacementKey(c.Bucket, c.Key+"/"+c.VersionID)
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
				rawPrefix := []byte("obj:" + c.Bucket + "/" + c.Key + "/")
				if serr := f.keys.scanGroupPrefix(txn, rawPrefix, func(raw []byte, _ *badger.Item) error {
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
					if derr := txn.Delete(latKey); derr != nil && derr != badger.ErrKeyNotFound {
						return derr
					}
					// No versions left — drop legacy latest-only key as well.
					if derr := txn.Delete(f.keys.ObjectMetaKey(c.Bucket, c.Key)); derr != nil && derr != badger.ErrKeyNotFound {
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
		Bucket:           c.Bucket,
		Key:              c.Key,
		CreatedAt:        c.CreatedAt,
		ContentType:      c.ContentType,
		PlacementGroupID: c.PlacementGroupID,
	})
	if err != nil {
		return fmt.Errorf("marshal multipart meta: %w", err)
	}
	return f.db.Update(func(txn *badger.Txn) error {
		return f.setValue(txn, f.keys.MultipartKey(c.UploadID), meta)
	})
}

func (f *FSM) applyCompleteMultipart(data []byte) error {
	c, err := decodeCompleteMultipartCmd(data)
	if err != nil {
		return err
	}
	objMeta, err := marshalObjectMeta(objectMeta{
		Key:              c.Key,
		Size:             c.Size,
		ContentType:      c.ContentType,
		ETag:             c.ETag,
		LastModified:     c.ModTime,
		ECData:           c.ECData,
		ECParity:         c.ECParity,
		NodeIDs:          c.NodeIDs,
		PlacementGroupID: c.PlacementGroupID,
	})
	if err != nil {
		return fmt.Errorf("marshal object meta: %w", err)
	}
	return f.db.Update(func(txn *badger.Txn) error {
		// Dual-write legacy + versioned, same pattern as applyPutObjectMeta.
		if err := f.setValue(txn, f.keys.ObjectMetaKey(c.Bucket, c.Key), objMeta); err != nil {
			return err
		}
		if c.VersionID != "" {
			if err := f.setValue(txn, f.keys.ObjectMetaKeyV(c.Bucket, c.Key, c.VersionID), objMeta); err != nil {
				return err
			}
			if err := txn.Set(f.keys.LatestKey(c.Bucket, c.Key), []byte(c.VersionID)); err != nil {
				return err
			}
		}
		return txn.Delete(f.keys.MultipartKey(c.UploadID))
	})
}

func (f *FSM) applyAbortMultipart(data []byte) error {
	c, err := decodeAbortMultipartCmd(data)
	if err != nil {
		return err
	}
	return f.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(f.keys.MultipartKey(c.UploadID))
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
		return f.setValue(txn, f.keys.BucketPolicyKey(c.Bucket), c.PolicyJSON)
	})
}

func (f *FSM) applyDeleteBucketPolicy(data []byte) error {
	c, err := decodeDeleteBucketPolicyCmd(data)
	if err != nil {
		return err
	}
	return f.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(f.keys.BucketPolicyKey(c.Bucket))
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
		if _, err := txn.Get(f.keys.BucketKey(c.Bucket)); err == badger.ErrKeyNotFound {
			return storage.ErrBucketNotFound
		} else if err != nil {
			return err
		}
		return txn.Set(f.keys.BucketVerKey(c.Bucket), []byte(c.State))
	})
}

func (f *FSM) applySetObjectACL(data []byte) error {
	c, err := decodeSetObjectACLCmd(data)
	if err != nil {
		return err
	}
	legacyKey := f.keys.ObjectMetaKey(c.Bucket, c.Key)
	return f.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(legacyKey)
		if err == badger.ErrKeyNotFound {
			return storage.ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(raw []byte) error {
			val, err := f.openValue(item.Key(), raw)
			if err != nil {
				return err
			}
			m, merr := unmarshalObjectMeta(val)
			if merr != nil {
				return merr
			}
			m.ACL = c.ACL
			newVal, merr := marshalObjectMeta(m)
			if merr != nil {
				return merr
			}
			if err := f.setValue(txn, legacyKey, newVal); err != nil {
				return err
			}
			// Also update the versioned record if this bucket has versioning enabled.
			latItem, lerr := txn.Get(f.keys.LatestKey(c.Bucket, c.Key))
			if lerr != nil {
				return nil //nolint:nilerr // no versioned key, nothing more to do
			}
			var versionID string
			_ = latItem.Value(func(v []byte) error { versionID = string(v); return nil })
			if versionID == "" {
				return nil
			}
			vKey := f.keys.ObjectMetaKeyV(c.Bucket, c.Key, versionID)
			vItem, verr := txn.Get(vKey)
			if verr != nil {
				return nil //nolint:nilerr // versioned record missing, skip
			}
			return vItem.Value(func(raw []byte) error {
				vval, err := f.openValue(vItem.Key(), raw)
				if err != nil {
					return err
				}
				vm, merr := unmarshalObjectMeta(vval)
				if merr != nil {
					return merr
				}
				vm.ACL = c.ACL
				vNewVal, merr := marshalObjectMeta(vm)
				if merr != nil {
					return merr
				}
				return f.setValue(txn, vKey, vNewVal)
			})
		})
	})
}

// pendingMigrationKey returns the BadgerDB key for a not-yet-executed migration task.
//
//nolint:unused // referenced by apply_test.go.
func pendingMigrationKey(bucket, key, versionID string) []byte {
	return []byte("pending-migration:" + bucket + "/" + key + "/" + versionID)
}

// persistPendingMigration writes a migration task to BadgerDB so it survives a crash.
func (f *FSM) persistPendingMigration(task MigrationTask) error {
	val, err := encodeMigrateShardCmd(MigrateShardFSMCmd(task))
	if err != nil {
		return fmt.Errorf("fsm: marshal pending migration: %w", err)
	}
	return f.db.Update(func(txn *badger.Txn) error {
		return txn.Set(f.keys.PendingMigrationKey(task.Bucket, task.Key, task.VersionID), val)
	})
}

// RecoverPending reads all pending-migration keys from BadgerDB and enqueues them
// to ch for re-execution. Call this at startup AFTER starting the executor goroutine
// so the channel consumer is running before tasks are sent.
func (f *FSM) RecoverPending(ctx context.Context, ch chan<- MigrationTask) error {
	return f.db.View(func(txn *badger.Txn) error {
		return f.keys.scanGroupPrefix(txn, []byte("pending-migration:"), func(_ []byte, item *badger.Item) error {
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

func (f *FSM) applyMigrateShard(data []byte) error {
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
		err := txn.Delete(f.keys.PendingMigrationKey(cmd.Bucket, cmd.Key, cmd.VersionID))
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

// Snapshot serializes this group's metadata state for Raft snapshots. Keys in
// the snapshot blob are GROUP-RELATIVE (the group prefix is stripped); Restore
// re-applies the prefix on write. For the empty keyspace the group prefix is nil,
// so this is a whole-DB scan exactly as before.
func (f *FSM) Snapshot() ([]byte, error) {
	state := make(map[string][]byte)
	err := f.db.View(func(txn *badger.Txn) error {
		return f.keys.scanGroupPrefix(txn, nil, func(rawKey []byte, item *badger.Item) error {
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
	return f.db.Update(func(txn *badger.Txn) error {
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
	return f.db.Update(func(txn *badger.Txn) error {
		for k, v := range state {
			if err := txn.Set(f.keys.Key([]byte(k)), v); err != nil {
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
