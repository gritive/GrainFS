package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// FSM applies committed Raft log entries to BadgerDB metadata store.
type FSM struct {
	db *badger.DB

	// Guards onMigrateShard and commitNotifier against concurrent SetMigrationHooks + Apply.
	mu sync.RWMutex

	// Optional hooks for Phase 13 balancer. Nil when no peers configured/non-balancer mode.
	// onMigrateShard is a buffered channel; Apply sends non-blocking to avoid blocking the Raft apply loop.
	onMigrateShard  chan<- MigrationTask
	commitNotifier  interface {
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
func NewFSM(db *badger.DB) *FSM {
	return &FSM{db: db}
}

// Apply processes a committed command and updates the metadata store.
func (f *FSM) Apply(raw []byte) error {
	cmd, err := DecodeCommand(raw)
	if err != nil {
		return fmt.Errorf("decode command: %w", err)
	}

	switch cmd.Type {
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
		return f.applyPutShardPlacement(cmd.Data)
	case CmdDeleteShardPlacement:
		return f.applyDeleteShardPlacement(cmd.Data)
	default:
		slog.Warn("fsm: unknown command type", "type", cmd.Type)
		return nil
	}
}

func bucketKey(bucket string) []byte       { return []byte("bucket:" + bucket) }
func bucketPolicyKey(bucket string) []byte { return []byte("policy:" + bucket) }
func objectMetaKey(bucket, key string) []byte { return []byte("obj:" + bucket + "/" + key) }
func multipartKey(uploadID string) []byte     { return []byte("mpu:" + uploadID) }
func shardPlacementKey(bucket, key string) []byte {
	return []byte("placement:" + bucket + "/" + key)
}

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
	meta, err := marshalObjectMeta(objectMeta{
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
		return txn.Set(objectMetaKey(c.Bucket, c.Key), meta)
	})
}

func (f *FSM) applyDeleteObject(data []byte) error {
	c, err := decodeDeleteObjectCmd(data)
	if err != nil {
		return err
	}
	return f.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(objectMetaKey(c.Bucket, c.Key)); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		// Cascade to shard placement if present (Phase 18 Cluster EC). ErrKeyNotFound
		// is expected for N×-replication objects with no placement record.
		if err := txn.Delete(shardPlacementKey(c.Bucket, c.Key)); err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		return nil
	})
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
		if err := txn.Set(objectMetaKey(c.Bucket, c.Key), objMeta); err != nil {
			return err
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
				slog.Error("fsm: recover pending migration: read failed", "err", err)
				continue
			}
			cmd, err := decodeMigrateShardCmd(taskData)
			if err != nil {
				slog.Error("fsm: recover pending migration: decode failed", "err", err)
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
				slog.Error("fsm: migration queue full and persist failed — task lost",
					"bucket", task.Bucket, "key", task.Key, "err", err)
			} else {
				slog.Warn("fsm: migration queue full, persisted to BadgerDB",
					"bucket", task.Bucket, "key", task.Key)
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
		slog.Warn("fsm: delete pending-migration key failed", "bucket", cmd.Bucket, "key", cmd.Key, "err", err)
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
