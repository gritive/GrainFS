package cluster

import (
	"fmt"
	"log"

	"github.com/dgraph-io/badger/v4"
)

// FSM applies committed Raft log entries to BadgerDB metadata store.
type FSM struct {
	db *badger.DB
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
	default:
		log.Printf("fsm: unknown command type %d", cmd.Type)
		return nil
	}
}

func bucketKey(bucket string) []byte       { return []byte("bucket:" + bucket) }
func bucketPolicyKey(bucket string) []byte { return []byte("policy:" + bucket) }
func objectMetaKey(bucket, key string) []byte { return []byte("obj:" + bucket + "/" + key) }
func multipartKey(uploadID string) []byte     { return []byte("mpu:" + uploadID) }

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
		err := txn.Delete(objectMetaKey(c.Bucket, c.Key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		return err
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
