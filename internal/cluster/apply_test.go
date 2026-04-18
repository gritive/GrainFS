package cluster

import (
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDB(t *testing.T) *badger.DB {
	t.Helper()
	dir, err := os.MkdirTemp("", "fsm-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })

	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestFSM_CreateBucket(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	data, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "test-bucket"})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	// Verify bucket exists in DB
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey("test-bucket"))
		return err
	})
	assert.NoError(t, err)
}

func TestFSM_DeleteBucket(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Create then delete
	data, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "to-delete"})
	require.NoError(t, fsm.Apply(data))

	data, _ = EncodeCommand(CmdDeleteBucket, DeleteBucketCmd{Bucket: "to-delete"})
	require.NoError(t, fsm.Apply(data))

	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey("to-delete"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestFSM_PutObjectMeta(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	data, _ := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      "b",
		Key:         "hello.txt",
		Size:        42,
		ContentType: "text/plain",
		ETag:        "abc123",
		ModTime:     1000,
	})
	require.NoError(t, fsm.Apply(data))

	// Verify
	var meta objectMeta
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKey("b", "hello.txt"))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			meta = m
			return nil
		})
	})
	require.NoError(t, err)
	assert.Equal(t, "hello.txt", meta.Key)
	assert.Equal(t, int64(42), meta.Size)
}

func TestFSM_DeleteObject(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Put then delete
	data, _ := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: "b", Key: "del.txt", Size: 1, ContentType: "text/plain", ETag: "e", ModTime: 1,
	})
	require.NoError(t, fsm.Apply(data))

	data, _ = EncodeCommand(CmdDeleteObject, DeleteObjectCmd{Bucket: "b", Key: "del.txt"})
	require.NoError(t, fsm.Apply(data))

	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(objectMetaKey("b", "del.txt"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestFSM_SnapshotRestore(t *testing.T) {
	db1 := newTestDB(t)
	fsm1 := NewFSM(db1)

	// Apply some commands
	data, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "snap-bucket"})
	require.NoError(t, fsm1.Apply(data))
	data, _ = EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: "snap-bucket", Key: "file.txt", Size: 10, ContentType: "text/plain", ETag: "e", ModTime: 1,
	})
	require.NoError(t, fsm1.Apply(data))

	// Take snapshot
	snap, err := fsm1.Snapshot()
	require.NoError(t, err)

	// Restore to a new DB
	db2 := newTestDB(t)
	fsm2 := NewFSM(db2)
	require.NoError(t, fsm2.Restore(snap))

	// Verify state
	err = db2.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey("snap-bucket"))
		if err != nil {
			return err
		}
		_, err = txn.Get(objectMetaKey("snap-bucket", "file.txt"))
		return err
	})
	assert.NoError(t, err)
}

func TestFSM_MultipartCycle(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Create multipart
	data, _ := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID: "upload-1", Bucket: "b", Key: "mp.bin", ContentType: "application/octet-stream", CreatedAt: 100,
	})
	require.NoError(t, fsm.Apply(data))

	// Verify exists
	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(multipartKey("upload-1"))
		return err
	})
	assert.NoError(t, err)

	// Complete multipart
	data, _ = EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket: "b", Key: "mp.bin", UploadID: "upload-1", Size: 1024,
		ContentType: "application/octet-stream", ETag: "final-etag", ModTime: 200,
	})
	require.NoError(t, fsm.Apply(data))

	// Upload record should be gone
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(multipartKey("upload-1"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)

	// Object should exist
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(objectMetaKey("b", "mp.bin"))
		return err
	})
	assert.NoError(t, err)
}

func TestFSM_AbortMultipart(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	data, _ := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID: "upload-abort", Bucket: "b", Key: "abort.bin", ContentType: "binary", CreatedAt: 100,
	})
	require.NoError(t, fsm.Apply(data))

	data, _ = EncodeCommand(CmdAbortMultipart, AbortMultipartCmd{
		Bucket: "b", Key: "abort.bin", UploadID: "upload-abort",
	})
	require.NoError(t, fsm.Apply(data))

	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(multipartKey("upload-abort"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestFSM_SetBucketPolicy(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	policyJSON := []byte(`{"Version":"2012-10-17","Statement":[]}`)

	data, err := EncodeCommand(CmdSetBucketPolicy, SetBucketPolicyCmd{
		Bucket:     "policy-bucket",
		PolicyJSON: policyJSON,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	// Verify policy is stored in DB
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(bucketPolicyKey("policy-bucket"))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			assert.Equal(t, policyJSON, val)
			return nil
		})
	})
	assert.NoError(t, err)
}

func TestFSM_DeleteBucketPolicy(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Set a policy first
	policyJSON := []byte(`{"Version":"2012-10-17"}`)
	data, _ := EncodeCommand(CmdSetBucketPolicy, SetBucketPolicyCmd{
		Bucket: "bp", PolicyJSON: policyJSON,
	})
	require.NoError(t, fsm.Apply(data))

	// Delete the policy
	data, _ = EncodeCommand(CmdDeleteBucketPolicy, DeleteBucketPolicyCmd{Bucket: "bp"})
	require.NoError(t, fsm.Apply(data))

	// Verify policy is removed
	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketPolicyKey("bp"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestFSM_DeleteBucketPolicy_NotExist(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Deleting a non-existent policy should not error (ErrKeyNotFound → nil)
	data, _ := EncodeCommand(CmdDeleteBucketPolicy, DeleteBucketPolicyCmd{Bucket: "no-policy"})
	err := fsm.Apply(data)
	assert.NoError(t, err)
}

func TestFSM_AbortMultipart_NotExist(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Aborting a non-existent multipart should not error (ErrKeyNotFound → nil)
	data, _ := EncodeCommand(CmdAbortMultipart, AbortMultipartCmd{
		Bucket: "b", Key: "nope.bin", UploadID: "nonexistent",
	})
	err := fsm.Apply(data)
	assert.NoError(t, err)
}

func TestFSM_DeleteObject_NotExist(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Deleting a non-existent object should not error (ErrKeyNotFound → nil)
	data, _ := EncodeCommand(CmdDeleteObject, DeleteObjectCmd{Bucket: "b", Key: "nope.txt"})
	err := fsm.Apply(data)
	assert.NoError(t, err)
}

func TestFSM_Apply_CorruptData(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	err := fsm.Apply([]byte("definitely not protobuf"))
	assert.Error(t, err, "Apply should fail on corrupt data")
}

func TestFSM_Restore_CorruptData(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	err := fsm.Restore([]byte("not valid protobuf snapshot"))
	assert.Error(t, err, "Restore should fail on corrupt snapshot data")
}

func TestFSM_SnapshotRestore_WithExistingData(t *testing.T) {
	// Test Restore overwrites existing data in the target DB
	db1 := newTestDB(t)
	fsm1 := NewFSM(db1)

	data, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "src-bucket"})
	require.NoError(t, fsm1.Apply(data))

	snap, err := fsm1.Snapshot()
	require.NoError(t, err)

	// Create a second DB with different data
	db2 := newTestDB(t)
	fsm2 := NewFSM(db2)
	data, _ = EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "old-bucket"})
	require.NoError(t, fsm2.Apply(data))

	// Restore overwrites db2
	require.NoError(t, fsm2.Restore(snap))

	// old-bucket should be gone, src-bucket should exist
	err = db2.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey("src-bucket"))
		return err
	})
	assert.NoError(t, err)

	err = db2.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey("old-bucket"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestFSM_MigrateShard_FiresCallback(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	ch := make(chan MigrationTask, 1)
	fsm.SetMigrationHooks(ch, nil)

	data, err := EncodeCommand(CmdMigrateShard, MigrateShardFSMCmd{
		Bucket:    "my-bucket",
		Key:       "my-key",
		VersionID: "v1",
		SrcNode:   "node-a",
		DstNode:   "node-b",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	var received MigrationTask
	select {
	case received = <-ch:
	default:
		t.Fatal("expected migration task on channel")
	}
	assert.Equal(t, "my-bucket", received.Bucket)
	assert.Equal(t, "node-b", received.DstNode)
}

func TestFSM_MigrationDone_NotifiesCommit(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	notified := make(chan struct{}, 1)
	fsm.SetMigrationHooks(nil, &migrationDoneNotifier{fn: func(bucket, key, versionID string) {
		notified <- struct{}{}
	}})

	data, err := EncodeCommand(CmdMigrationDone, MigrationDoneFSMCmd{
		Bucket:    "my-bucket",
		Key:       "my-key",
		VersionID: "v1",
		SrcNode:   "node-a",
		DstNode:   "node-b",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	select {
	case <-notified:
	default:
		t.Fatal("NotifyCommit was not called")
	}
}

// migrationDoneNotifier is a test helper implementing the commitNotifier interface.
type migrationDoneNotifier struct {
	fn func(bucket, key, versionID string)
}

func (n *migrationDoneNotifier) NotifyCommit(bucket, key, versionID string) {
	n.fn(bucket, key, versionID)
}

// --- F2: pending migration persistence tests ---

func TestFSM_MigrateShard_ChannelFull_PersistsToDB(t *testing.T) {
	// When migration channel is full, applyMigrateShard should persist the task
	// to BadgerDB under "pending-migration:" key.
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Zero-capacity channel — always full
	ch := make(chan MigrationTask, 0)
	fsm.SetMigrationHooks(ch, nil)

	data, err := EncodeCommand(CmdMigrateShard, MigrateShardFSMCmd{
		Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "src", DstNode: "dst",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	// Verify persisted to BadgerDB
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(pendingMigrationKey("b", "k", "v1"))
		return err
	})
	assert.NoError(t, err, "task should be persisted to BadgerDB when channel is full")
}

func TestFSM_RecoverPending_ReplaysTasks(t *testing.T) {
	// RecoverPending reads all pending-migration keys and sends them to the channel.
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Manually write a pending-migration key to simulate a crash after persistence
	task := MigrationTask{Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "src", DstNode: "dst"}
	require.NoError(t, fsm.persistPendingMigration(task))

	ch := make(chan MigrationTask, 10)
	require.NoError(t, fsm.RecoverPending(ch))

	var received MigrationTask
	select {
	case received = <-ch:
	default:
		t.Fatal("expected task on channel after RecoverPending")
	}
	assert.Equal(t, "b", received.Bucket)
	assert.Equal(t, "k", received.Key)
	assert.Equal(t, "v1", received.VersionID)
}

func TestFSM_MigrationDone_DeletesPendingKey(t *testing.T) {
	// applyMigrationDone should delete the pending-migration key from BadgerDB.
	db := newTestDB(t)
	fsm := NewFSM(db)

	// Pre-write a pending-migration entry
	task := MigrationTask{Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "src", DstNode: "dst"}
	require.NoError(t, fsm.persistPendingMigration(task))

	// Apply CmdMigrationDone — should clean up the key
	data, err := EncodeCommand(CmdMigrationDone, MigrationDoneFSMCmd{
		Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "src", DstNode: "dst",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	// Verify key is gone
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(pendingMigrationKey("b", "k", "v1"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound, "pending-migration key should be deleted after CmdMigrationDone")
}

func TestFSM_RecoverPending_EmptyDB_NoOp(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)
	ch := make(chan MigrationTask, 10)
	require.NoError(t, fsm.RecoverPending(ch))
	assert.Empty(t, ch)
}
