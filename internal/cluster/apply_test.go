package cluster

import (
	"encoding/json"
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
	var meta map[string]any
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKey("b", "hello.txt"))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &meta)
		})
	})
	require.NoError(t, err)
	assert.Equal(t, "hello.txt", meta["Key"])
	assert.Equal(t, float64(42), meta["Size"])
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
