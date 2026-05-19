package cluster

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
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
	fsm := NewFSM(db, newStateKeyspaceEmpty())

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

func TestFSM_EncryptedValuesHideObjectMultipartAndPolicyPayloads(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x46}, 32))
	require.NoError(t, err)
	fsm.SetEncryptor(enc)

	putData, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      "b",
		Key:         "secret-object",
		ContentType: "text/secret",
		UserMetadata: map[string]string{
			"x-amz-meta-secret": "customer-private-metadata",
		},
		VersionID: "v1",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(putData))

	mpuData, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID:    "upload-secret",
		Bucket:      "b",
		Key:         "secret-object",
		ContentType: "application/private",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(mpuData))

	policy := []byte(`{"Statement":[{"Resource":"secret-policy-resource"}]}`)
	policyData, err := EncodeCommand(CmdSetBucketPolicy, SetBucketPolicyCmd{Bucket: "b", PolicyJSON: policy})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(policyData))

	err = db.View(func(txn *badger.Txn) error {
		for _, tc := range []struct {
			key       []byte
			forbidden string
		}{
			{fsm.keys.ObjectMetaKeyV("b", "secret-object", "v1"), "customer-private-metadata"},
			{fsm.keys.MultipartKey("upload-secret"), "application/private"},
			{fsm.keys.BucketPolicyKey("b"), "secret-policy-resource"},
		} {
			item, err := txn.Get(tc.key)
			require.NoError(t, err)
			raw, err := item.ValueCopy(nil)
			require.NoError(t, err)
			require.True(t, encrypt.IsEncryptedValue(raw))
			require.NotContains(t, string(raw), tc.forbidden)

			plain, err := fsm.itemValueCopy(item)
			require.NoError(t, err)
			require.Contains(t, string(plain), tc.forbidden)
		}
		return nil
	})
	require.NoError(t, err)
}

func TestFSM_DeleteObjectRejectsCorruptEncryptedMeta(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{0x47}, 32))
	require.NoError(t, err)
	fsm.SetEncryptor(enc)

	putData, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: "b",
		Key:    "tampered",
		ETag:   "etag",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(putData))

	metaKey := fsm.keys.ObjectMetaKey("b", "tampered")
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey)
		if err != nil {
			return err
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		require.True(t, encrypt.IsEncryptedValue(raw))
		raw[len(raw)-1] ^= 0x01
		return txn.Set(metaKey, raw)
	}))

	deleteData, err := EncodeCommand(CmdDeleteObject, DeleteObjectCmd{Bucket: "b", Key: "tampered"})
	require.NoError(t, err)
	require.Error(t, fsm.Apply(deleteData))

	require.NoError(t, db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(metaKey)
		return err
	}))
}

func TestFSM_DeleteBucket(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())

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
	fsm1 := NewFSM(db1, newStateKeyspaceEmpty())

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
	fsm2 := NewFSM(db2, newStateKeyspaceEmpty())
	require.NoError(t, fsm2.Restore(raft.SnapshotMeta{FormatVersion: raft.FSMSnapshotFormatVersion}, snap))

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())

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

func TestFSM_CompleteMultipartPersistsPartsSegmentsAndDeletesUpload(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	data, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID: "upload-layout", Bucket: "b", Key: "mp.bin", ContentType: "application/octet-stream", CreatedAt: 100,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	parts := []storage.MultipartPartEntry{
		{PartNumber: 1, Size: 5 << 20, ETag: "part-1"},
		{PartNumber: 2, Size: 7 << 20, ETag: "part-2"},
	}
	tags := []storage.Tag{{Key: "env", Value: "prod"}}
	segments := []SegmentMetaEntry{
		{
			BlobID:           "blob-0",
			Size:             6 << 20,
			Checksum:         []byte{0x01, 0x02},
			PlacementGroupID: "group-a",
			ShardSize:        4 << 20,
			SegmentIdx:       0,
			NodeIDs:          []string{"n1", "n2", "n3"},
			ECData:           2,
			ECParity:         1,
			RingVersion:      11,
		},
		{
			BlobID:           "blob-1",
			Size:             6 << 20,
			Checksum:         []byte{0x03, 0x04},
			PlacementGroupID: "group-b",
			ShardSize:        4 << 20,
			SegmentIdx:       1,
			NodeIDs:          []string{"n4", "n5", "n6"},
			ECData:           2,
			ECParity:         1,
			RingVersion:      12,
		},
	}

	data, err = EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket:           "b",
		Key:              "mp.bin",
		UploadID:         "upload-layout",
		Size:             12 << 20,
		ContentType:      "application/octet-stream",
		ETag:             "complete-etag",
		ModTime:          200,
		VersionID:        "v1",
		PlacementGroupID: "group-a",
		ECData:           2,
		ECParity:         1,
		NodeIDs:          []string{"n1", "n2", "n3"},
		RingVersion:      17,
		Parts:            parts,
		Segments:         segments,
		Tags:             tags,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(multipartKey("upload-layout"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)

		item, err := txn.Get(objectMetaKey("b", "mp.bin"))
		require.NoError(t, err)
		raw, err := item.ValueCopy(nil)
		require.NoError(t, err)
		meta, err := unmarshalObjectMeta(raw)
		require.NoError(t, err)
		require.Equal(t, parts, meta.Parts)
		require.Equal(t, segmentMetaEntriesToRefs(segments), meta.Segments)
		require.Equal(t, uint64(17), meta.RingVersion)
		require.Equal(t, tags, meta.Tags)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), fsm.GetRingStore().refCount[RingVersion(17)])
}

func TestFSM_CompleteMultipartRejectsDuplicateApply(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	data, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID: "upload-once", Bucket: "b", Key: "mp.bin", ContentType: "application/octet-stream", CreatedAt: 100,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	first, err := EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket: "b", Key: "mp.bin", UploadID: "upload-once", Size: 1024,
		ContentType: "application/octet-stream", ETag: "first-etag", ModTime: 200, VersionID: "v1",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(first))

	duplicate, err := EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket: "b", Key: "mp.bin", UploadID: "upload-once", Size: 2048,
		ContentType: "application/octet-stream", ETag: "second-etag", ModTime: 300, VersionID: "v2",
	})
	require.NoError(t, err)
	require.ErrorIs(t, fsm.Apply(duplicate), storage.ErrUploadNotFound)

	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKey("b", "mp.bin"))
		require.NoError(t, err)
		raw, err := item.ValueCopy(nil)
		require.NoError(t, err)
		meta, err := unmarshalObjectMeta(raw)
		require.NoError(t, err)
		require.Equal(t, "first-etag", meta.ETag)

		item, err = txn.Get(fsm.keys.LatestKey("b", "mp.bin"))
		require.NoError(t, err)
		latest, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, "v1", string(latest))

		_, err = txn.Get(objectMetaKeyV("b", "mp.bin", "v2"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

func TestFSM_CompleteMultipartRejectsUploadMismatch(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	data, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID: "upload-other", Bucket: "b", Key: "expected.bin", ContentType: "application/octet-stream", CreatedAt: 100,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	data, err = EncodeCommand(CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket: "b", Key: "wrong.bin", UploadID: "upload-other", Size: 1024,
		ContentType: "application/octet-stream", ETag: "etag", ModTime: 200, VersionID: "v1",
	})
	require.NoError(t, err)
	require.Error(t, fsm.Apply(data))

	require.NoError(t, db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(fsm.keys.MultipartKey("upload-other"))
		require.NoError(t, err)
		_, err = txn.Get(objectMetaKey("b", "wrong.bin"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

func TestFSM_CreateMultipartUploadPersistsListingMetadata(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	data, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID:         "upload-listing",
		Bucket:           "bucket",
		Key:              "prefix/mp.bin",
		ContentType:      "application/octet-stream",
		CreatedAt:        123456,
		PlacementGroupID: "group-7",
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(multipartKey("upload-listing"))
		require.NoError(t, err)
		raw, err := item.ValueCopy(nil)
		require.NoError(t, err)
		meta, err := unmarshalClusterMultipartMeta(raw)
		require.NoError(t, err)
		require.Equal(t, "bucket", meta.Bucket)
		require.Equal(t, "prefix/mp.bin", meta.Key)
		require.Equal(t, int64(123456), meta.CreatedAt)
		require.Equal(t, "application/octet-stream", meta.ContentType)
		require.Equal(t, "group-7", meta.PlacementGroupID)
		return nil
	})
	require.NoError(t, err)
}

func TestFSM_AbortMultipart(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	// Deleting a non-existent policy should not error (ErrKeyNotFound → nil)
	data, _ := EncodeCommand(CmdDeleteBucketPolicy, DeleteBucketPolicyCmd{Bucket: "no-policy"})
	err := fsm.Apply(data)
	assert.NoError(t, err)
}

func TestFSM_AbortMultipart_NotExist(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	// Aborting a non-existent multipart should not error (ErrKeyNotFound → nil)
	data, _ := EncodeCommand(CmdAbortMultipart, AbortMultipartCmd{
		Bucket: "b", Key: "nope.bin", UploadID: "nonexistent",
	})
	err := fsm.Apply(data)
	assert.NoError(t, err)
}

func TestFSM_DeleteObject_NotExist(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	// Deleting a non-existent object should not error (ErrKeyNotFound → nil)
	data, _ := EncodeCommand(CmdDeleteObject, DeleteObjectCmd{Bucket: "b", Key: "nope.txt"})
	err := fsm.Apply(data)
	assert.NoError(t, err)
}

func TestFSM_Apply_CorruptData(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	err := fsm.Apply([]byte("definitely not protobuf"))
	assert.Error(t, err, "Apply should fail on corrupt data")
}

func TestFSM_Restore_CorruptData(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	err := fsm.Restore(raft.SnapshotMeta{FormatVersion: raft.FSMSnapshotFormatVersion}, []byte("not valid protobuf snapshot"))
	assert.Error(t, err, "Restore should fail on corrupt snapshot data")
}

func TestFSM_SnapshotRestore_WithExistingData(t *testing.T) {
	// Test Restore overwrites existing data in the target DB
	db1 := newTestDB(t)
	fsm1 := NewFSM(db1, newStateKeyspaceEmpty())

	data, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "src-bucket"})
	require.NoError(t, fsm1.Apply(data))

	snap, err := fsm1.Snapshot()
	require.NoError(t, err)

	// Create a second DB with different data
	db2 := newTestDB(t)
	fsm2 := NewFSM(db2, newStateKeyspaceEmpty())
	data, _ = EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "old-bucket"})
	require.NoError(t, fsm2.Apply(data))

	// Restore overwrites db2
	require.NoError(t, fsm2.Restore(raft.SnapshotMeta{FormatVersion: raft.FSMSnapshotFormatVersion}, snap))

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	ch := make(chan MigrationTask, 1)
	fsm.SetMigrationHooks(ch, nil, nil)

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	notified := make(chan struct{}, 1)
	fsm.SetMigrationHooks(nil, &migrationDoneNotifier{fn: func(bucket, key, versionID string) {
		notified <- struct{}{}
	}}, nil)

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	// Zero-capacity channel — always full
	ch := make(chan MigrationTask, 0)
	fsm.SetMigrationHooks(ch, nil, nil)

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	// Manually write a pending-migration key to simulate a crash after persistence
	task := MigrationTask{Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "src", DstNode: "dst"}
	require.NoError(t, fsm.persistPendingMigration(task))

	ch := make(chan MigrationTask, 10)
	require.NoError(t, fsm.RecoverPending(context.Background(), ch))

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())

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
	fsm := NewFSM(db, newStateKeyspaceEmpty())
	ch := make(chan MigrationTask, 10)
	require.NoError(t, fsm.RecoverPending(context.Background(), ch))
	assert.Empty(t, ch)
}

func TestFSM_SetBucketVersioning(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	// Bucket must exist first.
	data, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "vbucket"})
	require.NoError(t, fsm.Apply(data))

	data, err := EncodeCommand(CmdSetBucketVersioning, SetBucketVersioningCmd{Bucket: "vbucket", State: "Enabled"})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	var state string
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(bucketVerKey("vbucket"))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error { state = string(v); return nil })
	}))
	assert.Equal(t, "Enabled", state)
}

func TestFSM_SetBucketVersioning_NoBucket(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	data, _ := EncodeCommand(CmdSetBucketVersioning, SetBucketVersioningCmd{Bucket: "ghost", State: "Enabled"})
	err := fsm.Apply(data)
	assert.Error(t, err, "should fail when bucket does not exist")
}

func TestFSM_SetObjectACL(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	// Write object meta first.
	data, _ := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: "b", Key: "file.txt", Size: 10, ETag: "etag1", ModTime: 1000,
	})
	require.NoError(t, fsm.Apply(data))

	const aclPublicRead uint8 = 2
	data, err := EncodeCommand(CmdSetObjectACL, SetObjectACLCmd{Bucket: "b", Key: "file.txt", ACL: aclPublicRead})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	var m objectMeta
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKey("b", "file.txt"))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			m, err = unmarshalObjectMeta(val)
			return err
		})
	}))
	assert.Equal(t, aclPublicRead, m.ACL)
}

func TestFSM_SetObjectACL_NotFound(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	data, _ := EncodeCommand(CmdSetObjectACL, SetObjectACLCmd{Bucket: "b", Key: "ghost.txt", ACL: 2})
	err := fsm.Apply(data)
	assert.Error(t, err, "should fail when object does not exist")
}

func TestFSM_SetObjectACL_VersionedBucket(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	// Create bucket first.
	data, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "b"})
	require.NoError(t, fsm.Apply(data))

	// Enable versioning on the bucket.
	data, _ = EncodeCommand(CmdSetBucketVersioning, SetBucketVersioningCmd{Bucket: "b", State: "Enabled"})
	require.NoError(t, fsm.Apply(data))

	const vid = "v1"
	const aclPublicRead uint8 = 2

	// Write versioned object meta (dual-write: legacy + versioned key).
	data, _ = EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: "b", Key: "file.txt", Size: 5, ETag: "etag-v", ModTime: 2000, VersionID: vid,
	})
	require.NoError(t, fsm.Apply(data))

	// Set ACL — should update both legacy and versioned key.
	data, err := EncodeCommand(CmdSetObjectACL, SetObjectACLCmd{Bucket: "b", Key: "file.txt", ACL: aclPublicRead})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	var legacyACL, versionedACL uint8
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKey("b", "file.txt"))
		if err != nil {
			return err
		}
		if err := item.Value(func(val []byte) error {
			m, merr := unmarshalObjectMeta(val)
			if merr != nil {
				return merr
			}
			legacyACL = m.ACL
			return nil
		}); err != nil {
			return err
		}

		vItem, err := txn.Get(objectMetaKeyV("b", "file.txt", vid))
		if err != nil {
			return err
		}
		return vItem.Value(func(val []byte) error {
			m, merr := unmarshalObjectMeta(val)
			if merr != nil {
				return merr
			}
			versionedACL = m.ACL
			return nil
		})
	}))
	assert.Equal(t, aclPublicRead, legacyACL, "legacy key ACL")
	assert.Equal(t, aclPublicRead, versionedACL, "versioned key ACL")
}

func TestFSM_SetObjectTags(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	// Seed an object.
	data, _ := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: "b", Key: "file.txt", Size: 10, ETag: "etag1", ModTime: 1000,
	})
	require.NoError(t, fsm.Apply(data))

	tags := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}}
	data, err := EncodeCommand(CmdSetObjectTags, SetObjectTagsCmd{Bucket: "b", Key: "file.txt", Tags: tags})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	var m objectMeta
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKey("b", "file.txt"))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			m, err = unmarshalObjectMeta(val)
			return err
		})
	}))
	assert.Equal(t, tags, m.Tags, "tags should be updated")
	assert.Equal(t, "etag1", m.ETag, "ETag must be unchanged")
	assert.Equal(t, int64(10), m.Size, "Size must be unchanged")
}

func TestFSM_SetObjectTags_NotFound(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	data, _ := EncodeCommand(CmdSetObjectTags, SetObjectTagsCmd{Bucket: "b", Key: "ghost.txt", Tags: nil})
	err := fsm.Apply(data)
	assert.Error(t, err, "should fail when object does not exist")
}

func TestFSM_SetObjectTags_VersionedBucket(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	// Create bucket and enable versioning.
	data, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "b"})
	require.NoError(t, fsm.Apply(data))
	data, _ = EncodeCommand(CmdSetBucketVersioning, SetBucketVersioningCmd{Bucket: "b", State: "Enabled"})
	require.NoError(t, fsm.Apply(data))

	const vid = "v1"
	data, _ = EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: "b", Key: "file.txt", Size: 5, ETag: "etag-v", ModTime: 2000, VersionID: vid,
	})
	require.NoError(t, fsm.Apply(data))

	tags := []storage.Tag{{Key: "env", Value: "staging"}}
	data, err := EncodeCommand(CmdSetObjectTags, SetObjectTagsCmd{Bucket: "b", Key: "file.txt", Tags: tags})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	// Both legacy and versioned records should have the tags.
	var legacyTags, versionedTags []storage.Tag
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKey("b", "file.txt"))
		if err != nil {
			return err
		}
		if err := item.Value(func(val []byte) error {
			m, merr := unmarshalObjectMeta(val)
			if merr != nil {
				return merr
			}
			legacyTags = m.Tags
			return nil
		}); err != nil {
			return err
		}
		vItem, err := txn.Get(objectMetaKeyV("b", "file.txt", vid))
		if err != nil {
			return err
		}
		return vItem.Value(func(val []byte) error {
			m, merr := unmarshalObjectMeta(val)
			if merr != nil {
				return merr
			}
			versionedTags = m.Tags
			return nil
		})
	}))
	assert.Equal(t, tags, legacyTags, "legacy key tags")
	assert.Equal(t, tags, versionedTags, "versioned key tags")
}

func TestFSM_SetObjectTags_SpecificVersion(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	// Create bucket and enable versioning.
	data, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "b"})
	require.NoError(t, fsm.Apply(data))
	data, _ = EncodeCommand(CmdSetBucketVersioning, SetBucketVersioningCmd{Bucket: "b", State: "Enabled"})
	require.NoError(t, fsm.Apply(data))

	const vid = "v1"
	data, _ = EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: "b", Key: "file.txt", Size: 5, ETag: "etag-v", ModTime: 2000, VersionID: vid,
	})
	require.NoError(t, fsm.Apply(data))

	tags := []storage.Tag{{Key: "tier", Value: "archive"}}
	// Target the specific version only.
	data, err := EncodeCommand(CmdSetObjectTags, SetObjectTagsCmd{
		Bucket: "b", Key: "file.txt", VersionID: vid, Tags: tags,
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	// Only versioned record should have tags; legacy may or may not — but versioned must.
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		vItem, err := txn.Get(objectMetaKeyV("b", "file.txt", vid))
		if err != nil {
			return err
		}
		return vItem.Value(func(val []byte) error {
			m, merr := unmarshalObjectMeta(val)
			if merr != nil {
				return merr
			}
			assert.Equal(t, tags, m.Tags, "versioned key tags when VersionID specified")
			return nil
		})
	}))
}

func TestFSM_ApplyCreateBucket_KeyLayoutUnchanged(t *testing.T) {
	db := newTestDB(t)
	f := NewFSM(db, newStateKeyspaceEmpty())
	data, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "b1"})
	require.NoError(t, err)
	require.NoError(t, f.Apply(data))
	if err := db.View(func(txn *badger.Txn) error {
		_, e := txn.Get([]byte("bucket:b1")) // empty keyspace => raw layout preserved
		return e
	}); err != nil {
		t.Fatalf("expected bucket:b1 present with raw layout, got %v", err)
	}
}

func TestFSM_CreateMultipartUpload_PersistsTags(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	data, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID:    "upload-tags-1",
		Bucket:      "b",
		Key:         "k",
		ContentType: "text/plain",
		CreatedAt:   100,
		Tags:        []storage.Tag{{Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(multipartKey("upload-tags-1"))
		require.NoError(t, err)
		raw, err := item.ValueCopy(nil)
		require.NoError(t, err)
		meta, err := unmarshalClusterMultipartMeta(raw)
		require.NoError(t, err)
		require.Equal(t, []storage.Tag{{Key: "env", Value: "prod"}}, meta.Tags)
		return nil
	}))
}

// TestFSM_CompleteMultipartUpload_MaterialisesTags verifies that the
// finalisation command (CmdPutObjectMeta — what production proposes from
// CompleteMultipartUpload) writes Tags onto objectMeta. The Raft path for
// completion goes through commitECObjectWriteResult → CmdPutObjectMeta, not
// the legacy CmdCompleteMultipart, so this is where parity must hold.
func TestFSM_CompleteMultipartUpload_MaterialisesTags(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db, newStateKeyspaceEmpty())

	// 1) create multipart with tags
	data, err := EncodeCommand(CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID: "upload-mat-1", Bucket: "b", Key: "k", ContentType: "text/plain", CreatedAt: 100,
		Tags: []storage.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}},
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	// 2) finalise via the production cmd that materialises object meta on
	// CompleteMultipartUpload (CmdPutObjectMeta with Parts + Tags).
	data, err = EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: "b", Key: "k", Size: 1024, ContentType: "text/plain",
		ETag: "final-etag", ModTime: 200,
		Parts: []storage.MultipartPartEntry{{PartNumber: 1, Size: 1024, ETag: "p1"}},
		Tags:  []storage.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}},
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data))

	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKey("b", "k"))
		require.NoError(t, err)
		raw, err := item.ValueCopy(nil)
		require.NoError(t, err)
		m, err := unmarshalObjectMeta(raw)
		require.NoError(t, err)
		require.Equal(t, []storage.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}}, m.Tags)
		return nil
	}))
}
