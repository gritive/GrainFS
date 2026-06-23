package cluster

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgermeta"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

func newTestDB(t *testing.T) *badger.DB {
	t.Helper()
	dir, err := os.MkdirTemp("", "fsm-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })

	opts := badgerutil.SmallOptions(dir)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// newTestStore wraps a fresh test BadgerDB as a MetadataStore for tests that
// construct an FSM directly (S6.5-2: NewFSM takes the contract type).
func newTestStore(t *testing.T) MetadataStore {
	t.Helper()
	return badgermeta.Wrap(newTestDB(t))
}

// buildNoDataCommand constructs a minimal FlatBuffer Command with the given
// type and no payload data. Used by retirement tests to simulate stale
// raft-log replay without a live proposer (EncodeCommand rejects retired types).
func buildNoDataCommand(cmdType CommandType) ([]byte, error) {
	return buildRawCommand(cmdType, nil)
}

// buildRawCommand builds a FlatBuffer Command with the given type and raw payload.
// Unlike EncodeCommand it skips encodePayload so it works for retired command slots.
func buildRawCommand(cmdType CommandType, data []byte) ([]byte, error) {
	b := flatbuffers.NewBuilder(len(data) + 16)
	var dataOff flatbuffers.UOffsetT
	if len(data) > 0 {
		dataOff = b.CreateByteVector(data)
	}
	clusterpb.CommandStart(b)
	clusterpb.CommandAddType(b, uint32(cmdType))
	if len(data) > 0 {
		clusterpb.CommandAddData(b, dataOff)
	}
	root := clusterpb.CommandEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

func TestFSM_CreateBucket(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

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
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
	clusterID := bytes.Repeat([]byte{0x46}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x46}, encrypt.KEKSize), clusterID)
	require.NoError(t, err)
	fsm.SetDEKKeeper(keeper, clusterID)

	// CmdPutObjectMeta is a no-op on apply (data-plane raft-free Slice 2); seed via
	// persistPutObjectMetaUpdate directly to exercise FSM encryption.
	cmd := PutObjectMetaCmd{
		Bucket:      "b",
		Key:         "secret-object",
		ContentType: "text/secret",
		UserMetadata: map[string]string{
			"x-amz-meta-secret": "customer-private-metadata",
		},
		VersionID: "v1",
	}
	require.NoError(t, fsm.db.Update(func(txn MetadataTxn) error {
		return fsm.persistPutObjectMetaUpdate(txn, cmd, buildPutObjectMeta(cmd))
	}))

	// CmdCreateMultipartUpload removed in M4; mpu: key encryption is no longer
	// exercised here (no production writer). Remaining: obj: and policy: are checked.
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
			{fsm.keys.BucketPolicyKey("b"), "secret-policy-resource"},
		} {
			item, err := txn.Get(tc.key)
			require.NoError(t, err)
			raw, err := item.ValueCopy(nil)
			require.NoError(t, err)
			_, _, ok, err := decodeFSMValueFrameV2(raw)
			require.NoError(t, err)
			require.True(t, ok)
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
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
	clusterID := bytes.Repeat([]byte{0x47}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x47}, encrypt.KEKSize), clusterID)
	require.NoError(t, err)
	fsm.SetDEKKeeper(keeper, clusterID)

	// CmdPutObjectMeta is a no-op on apply (data-plane raft-free Slice 2); seed via
	// persistPutObjectMetaUpdate directly.
	seedCmd := PutObjectMetaCmd{Bucket: "b", Key: "tampered", ETag: "etag"}
	require.NoError(t, fsm.db.Update(func(txn MetadataTxn) error {
		return fsm.persistPutObjectMetaUpdate(txn, seedCmd, buildPutObjectMeta(seedCmd))
	}))

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
		_, _, ok, err := decodeFSMValueFrameV2(raw)
		require.NoError(t, err)
		require.True(t, ok)
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
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

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

// TestPutObjectMetaCmd_RetiredNoOp verifies that CmdPutObjectMeta is a replay-safe
// no-op in the FSM after data-plane raft-free Slice 2: FSM.Apply must return nil
// and must not write any object-meta key. The live write path is writeQuorumMeta.
func TestPutObjectMetaCmd_RetiredNoOp(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// EncodeCommand still works (codec/struct/constant kept for quorum-meta blob use).
	data, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket: "b", Key: "hello.txt", Size: 42, ETag: "abc123",
	})
	require.NoError(t, err)

	// Apply must succeed (no-op, not an error).
	require.NoError(t, fsm.Apply(data))

	// Must not have written any object-meta key.
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(objectMetaKey("b", "hello.txt"))
		return err
	})
	require.ErrorIs(t, err, badger.ErrKeyNotFound, "CmdPutObjectMeta must be a no-op — key must not exist")
}

func TestFSM_DeleteObject(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// Seed via persistPutObjectMetaUpdate (CmdPutObjectMeta is a no-op in Slice 2).
	seedCmd := PutObjectMetaCmd{Bucket: "b", Key: "del.txt", Size: 1, ContentType: "text/plain", ETag: "e", ModTime: 1}
	require.NoError(t, fsm.db.Update(func(txn MetadataTxn) error {
		return fsm.persistPutObjectMetaUpdate(txn, seedCmd, buildPutObjectMeta(seedCmd))
	}))

	data, _ := EncodeCommand(CmdDeleteObject, DeleteObjectCmd{Bucket: "b", Key: "del.txt"})
	require.NoError(t, fsm.Apply(data))

	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(objectMetaKey("b", "del.txt"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}

func TestFSM_SnapshotRestore(t *testing.T) {
	db1 := newTestDB(t)
	fsm1 := NewFSM(badgermeta.Wrap(db1), newStateKeyspaceEmpty())

	// Apply some commands. CmdPutObjectMeta is a no-op in Slice 2; seed via
	// persistPutObjectMetaUpdate directly.
	data, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "snap-bucket"})
	require.NoError(t, fsm1.Apply(data))
	seedCmd := PutObjectMetaCmd{Bucket: "snap-bucket", Key: "file.txt", Size: 10, ContentType: "text/plain", ETag: "e", ModTime: 1}
	require.NoError(t, fsm1.db.Update(func(txn MetadataTxn) error {
		return fsm1.persistPutObjectMetaUpdate(txn, seedCmd, buildPutObjectMeta(seedCmd))
	}))

	// Take snapshot
	snap, err := fsm1.Snapshot()
	require.NoError(t, err)

	// Restore to a new DB
	db2 := newTestDB(t)
	fsm2 := NewFSM(badgermeta.Wrap(db2), newStateKeyspaceEmpty())
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

// TestFSM_MultipartCycle exercises the apply-side of a non-versioned complete:
// applyCompleteMultipart writes the legacy obj:/lat: record and a done-marker.
// M2b moved the in-progress manifest off the FSM, so the apply no longer reads
// or deletes any mpu: key — the proposer owns the .qmeta_mpu blob lifecycle.
// TestFSM_MultipartCycle removed in M4: applyCompleteMultipart and the done-marker
// machinery are deleted. CmdCompleteMultipart is now a no-op reserved command.

// TestFSM_CompleteMultipartPersistsPartsSegments, TestFSM_CompleteMultipart_IdempotentOnDuplicateApply
// removed in M4: applyCompleteMultipart is deleted.

// TestMultipartComplete_RejectsUploadMismatch is the M2b proposer-level twin of
// the former apply-level mismatch guard. With the manifest off the FSM, the
// (bucket, key) mismatch is caught by CompleteMultipartUpload reading the
// manifest blob — not by applyCompleteMultipart (which no longer reads any mpu:
// key). Completing an upload created for one key against a different key fails,
// and no object is committed for the wrong key.
func TestMultipartComplete_RejectsUploadMismatch(t *testing.T) {
	b, _ := newTestDistributedBackendWithDB(t)
	configureChunkedMultipartTestBackend(b)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	up, err := b.CreateMultipartUpload(ctx, "b", "expected.bin", "application/octet-stream")
	require.NoError(t, err)
	part, err := b.UploadPart(ctx, "b", "expected.bin", up.UploadID, 1, bytes.NewReader([]byte("payload")), "")
	require.NoError(t, err)

	_, err = b.CompleteMultipartUpload(ctx, "b", "wrong.bin", up.UploadID, []storage.Part{*part})
	require.Error(t, err)

	_, err = b.HeadObject(ctx, "b", "wrong.bin")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

// TestFSM_CreateMultipartUploadPersistsListingMetadata, TestFSM_AbortMultipart
// removed in M4: applyCreateMultipartUpload and applyAbortMultipart are deleted.

func TestFSM_SetBucketPolicy(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

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
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

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
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// Deleting a non-existent policy should not error (ErrKeyNotFound → nil)
	data, _ := EncodeCommand(CmdDeleteBucketPolicy, DeleteBucketPolicyCmd{Bucket: "no-policy"})
	err := fsm.Apply(data)
	assert.NoError(t, err)
}

// TestFSM_AbortMultipart_NotExist removed in M4: applyAbortMultipart is deleted.

func TestFSM_DeleteObject_NotExist(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// Deleting a non-existent object should not error (ErrKeyNotFound → nil)
	data, _ := EncodeCommand(CmdDeleteObject, DeleteObjectCmd{Bucket: "b", Key: "nope.txt"})
	err := fsm.Apply(data)
	assert.NoError(t, err)
}

func TestFSM_Apply_CorruptData(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	err := fsm.Apply([]byte("definitely not protobuf"))
	assert.Error(t, err, "Apply should fail on corrupt data")
}

func TestFSM_Restore_CorruptData(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	err := fsm.Restore(raft.SnapshotMeta{FormatVersion: raft.FSMSnapshotFormatVersion}, []byte("not valid protobuf snapshot"))
	assert.Error(t, err, "Restore should fail on corrupt snapshot data")
}

func TestFSM_SnapshotRestore_WithExistingData(t *testing.T) {
	// Test Restore overwrites existing data in the target DB
	db1 := newTestDB(t)
	fsm1 := NewFSM(badgermeta.Wrap(db1), newStateKeyspaceEmpty())

	data, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "src-bucket"})
	require.NoError(t, fsm1.Apply(data))

	snap, err := fsm1.Snapshot()
	require.NoError(t, err)

	// Create a second DB with different data
	db2 := newTestDB(t)
	fsm2 := NewFSM(badgermeta.Wrap(db2), newStateKeyspaceEmpty())
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
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

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
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

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
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

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
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// Manually write a pending-migration key to simulate a crash after persistence
	task := MigrationTask{Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "src", DstNode: "dst"}
	require.NoError(t, fsm.db.Update(func(txn MetadataTxn) error { return fsm.persistPendingMigration(txn, task) }))

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
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// Pre-write a pending-migration entry
	task := MigrationTask{Bucket: "b", Key: "k", VersionID: "v1", SrcNode: "src", DstNode: "dst"}
	require.NoError(t, fsm.db.Update(func(txn MetadataTxn) error { return fsm.persistPendingMigration(txn, task) }))

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
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
	ch := make(chan MigrationTask, 10)
	require.NoError(t, fsm.RecoverPending(context.Background(), ch))
	assert.Empty(t, ch)
}

func TestFSM_SetBucketVersioning(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

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
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	data, _ := EncodeCommand(CmdSetBucketVersioning, SetBucketVersioningCmd{Bucket: "ghost", State: "Enabled"})
	err := fsm.Apply(data)
	assert.Error(t, err, "should fail when bucket does not exist")
}

// TestFSM_SetObjectACL and TestFSM_SetObjectTags and their subtests were removed
// in data-plane raft-free Slice 2: CmdSetObjectACL and CmdSetObjectTags are
// retired (no-op apply, codec returns reserved error). Public-API coverage via
// blob RMW is in bucket_tags_acl_retire_test.go:
//   TestSetObjectACL_BlobObject_NoRaftFallback
//   TestSetObjectTags_BlobObject_NoRaftFallback

func TestFSM_ApplyCreateBucket_KeyLayoutUnchanged(t *testing.T) {
	db := newTestDB(t)
	f := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
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

// TestFSM_CreateMultipartUpload_PersistsTags removed in M4: applyCreateMultipartUpload deleted.

// TestPersistPutObjectMetaUpdate_MaterialisesTags verifies that
// persistPutObjectMetaUpdate (the live write path via writeQuorumMeta, not FSM
// apply) correctly materialises Tags onto objectMeta. Previously exercised
// via CmdPutObjectMeta FSM apply; Slice 2 retires that raft path.
func TestPersistPutObjectMetaUpdate_MaterialisesTags(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	cmd := PutObjectMetaCmd{
		Bucket: "b", Key: "k", Size: 1024, ContentType: "text/plain",
		ETag: "final-etag", ModTime: 200,
		Parts: []storage.MultipartPartEntry{{PartNumber: 1, Size: 1024, ETag: "p1"}},
		Tags:  []storage.Tag{{Key: "env", Value: "prod"}, {Key: "owner", Value: "alice"}},
	}
	require.NoError(t, fsm.db.Update(func(txn MetadataTxn) error {
		return fsm.persistPutObjectMetaUpdate(txn, cmd, buildPutObjectMeta(cmd))
	}))

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

// TestAppendCoalesceCommands_Retired verifies that CmdAppendObject and
// CmdCoalesceSegments are retired in the append/coalesce-off-raft Slice 1:
//   - EncodeCommand returns an error (no production proposer must call these)
//   - FSM.Apply treats a stale raft-log entry as a no-op (returns nil)
func TestAppendCoalesceCommands_Retired(t *testing.T) {
	// EncodeCommand must reject both retired types.
	_, err := EncodeCommand(CmdAppendObject, PutObjectMetaCmd{})
	require.Error(t, err, "CmdAppendObject must return error from EncodeCommand")
	_, err = EncodeCommand(CmdCoalesceSegments, PutObjectMetaCmd{})
	require.Error(t, err, "CmdCoalesceSegments must return error from EncodeCommand")

	// Build a raw Command FlatBuffer for each type manually (bypasses encodePayload
	// so we can simulate a stale raft-log replay without a live proposer).
	rawAppend, err := buildNoDataCommand(CmdAppendObject)
	require.NoError(t, err)
	rawCoalesce, err := buildNoDataCommand(CmdCoalesceSegments)
	require.NoError(t, err)

	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// Both must be no-ops on apply.
	require.NoError(t, fsm.Apply(rawAppend), "CmdAppendObject apply must be a no-op")
	require.NoError(t, fsm.Apply(rawCoalesce), "CmdCoalesceSegments apply must be a no-op")
}
