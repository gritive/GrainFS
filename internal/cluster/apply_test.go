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

// TestFSM_CreateBucket_RetiredNoOp verifies that a stale raft-log entry
// carrying the retired CmdCreateBucket slot is a replay-safe no-op: Apply
// returns nil and NO bucket: key is written to BadgerDB (bucket control-plane
// is now meta-raft exclusively).
func TestFSM_CreateBucket_RetiredNoOp(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	data, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "test-bucket"})
	require.NoError(t, err)
	// Must succeed (no-op, not an error).
	require.NoError(t, fsm.Apply(data))

	// Must NOT write a bucket: key — the apply is a no-op.
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey("test-bucket"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound, "retired CmdCreateBucket must not write a bucket key")
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
	// exercised here (no production writer). CmdSetBucketPolicy is retired (Task 12:
	// bucket control-plane moved to meta-raft). Write the policy key directly via
	// FSM.setValue (the same encrypted write path) to exercise policy: key encryption.
	policy := []byte(`{"Statement":[{"Resource":"secret-policy-resource"}]}`)
	require.NoError(t, fsm.db.Update(func(txn MetadataTxn) error {
		return fsm.setValue(txn, fsm.keys.Key([]byte("policy:b")), policy)
	}))

	err = db.View(func(txn *badger.Txn) error {
		for _, tc := range []struct {
			key       []byte
			forbidden string
		}{
			{fsm.keys.ObjectMetaKeyV("b", "secret-object", "v1"), "customer-private-metadata"},
			{fsm.keys.Key([]byte("policy:b")), "secret-policy-resource"},
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

// retiredDeleteObjectSlot is the retired CommandType byte that once named
// CmdDeleteObject (= 4). The named constant was removed when the per-object FSM
// commands moved off-raft; a stale raft-log entry carrying this byte must still
// replay as a no-op via the apply default path.
const retiredDeleteObjectSlot = CommandType(4)

// TestCmdDeleteObject_RetiredNoOp verifies that a stale raft-log entry carrying
// the retired CmdDeleteObject slot (4) is a replay-safe no-op in the FSM after
// data-plane raft-free Slice 2: FSM.Apply must return nil and must not delete any
// object-meta key. Force-delete is now blob-physical (quorum-meta + shards). The
// named constant is gone, so we build the raw command byte buffer directly with
// the numeric slot to simulate a stale raft-log entry.
func TestCmdDeleteObject_RetiredNoOp(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// Seed an FSM obj: record directly (the retired per-object commands no-op).
	seedCmd := PutObjectMetaCmd{Bucket: "b", Key: "obj.txt", ETag: "e1"}
	require.NoError(t, fsm.db.Update(func(txn MetadataTxn) error {
		return fsm.persistPutObjectMetaUpdate(txn, seedCmd, buildPutObjectMeta(seedCmd))
	}))

	// Build a raw command entry with the retired slot byte and an empty payload —
	// the apply default path must not inspect the payload for a retired type.
	raw, err := buildNoDataCommand(retiredDeleteObjectSlot)
	require.NoError(t, err)

	// Apply must succeed (no-op, not an error).
	require.NoError(t, fsm.Apply(raw))

	// Key must still exist — the retired no-op must not delete FSM records.
	metaKey := fsm.keys.ObjectMetaKey("b", "obj.txt")
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(metaKey)
		return err
	}), "retired CmdDeleteObject slot must be a no-op — key must still exist after apply")
}

// TestFSM_DeleteBucket_RetiredNoOp verifies that stale raft-log entries carrying
// the retired CmdCreateBucket and CmdDeleteBucket slots are replay-safe no-ops:
// Apply returns nil and the bucket: key in BadgerDB is never touched.
func TestFSM_DeleteBucket_RetiredNoOp(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// Both applies must succeed (no-ops) and leave no keys in BadgerDB.
	data, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "to-delete"})
	require.NoError(t, fsm.Apply(data))

	data, _ = EncodeCommand(CmdDeleteBucket, DeleteBucketCmd{Bucket: "to-delete"})
	require.NoError(t, fsm.Apply(data))

	// Neither key must have been written.
	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey("to-delete"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound, "retired CmdDeleteBucket slot must not touch bucket keys")
}

// retiredPutObjectMetaSlot is the retired CommandType byte that once named
// CmdPutObjectMeta (= 3). The named constant was removed when the off-raft
// quorum-meta blob codec (encodeQuorumMetaBlob) replaced the raft Command
// envelope for object metadata; a stale raft-log entry carrying this byte must
// still replay as a no-op via the apply default path.
const retiredPutObjectMetaSlot = CommandType(3)

// TestPutObjectMetaCmd_RetiredNoOp verifies that a stale raft-log entry carrying
// the retired CmdPutObjectMeta slot (3) is a replay-safe no-op in the FSM after
// data-plane raft-free Slice 2: FSM.Apply must return nil and must not write any
// object-meta key. The live write path is writeQuorumMeta (off-raft blob).
func TestPutObjectMetaCmd_RetiredNoOp(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// A stale raft-log entry carrying the retired slot: a bare quorum-meta blob
	// wrapped in a raft Command envelope with the retired type byte. The apply
	// default path must drop it without touching the object-meta keyspace.
	payload, err := encodeQuorumMetaBlob(PutObjectMetaCmd{
		Bucket: "b", Key: "hello.txt", Size: 42, ETag: "abc123",
	})
	require.NoError(t, err)
	data, err := buildRawCommand(retiredPutObjectMetaSlot, payload)
	require.NoError(t, err)

	// Apply must succeed (no-op, not an error).
	require.NoError(t, fsm.Apply(data))

	// Must not have written any object-meta key.
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(objectMetaKey("b", "hello.txt"))
		return err
	})
	require.ErrorIs(t, err, badger.ErrKeyNotFound, "retired CmdPutObjectMeta slot must be a no-op — key must not exist")
}

// TestFSM_DeleteObject removed: CmdDeleteObject is retired (data-plane raft-free
// Slice 2 no-op). Force-delete is now covered by TestForceDeleteBucketNonVersioned_QmetaAndShards
// and TestForceDeleteBucketSoleAuthOn which exercise the blob-physical path.
// The no-op behavior is verified by TestCmdDeleteObject_RetiredNoOp.

func TestFSM_SnapshotRestore(t *testing.T) {
	db1 := newTestDB(t)
	fsm1 := NewFSM(badgermeta.Wrap(db1), newStateKeyspaceEmpty())

	// CmdCreateBucket is retired (Task 12 no-op); seed the bucket key directly.
	// CmdPutObjectMeta is a no-op in Slice 2; seed via persistPutObjectMetaUpdate.
	require.NoError(t, fsm1.db.Update(func(txn MetadataTxn) error {
		return txn.Set(bucketKey("snap-bucket"), []byte("{}"))
	}))
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

	// Verify state: both bucket key and object meta key must survive the snapshot/restore.
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

// TestFSM_SetBucketPolicy_RetiredNoOp verifies that a stale raft-log entry carrying
// CmdSetBucketPolicy is a replay-safe no-op: Apply returns nil and no policy: key
// is written to BadgerDB (policy now lives in MetaBucketStore / BucketRecord).
func TestFSM_SetBucketPolicy_RetiredNoOp(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	data, err := EncodeCommand(CmdSetBucketPolicy, SetBucketPolicyCmd{
		Bucket:     "policy-bucket",
		PolicyJSON: []byte(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data), "retired CmdSetBucketPolicy must not error")

	// Must NOT write a policy: key.
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketPolicyKey("policy-bucket"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound, "retired CmdSetBucketPolicy must not write policy key")
}

// TestFSM_DeleteBucketPolicy_RetiredNoOp verifies that stale CmdSetBucketPolicy
// and CmdDeleteBucketPolicy entries are replay-safe no-ops.
func TestFSM_DeleteBucketPolicy_RetiredNoOp(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	data, _ := EncodeCommand(CmdSetBucketPolicy, SetBucketPolicyCmd{
		Bucket: "bp", PolicyJSON: []byte(`{"Version":"2012-10-17"}`),
	})
	require.NoError(t, fsm.Apply(data))

	data, _ = EncodeCommand(CmdDeleteBucketPolicy, DeleteBucketPolicyCmd{Bucket: "bp"})
	require.NoError(t, fsm.Apply(data), "retired CmdDeleteBucketPolicy must not error")

	// No key was ever written; absence is the correct state.
	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketPolicyKey("bp"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound)
}

// TestFSM_DeleteBucketPolicy_NotExist_RetiredNoOp: deleting a policy for a
// nonexistent bucket must also be a no-op with retired commands.
func TestFSM_DeleteBucketPolicy_NotExist_RetiredNoOp(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	data, _ := EncodeCommand(CmdDeleteBucketPolicy, DeleteBucketPolicyCmd{Bucket: "no-policy"})
	err := fsm.Apply(data)
	assert.NoError(t, err, "retired CmdDeleteBucketPolicy must be a no-op even for absent bucket")
}

// TestFSM_AbortMultipart_NotExist removed in M4: applyAbortMultipart is deleted.

// TestFSM_DeleteObject_NotExist removed: CmdDeleteObject is retired (data-plane
// raft-free Slice 2 no-op). The no-op behavior on non-existent keys is covered
// by TestCmdDeleteObject_RetiredNoOp (Apply returns nil regardless of FSM state).

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
	// Test Restore overwrites existing data in the target DB.
	// CmdCreateBucket is retired (Task 12 no-op); seed bucket keys directly.
	db1 := newTestDB(t)
	fsm1 := NewFSM(badgermeta.Wrap(db1), newStateKeyspaceEmpty())

	require.NoError(t, fsm1.db.Update(func(txn MetadataTxn) error {
		return txn.Set(bucketKey("src-bucket"), []byte("{}"))
	}))

	snap, err := fsm1.Snapshot()
	require.NoError(t, err)

	// Create a second DB with different data
	db2 := newTestDB(t)
	fsm2 := NewFSM(badgermeta.Wrap(db2), newStateKeyspaceEmpty())
	require.NoError(t, fsm2.db.Update(func(txn MetadataTxn) error {
		return txn.Set(bucketKey("old-bucket"), []byte("{}"))
	}))

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

// TestFSM_SetBucketVersioning_RetiredNoOp verifies that stale CmdSetBucketVersioning
// raft-log entries are replay-safe no-ops: Apply returns nil and no versioning key
// is written to BadgerDB (versioning now lives in MetaBucketStore / BucketRecord).
func TestFSM_SetBucketVersioning_RetiredNoOp(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// CmdCreateBucket is also retired; both must be no-ops.
	data, _ := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "vbucket"})
	require.NoError(t, fsm.Apply(data))

	data, err := EncodeCommand(CmdSetBucketVersioning, SetBucketVersioningCmd{Bucket: "vbucket", State: "Enabled"})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(data), "retired CmdSetBucketVersioning must not error")

	// No versioning key must have been written.
	err = db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketVerKey("vbucket"))
		return err
	})
	assert.ErrorIs(t, err, badger.ErrKeyNotFound, "retired CmdSetBucketVersioning must not write versioning key")
}

// TestFSM_SetBucketVersioning_NoBucket_RetiredNoOp: retired CmdSetBucketVersioning
// must be a no-op even when the bucket does not exist (old error no longer fires).
func TestFSM_SetBucketVersioning_NoBucket_RetiredNoOp(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	data, _ := EncodeCommand(CmdSetBucketVersioning, SetBucketVersioningCmd{Bucket: "ghost", State: "Enabled"})
	err := fsm.Apply(data)
	assert.NoError(t, err, "retired CmdSetBucketVersioning must be a no-op regardless of bucket existence")
}

// TestFSM_SetObjectACL and TestFSM_SetObjectTags and their subtests were removed
// in data-plane raft-free Slice 2: CmdSetObjectACL and CmdSetObjectTags are
// retired (no-op apply, codec returns reserved error). Public-API coverage via
// blob RMW is in bucket_tags_acl_retire_test.go:
//   TestSetObjectACL_BlobObject_NoRaftFallback
//   TestSetObjectTags_BlobObject_NoRaftFallback

// TestFSM_ApplyCreateBucket_KeyLayout_RetiredNoOp verifies that the retired
// CmdCreateBucket slot is a replay-safe no-op: Apply returns nil and the
// raw bucket:b1 key is NOT written (bucket control-plane moved to meta-raft).
// The key layout itself (bucket: prefix) is still exercised by the BucketKey
// helper unit tests; this test merely confirms the retired apply does nothing.
func TestFSM_ApplyCreateBucket_KeyLayout_RetiredNoOp(t *testing.T) {
	db := newTestDB(t)
	f := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
	data, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "b1"})
	require.NoError(t, err)
	require.NoError(t, f.Apply(data), "retired CmdCreateBucket must not error")
	err = db.View(func(txn *badger.Txn) error {
		_, e := txn.Get([]byte("bucket:b1")) // empty keyspace => raw layout if written
		return e
	})
	require.ErrorIs(t, err, badger.ErrKeyNotFound, "retired CmdCreateBucket must not write a bucket key")
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

// Retired CommandType slot bytes that once named the append/coalesce-off-raft
// Slice 1 commands. The named constants were removed; the slots must stay
// reserved (never renumbered) and replay-safe.
const (
	retiredAppendObjectSlot     = CommandType(18)
	retiredCoalesceSegmentsSlot = CommandType(19)
)

// TestAppendCoalesceCommands_Retired verifies that the retired append/coalesce
// slots (18, 19) stay safe after the named constants were removed:
//   - encodePayload rejects an unknown/retired type (the default error branch)
//   - FSM.Apply treats a stale raft-log entry as a no-op (returns nil)
func TestAppendCoalesceCommands_Retired(t *testing.T) {
	// EncodeCommand must reject both retired type bytes (no encodePayload case).
	_, err := EncodeCommand(retiredAppendObjectSlot, PutObjectMetaCmd{})
	require.Error(t, err, "retired append slot must return error from EncodeCommand")
	_, err = EncodeCommand(retiredCoalesceSegmentsSlot, PutObjectMetaCmd{})
	require.Error(t, err, "retired coalesce slot must return error from EncodeCommand")

	// Build a raw Command FlatBuffer for each retired slot manually (bypasses
	// encodePayload so we can simulate a stale raft-log replay without a proposer).
	rawAppend, err := buildNoDataCommand(retiredAppendObjectSlot)
	require.NoError(t, err)
	rawCoalesce, err := buildNoDataCommand(retiredCoalesceSegmentsSlot)
	require.NoError(t, err)

	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// Both must be no-ops on apply (default path).
	require.NoError(t, fsm.Apply(rawAppend), "retired append slot apply must be a no-op")
	require.NoError(t, fsm.Apply(rawCoalesce), "retired coalesce slot apply must be a no-op")
}
