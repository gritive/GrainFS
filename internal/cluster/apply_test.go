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
// raft-log replay without a live proposer.
func buildNoDataCommand(cmdType uint32) ([]byte, error) {
	return buildRawCommand(cmdType, nil)
}

// buildRawCommand builds a FlatBuffer Command with the given type and raw payload.
func buildRawCommand(cmdType uint32, data []byte) ([]byte, error) {
	b := flatbuffers.NewBuilder(len(data) + 16)
	var dataOff flatbuffers.UOffsetT
	if len(data) > 0 {
		dataOff = b.CreateByteVector(data)
	}
	clusterpb.CommandStart(b)
	clusterpb.CommandAddType(b, cmdType)
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

func TestFSM_EncryptedValuesHideObjectMultipartAndPolicyPayloads(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
	clusterID := bytes.Repeat([]byte{0x46}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x46}, encrypt.KEKSize), clusterID)
	require.NoError(t, err)
	fsm.SetDEKKeeper(keeper, clusterID)

	// Object metadata apply is a no-op (data-plane raft-free Slice 2); seed via
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

	// Multipart data-group apply was removed in M4; mpu: key encryption is no
	// longer exercised here (no production writer). Bucket policy moved to
	// meta-raft in Task 12. Write the policy key directly via FSM.setValue (the
	// same encrypted write path) to exercise policy: key encryption.
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

func TestApplyActor_UnknownCommandReplayNoOp(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	// Seed an FSM obj: record directly.
	seedCmd := PutObjectMetaCmd{Bucket: "b", Key: "obj.txt", ETag: "e1"}
	require.NoError(t, fsm.db.Update(func(txn MetadataTxn) error {
		return fsm.persistPutObjectMetaUpdate(txn, seedCmd, buildPutObjectMeta(seedCmd))
	}))

	raw, err := buildNoDataCommand(250)
	require.NoError(t, err)
	a := &applyActor{fsm: fsm}
	b := &DistributedBackend{fsm: fsm}
	a.batch = append(a.batch[:0], raft.LogEntry{Index: 1, Term: 1, Type: raft.LogEntryCommand, Command: raw})
	a.commitBatch(b)
	require.Empty(t, b.applyErrs)

	metaKey := fsm.keys.ObjectMetaKey("b", "obj.txt")
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(metaKey)
		return err
	}), "unknown command must not mutate FSM records")
}

// TestFSM_DeleteObject removed: CmdDeleteObject is retired (data-plane raft-free
// Slice 2 no-op). Force-delete is now covered by TestForceDeleteBucketNonVersioned_QmetaAndShards
// and TestForceDeleteBucketBlobAuthOn which exercise the blob-physical path.
// The no-op behavior is verified by TestCmdDeleteObject_RetiredNoOp.

func TestFSM_SnapshotRestore(t *testing.T) {
	db1 := newTestDB(t)
	fsm1 := NewFSM(badgermeta.Wrap(db1), newStateKeyspaceEmpty())

	// Bucket creation is meta-raft-owned; seed the bucket key directly.
	// Object metadata apply is a no-op in Slice 2; seed via persistPutObjectMetaUpdate.
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

// TestFSM_AbortMultipart_NotExist removed in M4: applyAbortMultipart is deleted.

// TestFSM_DeleteObject_NotExist removed: CmdDeleteObject is retired (data-plane
// raft-free Slice 2 no-op). The no-op behavior on non-existent keys is covered
// by TestCmdDeleteObject_RetiredNoOp (Apply returns nil regardless of FSM state).

func TestFSM_Restore_CorruptData(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())

	err := fsm.Restore(raft.SnapshotMeta{FormatVersion: raft.FSMSnapshotFormatVersion}, []byte("not valid protobuf snapshot"))
	assert.Error(t, err, "Restore should fail on corrupt snapshot data")
}

func TestFSM_SnapshotRestore_WithExistingData(t *testing.T) {
	// Test Restore overwrites existing data in the target DB.
	// Bucket creation is meta-raft-owned; seed bucket keys directly.
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

func TestLegacyPendingMigrationKeySurvivesSnapshotRestoreWithoutRecovery(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
	legacyKey := fsm.keys.PendingMigrationKey("b", "k", "v1")
	legacyVal := []byte("legacy payload")

	require.NoError(t, fsm.db.Update(func(txn MetadataTxn) error {
		return txn.Set(legacyKey, legacyVal)
	}))

	snap, err := fsm.Snapshot()
	require.NoError(t, err)

	db2 := newTestDB(t)
	restored := NewFSM(badgermeta.Wrap(db2), newStateKeyspaceEmpty())
	require.NoError(t, restored.Restore(raft.SnapshotMeta{FormatVersion: raft.FSMSnapshotFormatVersion}, snap))

	require.NoError(t, db2.View(func(txn *badger.Txn) error {
		item, err := txn.Get(legacyKey)
		require.NoError(t, err)
		got, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, legacyVal, got)
		return nil
	}))
}

// TestFSM_SetObjectACL and TestFSM_SetObjectTags and their subtests were removed
// in data-plane raft-free Slice 2: CmdSetObjectACL and CmdSetObjectTags are
// retired (no-op apply, codec returns reserved error). Public-API coverage via
// blob RMW is in bucket_tags_acl_retire_test.go:
//   TestSetObjectACL_BlobObject_NoRaftFallback
//   TestSetObjectTags_BlobObject_NoRaftFallback

// TestFSM_CreateMultipartUpload_PersistsTags removed in M4: applyCreateMultipartUpload deleted.

// TestPersistPutObjectMetaUpdate_MaterialisesTags verifies that
// persistPutObjectMetaUpdate (the live write path via writeQuorumMeta, not FSM
// apply) correctly materialises Tags onto objectMeta. Previously exercised
// via object-metadata FSM apply; Slice 2 retires that raft path.
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
