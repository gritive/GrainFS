package cluster

// shared_fsm_isolation_test.go — Task 11: prefix-isolation invariants
//
// Three test suites:
//   TestSharedFSM_PrefixIsolation_AllPaths  — table-driven; proves no cross-group leakage
//                                             across every FSM command path.
//   TestSharedFSM_PathologicalGroupIDs_NoCollision — end-to-end through FSM+backend
//                                             with prefix-y group IDs (length header
//                                             prevents byte-prefix collisions).
//   TestSharedFSM_GroupCloseDoesNotCloseSharedDB — shared=true backend Close() must
//                                             not close the underlying BadgerDB.
//
// All helpers (putObjViaApply, fsmHasKey, newTestNodeForSharedDB) live in
// shared_fsm_test.go and are reused here without duplication.

import (
	"context"
	"errors"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgermeta"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/storage"
)

// mustNewKS is a test helper that fatals if newStateKeyspace returns an error.
func mustNewKS(t *testing.T, id string) *stateKeyspace {
	t.Helper()
	ks, err := newStateKeyspace(id)
	if err != nil {
		t.Fatalf("newStateKeyspace(%q): %v", id, err)
	}
	return ks
}

// setupTwoGroups creates a fresh in-memory BadgerDB shared by two groups.
// Returned FSMs and backends are isolated by distinct stateKeyspaces.
// Each backend is started with its own raft node; the apply loop is running.
// The caller should NOT close the DB — t.Cleanup handles it.
func setupTwoFSMs(t *testing.T) (
	db *badger.DB,
	ksA, ksB *stateKeyspace,
	fA, fB *FSM,
) {
	t.Helper()

	db, err := badger.Open(badgerutil.SmallOptions("").WithInMemory(true))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	ksA = mustNewKS(t, "iso-A")
	ksB = mustNewKS(t, "iso-B")

	fA = NewFSM(badgermeta.Wrap(db), ksA)
	fB = NewFSM(badgermeta.Wrap(db), ksB)
	return db, ksA, ksB, fA, fB
}

func setupTwoGroups(t *testing.T) (
	db *badger.DB,
	ksA, ksB *stateKeyspace,
	fA, fB *FSM,
	backendA, backendB *DistributedBackend,
) {
	t.Helper()

	db, ksA, ksB, fA, fB = setupTwoFSMs(t)

	nodeA, _ := newTestNodeForSharedDB(t, "isoA-node")
	var err error
	backendA, err = NewDistributedBackend(t.TempDir(), badgermeta.Wrap(db), nodeA, ksA, true)
	require.NoError(t, err)
	stopA := make(chan struct{})
	go backendA.RunApplyLoop(stopA)
	t.Cleanup(func() { close(stopA) })

	nodeB, _ := newTestNodeForSharedDB(t, "isoB-node")
	backendB, err = NewDistributedBackend(t.TempDir(), badgermeta.Wrap(db), nodeB, ksB, true)
	require.NoError(t, err)
	stopB := make(chan struct{})
	go backendB.RunApplyLoop(stopB)
	t.Cleanup(func() { close(stopB) })

	return db, ksA, ksB, fA, fB, backendA, backendB
}

// applyCmd is a convenience wrapper around EncodeCommand + FSM.Apply.
func applyCmd(t *testing.T, f *FSM, cmdType CommandType, payload any) {
	t.Helper()
	raw, err := EncodeCommand(cmdType, payload)
	require.NoError(t, err)
	require.NoError(t, f.Apply(raw))
}

// dbHasKey reports whether fullKey (already encoded — no extra prefix added) exists.
func dbHasKey(t *testing.T, db MetadataStore, fullKey []byte) bool {
	t.Helper()
	found := false
	require.NoError(t, db.View(func(txn MetadataTxn) error {
		_, err := txn.Get(fullKey)
		if errors.Is(err, ErrMetaKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return nil
	}))
	return found
}

// TestSharedFSM_PrefixIsolation_AllPaths is a table-driven test that exercises
// every FSM command path through real apply calls and asserts that data written
// by group A is never visible from group B and vice versa.
//
// Each row gets a fresh shared BadgerDB so mutations do not bleed between rows.
func TestSharedFSM_PrefixIsolation_AllPaths(t *testing.T) {
	const bucket = "b"

	rows := []struct {
		name     string
		exercise func(t *testing.T)
	}{
		{
			// CreateBucket / DeleteBucket: two groups with the same bucket name
			// produce two distinct keys; deleting one does not affect the other.
			name: "CreateBucket_DeleteBucket",
			exercise: func(t *testing.T) {
				_, ksA, ksB, fA, fB := setupTwoFSMs(t)

				applyCmd(t, fA, CmdCreateBucket, CreateBucketCmd{Bucket: bucket})
				applyCmd(t, fB, CmdCreateBucket, CreateBucketCmd{Bucket: bucket})

				assert.True(t, dbHasKey(t, fA.db, ksA.BucketKey(bucket)), "A's bucket key must exist")
				assert.True(t, dbHasKey(t, fB.db, ksB.BucketKey(bucket)), "B's bucket key must exist")
				// The two keys must be distinct encoded byte sequences.
				assert.NotEqual(t, ksA.BucketKey(bucket), ksB.BucketKey(bucket))

				// Delete A's bucket.
				applyCmd(t, fA, CmdDeleteBucket, DeleteBucketCmd{Bucket: bucket})

				assert.False(t, dbHasKey(t, fA.db, ksA.BucketKey(bucket)), "A's bucket key must be gone")
				assert.True(t, dbHasKey(t, fB.db, ksB.BucketKey(bucket)), "B's bucket key must survive A's delete")
			},
		},
		{
			// PutObjectMeta: same bucket + same key in both groups; each group sees
			// only its own value. A-only object not visible from B.
			name: "PutObjectMeta_Isolation",
			exercise: func(t *testing.T) {
				// Phase 4: LIST uses quorum meta (shardSvc path). Shared-FSM
				// backends have no shardSvc, so isolation is proved via
				// HeadObject point reads (BadgerDB path) instead of ListObjects.
				_, ksA, ksB, fA, fB, backendA, backendB := setupTwoGroups(t)

				putObjViaApply(t, fA, bucket, "obj1", "A-etag")
				putObjViaApply(t, fB, bucket, "obj1", "B-etag")
				putObjViaApply(t, fA, bucket, "a-only", "A2-etag")

				ctx := context.Background()

				// Point read: distinct values per group.
				objA, _, err := backendA.headObjectMeta(ctx, bucket, "obj1")
				require.NoError(t, err)
				assert.Equal(t, "A-etag", objA.ETag)

				objB, _, err := backendB.headObjectMeta(ctx, bucket, "obj1")
				require.NoError(t, err)
				assert.Equal(t, "B-etag", objB.ETag)

				// a-only belongs to A; B must not find it.
				_, _, err = backendB.headObjectMeta(ctx, bucket, "a-only")
				assert.Error(t, err, "a-only must not be visible from group B")

				// A can still read its own a-only.
				objA2, _, err := backendA.headObjectMeta(ctx, bucket, "a-only")
				require.NoError(t, err)
				assert.Equal(t, "A2-etag", objA2.ETag)

				// Encoded object meta keys must be distinct.
				assert.NotEqual(t, ksA.ObjectMetaKey(bucket, "obj1"), ksB.ObjectMetaKey(bucket, "obj1"))
			},
		},
		{
			// DeleteBucket: A deletes a bucket; B's same-name bucket survives.
			// CmdDeleteObject = 4 is retired (data-plane raft-free Slice 2 no-op);
			// bucket-level isolation via CmdDeleteBucket exercises the same keyspace
			// partitioning.
			name: "DeleteBucket_DoesNotAffectPeer",
			exercise: func(t *testing.T) {
				_, _, _, fA, fB := setupTwoFSMs(t)

				applyCmd(t, fA, CmdCreateBucket, CreateBucketCmd{Bucket: "bktX"})
				applyCmd(t, fB, CmdCreateBucket, CreateBucketCmd{Bucket: "bktX"})

				// A deletes the bucket; B's copy must survive.
				applyCmd(t, fA, CmdDeleteBucket, DeleteBucketCmd{Bucket: "bktX"})

				// fsmHasKey takes the raw (unprefixed) key — it adds the group prefix.
				assert.False(t, fsmHasKey(t, fA, "bucket:bktX"),
					"A's bucket key must be gone after delete")
				assert.True(t, fsmHasKey(t, fB, "bucket:bktX"),
					"B's bucket key must survive A's delete")
			},
		},
		{
			// SetBucketPolicy / DeleteBucketPolicy: distinct policy keys per group.
			name: "BucketPolicy_Isolation",
			exercise: func(t *testing.T) {
				_, ksA, ksB, fA, fB := setupTwoFSMs(t)

				applyCmd(t, fA, CmdCreateBucket, CreateBucketCmd{Bucket: bucket})
				applyCmd(t, fB, CmdCreateBucket, CreateBucketCmd{Bucket: bucket})

				applyCmd(t, fA, CmdSetBucketPolicy, SetBucketPolicyCmd{
					Bucket: bucket, PolicyJSON: []byte(`{"Effect":"Allow"}`),
				})
				applyCmd(t, fB, CmdSetBucketPolicy, SetBucketPolicyCmd{
					Bucket: bucket, PolicyJSON: []byte(`{"Effect":"Deny"}`),
				})

				pA := ksA.BucketPolicyKey(bucket)
				pB := ksB.BucketPolicyKey(bucket)
				assert.NotEqual(t, pA, pB, "policy keys must be distinct")
				assert.True(t, dbHasKey(t, fA.db, pA), "A's policy key must exist")
				assert.True(t, dbHasKey(t, fB.db, pB), "B's policy key must exist")

				// Delete A's policy.
				applyCmd(t, fA, CmdDeleteBucketPolicy, DeleteBucketPolicyCmd{Bucket: bucket})

				assert.False(t, dbHasKey(t, fA.db, pA), "A's policy must be gone")
				assert.True(t, dbHasKey(t, fB.db, pB), "B's policy must survive A's delete")
			},
		},
		{
			// SetBucketVersioning: A enables versioning, B does not. Keys distinct.
			name: "BucketVersioning_Isolation",
			exercise: func(t *testing.T) {
				_, ksA, ksB, fA, fB := setupTwoFSMs(t)

				applyCmd(t, fA, CmdCreateBucket, CreateBucketCmd{Bucket: bucket})
				applyCmd(t, fB, CmdCreateBucket, CreateBucketCmd{Bucket: bucket})

				applyCmd(t, fA, CmdSetBucketVersioning, SetBucketVersioningCmd{
					Bucket: bucket, State: "Enabled",
				})
				// B does not set versioning.

				vA := ksA.BucketVerKey(bucket)
				vB := ksB.BucketVerKey(bucket)
				assert.NotEqual(t, vA, vB, "versioning keys must be distinct")
				assert.True(t, dbHasKey(t, fA.db, vA), "A's versioning key must exist")
				assert.False(t, dbHasKey(t, fB.db, vB), "B must not have a versioning key")
			},
		},
		{
			// Quarantine_Isolation: CmdPutObjectQuarantine is reserved/removed in
			// data-plane raft-free Slice 2 (quarantine now lives in the quorum-meta blob
			// via IsQuarantined/QuarantineCause). Verify the FSM never writes quarantine
			// keys — the FSM keyspace is clean.
			name: "Quarantine_Isolation",
			exercise: func(t *testing.T) {
				_, ksA, ksB, fA, fB := setupTwoFSMs(t)

				putObjViaApply(t, fA, bucket, "obj1", "A-etag")
				putObjViaApply(t, fB, bucket, "obj1", "B-etag")

				// CmdPutObjectQuarantine is reserved/removed: quarantine now lives in
				// the quorum-meta blob, never in the FSM. The retired QuarantineKey
				// helper is gone (no production reader), so the legacy FSM key is
				// inlined here purely for this negative assertion.
				legacyQKey := func(ks *stateKeyspace) []byte {
					return ks.Key([]byte("quarantine:" + bucket + "\x00obj1\x00"))
				}
				qA := legacyQKey(ksA)
				qB := legacyQKey(ksB)
				assert.NotEqual(t, qA, qB, "quarantine keys must be distinct")
				assert.False(t, dbHasKey(t, fA.db, qA), "A's quarantine key must NOT be written (removed Slice 2)")
				assert.False(t, dbHasKey(t, fB.db, qB), "B must not have a quarantine key")
			},
		},
		{
			// WalkObjects: A's WalkObjects must not see B's objects.
			// Phase 4: WalkObjects uses quorum meta (shardSvc path). Shared-FSM
			// backends have no shardSvc so WalkObjects returns empty — B's keys
			// are absent by construction. Positive visibility is proved via
			// HeadObject (BadgerDB path).
			name: "WalkObjects_ScopedToGroup",
			exercise: func(t *testing.T) {
				_, _, _, fA, fB, backendA, backendB := setupTwoGroups(t)

				putObjViaApply(t, fA, bucket, "walk-a1", "A1")
				putObjViaApply(t, fA, bucket, "walk-a2", "A2")
				putObjViaApply(t, fB, bucket, "walk-b1", "B1")

				ctx := context.Background()

				// WalkObjects returns empty for no-shardSvc backends; B's keys
				// must never surface in A's view.
				var walkedA []string
				err := backendA.WalkObjects(ctx, bucket, "", func(o *storage.Object) error {
					walkedA = append(walkedA, o.Key)
					return nil
				})
				require.NoError(t, err)
				for _, k := range walkedA {
					assert.NotEqual(t, "walk-b1", k, "B's key must not appear in A's walk")
				}

				// HeadObject proves each group sees only its own objects.
				objA1, _, err := backendA.headObjectMeta(ctx, bucket, "walk-a1")
				require.NoError(t, err)
				assert.Equal(t, "A1", objA1.ETag)

				objA2, _, err := backendA.headObjectMeta(ctx, bucket, "walk-a2")
				require.NoError(t, err)
				assert.Equal(t, "A2", objA2.ETag)

				_, _, err = backendA.headObjectMeta(ctx, bucket, "walk-b1")
				assert.Error(t, err, "B's key must not be visible from A via HeadObject")

				objB1, _, err := backendB.headObjectMeta(ctx, bucket, "walk-b1")
				require.NoError(t, err)
				assert.Equal(t, "B1", objB1.ETag)
			},
		},
		{
			// ListAllObjectsStrict (live-version manifest): A's view excludes B's objects.
			name: "ListAllObjectsStrict_ScopedToGroup",
			exercise: func(t *testing.T) {
				_, _, _, fA, fB, backendA, _ := setupTwoGroups(t)

				// ListAllObjectsStrict iterates obj: versioned keys; need a VersionID.
				// CmdPutObjectMeta is a no-op in the FSM after Slice 2; write via
				// persistPutObjectMetaUpdate directly.
				cmdA := PutObjectMetaCmd{
					Bucket: bucket, Key: "snap-a", Size: 1, ContentType: "text/plain",
					ETag: "A-snap", ModTime: 1, VersionID: "v1",
				}
				require.NoError(t, fA.db.Update(func(txn MetadataTxn) error {
					return fA.persistPutObjectMetaUpdate(txn, cmdA, buildPutObjectMeta(cmdA))
				}))

				cmdB := PutObjectMetaCmd{
					Bucket: bucket, Key: "snap-b", Size: 1, ContentType: "text/plain",
					ETag: "B-snap", ModTime: 1, VersionID: "v1",
				}
				require.NoError(t, fB.db.Update(func(txn MetadataTxn) error {
					return fB.persistPutObjectMetaUpdate(txn, cmdB, buildPutObjectMeta(cmdB))
				}))

				// Also create the bucket in A (needed for ListAllObjectsStrict → ListBuckets).
				raw, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: bucket})
				require.NoError(t, err)
				_ = fA.Apply(raw)

				objs, err := backendA.ListAllObjectsStrict()
				require.NoError(t, err)
				var keys []string
				for _, o := range objs {
					keys = append(keys, o.Key)
				}
				for _, k := range keys {
					assert.NotEqual(t, "snap-b", k, "B's object must not appear in A's ListAllObjectsStrict")
				}
				// A's object must be present (if versioned format is written).
				// (snap-a is present only if at least one versioned key was written.)
				found := false
				for _, k := range keys {
					if k == "snap-a" {
						found = true
					}
				}
				assert.True(t, found, "A's snap-a must appear in ListAllObjectsStrict")
			},
		},
		// Shard placement (CmdPutShardPlacement / CmdDeleteShardPlacement) is
		// intentionally skipped: apply.go:94-100 treats both commands as no-ops
		// ("placement is now derived deterministically from the ring"). Driving
		// the placement path via apply is impossible, and there is no key written
		// to BadgerDB to assert on. The keyspace correctness for ShardPlacementKey
		// is covered by TestStateKeyspace_PrefixRoundTrip at the unit level.
		//
		// CmdMigrateShard is likewise omitted: applyMigrateShard writes a
		// PendingMigrationKey to BadgerDB only on the channel-overflow path, and
		// when no migration hooks are wired (the unit-test default) onMigrateShard
		// is nil and applyMigrateShard returns early without writing anything. It
		// is therefore not exercisable at the unit-test level without
		// SetMigrationHooks; the pending-migration: keyspace is covered by the
		// keyspace-level round-trip test (TestStateKeyspace_PrefixRoundTrip).
	}

	// Rows run sequentially (no t.Parallel). NewDistributedBackend calls
	// SetNoOpCommand after the per-row raft goroutine has started, which races
	// on raft.Node.noOpCmd — a pre-existing bug in internal/raft, out of scope
	// here. Running rows one at a time keeps the test green under -race without
	// touching raft. The fresh-DB-per-row setup means there is no correctness
	// reason the rows need to be parallel.
	for _, row := range rows {
		row := row
		t.Run(row.name, func(t *testing.T) {
			row.exercise(t)
		})
	}
}

// TestSharedFSM_PathologicalGroupIDs_NoCollision is the end-to-end version of
// TestStateKeyspace_NoCrossGroupCollision_PathologicalIDs: three groups whose
// IDs look prefix-y are written through the FSM and then read back through
// DistributedBackend.ListObjects to prove no cross-group leakage.
func TestSharedFSM_PathologicalGroupIDs_NoCollision(t *testing.T) {
	db, err := badger.Open(badgerutil.SmallOptions("").WithInMemory(true))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	ksG := mustNewKS(t, "g")
	ksGx := mustNewKS(t, "g\x00x") // looks like a byte-prefix of "g" without the len-header
	ksLong := mustNewKS(t, strings.Repeat("z", 300))

	fG := NewFSM(badgermeta.Wrap(db), ksG)
	fGx := NewFSM(badgermeta.Wrap(db), ksGx)
	fLong := NewFSM(badgermeta.Wrap(db), ksLong)

	putObjViaApply(t, fG, "b", "k", "G-payload")
	putObjViaApply(t, fGx, "b", "k", "Gx-payload")
	putObjViaApply(t, fLong, "b", "k", "Long-payload")

	// All three encoded object-meta keys must be pairwise distinct.
	eG := ksG.ObjectMetaKey("b", "k")
	eGx := ksGx.ObjectMetaKey("b", "k")
	eLong := ksLong.ObjectMetaKey("b", "k")

	assert.True(t, dbHasKey(t, badgermeta.Wrap(db), eG), "ksG's key must exist in DB")
	assert.True(t, dbHasKey(t, badgermeta.Wrap(db), eGx), "ksGx's key must exist in DB")
	assert.True(t, dbHasKey(t, badgermeta.Wrap(db), eLong), "ksLong's key must exist in DB")

	assert.NotEqual(t, eG, eGx, "g and g\\x00x must produce distinct encoded keys")
	assert.NotEqual(t, eG, eLong, "g and long-z must produce distinct encoded keys")
	assert.NotEqual(t, eGx, eLong, "g\\x00x and long-z must produce distinct encoded keys")

	// Backend-level: each group's HeadObject sees only its own payload.
	// Phase 4: LIST uses quorum meta (shardSvc path); shared-FSM backends have
	// no shardSvc, so isolation is proved via HeadObject (BadgerDB path).
	nodeG, _ := newTestNodeForSharedDB(t, "path-g")
	backendG, err := NewDistributedBackend(t.TempDir(), badgermeta.Wrap(db), nodeG, ksG, true)
	require.NoError(t, err)
	stopG := make(chan struct{})
	go backendG.RunApplyLoop(stopG)
	t.Cleanup(func() { close(stopG) })

	nodeGx, _ := newTestNodeForSharedDB(t, "path-gx")
	backendGx, err := NewDistributedBackend(t.TempDir(), badgermeta.Wrap(db), nodeGx, ksGx, true)
	require.NoError(t, err)
	stopGx := make(chan struct{})
	go backendGx.RunApplyLoop(stopGx)
	t.Cleanup(func() { close(stopGx) })

	nodeLong, _ := newTestNodeForSharedDB(t, "path-long")
	backendLong, err := NewDistributedBackend(t.TempDir(), badgermeta.Wrap(db), nodeLong, ksLong, true)
	require.NoError(t, err)
	stopLong := make(chan struct{})
	go backendLong.RunApplyLoop(stopLong)
	t.Cleanup(func() { close(stopLong) })

	ctx := context.Background()

	for _, tc := range []struct {
		name     string
		backend  *DistributedBackend
		wantETag string
	}{
		{"group-g", backendG, "G-payload"},
		{"group-gx", backendGx, "Gx-payload"},
		{"group-long", backendLong, "Long-payload"},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			obj, _, err := tc.backend.headObjectMeta(ctx, "b", "k")
			require.NoError(t, err)
			assert.Equal(t, tc.wantETag, obj.ETag, "each group must see only its own payload")
		})
	}
}

// TestSharedFSM_GroupCloseDoesNotCloseSharedDB verifies that a DistributedBackend
// opened with shared=true does NOT close the underlying BadgerDB when Close() is
// called. This is the regression test for "incorrect shared flag → group teardown
// closes the shared DB → every other group breaks".
//
// We use two DistributedBackends over one in-memory DB (the same pattern as
// TestSharedFSM_BackendListObjects_ScopedToGroup) because DistributedBackend.Close
// is the exact code path (backend.go:822-827) that decides DB lifetime.
// GroupBackend.Close delegates DB-closure to DistributedBackend.Close, so this
// test exercises the same invariant with far less stub scaffolding.
func TestSharedFSM_GroupCloseDoesNotCloseSharedDB(t *testing.T) {
	db, err := badger.Open(badgerutil.SmallOptions("").WithInMemory(true))
	require.NoError(t, err)
	// NOTE: db.Close() is called explicitly at the end of this test, after
	// both backends have been closed, to ensure the sequence is: backendA.Close
	// → backendB.Close → db.Close (not before). t.Cleanup is not used so we
	// can assert on db state between the backend closes.

	ksA := mustNewKS(t, "close-A")
	ksB := mustNewKS(t, "close-B")

	nodeA, _ := newTestNodeForSharedDB(t, "close-nodeA")
	backendA, err := NewDistributedBackend(t.TempDir(), badgermeta.Wrap(db), nodeA, ksA, true)
	require.NoError(t, err)
	stopA := make(chan struct{})
	go backendA.RunApplyLoop(stopA)

	nodeB, _ := newTestNodeForSharedDB(t, "close-nodeB")
	backendB, err := NewDistributedBackend(t.TempDir(), badgermeta.Wrap(db), nodeB, ksB, true)
	require.NoError(t, err)
	stopB := make(chan struct{})
	go backendB.RunApplyLoop(stopB)

	// Write data to both groups.
	fA := backendA.fsm
	fB := backendB.fsm
	applyCmd(t, fA, CmdCreateBucket, CreateBucketCmd{Bucket: "alive"})
	applyCmd(t, fB, CmdCreateBucket, CreateBucketCmd{Bucket: "alive"})

	// Write a distinguishable key directly so the assertion is unambiguous.
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(ksB.BucketKey("alive"), []byte("B-bucket-data"))
	}))

	// Close backend A — must NOT close the shared DB.
	close(stopA)
	require.NoError(t, backendA.Close(), "backendA.Close() must succeed")

	// Assert DB is still usable: B's data must be readable.
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(ksB.BucketKey("alive"))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			if string(v) != "B-bucket-data" {
				t.Errorf("B's bucket data corrupted after A.Close: got %q", v)
			}
			return nil
		})
	})
	require.NoError(t, err,
		"DB must still be readable after backendA.Close() — if this fails with ErrDBClosed "+
			"or a panic, shared=true was not honoured in DistributedBackend.Close")

	// Backend B can still list its bucket.
	ctx := context.Background()
	buckets, err := backendB.ListBuckets(ctx)
	require.NoError(t, err, "backendB.ListBuckets must succeed after backendA.Close")
	assert.Contains(t, buckets, "alive", "B's bucket must still be visible")

	// Clean up B.
	close(stopB)
	require.NoError(t, backendB.Close())

	// Finally close the DB (the test owns it).
	require.NoError(t, db.Close(), "db.Close() must succeed after both shared backends are closed")
}
