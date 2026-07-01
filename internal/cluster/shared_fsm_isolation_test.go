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
			// produce distinct MetaFSM entries; deleting one does not affect the other.
			// (Task 12: bucket control-plane moved to meta-raft; assertions now via
			// MetaBucketStore / MetaFSM rather than group-0 BadgerDB keys.)
			name: "CreateBucket_DeleteBucket",
			exercise: func(t *testing.T) {
				ctx := context.Background()
				// Each group gets its own MetaBucketStore (separate MetaFSM).
				mbsA := newTestMetaBucketStore(t)
				mbsB := newTestMetaBucketStore(t)

				require.NoError(t, mbsA.CreateBucket(ctx, bucket, "gA", false))
				require.NoError(t, mbsB.CreateBucket(ctx, bucket, "gB", false))

				_, okA := mbsA.Record(bucket)
				_, okB := mbsB.Record(bucket)
				assert.True(t, okA, "A's bucket must exist in MetaFSM")
				assert.True(t, okB, "B's bucket must exist in MetaFSM")

				// Delete A's bucket.
				require.NoError(t, mbsA.DeleteBucket(ctx, bucket))

				_, okA = mbsA.Record(bucket)
				_, okB = mbsB.Record(bucket)
				assert.False(t, okA, "A's bucket must be gone from MetaFSM")
				assert.True(t, okB, "B's bucket must survive A's delete")
			},
		},
		{
			// PutObjectMeta keyspace isolation: same bucket + key in both groups
			// produce DISTINCT encoded object-meta keys. (Object metadata no longer
			// lives in or is read from the shared FSM DB under blob-primary — it is
			// blob-resident — so the former HeadObject point-read isolation probe is
			// retired; this row keeps the keyspace-distinctness invariant, which is
			// what guards against cross-group collision.)
			name: "PutObjectMeta_KeyspaceIsolation",
			exercise: func(t *testing.T) {
				ksA := mustNewKS(t, "iso-A")
				ksB := mustNewKS(t, "iso-B")
				assert.NotEqual(t, ksA.ObjectMetaKey(bucket, "obj1"), ksB.ObjectMetaKey(bucket, "obj1"),
					"two groups must produce distinct encoded object-meta keys")
			},
		},
		{
			// DeleteBucket: A deletes a bucket; B's same-name bucket survives.
			// (Task 12: bucket control-plane moved to meta-raft; assertions via
			// MetaBucketStore rather than group-0 BadgerDB keys.)
			name: "DeleteBucket_DoesNotAffectPeer",
			exercise: func(t *testing.T) {
				ctx := context.Background()
				mbsA := newTestMetaBucketStore(t)
				mbsB := newTestMetaBucketStore(t)

				require.NoError(t, mbsA.CreateBucket(ctx, "bktX", "gA", false))
				require.NoError(t, mbsB.CreateBucket(ctx, "bktX", "gB", false))

				// A deletes the bucket; B's copy must survive.
				require.NoError(t, mbsA.DeleteBucket(ctx, "bktX"))

				_, okA := mbsA.Record("bktX")
				_, okB := mbsB.Record("bktX")
				assert.False(t, okA, "A's bucket must be gone after delete")
				assert.True(t, okB, "B's bucket must survive A's delete")
			},
		},
		{
			// SetBucketPolicy / DeleteBucketPolicy: distinct MetaFSM entries per group.
			// (Task 12: bucket control-plane moved to meta-raft.)
			name: "BucketPolicy_Isolation",
			exercise: func(t *testing.T) {
				ctx := context.Background()
				mbsA := newTestMetaBucketStore(t)
				mbsB := newTestMetaBucketStore(t)

				require.NoError(t, mbsA.CreateBucket(ctx, bucket, "gA", false))
				require.NoError(t, mbsB.CreateBucket(ctx, bucket, "gB", false))

				require.NoError(t, mbsA.SetPolicy(ctx, bucket, []byte(`{"Effect":"Allow"}`)))
				require.NoError(t, mbsB.SetPolicy(ctx, bucket, []byte(`{"Effect":"Deny"}`)))

				recA, _ := mbsA.Record(bucket)
				recB, _ := mbsB.Record(bucket)
				assert.Equal(t, `{"Effect":"Allow"}`, string(recA.Policy), "A's policy must be Allow")
				assert.Equal(t, `{"Effect":"Deny"}`, string(recB.Policy), "B's policy must be Deny")
				assert.NotEqual(t, recA.Policy, recB.Policy, "policies must be distinct per group")

				// Delete A's policy.
				require.NoError(t, mbsA.DeletePolicy(ctx, bucket))

				recA, _ = mbsA.Record(bucket)
				recB, _ = mbsB.Record(bucket)
				assert.Empty(t, recA.Policy, "A's policy must be gone")
				assert.NotEmpty(t, recB.Policy, "B's policy must survive A's delete")
			},
		},
		{
			// SetBucketVersioning: A enables versioning, B does not. Distinct MetaFSM state.
			// (Task 12: bucket control-plane moved to meta-raft.)
			name: "BucketVersioning_Isolation",
			exercise: func(t *testing.T) {
				ctx := context.Background()
				mbsA := newTestMetaBucketStore(t)
				mbsB := newTestMetaBucketStore(t)

				require.NoError(t, mbsA.CreateBucket(ctx, bucket, "gA", false))
				require.NoError(t, mbsB.CreateBucket(ctx, bucket, "gB", false))

				require.NoError(t, mbsA.SetVersioning(ctx, bucket, "Enabled"))
				// B does not set versioning.

				recA, _ := mbsA.Record(bucket)
				recB, _ := mbsB.Record(bucket)
				assert.Equal(t, "Enabled", recA.Versioning, "A's versioning must be Enabled")
				assert.Equal(t, "", recB.Versioning, "B must not have versioning set")
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
		// Object-read isolation rows (PutObjectMeta HeadObject probe,
		// WalkObjects_ScopedToGroup, ListAllObjectsStrict_ScopedToGroup) were
		// removed: object metadata is no longer written to or read from the shared
		// FSM BadgerDB (it is blob-resident under blob-primary), so the shared-DB
		// object-collision risk these probed is structurally gone. FSM-keyspace
		// isolation is still covered by the keyspace-distinctness rows above and by
		// the snapshot/restore prefix-isolation tests in shared_fsm_test.go.
		//
		// Shard placement is intentionally skipped: the data-group placement apply
		// path is gone ("placement is now derived deterministically from the ring").
		// Driving the placement path via apply is impossible, and there is no key
		// written to BadgerDB to assert on. The keyspace correctness for
		// ShardPlacementKey is covered by TestStateKeyspace_PrefixRoundTrip at the
		// unit level.
		//
		// Balancer shard migration is likewise omitted: it is retired, and stale
		// log replay is a no-op. The legacy pending-migration: keyspace is covered
		// by the keyspace-level round-trip test (TestStateKeyspace_PrefixRoundTrip).
	}

	// Rows run sequentially (no t.Parallel). Each row owns a fresh DB and raft
	// node; there is no correctness reason the rows need to be parallel.
	for _, row := range rows {
		row := row
		t.Run(row.name, func(t *testing.T) {
			row.exercise(t)
		})
	}
}

// TestSharedFSM_PathologicalGroupIDs_NoCollision is the end-to-end version of
// TestStateKeyspace_NoCrossGroupCollision_PathologicalIDs: three groups whose
// IDs look prefix-y are written through the FSM and proven to occupy distinct,
// non-colliding encoded keyspaces (the length header prevents byte-prefix
// collisions). The former backend HeadObject probe is retired — object metadata
// is blob-resident under blob-primary and no longer read from the shared FSM DB.
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

	// All three encoded object-meta keys must be pairwise distinct and present.
	eG := ksG.ObjectMetaKey("b", "k")
	eGx := ksGx.ObjectMetaKey("b", "k")
	eLong := ksLong.ObjectMetaKey("b", "k")

	assert.True(t, dbHasKey(t, badgermeta.Wrap(db), eG), "ksG's key must exist in DB")
	assert.True(t, dbHasKey(t, badgermeta.Wrap(db), eGx), "ksGx's key must exist in DB")
	assert.True(t, dbHasKey(t, badgermeta.Wrap(db), eLong), "ksLong's key must exist in DB")

	assert.NotEqual(t, eG, eGx, "g and g\\x00x must produce distinct encoded keys")
	assert.NotEqual(t, eG, eLong, "g and long-z must produce distinct encoded keys")
	assert.NotEqual(t, eGx, eLong, "g\\x00x and long-z must produce distinct encoded keys")
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

	// Write a distinguishable raw key into the shared DB. This is a DB-liveness
	// probe (read back via db.View after A.Close), independent of bucket semantics
	// — it proves the shared DB survives A.Close. Bucket existence for B's
	// ListBuckets is supplied separately via B's MetaBucketStore below (the sole
	// authority since Task 12).
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set(ksB.Key([]byte("liveness-probe")), []byte("B-bucket-data"))
	}))

	// HeadBucket / ListBuckets read MetaBucketStore (sole authority since Task 12);
	// register "alive" in B's MBS so backendB.ListBuckets sees it below.
	seedBucketsInMBS(t, backendB, "alive")

	// Close backend A — must NOT close the shared DB.
	close(stopA)
	require.NoError(t, backendA.Close(), "backendA.Close() must succeed")

	// Assert DB is still usable: B's data must be readable.
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(ksB.Key([]byte("liveness-probe")))
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
