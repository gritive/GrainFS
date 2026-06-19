package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// newSnapshotTestBackend returns a DistributedBackend with a running ShardService
// for snapshotable tests. It is an alias for newTestDistributedBackend.
func newSnapshotTestBackend(t *testing.T) *DistributedBackend {
	t.Helper()
	return newTestDistributedBackend(t)
}

// seedVersionBlob writes a per-version quorum-meta blob for (bucket, key, vid)
// on b's local ShardService. The supplied PutObjectMetaCmd fields are copied
// verbatim; Bucket/Key/VersionID are always overridden to match the seed coordinates.
func seedVersionBlob(t *testing.T, b *DistributedBackend, bucket, key, vid string, cmd PutObjectMetaCmd) {
	t.Helper()
	cmd.Bucket = bucket
	cmd.Key = key
	cmd.VersionID = vid
	blob, err := EncodeCommand(CmdPutObjectMeta, cmd)
	require.NoError(t, err)
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal(bucket, filepath.Join(key, vid), blob, 0))
}

// setVersioningForTest sets a bucket's versioning state via Raft proposal.
// The bucket must already exist.
func setVersioningForTest(t *testing.T, b *DistributedBackend, bucket, state string) {
	t.Helper()
	require.NoError(t, b.SetBucketVersioning(bucket, state))
}

// setSoleAuthForTest advances the bucket's soleauth to the target state by
// walking valid transitions (mirrors seedSoleAuth in soleauth_test.go but
// operates on a DistributedBackend rather than an FSM directly).
func setSoleAuthForTest(t *testing.T, b *DistributedBackend, bucket, target string) {
	t.Helper()
	switch target {
	case soleAuthOff:
		// default — no proposal needed
	case soleAuthPending:
		require.NoError(t, b.SetBucketSoleAuthority(bucket, soleAuthPending))
	case soleAuthOn:
		require.NoError(t, b.SetBucketSoleAuthority(bucket, soleAuthPending))
		require.NoError(t, b.SetBucketSoleAuthority(bucket, soleAuthOn))
	default:
		t.Fatalf("setSoleAuthForTest: unknown target %q", target)
	}
}

// filterBucket returns only the SnapshotObjects whose Bucket field equals name.
func filterBucket(objs []storage.SnapshotObject, name string) []storage.SnapshotObject {
	var out []storage.SnapshotObject
	for _, o := range objs {
		if o.Bucket == name {
			out = append(out, o)
		}
	}
	return out
}

// byVID finds the single SnapshotObject with VersionID == vid in the slice.
// Fails the test if none is found or if more than one matches.
func byVID(t *testing.T, objs []storage.SnapshotObject, vid string) storage.SnapshotObject {
	t.Helper()
	var found *storage.SnapshotObject
	for i := range objs {
		if objs[i].VersionID == vid {
			if found != nil {
				t.Fatalf("byVID: duplicate VersionID %q in snapshot objects", vid)
			}
			cp := objs[i]
			found = &cp
		}
	}
	if found == nil {
		t.Fatalf("byVID: VersionID %q not found in %d objects", vid, len(objs))
	}
	return *found
}

// TestListAllObjects_SoleAuthOn_CapturesPerVersionWithFidelity proves that a
// soleauth-on bucket's objects are captured from the per-version quorum-meta
// tree (not the FSM walk) with full fidelity: placement fields (NodeIDs,
// PlacementGroupID), UserMetadata, and IsLatest on the max-VID version.
func TestListAllObjects_SoleAuthOn_CapturesPerVersionWithFidelity(t *testing.T) {
	b := newSnapshotTestBackend(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "vb"))

	seedVersionBlob(t, b, "vb", "k", "v1", PutObjectMetaCmd{
		ECData: 2, ECParity: 1,
		NodeIDs:          []string{"n1", "n2", "n3"},
		PlacementGroupID: "g1",
		UserMetadata:     map[string]string{"x": "1"},
	})
	seedVersionBlob(t, b, "vb", "k", "v2", PutObjectMetaCmd{
		ECData: 2, ECParity: 1,
		NodeIDs:          []string{"n1", "n2", "n3"},
		PlacementGroupID: "g1",
		MetaSeq:          1,
	})

	setVersioningForTest(t, b, "vb", "Enabled")
	setSoleAuthForTest(t, b, "vb", soleAuthOn)

	objs, err := b.ListAllObjects()
	require.NoError(t, err)

	got := filterBucket(objs, "vb")
	require.Len(t, got, 2)

	v1obj := byVID(t, got, "v1")
	require.Equal(t, map[string]string{"x": "1"}, v1obj.UserMetadata)
	require.Equal(t, "g1", v1obj.PlacementGroupID)
	require.False(t, v1obj.IsLatest)

	v2obj := byVID(t, got, "v2")
	require.True(t, v2obj.IsLatest)
	require.Equal(t, "g1", v2obj.PlacementGroupID)
}

// TestListAllBuckets_SoleAuthEpoch_Captured verifies that ListAllBuckets sets
// SoleAuthEpoch when the epoch is > 0, and omits it (leaves it 0) for a fresh
// bucket that was never flipped.
func TestListAllBuckets_SoleAuthEpoch_Captured(t *testing.T) {
	b := newSnapshotTestBackend(t)
	ctx := context.Background()

	// "flipped": pending→off abort leaves epoch=2, state=off.
	require.NoError(t, b.CreateBucket(ctx, "flipped"))
	require.NoError(t, b.SetBucketSoleAuthority("flipped", soleAuthPending)) // epoch 1
	require.NoError(t, b.SetBucketSoleAuthority("flipped", soleAuthOff))     // epoch 2 (abort)

	// "fresh": never flipped, epoch stays 0.
	require.NoError(t, b.CreateBucket(ctx, "fresh"))

	buckets, err := b.ListAllBuckets()
	require.NoError(t, err)

	// Find each bucket in the result.
	byName := make(map[string]storage.SnapshotBucket, len(buckets))
	for _, bkt := range buckets {
		byName[bkt.Name] = bkt
	}

	flipped, ok := byName["flipped"]
	require.True(t, ok, "flipped bucket must appear in snapshot")
	require.Equal(t, uint32(2), flipped.SoleAuthEpoch, "abort cycle epoch must be captured")

	fresh, ok := byName["fresh"]
	require.True(t, ok, "fresh bucket must appear in snapshot")
	require.Equal(t, uint32(0), fresh.SoleAuthEpoch, "never-flipped bucket must have epoch 0")
}

// TestRestoreBuckets_EpochFloor_Monotonic verifies that RestoreBuckets raises
// the stored epoch to the snapshot's SoleAuthEpoch floor even when the
// transition-replay alone would land at a lower epoch.
//
// Scenario: the DESTINATION cluster is fresh (epoch 0). The snapshot records
// SoleAuthEpoch=5 with state=off (the pending↔off abort case). After restore
// the destination must have epoch ≥ 5.
func TestRestoreBuckets_EpochFloor_Monotonic(t *testing.T) {
	b := newSnapshotTestBackend(t)

	snap := []storage.SnapshotBucket{{
		Name:          "b",
		SoleAuthState: "", // off (default)
		SoleAuthEpoch: 5,  // accrued from prior pending↔off cycles
	}}
	require.NoError(t, b.RestoreBuckets(snap))

	ep, err := b.GetBucketSoleAuthEpoch("b")
	require.NoError(t, err)
	require.Equal(t, uint32(5), ep, "restored epoch must equal the snapshot floor")
}

// TestRestoreBuckets_EpochFloor_DoesNotLower verifies that the floor is monotonic:
// if the destination already has a higher epoch than the snapshot, the floor does
// not lower it.
func TestRestoreBuckets_EpochFloor_DoesNotLower(t *testing.T) {
	b := newSnapshotTestBackend(t)
	ctx := context.Background()

	// Destination already has epoch 7 via real transitions.
	require.NoError(t, b.CreateBucket(ctx, "b"))
	require.NoError(t, b.SetBucketSoleAuthority("b", soleAuthPending)) // epoch 1
	require.NoError(t, b.SetBucketSoleAuthority("b", soleAuthOff))     // epoch 2
	require.NoError(t, b.SetBucketSoleAuthority("b", soleAuthPending)) // epoch 3
	require.NoError(t, b.SetBucketSoleAuthority("b", soleAuthOff))     // epoch 4
	require.NoError(t, b.SetBucketSoleAuthority("b", soleAuthPending)) // epoch 5
	require.NoError(t, b.SetBucketSoleAuthority("b", soleAuthOff))     // epoch 6
	require.NoError(t, b.SetBucketSoleAuthority("b", soleAuthPending)) // epoch 7

	// Restore with a stale snapshot floor (3 < 7).
	snap := []storage.SnapshotBucket{{
		Name:          "b",
		SoleAuthState: soleAuthPending,
		SoleAuthEpoch: 3,
	}}
	require.NoError(t, b.RestoreBuckets(snap))

	ep, err := b.GetBucketSoleAuthEpoch("b")
	require.NoError(t, err)
	require.GreaterOrEqual(t, ep, uint32(7), "floor must not lower the epoch; destination keeps its higher value")
}

// TestPlanRestoreBucketSoleAuth verifies the soleauth state reconciliation plan
// for all (curSA, target) combinations (pure function, no proposals).
func TestPlanRestoreBucketSoleAuth(t *testing.T) {
	cases := []struct {
		cur, target string
		wantPlan    []string
		wantErr     bool
	}{
		// off target
		{soleAuthOff, soleAuthOff, []string{}, false},
		{soleAuthPending, soleAuthOff, []string{soleAuthOff}, false},
		{soleAuthOn, soleAuthOff, nil, true}, // terminal cannot downgrade

		// pending target
		{soleAuthOff, soleAuthPending, []string{soleAuthPending}, false},
		{soleAuthPending, soleAuthPending, []string{}, false},
		{soleAuthOn, soleAuthPending, nil, true}, // terminal

		// on target
		{soleAuthOff, soleAuthOn, []string{soleAuthPending, soleAuthOn}, false},
		{soleAuthPending, soleAuthOn, []string{soleAuthOn}, false},
		{soleAuthOn, soleAuthOn, []string{}, false}, // idempotent

		// empty target treated as off
		{soleAuthOff, "", []string{}, false},
	}
	for _, tc := range cases {
		tc := tc
		name := tc.cur + "->" + tc.target
		t.Run(name, func(t *testing.T) {
			plan, err := planRestoreBucketSoleAuth(tc.cur, tc.target)
			if tc.wantErr {
				require.Error(t, err, "expected error for %s", name)
				return
			}
			require.NoError(t, err)
			if tc.wantPlan == nil || len(tc.wantPlan) == 0 {
				require.Empty(t, plan)
			} else {
				require.Equal(t, tc.wantPlan, plan)
			}
		})
	}
}
