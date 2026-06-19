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
