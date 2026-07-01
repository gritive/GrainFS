package cluster

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// TestHeadObjectVersionCtx_StampReachesGate proves the ctx-threaded version read
// path honors the stamped bucket-versioning decision instead of the local store.
//
// Setup: the bucket is NOT versioning-enabled in the local store (no
// SetBucketVersioning), but per-version blobs exist for the key. With the
// versioning gate stamped Enabled in ctx, headObjectMetaV MUST consult the
// per-version store and resolve the specific version. RED before T1: the public
// version read path hardcodes context.Background() (object_version.go:28), so the
// stamp is dropped, the gate reads the local store (Unversioned), the per-version
// block is skipped, and the FSM ObjectMetaKeyV read (no record) → 404.
func TestHeadObjectVersionCtx_StampReachesGate(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	const bkt, key = "vbkt", "obj"
	require.NoError(t, b.CreateBucket(ctx, bkt))
	// Deliberately do NOT enable versioning in the local store.

	const vid = "019ed400-0000-7000-8000-000000000001"
	blob, err := encodeQuorumMetaBlob(PutObjectMetaCmd{
		Bucket: bkt, Key: key, VersionID: vid, ETag: "etag-v1", NodeIDs: []string{"self"}, ECData: 1,
	})
	require.NoError(t, err)
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal(bkt, filepath.Join(key, vid), blob))

	// No stamp: the gate falls back to the local (Unversioned) store → per-version
	// path is skipped → 404 (sanity that the blob alone is not enough).
	_, err = b.headObjectVersionCtx(context.Background(), bkt, key, vid)
	require.ErrorIs(t, err, storage.ErrObjectNotFound,
		"unstamped + locally-unversioned → per-version path inactive → 404")

	// Stamped Enabled: the gate must resolve from ctx → per-version path active →
	// the version resolves from its blob.
	stamped := ContextWithBucketVersioning(context.Background(), true)
	obj, err := b.headObjectVersionCtx(stamped, bkt, key, vid)
	require.NoError(t, err, "stamped Enabled ctx must reach the gate so the per-version blob is read")
	require.Equal(t, vid, obj.VersionID)
	require.Equal(t, "etag-v1", obj.ETag)
}
