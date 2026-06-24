package cluster

import (
	"context"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// residualShardFilesForTest counts leaf files under the shard directory for
// bucket: {dataDir}/{bucket}/. Returns the leaf file paths (WalkDir count).
// shards are stored at {dataDir}/{bucket}/{shardKey}/shard_{N}.
func residualShardFilesForTest(t *testing.T, dataDir, bucket string) []string {
	t.Helper()
	bucketShardRoot := filepath.Join(dataDir, bucket)
	var files []string
	if err := filepath.WalkDir(bucketShardRoot, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			// If root doesn't exist, no files remain — that's the success case.
			return nil
		}
		if !d.IsDir() {
			files = append(files, path)
		}
		return nil
	}); err != nil {
		// Root absence is fine (already deleted).
		return nil
	}
	return files
}

// TestForceDeleteBucket_NonVersioned_NoResidue verifies that ForceDeleteBucket
// on a non-versioned bucket physically removes both:
//   - .quorum_meta/{bucket} blobs (the qmeta placement records)
//   - shards/{bucket}/{key} shard files
//
// Before this fix, os.RemoveAll(bucketDir) in DeleteBucket only removed
// root/data/{bucket}; it did NOT reach .quorum_meta/{bucket} or
// the shard files — so they leaked. This test must FAIL before the fix and
// PASS after.
func TestForceDeleteBucket_NonVersioned_NoResidue(t *testing.T) {
	b, dataDir := newSingleNodeBackendWithDirForTest(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	putTestObjectForRetire(t, b, "b", "k1", []byte("a"))
	putTestObjectForRetire(t, b, "b", "k2", []byte("bb"))

	require.NoError(t, b.ForceDeleteBucket(ctx, "b"))
	require.ErrorIs(t, b.HeadBucket(ctx, "b"), storage.ErrBucketNotFound)

	// Physical residue check: no .quorum_meta/b blobs remain.
	require.NoFileExists(t, filepath.Join(dataDir, ".quorum_meta", "b"))

	// Physical residue check: no shard files remain under the bucket shard dir.
	residual := residualShardFilesForTest(t, dataDir, "b")
	require.Empty(t, residual, "expected no shard files after ForceDeleteBucket, found: %v", residual)
}

// TestForceDeleteBucket_Versioned_BlobOnly verifies that ForceDeleteBucket on a
// versioned bucket still works correctly (the versioned path uses
// DeleteObjectVersion which already purges per-version blobs + shards).
func TestForceDeleteBucket_Versioned_BlobOnly(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	require.NoError(t, b.SetBucketVersioning("b", "Enabled"))
	putTestObjectForRetire(t, b, "b", "k1", []byte("a"))
	putTestObjectForRetire(t, b, "b", "k2", []byte("bb"))

	require.NoError(t, b.ForceDeleteBucket(ctx, "b"))
	require.ErrorIs(t, b.HeadBucket(ctx, "b"), storage.ErrBucketNotFound)
}
