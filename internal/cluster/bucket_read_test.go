package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestHeadBucketFalseAfterDelete verifies that HeadBucket returns ErrBucketNotFound
// for a bucket that was created and then deleted.
func TestHeadBucketFalseAfterDelete(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(ctx, "to-delete"))
	require.NoError(t, b.HeadBucket(ctx, "to-delete"), "bucket must exist after create")

	require.NoError(t, b.DeleteBucket(ctx, "to-delete"))

	err := b.HeadBucket(ctx, "to-delete")
	require.ErrorIs(t, err, storage.ErrBucketNotFound, "HeadBucket must return ErrBucketNotFound after delete")
}

// TestGetBucketVersioningReadsMetaRecord verifies that GetBucketVersioning returns
// the versioning state that was set via the meta record write path.
func TestGetBucketVersioningReadsMetaRecord(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(ctx, "ver-bucket"))

	// Before setting versioning, must return default "Unversioned".
	state, err := b.GetBucketVersioning("ver-bucket")
	require.NoError(t, err)
	assert.Equal(t, "Unversioned", state, "default versioning must be Unversioned")

	// Set versioning via write path (goes through MetaBucketStore).
	require.NoError(t, b.SetBucketVersioning("ver-bucket", "Enabled"))

	// Read path must see "Enabled" from the meta record.
	state, err = b.GetBucketVersioning("ver-bucket")
	require.NoError(t, err)
	assert.Equal(t, "Enabled", state, "GetBucketVersioning must return Enabled after set")
}

// TestGetBucketPolicyReadsMetaRecord verifies that GetBucketPolicy returns the policy
// set via the write path and empty after deletion.
func TestGetBucketPolicyReadsMetaRecord(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)

	require.NoError(t, b.CreateBucket(ctx, "pol-bucket"))

	policy := []byte(`{"Version":"2012-10-17","Statement":[]}`)
	require.NoError(t, b.SetBucketPolicy("pol-bucket", policy))

	// Read path must return the set policy.
	got, err := b.GetBucketPolicy("pol-bucket")
	require.NoError(t, err)
	assert.Equal(t, policy, got, "GetBucketPolicy must return the set policy")

	// Delete the policy and verify it's gone.
	// GetBucketPolicy returns ErrBucketNotFound when no policy is set, matching
	// the old BadgerDB semantics and the S3 "NoSuchBucketPolicy" (404) behavior.
	require.NoError(t, b.DeleteBucketPolicy("pol-bucket"))

	got, err = b.GetBucketPolicy("pol-bucket")
	require.ErrorIs(t, err, storage.ErrBucketNotFound, "GetBucketPolicy must return ErrBucketNotFound when no policy is set")
	assert.Empty(t, got)
}
