package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// makeCreateBucketCmd is a test helper that encodes a MetaCreateBucketCmd
// wrapped in a MetaCmd envelope, mirroring makePutBucketAssignmentCmd.
func makeCreateBucketCmd(t *testing.T, bucket, groupID string, bypassReserved bool) []byte {
	t.Helper()
	data, err := encodeMetaCreateBucketCmd(bucket, groupID, bypassReserved)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeCreateBucket, data)
	require.NoError(t, err)
	return cmd
}

// TestApplyCreateBucketSetsRecordExclusive verifies that applyCreateBucket:
//   - creates a BucketRecord with the given GroupID on first call
//   - returns ErrBucketAlreadyExists on a duplicate create
func TestApplyCreateBucketSetsRecordExclusive(t *testing.T) {
	f := NewMetaFSM()

	// First create must succeed.
	require.NoError(t, f.applyCmd(makeCreateBucketCmd(t, "b1", "group-2", false)))

	rec, ok := f.BucketRecord("b1")
	require.True(t, ok, "BucketRecord must exist after create")
	assert.Equal(t, "group-2", rec.GroupID)

	// Second create of same bucket must return ErrBucketAlreadyExists.
	err := f.applyCmd(makeCreateBucketCmd(t, "b1", "group-2", false))
	require.Error(t, err)
	require.ErrorIs(t, err, storage.ErrBucketAlreadyExists)
}

// TestApplyCreateBucketReservedGuard verifies that:
//   - creating "default" without bypass returns an error
//   - creating "__grainfs_x" (internal prefix) without bypass returns an error
//   - creating "__grainfs_x" with bypassReserved=true succeeds (bootstrap path)
func TestApplyCreateBucketReservedGuard(t *testing.T) {
	t.Run("reserved_default_blocked", func(t *testing.T) {
		f := NewMetaFSM()
		err := f.applyCmd(makeCreateBucketCmd(t, "default", "group-0", false))
		require.Error(t, err, "creating reserved bucket 'default' must fail")
	})

	t.Run("reserved_internal_prefix_blocked", func(t *testing.T) {
		f := NewMetaFSM()
		err := f.applyCmd(makeCreateBucketCmd(t, "_grainfs_x", "group-0", false))
		require.Error(t, err, "creating reserved bucket '_grainfs_x' must fail")
	})

	t.Run("bypass_reserved_allows_internal_bucket", func(t *testing.T) {
		f := NewMetaFSM()
		err := f.applyCmd(makeCreateBucketCmd(t, "_grainfs_x", "group-0", true))
		require.NoError(t, err, "creating reserved bucket with bypassReserved=true must succeed")

		rec, ok := f.BucketRecord("_grainfs_x")
		require.True(t, ok)
		assert.Equal(t, "group-0", rec.GroupID)
	})
}

// TestApplyCreateBucketFiresOnBucketAssignedCallback verifies that the
// onBucketAssigned callback (set via SetOnBucketAssigned) is fired after
// a successful CreateBucket apply, mirroring applyPutBucketAssignment behavior.
func TestApplyCreateBucketFiresOnBucketAssignedCallback(t *testing.T) {
	f := NewMetaFSM()

	var cbBucket, cbGroupID string
	f.SetOnBucketAssigned(func(bucket, groupID string) {
		cbBucket = bucket
		cbGroupID = groupID
	})

	require.NoError(t, f.applyCmd(makeCreateBucketCmd(t, "photos", "group-3", false)))

	assert.Equal(t, "photos", cbBucket, "callback must receive bucket name")
	assert.Equal(t, "group-3", cbGroupID, "callback must receive group ID")
}

// makeDeleteBucketCmd is a test helper that encodes a MetaDeleteBucketCmd
// wrapped in a MetaCmd envelope.
func makeDeleteBucketCmd(t *testing.T, bucket string) []byte {
	t.Helper()
	data, err := encodeMetaDeleteBucketCmd(bucket)
	require.NoError(t, err)
	cmd, err := encodeMetaCmd(MetaCmdTypeDeleteBucket, data)
	require.NoError(t, err)
	return cmd
}

// TestApplyDeleteBucketRemovesRecordIdempotent verifies that applyDeleteBucket:
//   - removes the BucketRecord on first call
//   - fires onBucketUnassigned callback with the bucket name
//   - is idempotent (second call is a no-op with no error)
func TestApplyDeleteBucketRemovesRecordIdempotent(t *testing.T) {
	f := NewMetaFSM()

	// Seed a record with versioning + policy.
	require.NoError(t, f.applyCmd(makeCreateBucketCmd(t, "b1", "group-2", false)))
	f.mu.Lock()
	rec := f.bucketRecords["b1"]
	rec.Versioning = "Enabled"
	rec.Policy = []byte(`{"Version":"2012-10-17"}`)
	f.bucketRecords["b1"] = rec
	f.mu.Unlock()

	// Confirm the record exists.
	_, ok := f.BucketRecord("b1")
	require.True(t, ok, "record must exist before delete")

	// Register a capturing onBucketUnassigned callback.
	var cbBucket string
	var cbCount int
	f.SetOnBucketUnassigned(func(bucket string) {
		cbBucket = bucket
		cbCount++
	})

	// First delete: record must be gone, callback must fire.
	require.NoError(t, f.applyCmd(makeDeleteBucketCmd(t, "b1")))
	_, ok = f.BucketRecord("b1")
	require.False(t, ok, "BucketRecord must be absent after delete")
	assert.Equal(t, "b1", cbBucket, "callback must receive the bucket name")
	assert.Equal(t, 1, cbCount, "callback must fire exactly once on first delete")

	// Second delete: idempotent — no error.
	require.NoError(t, f.applyCmd(makeDeleteBucketCmd(t, "b1")))
}
