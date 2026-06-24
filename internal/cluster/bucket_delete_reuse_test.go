package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// newTestMetaBucketStore returns a directFSMMetaBucketStore backed by a fresh
// MetaFSM — the meta path for all bucket-lifecycle operations post Task 12.
func newTestMetaBucketStore(t *testing.T) *directFSMMetaBucketStore {
	t.Helper()
	return newDirectFSMMetaBucketStore(nil).(*directFSMMetaBucketStore)
}

// TestDeleteBucket_ClearsVersioning proves a recreated same-name bucket does not
// inherit the prior incarnation's versioning state (live S3-correctness: a new
// bucket is unversioned). Verified via MetaBucketStore (meta-FSM path).
func TestDeleteBucket_ClearsVersioning(t *testing.T) {
	ctx := context.Background()
	mbs := newTestMetaBucketStore(t)
	const b = "b"

	require.NoError(t, mbs.CreateBucket(ctx, b, "local", false))
	require.NoError(t, mbs.SetVersioning(ctx, b, "Enabled"))

	rec, ok := mbs.Record(b)
	require.True(t, ok, "bucket must exist after create")
	require.Equal(t, "Enabled", rec.Versioning, "versioning must be set")

	require.NoError(t, mbs.DeleteBucket(ctx, b))

	_, ok = mbs.Record(b)
	require.False(t, ok, "bucket must not exist after delete")

	// Recreate: versioning must start fresh (unversioned).
	require.NoError(t, mbs.CreateBucket(ctx, b, "local", false))
	rec, ok = mbs.Record(b)
	require.True(t, ok)
	require.Equal(t, "", rec.Versioning, "recreated bucket must be unversioned")
}

// TestDeleteBucket_ClearsPolicy proves a recreated same-name bucket does not
// inherit the prior incarnation's bucket policy. Verified via MetaBucketStore.
func TestDeleteBucket_ClearsPolicy(t *testing.T) {
	ctx := context.Background()
	mbs := newTestMetaBucketStore(t)
	const b = "b"

	require.NoError(t, mbs.CreateBucket(ctx, b, "local", false))
	require.NoError(t, mbs.SetPolicy(ctx, b, []byte(`{"Version":"2012-10-17"}`)))

	rec, ok := mbs.Record(b)
	require.True(t, ok)
	require.NotEmpty(t, rec.Policy, "policy must be set")

	require.NoError(t, mbs.DeleteBucket(ctx, b))

	_, ok = mbs.Record(b)
	require.False(t, ok, "bucket must not exist after delete")

	// Recreate: policy must start fresh (empty).
	require.NoError(t, mbs.CreateBucket(ctx, b, "local", false))
	rec, ok = mbs.Record(b)
	require.True(t, ok)
	require.Empty(t, rec.Policy, "recreated bucket must have no policy")
}

// TestDeleteBucket_AbsentStateKeys_NoError proves deleting a plain bucket (no
// policy/versioning ever set) succeeds without error. Verified via MetaBucketStore.
func TestDeleteBucket_AbsentStateKeys_NoError(t *testing.T) {
	ctx := context.Background()
	mbs := newTestMetaBucketStore(t)
	const b = "b"

	require.NoError(t, mbs.CreateBucket(ctx, b, "local", false))
	require.NoError(t, mbs.DeleteBucket(ctx, b), "delete must tolerate absent per-bucket state keys")

	_, ok := mbs.Record(b)
	require.False(t, ok, "bucket must be gone after delete")
}
