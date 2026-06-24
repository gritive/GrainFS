package storage

import (
	"context"
)

func (o *Operations) CreateBucket(ctx context.Context, bucket string) error {
	return o.backend.CreateBucket(ctx, bucket)
}

func (o *Operations) HeadBucket(ctx context.Context, bucket string) error {
	return o.backend.HeadBucket(ctx, bucket)
}

func (o *Operations) DeleteBucket(ctx context.Context, bucket string) error {
	return o.backend.DeleteBucket(ctx, bucket)
}

func (o *Operations) ListBuckets(ctx context.Context) ([]string, error) {
	return o.backend.ListBuckets(ctx)
}

// ForceDeleteBucket deletes all objects in the bucket and then removes it.
// Used by admin force-delete; regular S3 delete rejects non-empty buckets.
//
// Forwards to the backend's version-aware force-delete (like CreateBucket /
// DeleteBucket / HeadBucket). The backend HARD-purges BOTH blob trees — latest-only
// shards+qmeta AND per-version blobs+shards — which the cluster backend requires: a
// generic latest-only soft-delete walk could not purge a versioned/Suspended
// bucket's versions (leaving the bucket undeleteable behind DeleteBucket's
// per-version emptiness check) and would orphan non-versioned objects' shards.
// LocalBackend keeps its own generic walk for the non-cluster path.
func (o *Operations) ForceDeleteBucket(ctx context.Context, bucket string) error {
	// Upfront existence check so a missing bucket surfaces as ErrBucketNotFound
	// (→ admin 404), matching the prior generic-walk behavior: the cluster
	// coordinator's force path resolves versioning state before RouteBucket and
	// would otherwise return an internal error for a never-assigned bucket.
	if err := o.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	return o.backend.ForceDeleteBucket(ctx, bucket)
}

// CountObjects counts all objects in bucket. O(N in objects) — use for
// bucket info only, not bulk list operations.
func (o *Operations) CountObjects(ctx context.Context, bucket string) (int64, error) {
	var n int64
	err := o.WalkObjects(ctx, bucket, "", func(*Object) error {
		n++
		return nil
	})
	return n, err
}
