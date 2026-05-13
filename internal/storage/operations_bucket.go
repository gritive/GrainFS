package storage

import (
	"context"
	"fmt"
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
func (o *Operations) ForceDeleteBucket(ctx context.Context, bucket string) error {
	if err := o.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	if err := o.WalkObjects(ctx, bucket, "", func(obj *Object) error {
		return o.DeleteObject(ctx, bucket, obj.Key)
	}); err != nil {
		return fmt.Errorf("force delete: walk objects: %w", err)
	}
	return o.DeleteBucket(ctx, bucket)
}
