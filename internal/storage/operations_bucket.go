package storage

import "context"

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
