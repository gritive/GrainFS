package storage

import (
	"context"
	"io"
)

func (o *Operations) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	return o.backend.PutObject(ctx, bucket, key, r, contentType)
}

func (o *Operations) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *Object, error) {
	return o.backend.GetObject(ctx, bucket, key)
}

func (o *Operations) HeadObject(ctx context.Context, bucket, key string) (*Object, error) {
	return o.backend.HeadObject(ctx, bucket, key)
}

func (o *Operations) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*Object, error) {
	return o.backend.ListObjects(ctx, bucket, prefix, maxKeys)
}

func (o *Operations) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*Object) error) error {
	return o.backend.WalkObjects(ctx, bucket, prefix, fn)
}
