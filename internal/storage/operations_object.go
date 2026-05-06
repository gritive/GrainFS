package storage

import (
	"context"
	"errors"
	"io"
)

type PreviousObject struct {
	Exists    bool
	Size      int64
	ETag      string
	VersionID string
}

type PutObjectResult struct {
	Object   *Object
	Previous PreviousObject
}

func (o *Operations) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	return o.backend.PutObject(ctx, bucket, key, r, contentType)
}

func (o *Operations) PutObjectWithResult(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*PutObjectResult, error) {
	previous, err := o.previousObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	obj, err := o.backend.PutObject(ctx, bucket, key, r, contentType)
	if err != nil {
		return nil, err
	}
	return &PutObjectResult{Object: obj, Previous: previous}, nil
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

func (o *Operations) previousObject(ctx context.Context, bucket, key string) (PreviousObject, error) {
	obj, err := o.backend.HeadObject(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return PreviousObject{}, nil
		}
		return PreviousObject{}, err
	}
	if obj == nil {
		return PreviousObject{}, nil
	}
	return PreviousObject{
		Exists:    true,
		Size:      obj.Size,
		ETag:      obj.ETag,
		VersionID: obj.VersionID,
	}, nil
}
