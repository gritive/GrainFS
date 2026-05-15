package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
)

type InvalidMutationResultError struct {
	Op     string
	Field  string
	Reason string
}

func (e InvalidMutationResultError) Error() string {
	return fmt.Sprintf("%s returned invalid mutation result field %q: %s", e.Op, e.Field, e.Reason)
}

type ObjectFacts struct {
	Size         int64
	ETag         string
	VersionID    string
	LastModified int64
}

type PreviousObject struct {
	Exists    bool
	Size      int64
	ETag      string
	VersionID string
}

type PutObjectResult struct {
	Object   ObjectFacts
	Previous PreviousObject
}

type userMetadataResultPutter interface {
	PutObjectWithUserMetadataResult(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*PutObjectResult, error)
}

func (o *Operations) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	return o.backend.PutObject(ctx, bucket, key, r, contentType)
}

func (o *Operations) PutObjectWithResult(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*PutObjectResult, error) {
	return o.PutObjectWithUserMetadataResult(ctx, bucket, key, r, contentType, nil)
}

func (o *Operations) PutObjectWithUserMetadata(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*Object, error) {
	if len(userMetadata) == 0 {
		return o.backend.PutObject(ctx, bucket, key, r, contentType)
	}
	putter, ok := o.backend.(UserMetadataPutter)
	if !ok {
		return nil, UnsupportedOperationError{Op: "PutObjectWithUserMetadata", Reason: UnsupportedReasonNoAdapter}
	}
	return putter.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, userMetadata)
}

func (o *Operations) PutObjectWithUserMetadataResult(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*PutObjectResult, error) {
	if putter, ok := o.backend.(userMetadataResultPutter); ok {
		return putter.PutObjectWithUserMetadataResult(ctx, bucket, key, r, contentType, userMetadata)
	}
	previous, err := o.previousObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	obj, err := o.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, userMetadata)
	if err != nil {
		return nil, err
	}
	facts, err := mutationObjectFacts("PutObject", obj)
	if err != nil {
		return nil, err
	}
	return &PutObjectResult{Object: facts, Previous: previous}, nil
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
		return PreviousObject{}, InvalidMutationResultError{Op: "HeadObject", Field: "object", Reason: "nil object"}
	}
	if obj.Size < 0 {
		return PreviousObject{}, InvalidMutationResultError{Op: "HeadObject", Field: "size", Reason: "negative size"}
	}
	return PreviousObject{
		Exists:    true,
		Size:      obj.Size,
		ETag:      obj.ETag,
		VersionID: obj.VersionID,
	}, nil
}

func mutationObjectFacts(op string, obj *Object) (ObjectFacts, error) {
	if obj == nil {
		return ObjectFacts{}, InvalidMutationResultError{Op: op, Field: "object", Reason: "nil object"}
	}
	if obj.Size < 0 {
		return ObjectFacts{}, InvalidMutationResultError{Op: op, Field: "size", Reason: "negative size"}
	}
	if obj.ETag == "" {
		return ObjectFacts{}, InvalidMutationResultError{Op: op, Field: "etag", Reason: "empty etag"}
	}
	return ObjectFacts{
		Size:         obj.Size,
		ETag:         obj.ETag,
		VersionID:    obj.VersionID,
		LastModified: obj.LastModified,
	}, nil
}
