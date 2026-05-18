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
	SSEAlgorithm string
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
	return o.PutObjectWithRequestResult(ctx, PutObjectRequest{
		Bucket:       bucket,
		Key:          key,
		Body:         r,
		ContentType:  contentType,
		UserMetadata: userMetadata,
	})
}

func (o *Operations) PutObjectWithRequestResult(ctx context.Context, req PutObjectRequest) (*PutObjectResult, error) {
	if req.Body == nil {
		return nil, InvalidMutationResultError{Op: "PutObjectWithRequest", Field: "body", Reason: "nil body"}
	}
	if putter, ok := o.backend.(userMetadataResultPutter); ok {
		if req.ACL == nil && req.SystemMetadata.empty() {
			return putter.PutObjectWithUserMetadataResult(ctx, req.Bucket, req.Key, req.Body, req.ContentType, req.UserMetadata)
		}
	}
	previous, err := o.previousObject(ctx, req.Bucket, req.Key)
	if err != nil {
		return nil, err
	}
	obj, err := o.putObjectWithRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	facts, err := mutationObjectFacts(putObjectRequestMutationOp(req), obj)
	if err != nil {
		return nil, err
	}
	return &PutObjectResult{Object: facts, Previous: previous}, nil
}

func (o *Operations) putObjectWithRequest(ctx context.Context, req PutObjectRequest) (*Object, error) {
	if req.ACL == nil && req.SystemMetadata.empty() {
		return o.PutObjectWithUserMetadata(ctx, req.Bucket, req.Key, req.Body, req.ContentType, req.UserMetadata)
	}
	if req.ACL != nil && req.SystemMetadata.empty() && len(req.UserMetadata) == 0 {
		return o.PutObjectWithACL(ctx, req.Bucket, req.Key, req.Body, req.ContentType, *req.ACL)
	}
	if putter, ok := o.backend.(RequestPutter); ok {
		return putter.PutObjectWithRequest(ctx, req)
	}
	if !req.SystemMetadata.empty() {
		return nil, UnsupportedOperationError{Op: "PutObjectWithRequest", Reason: UnsupportedReasonNoAdapter}
	}
	obj, err := o.PutObjectWithUserMetadata(ctx, req.Bucket, req.Key, req.Body, req.ContentType, req.UserMetadata)
	if err != nil {
		return nil, err
	}
	if req.ACL != nil {
		if err := o.SetObjectACL(req.Bucket, req.Key, *req.ACL); err != nil {
			return nil, err
		}
		obj.ACL = *req.ACL
	}
	return obj, nil
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

// ListObjectsPage returns one S3 ListObjects page. Entries with key strictly
// greater than `marker` are returned, capped at `maxKeys`. `truncated` is true
// when more entries match beyond the returned slice — the S3 handler maps it
// to <IsTruncated> and <NextMarker>. If any backend in the wrapper chain
// supports native pagination (e.g. cluster meta-FSM with a sorted index), the
// call is forwarded there; otherwise the operation falls back to a single
// ListObjects call with the requested cap and filters in-process.
func (o *Operations) ListObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]*Object, bool, error) {
	type pager interface {
		ListObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]*Object, bool, error)
	}
	type unwrapper interface{ Unwrap() Backend }
	for b := o.backend; b != nil; {
		if p, ok := b.(pager); ok {
			return p.ListObjectsPage(ctx, bucket, prefix, marker, maxKeys)
		}
		u, ok := b.(unwrapper)
		if !ok {
			break
		}
		b = u.Unwrap()
	}
	objects, err := o.backend.ListObjects(ctx, bucket, prefix, maxKeys+1)
	if err != nil {
		return nil, false, err
	}
	if marker != "" {
		filtered := objects[:0]
		for _, obj := range objects {
			if obj.Key > marker {
				filtered = append(filtered, obj)
			}
		}
		objects = filtered
	}
	truncated := maxKeys > 0 && len(objects) > maxKeys
	if truncated {
		objects = objects[:maxKeys]
	}
	return objects, truncated, nil
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
		SSEAlgorithm: obj.SSEAlgorithm,
	}, nil
}

func (m ObjectSystemMetadata) empty() bool {
	return m.SSEAlgorithm == ""
}

func putObjectRequestMutationOp(req PutObjectRequest) string {
	if req.ACL != nil {
		return "PutObjectWithACL"
	}
	return "PutObject"
}
