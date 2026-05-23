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

type requestResultPutter interface {
	PutObjectWithRequestResult(ctx context.Context, req PutObjectRequest) (*PutObjectResult, error)
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
	if putter, ok := o.backend.(requestResultPutter); ok {
		return putter.PutObjectWithRequestResult(ctx, req)
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
	// SizeHint must reach RequestPutter-aware backends (e.g. the actor
	// pipeline gate in cluster.DistributedBackend) — falling through to
	// PutObjectWithUserMetadata silently strips it. Only the no-ACL/
	// no-SSE branch gets the new behavior; the PutObjectWithACL path
	// stays unchanged because ACL plumbing through PutObjectWithRequest
	// has different semantics than the dedicated ACL setter.
	if req.SizeHint == nil && req.ACL == nil && req.SystemMetadata.empty() {
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
// to <IsTruncated> and <NextMarker>. Walks the backend wrapper chain looking
// for a native pager; LocalBackend and DistributedBackend both implement one,
// so production deployments never hit the fallback. The fallback path
// (Unwrap-less, no-pager backend) fetches maxKeys+1 unpaged entries and
// applies `marker` in-process — this is only correct when `marker` falls
// within the first maxKeys matches and silently truncates the listing
// otherwise; we surface the gap with an error rather than mis-paginate.
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
	if marker != "" {
		return nil, false, UnsupportedOperationError{
			Op:     "ListObjectsPage",
			Reason: "backend has no native pager; passing a non-empty marker would silently truncate beyond the first window",
		}
	}
	objects, err := o.backend.ListObjects(ctx, bucket, prefix, maxKeys+1)
	if err != nil {
		return nil, false, err
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
