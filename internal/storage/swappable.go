package storage

import (
	"context"
	"io"
	"sync/atomic"
)

// SwappableBackend wraps a Backend and allows hot-swapping at runtime.
// All operations are forwarded to the inner backend. The swap is atomic
// and safe for concurrent use.
type SwappableBackend struct {
	inner atomic.Pointer[Backend]
}

// NewSwappableBackend creates a swappable wrapper around the given backend.
func NewSwappableBackend(b Backend) *SwappableBackend {
	sb := &SwappableBackend{}
	sb.inner.Store(&b)
	return sb
}

// Swap replaces the inner backend atomically. In-flight requests on the old
// backend will complete normally; new requests will use the new backend.
func (sb *SwappableBackend) Swap(b Backend) {
	sb.inner.Store(&b)
}

// Inner returns the current inner backend.
func (sb *SwappableBackend) Inner() Backend {
	return *sb.inner.Load()
}

// Unwrap returns the current inner backend for interface delegation.
func (sb *SwappableBackend) Unwrap() Backend {
	return *sb.inner.Load()
}

func (sb *SwappableBackend) CreateBucket(ctx context.Context, bucket string) error {
	return (*sb.inner.Load()).CreateBucket(ctx, bucket)
}

func (sb *SwappableBackend) HeadBucket(ctx context.Context, bucket string) error {
	return (*sb.inner.Load()).HeadBucket(ctx, bucket)
}

func (sb *SwappableBackend) DeleteBucket(ctx context.Context, bucket string) error {
	return (*sb.inner.Load()).DeleteBucket(ctx, bucket)
}

func (sb *SwappableBackend) ListBuckets(ctx context.Context) ([]string, error) {
	return (*sb.inner.Load()).ListBuckets(ctx)
}

func (sb *SwappableBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	return (*sb.inner.Load()).PutObject(ctx, bucket, key, r, contentType)
}

func (sb *SwappableBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *Object, error) {
	return (*sb.inner.Load()).GetObject(ctx, bucket, key)
}

func (sb *SwappableBackend) HeadObject(ctx context.Context, bucket, key string) (*Object, error) {
	return (*sb.inner.Load()).HeadObject(ctx, bucket, key)
}

func (sb *SwappableBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	return (*sb.inner.Load()).DeleteObject(ctx, bucket, key)
}

func (sb *SwappableBackend) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*Object, error) {
	return (*sb.inner.Load()).ListObjects(ctx, bucket, prefix, maxKeys)
}

func (sb *SwappableBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*Object) error) error {
	return (*sb.inner.Load()).WalkObjects(ctx, bucket, prefix, fn)
}

func (sb *SwappableBackend) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*MultipartUpload, error) {
	return (*sb.inner.Load()).CreateMultipartUpload(ctx, bucket, key, contentType)
}

func (sb *SwappableBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader) (*Part, error) {
	return (*sb.inner.Load()).UploadPart(ctx, bucket, key, uploadID, partNumber, r)
}

func (sb *SwappableBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []Part) (*Object, error) {
	return (*sb.inner.Load()).CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
}

func (sb *SwappableBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	return (*sb.inner.Load()).AbortMultipartUpload(ctx, bucket, key, uploadID)
}

// ListAllObjects implements Snapshotable by delegating to the inner backend.
func (sb *SwappableBackend) ListAllObjects() ([]SnapshotObject, error) {
	if snap, ok := (*sb.inner.Load()).(Snapshotable); ok {
		return snap.ListAllObjects()
	}
	return nil, ErrSnapshotNotSupported
}

// RestoreObjects implements Snapshotable by delegating to the inner backend.
func (sb *SwappableBackend) RestoreObjects(objects []SnapshotObject) (int, []StaleBlob, error) {
	if snap, ok := (*sb.inner.Load()).(Snapshotable); ok {
		return snap.RestoreObjects(objects)
	}
	return 0, nil, ErrSnapshotNotSupported
}

// ListAllBuckets implements BucketSnapshotable by delegating to the inner backend.
func (sb *SwappableBackend) ListAllBuckets() ([]SnapshotBucket, error) {
	if bs, ok := (*sb.inner.Load()).(BucketSnapshotable); ok {
		return bs.ListAllBuckets()
	}
	return nil, nil
}

// RestoreBuckets implements BucketSnapshotable by delegating to the inner backend.
func (sb *SwappableBackend) RestoreBuckets(buckets []SnapshotBucket) error {
	if bs, ok := (*sb.inner.Load()).(BucketSnapshotable); ok {
		return bs.RestoreBuckets(buckets)
	}
	return nil
}
