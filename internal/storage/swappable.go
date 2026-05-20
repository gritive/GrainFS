package storage

import (
	"context"
	"io"
	"sync/atomic"
)

// SwappableBackend wraps a Backend and allows hot-swapping at runtime.
// All operations are forwarded to the inner backend. The swap is atomic
// and safe for concurrent use.
//
// Result-shape methods (PutObjectWith*Result) reuse a cached *Operations
// over the inner backend; Swap invalidates that cache so the next call
// rebuilds against the new inner.
type SwappableBackend struct {
	inner atomic.Pointer[Backend]
	gen   atomic.Uint64
	ops   atomic.Pointer[Operations] // cached Operations(inner); reset on Swap
}

var _ Backend = (*SwappableBackend)(nil)

// NewSwappableBackend creates a swappable wrapper around the given backend.
func NewSwappableBackend(b Backend) *SwappableBackend {
	sb := &SwappableBackend{}
	sb.inner.Store(&b)
	return sb
}

// Swap replaces the inner backend atomically. In-flight requests on the old
// backend will complete normally; new requests will use the new backend.
//
// Ordering rationale: ops is reset BEFORE inner is swapped so a reader that
// sees the new inner cannot pick up the stale ops; the gen bump happens LAST
// so any racing planForCall on the freshly-built ops sees the new generation
// and invalidates correctly.
func (sb *SwappableBackend) Swap(b Backend) {
	sb.ops.Store(nil)
	sb.inner.Store(&b)
	sb.gen.Add(1)
}

// cachedOps returns the cached Operations over the current inner backend,
// lazily building it on first use after construction or after a Swap.
//
// Race protection: a concurrent Swap may complete while we are reading inner
// and building the Operations. If sb.gen bumps during the window between
// reading inner and publishing ops, we discard the freshly-built ops and
// retry — otherwise we would cache an *Operations that wraps the OLD inner
// and a subsequent Swap could be silently defeated by the stale cache entry.
// CompareAndSwap on a nil slot ensures we never overwrite an entry a racing
// reader already published (or a later Swap reset to nil).
func (sb *SwappableBackend) cachedOps() *Operations {
	for {
		if ops := sb.ops.Load(); ops != nil {
			return ops
		}
		startGen := sb.gen.Load()
		inner := *sb.inner.Load()
		ops := NewOperations(inner)
		if sb.gen.Load() != startGen {
			continue // Swap raced our build; discard and retry
		}
		if sb.ops.CompareAndSwap(nil, ops) {
			return ops
		}
		// Another reader published first, or Swap reset to nil; loop and
		// re-read sb.ops on the next iteration.
	}
}

// Inner returns the current inner backend.
func (sb *SwappableBackend) Inner() Backend {
	return *sb.inner.Load()
}

// Generation changes every time the inner backend is swapped.
func (sb *SwappableBackend) Generation() uint64 {
	return sb.gen.Load()
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

func (sb *SwappableBackend) ForceDeleteBucket(ctx context.Context, bucket string) error {
	return (*sb.inner.Load()).ForceDeleteBucket(ctx, bucket)
}

func (sb *SwappableBackend) ListBuckets(ctx context.Context) ([]string, error) {
	return (*sb.inner.Load()).ListBuckets(ctx)
}

func (sb *SwappableBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	return (*sb.inner.Load()).PutObject(ctx, bucket, key, r, contentType)
}

func (sb *SwappableBackend) PutObjectWithUserMetadata(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*Object, error) {
	inner := *sb.inner.Load()
	putter, ok := inner.(UserMetadataPutter)
	if !ok {
		return nil, UnsupportedOperationError{Op: "PutObjectWithUserMetadata", Reason: UnsupportedReasonNoAdapter}
	}
	return putter.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, userMetadata)
}

func (sb *SwappableBackend) PutObjectWithUserMetadataResult(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*PutObjectResult, error) {
	return sb.cachedOps().PutObjectWithUserMetadataResult(ctx, bucket, key, r, contentType, userMetadata)
}

func (sb *SwappableBackend) PutObjectWithRequest(ctx context.Context, req PutObjectRequest) (*Object, error) {
	inner := *sb.inner.Load()
	putter, ok := inner.(RequestPutter)
	if !ok {
		return nil, UnsupportedOperationError{Op: "PutObjectWithRequest", Reason: UnsupportedReasonNoAdapter}
	}
	return putter.PutObjectWithRequest(ctx, req)
}

func (sb *SwappableBackend) PutObjectWithRequestResult(ctx context.Context, req PutObjectRequest) (*PutObjectResult, error) {
	return sb.cachedOps().PutObjectWithRequestResult(ctx, req)
}

func (sb *SwappableBackend) PutObjectWithACL(bucket, key string, r io.Reader, contentType string, acl uint8) (*Object, error) {
	inner := *sb.inner.Load()
	return putObjectWithACLOnBackend(context.Background(), inner, bucket, key, r, contentType, acl)
}

func (sb *SwappableBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *Object, error) {
	return (*sb.inner.Load()).GetObject(ctx, bucket, key)
}

func (sb *SwappableBackend) HeadObject(ctx context.Context, bucket, key string) (*Object, error) {
	return (*sb.inner.Load()).HeadObject(ctx, bucket, key)
}

func (sb *SwappableBackend) SetObjectACL(bucket, key string, acl uint8) error {
	inner := *sb.inner.Load()
	setter, ok := inner.(ACLSetter)
	if !ok {
		return UnsupportedOperationError{Op: "SetObjectACL", Reason: UnsupportedReasonNoAdapter}
	}
	return setter.SetObjectACL(bucket, key, acl)
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

func (sb *SwappableBackend) ListMultipartUploads(ctx context.Context, bucket, prefix string, maxUploads int) ([]*MultipartUpload, error) {
	return (*sb.inner.Load()).ListMultipartUploads(ctx, bucket, prefix, maxUploads)
}

func (sb *SwappableBackend) ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]Part, error) {
	return (*sb.inner.Load()).ListParts(ctx, bucket, key, uploadID, maxParts)
}

// MultipartUploadPartCount forwards to the inner backend when it implements
// MultipartPartCounter. Returns (0, nil) otherwise so callers using the
// count as a rate-limiter weight fall back to weight=1.
func (sb *SwappableBackend) MultipartUploadPartCount(bucket, key, uploadID string) (int, error) {
	if c, ok := (*sb.inner.Load()).(MultipartPartCounter); ok {
		return c.MultipartUploadPartCount(bucket, key, uploadID)
	}
	return 0, nil
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
