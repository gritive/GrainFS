// Package pullthrough provides a Backend decorator that transparently fetches
// objects from an upstream S3-compatible source on cache miss and stores them
// locally (pull-through caching pattern).
package pullthrough

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/gritive/GrainFS/internal/storage"
)

// Upstream is the minimal interface required from the upstream source.
type Upstream interface {
	GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error)
}

// Backend wraps a local storage.Backend with pull-through caching. The Resolver
// returns per-bucket Upstreams; on Resolve miss the backend treats the request
// as a plain local lookup with no fallback fetch.
type Backend struct {
	storage.Backend
	resolver Resolver
	ops      *storage.Operations // long-lived facade over local; reused by result-shape helpers
}

var (
	_ storage.Backend            = (*Backend)(nil)
	_ storage.Snapshotable       = (*Backend)(nil)
	_ storage.BucketSnapshotable = (*Backend)(nil)
	_ storage.PartialIO          = (*Backend)(nil)
)

// NewBackend creates a pull-through caching backend. The Resolver returns
// per-bucket Upstreams; pass NewIAMResolver(store) for the IAM-backed routing.
// For tests, pass a fake Resolver implementation.
func NewBackend(local storage.Backend, resolver Resolver) *Backend {
	return &Backend{Backend: local, resolver: resolver, ops: storage.NewOperations(local)}
}

// Unwrap exposes the wrapped backend so capability detection (and other
// wrapper-aware code) can peel this decorator off.
func (b *Backend) Unwrap() storage.Backend { return b.Backend }

func (b *Backend) PutObjectWithUserMetadata(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*storage.Object, error) {
	putter, ok := b.Backend.(storage.UserMetadataPutter)
	if !ok {
		return nil, storage.UnsupportedOperationError{Op: "PutObjectWithUserMetadata", Reason: storage.UnsupportedReasonNoAdapter}
	}
	return putter.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, userMetadata)
}

func (b *Backend) PutObjectWithUserMetadataResult(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*storage.PutObjectResult, error) {
	return b.ops.PutObjectWithUserMetadataResult(ctx, bucket, key, r, contentType, userMetadata)
}

func (b *Backend) PutObjectWithRequest(ctx context.Context, req storage.PutObjectRequest) (*storage.Object, error) {
	putter, ok := b.Backend.(storage.RequestPutter)
	if !ok {
		return nil, storage.UnsupportedOperationError{Op: "PutObjectWithRequest", Reason: storage.UnsupportedReasonNoAdapter}
	}
	return putter.PutObjectWithRequest(ctx, req)
}

func (b *Backend) PutObjectWithRequestResult(ctx context.Context, req storage.PutObjectRequest) (*storage.PutObjectResult, error) {
	return b.ops.PutObjectWithRequestResult(ctx, req)
}

func (b *Backend) PutObjectAsync(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, func() error, error) {
	type asyncPutter interface {
		PutObjectAsync(context.Context, string, string, io.Reader, string) (*storage.Object, func() error, error)
	}
	ap, ok := b.Backend.(asyncPutter)
	if !ok {
		obj, err := b.Backend.PutObject(ctx, bucket, key, r, contentType)
		return obj, func() error { return nil }, err
	}
	return ap.PutObjectAsync(ctx, bucket, key, r, contentType)
}

func (b *Backend) WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*storage.Object, error) {
	partial, ok := b.Backend.(storage.PartialIO)
	if !ok {
		return nil, fmt.Errorf("pullthrough: inner backend does not support WriteAt")
	}
	return partial.WriteAt(ctx, bucket, key, offset, data)
}

func (b *Backend) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	partial, ok := b.Backend.(storage.PartialIO)
	if !ok {
		return 0, fmt.Errorf("pullthrough: inner backend does not support ReadAt")
	}
	return partial.ReadAt(ctx, bucket, key, offset, buf)
}

// ReadAtObject preserves prepared-object fast paths through pullthrough.
func (b *Backend) ReadAtObject(ctx context.Context, bucket, key string, obj *storage.Object, offset int64, buf []byte) (int, error) {
	if prepared, ok := b.Backend.(storage.PreparedReadAt); ok {
		return prepared.ReadAtObject(ctx, bucket, key, obj, offset, buf)
	}
	return b.ReadAt(ctx, bucket, key, offset, buf)
}

func (b *Backend) Truncate(ctx context.Context, bucket, key string, size int64) error {
	partial, ok := b.Backend.(storage.PartialIO)
	if !ok {
		return fmt.Errorf("pullthrough: inner backend does not support Truncate")
	}
	return partial.Truncate(ctx, bucket, key, size)
}

// CreateMultipartUploadWithTags forwards to the inner backend when it supports
// the tagsCreator extension. Without this method the embedded storage.Backend
// interface does not promote CreateMultipartUploadWithTags from the underlying
// concrete type, so Operations.CreateMultipartUploadWithTags' type assertion
// fails on the pullthrough.Backend wrapper and silently falls back to the
// no-tags overload — dropping x-amz-tagging on multipart-initiate.
func (b *Backend) CreateMultipartUploadWithTags(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (string, error) {
	type tagsCreator interface {
		CreateMultipartUploadWithTags(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (string, error)
	}
	inner, ok := b.Backend.(tagsCreator)
	if !ok {
		return "", storage.UnsupportedOperationError{Op: "CreateMultipartUploadWithTags", Reason: storage.UnsupportedReasonNoAdapter}
	}
	return inner.CreateMultipartUploadWithTags(ctx, bucket, key, contentType, tags)
}

// AppendObject forwards the S3 Express append to an inner AppendObjecter when
// supported. The pull-through decorator does not have its own append semantics
// (upstream resolution is read-side only), so this is a thin delegate.
func (b *Backend) AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*storage.Object, error) {
	ap, ok := b.Backend.(storage.AppendObjecter)
	if !ok {
		return nil, storage.ErrAppendNotSupported
	}
	return ap.AppendObject(ctx, bucket, key, expectedOffset, r)
}

func (b *Backend) PreferReadAt(bucket string) bool {
	type readAtPreference interface {
		PreferReadAt(bucket string) bool
	}
	pref, ok := b.Backend.(readAtPreference)
	return ok && pref.PreferReadAt(bucket)
}

func (b *Backend) PreferWriteAt(bucket string) bool {
	type writeAtPreference interface {
		PreferWriteAt(bucket string) bool
	}
	pref, ok := b.Backend.(writeAtPreference)
	return ok && pref.PreferWriteAt(bucket)
}

// WALOffset forwards PITR snapshot anchors through the pull-through decorator.
func (b *Backend) WALOffset() uint64 {
	type walProvider interface{ WALOffset() uint64 }
	if wp, ok := b.Backend.(walProvider); ok {
		return wp.WALOffset()
	}
	return 0
}

// ListAllObjects implements storage.Snapshotable by delegating to the wrapped
// backend. Embedding storage.Backend does not promote Snapshotable, so this
// forwarding is required for PITR snapshots to see through the pull-through layer.
func (b *Backend) ListAllObjects() ([]storage.SnapshotObject, error) {
	if snap, ok := b.Backend.(storage.Snapshotable); ok {
		return snap.ListAllObjects()
	}
	return nil, storage.ErrSnapshotNotSupported
}

// RestoreObjects implements storage.Snapshotable by delegating to the wrapped backend.
func (b *Backend) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	if snap, ok := b.Backend.(storage.Snapshotable); ok {
		return snap.RestoreObjects(objects)
	}
	return 0, nil, storage.ErrSnapshotNotSupported
}

// ListAllBuckets implements storage.BucketSnapshotable by delegating to the wrapped backend.
func (b *Backend) ListAllBuckets() ([]storage.SnapshotBucket, error) {
	if bs, ok := b.Backend.(storage.BucketSnapshotable); ok {
		return bs.ListAllBuckets()
	}
	return nil, nil
}

// RestoreBuckets implements storage.BucketSnapshotable by delegating to the wrapped backend.
func (b *Backend) RestoreBuckets(buckets []storage.SnapshotBucket) error {
	if bs, ok := b.Backend.(storage.BucketSnapshotable); ok {
		return bs.RestoreBuckets(buckets)
	}
	return nil
}

// HeadObject returns metadata from the local cache. On miss it pulls via GetObject
// (since S3Upstream has no HeadObject), caches the body, and returns the metadata.
func (b *Backend) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	obj, err := b.Backend.HeadObject(ctx, bucket, key)
	if err == nil {
		return obj, nil
	}
	if !isNotFound(err) {
		return nil, err
	}

	// Trigger a full GET to populate the cache, then return the metadata.
	rc, upObj, err := b.GetObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	rc.Close()
	return upObj, nil
}

// GetObject returns the object from the local cache. On miss it pulls from upstream,
// stores a copy locally, and returns the data.
//
// Implementation: 2-pass streaming. The upstream body is streamed directly into
// the local backend via PutObject (no in-memory buffering), then a fresh
// GetObject call on the local backend serves the caller. This keeps memory
// bounded regardless of object size. Trade-off: cache miss pays 2× local disk
// I/O (one write + one read), but avoids unbounded memory allocation.
//
// Semantic change: if local caching fails, the call returns an error (previously
// "best-effort" with log-and-return). Streaming makes cache-or-fail unavoidable
// because the upstream reader is consumed during PutObject. Callers see a
// reliable signal when the cache cannot accept data.
func (b *Backend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	rc, obj, err := b.Backend.GetObject(ctx, bucket, key)
	if err == nil {
		return rc, obj, nil
	}
	if !isNotFound(err) {
		return nil, nil, err
	}

	// Cache miss: fetch from upstream if one is configured for this bucket.
	upstream, ok := b.resolver.Resolve(bucket)
	if !ok {
		// No upstream registered — return the original 404 unchanged.
		return nil, nil, err
	}
	upRC, upObj, err := upstream.GetObject(bucket, key)
	if err != nil {
		return nil, nil, err
	}
	defer upRC.Close()

	ct := ""
	if upObj != nil {
		ct = upObj.ContentType
	}

	// Stream upstream → local cache. If upstream fails mid-stream, PutObject
	// returns the upstream error and the local backend removes any partial file.
	if _, err := b.Backend.PutObject(ctx, bucket, key, upRC, ct); err != nil {
		return nil, nil, fmt.Errorf("cache upstream object: %w", err)
	}

	// 2-pass: return a fresh reader from the local cache.
	return b.Backend.GetObject(ctx, bucket, key)
}

func isNotFound(err error) bool {
	return errors.Is(err, storage.ErrObjectNotFound)
}
