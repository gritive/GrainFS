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
}

var (
	_ storage.Backend            = (*Backend)(nil)
	_ storage.Snapshotable       = (*Backend)(nil)
	_ storage.BucketSnapshotable = (*Backend)(nil)
)

// NewBackend creates a pull-through caching backend. The Resolver returns
// per-bucket Upstreams; pass NewIAMResolver(store) for the IAM-backed routing.
// For tests, pass a fake Resolver implementation.
func NewBackend(local storage.Backend, resolver Resolver) *Backend {
	return &Backend{Backend: local, resolver: resolver}
}

// Unwrap exposes the wrapped backend so capability detection (and other
// wrapper-aware code) can peel this decorator off.
func (b *Backend) Unwrap() storage.Backend { return b.Backend }

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
