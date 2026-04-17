// Package pullthrough provides a Backend decorator that transparently fetches
// objects from an upstream S3-compatible source on cache miss and stores them
// locally (pull-through caching pattern).
package pullthrough

import (
	"bytes"
	"errors"
	"io"
	"log/slog"

	"github.com/gritive/GrainFS/internal/storage"
)

// Upstream is the minimal interface required from the upstream source.
type Upstream interface {
	GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error)
}

// Backend wraps a local storage.Backend with pull-through caching from upstream.
// On GetObject miss, it fetches from upstream, stores locally, and returns the data.
type Backend struct {
	storage.Backend
	upstream Upstream
}

// NewBackend creates a pull-through caching backend.
// local: local GrainFS backend that acts as the cache
// upstream: S3-compatible upstream to pull from on cache miss
func NewBackend(local storage.Backend, upstream Upstream) *Backend {
	return &Backend{Backend: local, upstream: upstream}
}

// HeadObject returns metadata from the local cache. On miss it pulls via GetObject
// (since S3Upstream has no HeadObject), caches the body, and returns the metadata.
func (b *Backend) HeadObject(bucket, key string) (*storage.Object, error) {
	obj, err := b.Backend.HeadObject(bucket, key)
	if err == nil {
		return obj, nil
	}
	if !isNotFound(err) {
		return nil, err
	}

	// Trigger a full GET to populate the cache, then return the metadata.
	rc, upObj, err := b.GetObject(bucket, key)
	if err != nil {
		return nil, err
	}
	rc.Close()
	return upObj, nil
}

// GetObject returns the object from the local cache. On miss it pulls from upstream,
// stores a copy locally, and returns the data.
func (b *Backend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	rc, obj, err := b.Backend.GetObject(bucket, key)
	if err == nil {
		return rc, obj, nil
	}
	if !isNotFound(err) {
		return nil, nil, err
	}

	// Cache miss: fetch from upstream
	upRC, upObj, err := b.upstream.GetObject(bucket, key)
	if err != nil {
		return nil, nil, err
	}
	defer upRC.Close()

	data, err := io.ReadAll(upRC)
	if err != nil {
		return nil, nil, err
	}

	ct := ""
	if upObj != nil {
		ct = upObj.ContentType
	}

	// Store locally (best-effort; log on failure but still return the data)
	if _, err := b.Backend.PutObject(bucket, key, bytes.NewReader(data), ct); err != nil {
		slog.Warn("pull-through: failed to cache locally", "bucket", bucket, "key", key, "err", err)
	}

	return io.NopCloser(bytes.NewReader(data)), upObj, nil
}

func isNotFound(err error) bool {
	return errors.Is(err, storage.ErrObjectNotFound)
}
