package storage

import (
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

func (sb *SwappableBackend) CreateBucket(bucket string) error {
	return (*sb.inner.Load()).CreateBucket(bucket)
}

func (sb *SwappableBackend) HeadBucket(bucket string) error {
	return (*sb.inner.Load()).HeadBucket(bucket)
}

func (sb *SwappableBackend) DeleteBucket(bucket string) error {
	return (*sb.inner.Load()).DeleteBucket(bucket)
}

func (sb *SwappableBackend) ListBuckets() ([]string, error) {
	return (*sb.inner.Load()).ListBuckets()
}

func (sb *SwappableBackend) PutObject(bucket, key string, r io.Reader, contentType string) (*Object, error) {
	return (*sb.inner.Load()).PutObject(bucket, key, r, contentType)
}

func (sb *SwappableBackend) GetObject(bucket, key string) (io.ReadCloser, *Object, error) {
	return (*sb.inner.Load()).GetObject(bucket, key)
}

func (sb *SwappableBackend) HeadObject(bucket, key string) (*Object, error) {
	return (*sb.inner.Load()).HeadObject(bucket, key)
}

func (sb *SwappableBackend) DeleteObject(bucket, key string) error {
	return (*sb.inner.Load()).DeleteObject(bucket, key)
}

func (sb *SwappableBackend) ListObjects(bucket, prefix string, maxKeys int) ([]*Object, error) {
	return (*sb.inner.Load()).ListObjects(bucket, prefix, maxKeys)
}

func (sb *SwappableBackend) CreateMultipartUpload(bucket, key, contentType string) (*MultipartUpload, error) {
	return (*sb.inner.Load()).CreateMultipartUpload(bucket, key, contentType)
}

func (sb *SwappableBackend) UploadPart(bucket, key, uploadID string, partNumber int, r io.Reader) (*Part, error) {
	return (*sb.inner.Load()).UploadPart(bucket, key, uploadID, partNumber, r)
}

func (sb *SwappableBackend) CompleteMultipartUpload(bucket, key, uploadID string, parts []Part) (*Object, error) {
	return (*sb.inner.Load()).CompleteMultipartUpload(bucket, key, uploadID, parts)
}

func (sb *SwappableBackend) AbortMultipartUpload(bucket, key, uploadID string) error {
	return (*sb.inner.Load()).AbortMultipartUpload(bucket, key, uploadID)
}
