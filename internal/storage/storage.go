package storage

import (
	"errors"
	"io"
)

// ErrSnapshotNotSupported is returned when the backend does not implement Snapshotable.
var ErrSnapshotNotSupported = errors.New("snapshot not supported by this backend")

// Object represents a stored object with metadata.
type Object struct {
	Key            string
	Size           int64
	ContentType    string
	ETag           string
	LastModified   int64  // Unix timestamp
	VersionID      string // non-empty when bucket versioning is Enabled
	IsDeleteMarker bool   // true when this object is a delete marker
}

// MultipartUpload tracks an in-progress multipart upload.
type MultipartUpload struct {
	UploadID    string
	Bucket      string
	Key         string
	ContentType string
	CreatedAt   int64
}

// Part represents a completed upload part.
type Part struct {
	PartNumber int
	ETag       string
	Size       int64
}

// Copier is an optional interface for backends that support metadata-only copy.
// Backends implement this via type assertion; the HTTP handler falls back to
// read+write if the backend does not implement Copier.
type Copier interface {
	CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*Object, error)
}

// SnapshotObject is a point-in-time metadata record for a stored object.
type SnapshotObject struct {
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	ETag        string `json:"etag"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
	Modified    int64  `json:"modified"`
}

// StaleBlob reports an object whose blob data was not found during restore.
type StaleBlob struct {
	Bucket       string `json:"bucket"`
	Key          string `json:"key"`
	ExpectedETag string `json:"expected_etag"`
}

// Snapshotable is an optional interface for backends that support metadata snapshots.
// ListAllObjects enumerates every object across all buckets.
// RestoreObjects replaces the current metadata state with the given snapshot objects.
type Snapshotable interface {
	ListAllObjects() ([]SnapshotObject, error)
	RestoreObjects(objects []SnapshotObject) (restoredCount int, staleBlobs []StaleBlob, err error)
}

// Backend defines the storage operations for GrainFS.
type Backend interface {
	CreateBucket(bucket string) error
	HeadBucket(bucket string) error
	DeleteBucket(bucket string) error
	ListBuckets() ([]string, error)

	PutObject(bucket, key string, r io.Reader, contentType string) (*Object, error)
	GetObject(bucket, key string) (io.ReadCloser, *Object, error)
	HeadObject(bucket, key string) (*Object, error)
	DeleteObject(bucket, key string) error
	ListObjects(bucket, prefix string, maxKeys int) ([]*Object, error)

	CreateMultipartUpload(bucket, key, contentType string) (*MultipartUpload, error)
	UploadPart(bucket, key, uploadID string, partNumber int, r io.Reader) (*Part, error)
	CompleteMultipartUpload(bucket, key, uploadID string, parts []Part) (*Object, error)
	AbortMultipartUpload(bucket, key, uploadID string) error
}
