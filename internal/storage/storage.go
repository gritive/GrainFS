package storage

import "io"

// Object represents a stored object with metadata.
type Object struct {
	Key          string
	Size         int64
	ContentType  string
	ETag         string
	LastModified int64 // Unix timestamp
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
