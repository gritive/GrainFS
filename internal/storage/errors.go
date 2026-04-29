package storage

import "errors"

var (
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrBucketNotEmpty      = errors.New("bucket not empty")
	ErrObjectNotFound      = errors.New("object not found")
	ErrUploadNotFound      = errors.New("upload not found")
	ErrInvalidPart         = errors.New("invalid part")
	ErrMethodNotAllowed    = errors.New("method not allowed on delete marker")

	// ErrECDegraded is returned when an erasure-coded write cannot proceed
	// because too many shards are unavailable.
	ErrECDegraded = errors.New("erasure coding degraded: insufficient shards")
	// ErrNoSpace is returned when the backend has no space for new data.
	ErrNoSpace = errors.New("no space left on device")
	// ErrQuotaExceeded is returned when a volume or bucket quota is exceeded.
	ErrQuotaExceeded = errors.New("quota exceeded")
	// ErrInvalidVersion is returned when the object version does not match
	// the expected value (optimistic concurrency conflict).
	ErrInvalidVersion = errors.New("invalid object version")
)
