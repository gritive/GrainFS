package storage

import (
	"context"
	"errors"
	"io"

	badger "github.com/dgraph-io/badger/v4"
)

// DBProvider is implemented by backends that expose their underlying BadgerDB
// for shared use (lifecycle, events).
type DBProvider interface{ DB() *badger.DB }

// ErrSnapshotNotSupported is returned when the backend does not implement Snapshotable.
var ErrSnapshotNotSupported = errors.New("snapshot not supported by this backend")

// Object represents a stored object with metadata.
type Object struct {
	Key              string
	Size             int64
	ContentType      string
	ETag             string
	LastModified     int64  // Unix timestamp
	VersionID        string // non-empty when bucket versioning is Enabled
	IsDeleteMarker   bool   // true when this object is a delete marker
	ACL              uint8  // s3auth.ACLGrant bitmask; 0 = private (backward compat)
	UserMetadata     map[string]string
	SSEAlgorithm     string
	PlacementGroupID string
	ECData           uint8
	ECParity         uint8
	NodeIDs          []string
	Segments         []SegmentRef
	// Coalesced lists merged segment refs produced by background coalesce.
	// Read path stitches Coalesced first, then Segments. Empty for legacy /
	// pre-B2 appendable objects.
	Coalesced    []CoalescedRef
	IsAppendable bool
	// AppendCallMD5s holds one MD5 digest per AppendObject call (per S3
	// AppendObject semantics) so Object.ETag can be recomputed independent
	// of how many segment blobs each call produced. Empty for non-appendable
	// objects. Wire-up is Task 3.1.
	AppendCallMD5s [][]byte
	// Parts is non-empty only for objects produced by CompleteMultipartUpload.
	// Entries are sorted ascending by PartNumber. The S3 GetObject/HeadObject
	// ?partNumber=N handler uses this to compute the byte range for part N.
	Parts []MultipartPartEntry
	// Tags holds the user-defined object tags (up to 10 per AWS S3 spec).
	Tags []Tag
}

// MultipartPartEntry records one part of a CompleteMultipartUpload object.
// Offset within the assembled object is the prefix-sum of preceding
// entries' Size; storing offset is redundant and risks divergence.
type MultipartPartEntry struct {
	PartNumber int
	Size       int64
	ETag       string
}

// SegmentRef identifies one segment of an object. Appendable objects store
// owner-local append blobs in append order; chunked cluster PUTs store
// EC-encoded segment placement alongside the same ordered segment list.
// Per-segment offset is derived as the prefix-sum of preceding sizes.
type SegmentRef struct {
	BlobID           string // EC/encrypted blob 식별자 (UUIDv7)
	Size             int64  // plaintext bytes in this segment
	Checksum         []byte // xxhash3-128 of plaintext segment bytes (16 B)
	PlacementGroupID string // placement group (EC stripe) identifier; empty for legacy
	ShardSize        int32  // EC shard size for this segment; 0 for legacy
	ECData           uint8  // EC data shard count; 0 for legacy/local segments
	ECParity         uint8  // EC parity shard count; 0 for legacy/local segments
	NodeIDs          []string
}

// CoalescedRef identifies one coalesced blob produced by merging a prefix of
// Object.Segments. Phase B2 stores each entry owner-locally; Phase B3 distributes
// via EC across NodeIDs (k=ECData + m=ECParity).
//
// EC fields are zero-valued for legacy/B2 owner-local entries; reader falls
// back to owner-local + forward-on-read in that case.
type CoalescedRef struct {
	CoalescedID string // UUIDv7
	Size        int64  // plaintext bytes in this coalesced blob
	ETag        string // MD5 hex of the concatenated body
	ShardKey    string // "<key>/coalesced/<coalescedID>" — used by EC reader (B3)
	ECData      uint8
	ECParity    uint8
	NodeIDs     []string
}

// ACLSetter is an optional interface for backends that support per-object ACL updates.
type ACLSetter interface {
	SetObjectACL(bucket, key string, acl uint8) error
}

// AtomicACLPutter is an optional interface for backends that can store an object
// and its ACL atomically in a single transaction.
type AtomicACLPutter interface {
	PutObjectWithACL(bucket, key string, r io.Reader, contentType string, acl uint8) (*Object, error)
}

// UserMetadataPutter is an optional interface for backends that can persist
// S3 x-amz-meta-* headers with object metadata.
type UserMetadataPutter interface {
	PutObjectWithUserMetadata(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*Object, error)
}

// ObjectSystemMetadata stores S3 system metadata that must not be exposed as
// user metadata headers.
type ObjectSystemMetadata struct {
	SSEAlgorithm string
}

// PutObjectRequest carries optional object metadata that cannot fit in the
// legacy PutObject signature without overloading user metadata.
type PutObjectRequest struct {
	Bucket         string
	Key            string
	Body           io.Reader
	SizeHint       *int64
	ContentType    string
	ACL            *uint8
	UserMetadata   map[string]string
	SystemMetadata ObjectSystemMetadata
	// ContentMD5Hex is the hex-encoded MD5 of Body, supplied by clients
	// via the Content-MD5 header. When set, the actor PUT pipeline uses
	// it directly as the object ETag and skips recomputing MD5 from Body
	// — the dominant CPU cost for large PUTs.
	ContentMD5Hex string
}

// RequestPutter is an optional interface for backends that can persist full
// object write requests, including system metadata.
type RequestPutter interface {
	PutObjectWithRequest(ctx context.Context, req PutObjectRequest) (*Object, error)
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

// ObjectVersion represents a specific version of an object, including delete markers.
type ObjectVersion struct {
	Key            string
	VersionID      string
	IsLatest       bool
	IsDeleteMarker bool
	LastModified   int64
	ETag           string
	Size           int64
	Tags           []Tag
}

// Copier is an optional primitive for backends that support metadata-only copy.
// Operations.CopyObject validates CopyObject semantics before using this
// acceleration path, and falls back to read+write when it is unavailable.
type Copier interface {
	CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*Object, error)
}

// SnapshotObject is a point-in-time metadata record for a stored object.
//
// Segments carries the per-object chunk refs introduced in Phase 1.6: a
// chunked object's blobs live under <key>_segments/<blob_id>, not at the
// legacy objectPath. Snapshots must capture Segments so PITR restore can
// verify the right blobs and rehydrate Object metadata. Older snapshots
// without this field are still readable; they fall back to the legacy
// single-file blob check during restore.
type SnapshotObject struct {
	Bucket         string       `json:"bucket"`
	Key            string       `json:"key"`
	ETag           string       `json:"etag"`
	Size           int64        `json:"size"`
	ContentType    string       `json:"content_type"`
	Modified       int64        `json:"modified"`
	VersionID      string       `json:"version_id,omitempty"`
	IsDeleteMarker bool         `json:"is_delete_marker,omitempty"`
	IsLatest       bool         `json:"is_latest,omitempty"`
	ACL            uint8        `json:"acl,omitempty"` // ACLGrant bitmask; 0 = private (backward compat)
	SSEAlgorithm   string       `json:"sse_algorithm,omitempty"`
	Segments       []SegmentRef `json:"segments,omitempty"`
	Tags           []Tag        `json:"tags,omitempty"`
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

// SnapshotBucket is a point-in-time record of bucket-level metadata
// (versioning state). Captured by BucketSnapshotable backends so PITR
// restores reproduce the full bucket configuration.
type SnapshotBucket struct {
	Name            string `json:"name"`
	VersioningState string `json:"versioning_state,omitempty"` // "Unversioned" | "Enabled" | "Suspended"
}

// BucketSnapshotable is an optional interface for backends that persist
// per-bucket metadata (versioning) and want that state preserved across
// snapshot/restore cycles.
type BucketSnapshotable interface {
	ListAllBuckets() ([]SnapshotBucket, error)
	RestoreBuckets(buckets []SnapshotBucket) error
}

// Backend defines the storage operations for GrainFS.
type Backend interface {
	CreateBucket(ctx context.Context, bucket string) error
	HeadBucket(ctx context.Context, bucket string) error
	DeleteBucket(ctx context.Context, bucket string) error
	// ForceDeleteBucket deletes all objects in the bucket and then removes it.
	// Unlike DeleteBucket, it does not fail when the bucket is non-empty.
	ForceDeleteBucket(ctx context.Context, bucket string) error
	ListBuckets(ctx context.Context) ([]string, error)

	PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error)
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *Object, error)
	HeadObject(ctx context.Context, bucket, key string) (*Object, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*Object, error)
	// WalkObjects iterates over all objects with the given prefix, calling fn
	// for each. Unlike ListObjects, it is not bounded by a maxKeys limit and
	// uses O(1) memory regardless of object count. fn returning a non-nil error
	// stops the walk and that error is returned.
	WalkObjects(ctx context.Context, bucket, prefix string, fn func(*Object) error) error

	CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*MultipartUpload, error)
	UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader) (*Part, error)
	CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []Part) (*Object, error)
	AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error
	// ListMultipartUploads returns in-progress multipart uploads in bucket whose
	// keys start with prefix, capped at maxUploads (0 = no cap). Order is not
	// guaranteed across backends; LocalBackend returns by Initiated time.
	ListMultipartUploads(ctx context.Context, bucket, prefix string, maxUploads int) ([]*MultipartUpload, error)
	// ListParts returns the parts already uploaded for one in-progress multipart
	// upload, sorted by part number ascending, capped at maxParts (0 = no cap).
	// Returns ErrUploadNotFound if uploadID does not match an active upload.
	ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]Part, error)
}

// Truncatable is an optional interface for backends that can efficiently truncate an object.
// Backends that do not implement this interface fall back to GetObject→slice→PutObject.
type Truncatable interface {
	Truncate(ctx context.Context, bucket, key string, size int64) error
}

// PartialIO is an optional interface for backends that can efficiently read and
// write object ranges without rewriting the full object.
type PartialIO interface {
	WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*Object, error)
	ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error)
	Truncate(ctx context.Context, bucket, key string, size int64) error
}

// PreparedReadAt is an optional fast path for callers that already loaded the
// current object metadata and want to avoid a second lookup before ReadAt.
type PreparedReadAt interface {
	ReadAtObject(ctx context.Context, bucket, key string, obj *Object, offset int64, buf []byte) (int, error)
}

// Syncable is an optional interface for backends that can fsync a specific object.
// Backends that do not implement this interface skip the fsync in COMMIT.
type Syncable interface {
	Sync(bucket, key string) error
}

// TaggingDirective controls how tags are applied on CopyObject.
type TaggingDirective uint8

const (
	TaggingDirectiveCopy    TaggingDirective = 0 // default: inherit source tags
	TaggingDirectiveReplace TaggingDirective = 1 // use request.Tags
)

// Tag is a single object tag key/value pair.
type Tag struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

// ObjectTagsSetter is the per-version tag mutation contract. versionID=""
// targets the current version. Passing nil clears all tags. Does not
// modify ETag or LastModified — matches AWS S3 semantics.
type ObjectTagsSetter interface {
	SetObjectTags(bucket, key, versionID string, tags []Tag) error
}

// ObjectTagsGetter returns the current tag set on a target version.
type ObjectTagsGetter interface {
	GetObjectTags(bucket, key, versionID string) ([]Tag, error)
}
