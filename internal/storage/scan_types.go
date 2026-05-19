package storage

// ObjectKeyGroup carries every version of one object key, newest-first.
// Emitted by ScanObjectsGrouped (LocalBackend and DistributedBackend
// implementations) for the lifecycle worker.
//
// The Versions slice may be reused from a sync.Pool by the emitter —
// callers must not retain references after the receive loop iteration
// completes. Copy with `append([]ObjectVersionRecord(nil), g.Versions...)`
// if persistence beyond one iteration is needed.
type ObjectKeyGroup struct {
	Bucket   string
	Key      string
	Versions []ObjectVersionRecord
}

// ObjectVersionRecord is the per-version slice consumed by lifecycle's
// Filter/Expiration evaluation. Tags is populated from the FBS
// Object.tags field; nil/empty == no tags.
type ObjectVersionRecord struct {
	VersionID      string
	IsLatest       bool
	IsDeleteMarker bool
	LastModified   int64
	Size           int64
	ETag           string
	Tags           []Tag
}

// MultipartUploadRecord is one in-progress multipart upload, surfaced for
// AbortIncompleteMultipartUpload evaluation. Distinct from MultipartUpload
// (in-progress upload state) — this is the per-record view used by the
// lifecycle worker.
type MultipartUploadRecord struct {
	Bucket      string
	Key         string
	UploadID    string
	InitiatedAt int64 // Unix seconds
}
