package putpipeline

import (
	"io"
	"os"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage"
)

// StripePlaintext is the unit IngestActor emits and CPUPool consumes.
//
// A stripe holds k * blockSize plaintext bytes, ready for Reed-Solomon
// Split. The last stripe of an object may be partial; Padding records
// how many zero-bytes were appended to fill the stripe so the reader
// can strip them.
type StripePlaintext struct {
	PutID     uint64
	StripeIdx uint32
	Data      []byte
	Padding   uint32
	LastInPut bool
}

// EncryptedShardChunk is what CPUPool emits to DriveActor.
//
// Ciphertext is GFSENC3-formatted bytes ready to append to the shard's
// tmp file. The first chunk for a (PutID, ShardIdx) carries the 20-byte
// GFSENC3 header; subsequent chunks carry only chunk-frame bytes.
// Err, when non-nil, signals that seal/write failed for this shard upstream;
// DriveActor turns it into exactly one failed ShardWriteResult.
type EncryptedShardChunk struct {
	PutID      uint64
	StripeIdx  uint32
	ShardIdx   int
	Ciphertext []byte
	Padding    uint32
	LastInPut  bool
	Err        error // non-nil ⟹ seal/write failed upstream for this shard; DriveActor turns it into ONE failed ShardWriteResult
}

// ShardWriteResult is what DriveActor emits to CommitCoord.
type ShardWriteResult struct {
	PutID    uint64
	ShardIdx int
	Bytes    int64
	Err      error
}

// MetadataRecord is what CommitCoord queues to MetadataBatcher.
type MetadataRecord struct {
	Bucket     string
	Key        string
	VersionID  string
	Size       int64
	ETag       string
	ShardSizes []int64
	System     storage.ObjectSystemMetadata
	UserMeta   map[string]string
}

// shardWriteState lives inside DriveActor: per-PUT tmp file + bookkeeping.
// Writes go straight to f without a bufio.Writer — encrypted chunks already
// arrive at ~1 MiB granularity, so an extra user-space buffer only adds a
// memcpy hop without amortizing syscalls further. Removing it cut the
// per-chunk runtime.memmove dominator from the PUT CPU profile.
type shardWriteState struct {
	f            *os.File
	finalPath    string
	tmpPath      string
	bucket       string
	shardKey     string
	shardIdx     int
	bytesWritten int64
}

// putWaiter lives inside CommitCoord: per-PUT result aggregation + ack gates.
type putWaiter struct {
	shardsTotal  int
	shardsOK     int
	shardsFailed int
	dataShardsOK int
	cfg          cluster.ECConfig
	earlyAck     chan<- error
	finalDone    chan<- error
	metadata     MetadataRecord
	earlyAckSent bool
}

// PutRequest is what Pipeline.Put consumes (matches storage.PutObjectRequest
// shape but locally typed so the pipeline package doesn't pull a circular
// dep on cluster).
type PutRequest struct {
	Bucket      string
	Key         string
	Body        io.Reader
	SizeHint    *int64
	ContentType string
	UserMeta    map[string]string
	System      storage.ObjectSystemMetadata
	// PrecomputedETag, when non-empty, is the ETag the pipeline should
	// return without computing MD5 over Body. Wired from
	// PutObjectRequest.ContentMD5Hex (client-supplied Content-MD5).
	PrecomputedETag string
}
