package putpipeline

import (
	"bufio"
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
// Ciphertext is GFSENC2-formatted bytes ready to append to the shard's
// tmp file. The first chunk for a (PutID, ShardIdx) carries the 20-byte
// GFSENC2 header; subsequent chunks carry only chunk-frame bytes.
type EncryptedShardChunk struct {
	PutID      uint64
	StripeIdx  uint32
	ShardIdx   int
	Ciphertext []byte
	Padding    uint32
	LastInPut  bool
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
type shardWriteState struct {
	f            *os.File
	bw           *bufio.Writer // user-space buffer; coalesces small encrypted chunks into large kernel writes
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
