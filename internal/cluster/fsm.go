package cluster

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/storage"
)

// CommandType identifies the type of FSM command replicated through Raft.
type CommandType uint8

const (
	// CmdNoOp is proposed by a new leader to commit entries from previous terms.
	// The FSM ignores it; it exists only to advance advanceCommitIndex.
	CmdNoOp CommandType = 0

	CmdCreateBucket          CommandType = 1
	CmdDeleteBucket          CommandType = 2
	CmdPutObjectMeta         CommandType = 3
	CmdDeleteObject          CommandType = 4
	CmdCreateMultipartUpload CommandType = 5
	CmdCompleteMultipart     CommandType = 6
	CmdAbortMultipart        CommandType = 7
	CmdSetBucketPolicy       CommandType = 8
	CmdDeleteBucketPolicy    CommandType = 9
	CmdMigrateShard          CommandType = 10
	CmdMigrationDone         CommandType = 11
	// Phase 18 Cluster EC — Slice 1: shard placement metadata
	CmdPutShardPlacement    CommandType = 12
	CmdDeleteShardPlacement CommandType = 13
	// Versioning — Slice 1 of the unify-storage-paths refactor.
	// CmdDeleteObjectVersion hard-deletes a specific version (no tombstone);
	// used by lifecycle/scrubber. Plain CmdDeleteObject creates a tombstone marker.
	CmdDeleteObjectVersion CommandType = 14
	// Phase 18 v0.0.4.0 follow-up: Raft-serialized bucket versioning + object ACL.
	CmdSetBucketVersioning CommandType = 15
	CmdSetObjectACL        CommandType = 16
	CmdSetRing             CommandType = 17
	// Phase 4 Append-Object: records one appended segment.
	CmdAppendObject CommandType = 18
	// Phase B2 Coalesce: merges a prefix of appendable object segments into a
	// single coalesced blob ref.
	CmdCoalesceSegments CommandType = 19
	// CmdSetObjectTags replaces the tag set on an object version.
	// VersionID="" targets the current (legacy + latest) records;
	// VersionID!="" targets a specific versioned record only.
	CmdSetObjectTags       CommandType = 20
	CmdPutObjectQuarantine CommandType = 40
)

// Command is a serializable FSM command for Raft log entries.
type Command struct {
	Type CommandType
	Data []byte
}

type CreateBucketCmd struct {
	Bucket         string
	BypassReserved bool
}

type DeleteBucketCmd struct {
	Bucket string
}

type PutObjectMetaCmd struct {
	Bucket      string
	Key         string
	Size        int64
	ContentType string
	ETag        string
	ModTime     int64
	VersionID   string
	ECData      uint8    // EC k (data shards)
	ECParity    uint8    // EC m (parity shards)
	NodeIDs     []string // EC 샤드 배치 노드 (index i = shard i); N× 오브젝트는 빈 슬라이스
	// PlacementGroupID is the data Raft group that owns this object version.
	PlacementGroupID string
	UserMetadata     map[string]string
	SSEAlgorithm     string
	// ExpectedETag makes the metadata write conditional on the latest object
	// still matching this ETag. Empty means unconditional.
	ExpectedETag string
	// PreserveLatest writes this version without moving lat:{bucket}/{key}.
	// Snapshot restore uses it for non-latest versions.
	PreserveLatest bool
	IsDeleteMarker bool
	// Parts carries the multipart parts list when this command commits a
	// CompleteMultipartUpload object; nil for ordinary PUT and AppendObject
	// commits. Replicas write the slice onto ObjectIndexEntry.Parts so any
	// node can answer GET ?partNumber=N without re-reading the body.
	Parts []storage.MultipartPartEntry
	// Segments carries per-segment placement metadata for chunked PUTs
	// (sp.Size > storage.DefaultChunkSize). Empty/nil for legacy single-segment,
	// appendable, and multipart objects. When non-empty, top-level
	// NodeIDs/ECData/ECParity/PlacementGroupID mirror Segments[0] for
	// backward-compatible single-segment readers; per-segment placements live
	// in Segments[].
	Segments []SegmentMetaEntry
	// Tags carries tagging metadata to materialise onto objectMeta.Tags at
	// finalisation. Currently populated on the CompleteMultipartUpload path
	// (carried from clusterMultipartMeta.Tags) so cluster reaches single-node
	// parity for CreateMultipartUploadWithTags.
	Tags []storage.Tag
}

// SegmentMetaEntry records the placement of one chunked-PUT segment. The
// per-segment EC params (NodeIDs/ECData/ECParity) are required
// for Task 2.4 GETs to read segments outside the segment-0 mirror.
type SegmentMetaEntry struct {
	BlobID           string
	Size             int64
	Checksum         []byte // xxhash3-128 (16 B)
	PlacementGroupID string
	ShardSize        int32
	SegmentIdx       int32 // index in Segments[]; ensures deterministic ordering on apply
	NodeIDs          []string
	ECData           uint8
	ECParity         uint8
}

// virtualNode는 SetRingCmd 역직렬화 경로에서 사용되는 레거시 타입이다.
// SetRingCmd.applySetRing은 no-op stub — 레거시 Raft 로그 재생 호환용.
type virtualNode struct {
	Token  uint32
	NodeID string
}

// SetRingCmd는 컨시스턴트 해시 링을 FSM에 커밋하는 명령이다.
// 멤버십 변경 시에만 propose됐다. applySetRing은 no-op stub — 레거시 Raft 로그 재생용.
type SetRingCmd struct {
	Version  uint64
	VNodes   []virtualNode
	VPerNode int
}

type DeleteObjectCmd struct {
	Bucket    string
	Key       string
	VersionID string // version id of the tombstone marker created by this delete (may be empty for legacy replay)
}

// DeleteObjectVersionCmd hard-deletes a specific version by id.
type DeleteObjectVersionCmd struct {
	Bucket    string
	Key       string
	VersionID string
}

type PutObjectQuarantineCmd struct {
	Bucket    string
	Key       string
	VersionID string
	Cause     string
	Reason    string
}

type CreateMultipartUploadCmd struct {
	UploadID    string
	Bucket      string
	Key         string
	ContentType string
	CreatedAt   int64
	// PlacementGroupID is selected at upload creation and reused on complete.
	PlacementGroupID string
	// Tags are carried through Raft and persisted on clusterMultipartMeta.
	// Materialised onto the finalised object at CompleteMultipartUpload.
	Tags []storage.Tag
}

type CompleteMultipartCmd struct {
	Bucket      string
	Key         string
	UploadID    string
	Size        int64
	ContentType string
	ETag        string
	ModTime     int64
	VersionID   string
	// PlacementGroupID is the data Raft group that owns the completed object.
	PlacementGroupID string
	ECData           uint8
	ECParity         uint8
	NodeIDs          []string
	Parts            []storage.MultipartPartEntry
	Segments         []SegmentMetaEntry
	Tags             []storage.Tag
}

type AbortMultipartCmd struct {
	Bucket   string
	Key      string
	UploadID string
}

type SetBucketPolicyCmd struct {
	Bucket     string
	PolicyJSON []byte
}

type DeleteBucketPolicyCmd struct {
	Bucket string
}

// MigrateShardFSMCmd is the FSM-level command requesting a shard migration.
type MigrateShardFSMCmd struct {
	Bucket    string
	Key       string
	VersionID string
	SrcNode   string
	DstNode   string
}

// MigrationDoneFSMCmd is the FSM-level command confirming a migration is complete.
type MigrationDoneFSMCmd struct {
	Bucket    string
	Key       string
	VersionID string
	SrcNode   string
	DstNode   string
}

// PutShardPlacementCmd records the EC shard placement for an object.
// NodeIDs[i] holds shard index i; len(NodeIDs) == k+m. Phase 18 Cluster EC.
// K and M store the actual EC parameters used (dynamic EC). Zero values mean
// legacy placement recorded before dynamic EC; callers fall back to global ecConfig.
type PutShardPlacementCmd struct {
	Bucket  string
	Key     string
	NodeIDs []string
	K       int
	M       int
}

// DeleteShardPlacementCmd removes the EC placement record for an object.
type DeleteShardPlacementCmd struct {
	Bucket string
	Key    string
}

// SetBucketVersioningCmd persists the S3 versioning state for a bucket.
type SetBucketVersioningCmd struct {
	Bucket string
	State  string // "Enabled" | "Suspended"
}

// SetObjectACLCmd updates the ACL bitmask for an existing object.
type SetObjectACLCmd struct {
	Bucket string
	Key    string
	ACL    uint8
}

// SetObjectTagsCmd replaces the tag set on an object version.
// VersionID="" targets the current (legacy + latest) records;
// VersionID!="" targets a specific versioned record only.
type SetObjectTagsCmd struct {
	Bucket    string
	Key       string
	VersionID string
	Tags      []storage.Tag
}

// AppendObjectCmd records one appended segment. Only PlacementGroupID is
// frozen at propose time — FSM apply derives NodeIDs/EC params from the
// current ShardGroup record (single source of truth, avoids drift).
type AppendObjectCmd struct {
	Bucket           string
	Key              string
	ExpectedOffset   int64
	BlobID           string
	SegmentSize      int64
	SegmentETag      string
	PlacementGroupID string
	// VersionID is frozen into the Raft log so all replicas write the same
	// versioned key. AppendObject reuses the current latest version after the
	// first segment because versioning-enabled buckets reject AppendObject.
	// When empty (legacy replay / direct apply-test fixtures), apply falls back
	// to the legacy ObjectMetaKey-only single-write path.
	VersionID       string
	ModifiedUnixSec int64
}

// EncodeNoOpCommand returns a serialized CmdNoOp suitable for SetNoOpCommand.
func EncodeNoOpCommand() ([]byte, error) {
	return EncodeCommand(CmdNoOp, nil)
}

// EncodeCommand serializes a command for Raft proposal using FlatBuffers.
func EncodeCommand(cmdType CommandType, payload any) ([]byte, error) {
	data, err := encodePayload(cmdType, payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	b := flatbuffers.NewBuilder(len(data) + 16)
	var dataOff flatbuffers.UOffsetT
	if len(data) > 0 {
		dataOff = b.CreateByteVector(data)
	}
	clusterpb.CommandStart(b)
	clusterpb.CommandAddType(b, uint32(cmdType))
	if len(data) > 0 {
		clusterpb.CommandAddData(b, dataOff)
	}
	root := clusterpb.CommandEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

// DecodeCommand deserializes a command from a Raft log entry using FlatBuffers.
func DecodeCommand(raw []byte) (cmd *Command, err error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("unmarshal command: empty data")
	}
	defer func() {
		if r := recover(); r != nil {
			cmd = nil
			err = fmt.Errorf("unmarshal command: invalid flatbuffer: %v", r)
		}
	}()
	t := clusterpb.GetRootAsCommand(raw, 0)
	return &Command{
		Type: CommandType(t.Type()),
		Data: t.DataBytes(),
	}, nil
}
