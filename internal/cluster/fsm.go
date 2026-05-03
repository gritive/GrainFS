package cluster

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
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
)

// Command is a serializable FSM command for Raft log entries.
type Command struct {
	Type CommandType
	Data []byte
}

type CreateBucketCmd struct {
	Bucket string
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
	RingVersion RingVersion // 쓰기에 사용된 Ring 버전 (0 = 링 사용 전 레거시)
	ECData      uint8       // EC k (data shards)
	ECParity    uint8       // EC m (parity shards)
	NodeIDs     []string    // EC 샤드 배치 노드 (index i = shard i); N× 오브젝트는 빈 슬라이스
	// PreserveLatest writes this version without moving lat:{bucket}/{key}.
	// Snapshot restore uses it for non-latest versions.
	PreserveLatest bool
	IsDeleteMarker bool
}

// SetRingCmd는 컨시스턴트 해시 링을 FSM에 커밋하는 명령이다.
// 멤버십 변경 시에만 propose된다.
type SetRingCmd struct {
	Version  RingVersion
	VNodes   []VirtualNode
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

type CreateMultipartUploadCmd struct {
	UploadID    string
	Bucket      string
	Key         string
	ContentType string
	CreatedAt   int64
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
