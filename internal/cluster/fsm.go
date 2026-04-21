package cluster

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// CommandType identifies the type of FSM command replicated through Raft.
type CommandType uint8

const (
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
type PutShardPlacementCmd struct {
	Bucket  string
	Key     string
	NodeIDs []string
}

// DeleteShardPlacementCmd removes the EC placement record for an object.
type DeleteShardPlacementCmd struct {
	Bucket string
	Key    string
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
