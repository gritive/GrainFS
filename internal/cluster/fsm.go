package cluster

import (
	"fmt"

	"google.golang.org/protobuf/proto"

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
}

type DeleteObjectCmd struct {
	Bucket string
	Key    string
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
}

type AbortMultipartCmd struct {
	Bucket   string
	Key      string
	UploadID string
}

// EncodeCommand serializes a command for Raft proposal using protobuf.
func EncodeCommand(cmdType CommandType, payload any) ([]byte, error) {
	data, err := encodePayload(cmdType, payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	pbCmd := &clusterpb.Command{
		Type: uint32(cmdType),
		Data: data,
	}
	return proto.Marshal(pbCmd)
}

// DecodeCommand deserializes a command from a Raft log entry using protobuf.
func DecodeCommand(raw []byte) (*Command, error) {
	var pbCmd clusterpb.Command
	if err := proto.Unmarshal(raw, &pbCmd); err != nil {
		return nil, fmt.Errorf("unmarshal command: %w", err)
	}
	return &Command{
		Type: CommandType(pbCmd.Type),
		Data: pbCmd.Data,
	}, nil
}
