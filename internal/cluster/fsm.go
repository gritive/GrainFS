package cluster

import (
	"encoding/json"
	"fmt"
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
	Type CommandType     `json:"type"`
	Data json.RawMessage `json:"data"`
}

type CreateBucketCmd struct {
	Bucket string `json:"bucket"`
}

type DeleteBucketCmd struct {
	Bucket string `json:"bucket"`
}

type PutObjectMetaCmd struct {
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
	ETag        string `json:"etag"`
	ModTime     int64  `json:"mod_time"`
}

type DeleteObjectCmd struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

type CreateMultipartUploadCmd struct {
	UploadID    string `json:"upload_id"`
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	ContentType string `json:"content_type"`
	CreatedAt   int64  `json:"created_at"`
}

type CompleteMultipartCmd struct {
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	UploadID    string `json:"upload_id"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
	ETag        string `json:"etag"`
	ModTime     int64  `json:"mod_time"`
}

type AbortMultipartCmd struct {
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	UploadID string `json:"upload_id"`
}

// EncodeCommand serializes a command for Raft proposal.
func EncodeCommand(cmdType CommandType, payload any) ([]byte, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	cmd := Command{Type: cmdType, Data: data}
	return json.Marshal(cmd)
}

// DecodeCommand deserializes a command from a Raft log entry.
func DecodeCommand(raw []byte) (*Command, error) {
	var cmd Command
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return nil, fmt.Errorf("unmarshal command: %w", err)
	}
	return &cmd, nil
}
