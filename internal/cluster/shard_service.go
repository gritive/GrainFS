package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"

	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/transport"
)

var shardBuilderPool = sync.Pool{
	New: func() any { return flatbuffers.NewBuilder(512) },
}

// ShardService handles remote shard storage via QUIC Data Streams.
// Each node runs a ShardService that stores/retrieves shard data locally.
type ShardService struct {
	dataDir   string
	transport *transport.QUICTransport
}

// ShardServiceOption is a functional option for ShardService.
type ShardServiceOption func(*ShardService)

// NewShardService creates a shard service rooted at dataDir/shards/.
func NewShardService(dataDir string, tr *transport.QUICTransport, opts ...ShardServiceOption) *ShardService {
	s := &ShardService{
		dataDir:   filepath.Join(dataDir, "shards"),
		transport: tr,
	}
	for _, opt := range opts {
		opt(s)
	}
	os.MkdirAll(s.dataDir, 0o755)
	return s
}

// HandleRPC returns the stream handler function for use with a StreamRouter.
func (s *ShardService) HandleRPC() func(req *transport.Message) *transport.Message {
	return s.handleRPC
}

// WriteShard sends a shard to a remote node for storage.
func (s *ShardService) WriteShard(ctx context.Context, peer, bucket, key string, shardIdx int, data []byte) error {
	payload := marshalEnvelope("WriteShard", marshalShardRequest(bucket, key, int32(shardIdx), data))
	msg := &transport.Message{Type: transport.StreamData, Payload: payload}

	resp, err := s.transport.Call(ctx, peer, msg)
	if err != nil {
		return fmt.Errorf("write shard to %s: %w", peer, err)
	}

	rpcType, _, err := unmarshalEnvelope(resp.Payload)
	if err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}
	if rpcType == "Error" {
		return fmt.Errorf("remote error from %s", peer)
	}
	return nil
}

// ReadShard fetches a shard from a remote node.
func (s *ShardService) ReadShard(ctx context.Context, peer, bucket, key string, shardIdx int) ([]byte, error) {
	payload := marshalEnvelope("ReadShard", marshalShardRequest(bucket, key, int32(shardIdx), nil))
	msg := &transport.Message{Type: transport.StreamData, Payload: payload}

	resp, err := s.transport.Call(ctx, peer, msg)
	if err != nil {
		return nil, fmt.Errorf("read shard from %s: %w", peer, err)
	}

	rpcType, data, err := unmarshalEnvelope(resp.Payload)
	if err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	if rpcType == "Error" {
		return nil, fmt.Errorf("remote error from %s", peer)
	}
	return data, nil
}

// DeleteShards removes all shards for a key from a remote node.
func (s *ShardService) DeleteShards(ctx context.Context, peer, bucket, key string) error {
	payload := marshalEnvelope("DeleteShards", marshalShardRequest(bucket, key, 0, nil))
	msg := &transport.Message{Type: transport.StreamData, Payload: payload}

	_, err := s.transport.Call(ctx, peer, msg)
	return err
}

// handleRPC processes incoming shard RPCs.
func (s *ShardService) handleRPC(req *transport.Message) *transport.Message {
	rpcType, srData, err := unmarshalEnvelope(req.Payload)
	if err != nil {
		return s.errorResponse("unmarshal error")
	}

	sr, err := unmarshalShardRequest(srData)
	if err != nil {
		return s.errorResponse("unmarshal shard request error")
	}

	switch rpcType {
	case "WriteShard":
		return s.handleWrite(sr)
	case "ReadShard":
		return s.handleRead(sr)
	case "DeleteShards":
		return s.handleDelete(sr)
	default:
		return s.errorResponse("unknown shard RPC: " + rpcType)
	}
}

// marshalEnvelope serializes an RPCMessage as FlatBuffers.
func marshalEnvelope(msgType string, innerData []byte) []byte {
	b := shardBuilderPool.Get().(*flatbuffers.Builder)
	defer func() {
		b.Reset()
		shardBuilderPool.Put(b)
	}()

	typeOff := b.CreateString(msgType)
	var dataOff flatbuffers.UOffsetT
	if len(innerData) > 0 {
		dataOff = b.CreateByteVector(innerData)
	}
	pb.RPCMessageStart(b)
	pb.RPCMessageAddType(b, typeOff)
	if len(innerData) > 0 {
		pb.RPCMessageAddData(b, dataOff)
	}
	root := pb.RPCMessageEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

// unmarshalEnvelope decodes an RPCMessage FlatBuffer.
func unmarshalEnvelope(payload []byte) (msgType string, data []byte, err error) {
	if len(payload) == 0 {
		return "", nil, fmt.Errorf("empty envelope payload")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unmarshal envelope: invalid flatbuffer: %v", r)
		}
	}()
	t := pb.GetRootAsRPCMessage(payload, 0)
	return string(t.Type()), t.DataBytes(), nil
}

// unmarshalShardRequest decodes a ShardRequest FlatBuffer.
func unmarshalShardRequest(data []byte) (*shardRequest, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty shard request")
	}
	t := pb.GetRootAsShardRequest(data, 0)
	return &shardRequest{
		Bucket:   string(t.Bucket()),
		Key:      string(t.Key()),
		ShardIdx: t.ShardIdx(),
		Data:     t.DataBytes(),
	}, nil
}

// shardRequest is the in-memory representation of a shard RPC request.
type shardRequest struct {
	Bucket   string
	Key      string
	ShardIdx int32
	Data     []byte
}

// marshalShardRequest serializes a ShardRequest to FlatBuffers.
func marshalShardRequest(bucket, key string, shardIdx int32, data []byte) []byte {
	b := shardBuilderPool.Get().(*flatbuffers.Builder)
	defer func() {
		b.Reset()
		shardBuilderPool.Put(b)
	}()

	bucketOff := b.CreateString(bucket)
	keyOff := b.CreateString(key)
	var dataOff flatbuffers.UOffsetT
	if len(data) > 0 {
		dataOff = b.CreateByteVector(data)
	}

	pb.ShardRequestStart(b)
	pb.ShardRequestAddBucket(b, bucketOff)
	pb.ShardRequestAddKey(b, keyOff)
	pb.ShardRequestAddShardIdx(b, shardIdx)
	if len(data) > 0 {
		pb.ShardRequestAddData(b, dataOff)
	}
	root := pb.ShardRequestEnd(b)
	b.Finish(root)

	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

func (s *ShardService) handleWrite(sr *shardRequest) *transport.Message {
	dir := filepath.Join(s.dataDir, sr.Bucket, sr.Key)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return s.errorResponse(err.Error())
	}

	path := filepath.Join(dir, fmt.Sprintf("shard_%d", sr.ShardIdx))
	if err := os.WriteFile(path, sr.Data, 0o644); err != nil {
		return s.errorResponse(err.Error())
	}

	return s.okResponse(nil)
}

func (s *ShardService) handleRead(sr *shardRequest) *transport.Message {
	path := filepath.Join(s.dataDir, sr.Bucket, sr.Key, fmt.Sprintf("shard_%d", sr.ShardIdx))
	data, err := os.ReadFile(path)
	if err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(data)
}

func (s *ShardService) handleDelete(sr *shardRequest) *transport.Message {
	dir := filepath.Join(s.dataDir, sr.Bucket, sr.Key)
	os.RemoveAll(dir)
	return s.okResponse(nil)
}

func (s *ShardService) okResponse(data []byte) *transport.Message {
	return &transport.Message{
		Type:    transport.StreamData,
		Payload: marshalEnvelope("OK", data),
	}
}

func (s *ShardService) errorResponse(msg string) *transport.Message {
	return &transport.Message{
		Type:    transport.StreamData,
		Payload: marshalEnvelope("Error", []byte(msg)),
	}
}
