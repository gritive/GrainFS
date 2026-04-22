package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/encrypt"
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
	encryptor *encrypt.Encryptor
}

// ShardServiceOption is a functional option for ShardService.
type ShardServiceOption func(*ShardService)

// WithEncryptor wires an AES-256-GCM encryptor into the shard service so that
// all shards are encrypted at rest. Pass nil to disable encryption.
func WithEncryptor(enc *encrypt.Encryptor) ShardServiceOption {
	return func(s *ShardService) { s.encryptor = enc }
}

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
//
// NOTE: In cluster mode, PutObject calls this with shardIdx=0 and the full object
// (N× full-replication). migration_executor iterates shardIdx 0..N-1 but only
// shard_0 actually exists on peers, so balancer-triggered migration currently
// fails at ReadShard(idx>=1) — data remains safe (FSM atomic cancel).
// Phase 18 Cluster EC will use real shardIdx routing per Reed-Solomon split.
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
func unmarshalShardRequest(data []byte) (req *shardRequest, err error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty shard request")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unmarshal shard request: invalid flatbuffer: %v", r)
		}
	}()
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
	if err := s.WriteLocalShard(sr.Bucket, sr.Key, int(sr.ShardIdx), sr.Data); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

// WriteLocalShard stores a shard on the local node's disk without involving
// the QUIC transport. Used by PutObject when this node is the destination for
// one of an object's shards (self-placement); avoids a loopback RPC.
// When an encryptor is configured, the shard is AES-256-GCM encrypted before writing.
func (s *ShardService) WriteLocalShard(bucket, key string, shardIdx int, data []byte) error {
	dir := filepath.Join(s.dataDir, bucket, key)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create shard dir: %w", err)
	}
	payload := data
	if s.encryptor != nil {
		var err error
		payload, err = s.encryptor.Encrypt(data)
		if err != nil {
			return fmt.Errorf("encrypt shard: %w", err)
		}
	}
	path := filepath.Join(dir, fmt.Sprintf("shard_%d", shardIdx))
	return os.WriteFile(path, payload, 0o644)
}

// ReadLocalShard fetches a shard from the local node's disk.
// Decrypts the data if an encryptor is configured.
func (s *ShardService) ReadLocalShard(bucket, key string, shardIdx int) ([]byte, error) {
	path := filepath.Join(s.dataDir, bucket, key, fmt.Sprintf("shard_%d", shardIdx))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if s.encryptor != nil {
		data, err = s.encryptor.Decrypt(data)
		if err != nil {
			return nil, fmt.Errorf("decrypt shard: %w", err)
		}
	}
	return data, nil
}

// DeleteLocalShards removes every shard for key on the local node (all indices).
func (s *ShardService) DeleteLocalShards(bucket, key string) error {
	dir := filepath.Join(s.dataDir, bucket, key)
	return os.RemoveAll(dir)
}

func (s *ShardService) handleRead(sr *shardRequest) *transport.Message {
	data, err := s.ReadLocalShard(sr.Bucket, sr.Key, int(sr.ShardIdx))
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
