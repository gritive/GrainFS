package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gritive/GrainFS/internal/transport"
	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
	"google.golang.org/protobuf/proto"
)

// ShardService handles remote shard storage via QUIC Data Streams.
// Each node runs a ShardService that stores/retrieves shard data locally.
type ShardService struct {
	dataDir   string
	transport *transport.QUICTransport
}

// NewShardService creates a shard service rooted at dataDir/shards/.
func NewShardService(dataDir string, tr *transport.QUICTransport) *ShardService {
	s := &ShardService{
		dataDir:   filepath.Join(dataDir, "shards"),
		transport: tr,
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
	req := &pb.RPCMessage{
		Type: "WriteShard",
		Data: mustMarshal(&pb.ShardRequest{
			Bucket:   bucket,
			Key:      key,
			ShardIdx: int32(shardIdx),
			Data:     data,
		}),
	}
	envelope, _ := proto.Marshal(req)
	msg := &transport.Message{Type: transport.StreamData, Payload: envelope}

	resp, err := s.transport.Call(ctx, peer, msg)
	if err != nil {
		return fmt.Errorf("write shard to %s: %w", peer, err)
	}

	rpc := &pb.RPCMessage{}
	if err := proto.Unmarshal(resp.Payload, rpc); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}
	if rpc.Type == "Error" {
		return fmt.Errorf("remote error: %s", string(rpc.Data))
	}
	return nil
}

// ReadShard fetches a shard from a remote node.
func (s *ShardService) ReadShard(ctx context.Context, peer, bucket, key string, shardIdx int) ([]byte, error) {
	req := &pb.RPCMessage{
		Type: "ReadShard",
		Data: mustMarshal(&pb.ShardRequest{
			Bucket:   bucket,
			Key:      key,
			ShardIdx: int32(shardIdx),
		}),
	}
	envelope, _ := proto.Marshal(req)
	msg := &transport.Message{Type: transport.StreamData, Payload: envelope}

	resp, err := s.transport.Call(ctx, peer, msg)
	if err != nil {
		return nil, fmt.Errorf("read shard from %s: %w", peer, err)
	}

	rpc := &pb.RPCMessage{}
	if err := proto.Unmarshal(resp.Payload, rpc); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	if rpc.Type == "Error" {
		return nil, fmt.Errorf("remote error: %s", string(rpc.Data))
	}
	return rpc.Data, nil
}

// DeleteShards removes all shards for a key from a remote node.
func (s *ShardService) DeleteShards(ctx context.Context, peer, bucket, key string) error {
	req := &pb.RPCMessage{
		Type: "DeleteShards",
		Data: mustMarshal(&pb.ShardRequest{
			Bucket: bucket,
			Key:    key,
		}),
	}
	envelope, _ := proto.Marshal(req)
	msg := &transport.Message{Type: transport.StreamData, Payload: envelope}

	_, err := s.transport.Call(ctx, peer, msg)
	return err
}

// handleRPC processes incoming shard RPCs.
func (s *ShardService) handleRPC(req *transport.Message) *transport.Message {
	rpc := &pb.RPCMessage{}
	if err := proto.Unmarshal(req.Payload, rpc); err != nil {
		return s.errorResponse("unmarshal error")
	}

	sr := &pb.ShardRequest{}
	if err := proto.Unmarshal(rpc.Data, sr); err != nil {
		return s.errorResponse("unmarshal shard request error")
	}

	switch rpc.Type {
	case "WriteShard":
		return s.handleWrite(sr)
	case "ReadShard":
		return s.handleRead(sr)
	case "DeleteShards":
		return s.handleDelete(sr)
	default:
		return s.errorResponse("unknown shard RPC: " + rpc.Type)
	}
}

func (s *ShardService) handleWrite(sr *pb.ShardRequest) *transport.Message {
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

func (s *ShardService) handleRead(sr *pb.ShardRequest) *transport.Message {
	path := filepath.Join(s.dataDir, sr.Bucket, sr.Key, fmt.Sprintf("shard_%d", sr.ShardIdx))
	data, err := os.ReadFile(path)
	if err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(data)
}

func (s *ShardService) handleDelete(sr *pb.ShardRequest) *transport.Message {
	dir := filepath.Join(s.dataDir, sr.Bucket, sr.Key)
	os.RemoveAll(dir)
	return s.okResponse(nil)
}

func (s *ShardService) okResponse(data []byte) *transport.Message {
	rpc := &pb.RPCMessage{Type: "OK", Data: data}
	envelope, _ := proto.Marshal(rpc)
	return &transport.Message{Type: transport.StreamData, Payload: envelope}
}

func (s *ShardService) errorResponse(msg string) *transport.Message {
	rpc := &pb.RPCMessage{Type: "Error", Data: []byte(msg)}
	envelope, _ := proto.Marshal(rpc)
	return &transport.Message{Type: transport.StreamData, Payload: envelope}
}

func mustMarshal(m proto.Message) []byte {
	data, err := proto.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("marshal: %v", err))
	}
	return data
}
