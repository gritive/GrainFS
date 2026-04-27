package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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

// SendRequest sends a request to a peer and returns the response (bidirectional RPC).
func (s *ShardService) SendRequest(ctx context.Context, peerAddr string, msg *transport.Message) (*transport.Message, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("shard service: no transport")
	}
	return s.transport.Call(ctx, peerAddr, msg)
}

// RegisterHandler registers a per-type stream handler on the transport.
func (s *ShardService) RegisterHandler(st transport.StreamType, h func(*transport.Message) *transport.Message) {
	if s.transport == nil {
		return
	}
	s.transport.Handle(st, h)
}

// WriteShard sends a shard to a remote node for storage.
//
// NOTE: In cluster mode, PutObject calls this with shardIdx=0 and the full object
// (N× full-replication). migration_executor iterates shardIdx 0..N-1 but only
// shard_0 actually exists on peers, so balancer-triggered migration currently
// fails at ReadShard(idx>=1) — data remains safe (FSM atomic cancel).
// Phase 18 Cluster EC will use real shardIdx routing per Reed-Solomon split.
func (s *ShardService) WriteShard(ctx context.Context, peer, bucket, key string, shardIdx int, data []byte) error {
	fw := buildShardEnvelope("WriteShard", bucket, key, int32(shardIdx), data)
	defer func() { fw.Builder.Reset(); shardBuilderPool.Put(fw.Builder) }()
	resp, err := s.transport.CallFlatBuffer(ctx, peer, fw)
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
	fw := buildShardEnvelope("ReadShard", bucket, key, int32(shardIdx), nil)
	defer func() { fw.Builder.Reset(); shardBuilderPool.Put(fw.Builder) }()
	resp, err := s.transport.CallFlatBuffer(ctx, peer, fw)
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
	fw := buildShardEnvelope("DeleteShards", bucket, key, 0, nil)
	defer func() { fw.Builder.Reset(); shardBuilderPool.Put(fw.Builder) }()
	_, err := s.transport.CallFlatBuffer(ctx, peer, fw)
	return err
}

// buildShardEnvelope builds an RPCMessage FlatBuffer wrapping a ShardRequest without make+copy.
// Returns a FlatBuffersWriter whose Builder MUST be Reset()+Put() to shardBuilderPool after use.
func buildShardEnvelope(msgType, bucket, key string, shardIdx int32, data []byte) *transport.FlatBuffersWriter {
	// Build ShardRequest in b; b.FinishedBytes() points into b's internal buffer.
	b := shardBuilderPool.Get().(*flatbuffers.Builder)
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
	b.Finish(pb.ShardRequestEnd(b))
	srBytes := b.FinishedBytes()

	// Build RPCMessage in b2; CreateByteVector copies srBytes into b2's buffer.
	b2 := shardBuilderPool.Get().(*flatbuffers.Builder)
	typeOff := b2.CreateString(msgType)
	srVec := b2.CreateByteVector(srBytes) // srBytes copied — b can now be returned
	b.Reset()
	shardBuilderPool.Put(b)

	pb.RPCMessageStart(b2)
	pb.RPCMessageAddType(b2, typeOff)
	pb.RPCMessageAddData(b2, srVec)
	b2.Finish(pb.RPCMessageEnd(b2))

	return &transport.FlatBuffersWriter{Typ: transport.StreamData, Builder: b2}
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
// Uses a pooled builder; the returned slice is an owned copy safe after the builder is Reset.
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

// marshalResponseDirect serializes an RPCMessage without pool-and-copy.
// The returned bytes reference the builder's internal buffer; safe as long as the
// owning *Message is alive (GC keeps the backing array live via the slice header).
// Use for response paths where the builder is not returned to the pool.
func marshalResponseDirect(msgType string, innerData []byte) []byte {
	b := flatbuffers.NewBuilder(len(innerData) + 128)
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
	b.Finish(pb.RPCMessageEnd(b))
	return b.FinishedBytes()
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

func (s *ShardService) handleWrite(sr *shardRequest) *transport.Message {
	if err := s.WriteLocalShard(sr.Bucket, sr.Key, int(sr.ShardIdx), sr.Data); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

// EncryptPayload encrypts data with AAD if an encryptor is configured.
// Used by DistributedBackend.WriteShard (scrubber path).
func (s *ShardService) EncryptPayload(data, aad []byte) ([]byte, error) {
	if s.encryptor == nil {
		return data, nil
	}
	return s.encryptor.EncryptWithAAD(data, aad)
}

// DecryptPayload decrypts data with AAD if an encryptor is configured.
// Used by DistributedBackend.ReadShard (scrubber path).
func (s *ShardService) DecryptPayload(data, aad []byte) ([]byte, error) {
	if s.encryptor == nil {
		if encrypt.IsEncryptedBlob(data) {
			return nil, fmt.Errorf("shard is encrypted but encryption is disabled; start server with --encryption-key-file")
		}
		return data, nil
	}
	decrypted, err := s.encryptor.DecryptWithAAD(data, aad)
	if err != nil {
		return nil, fmt.Errorf("decrypt shard: %w", err)
	}
	return decrypted, nil
}

// WriteLocalShard stores a shard on the local node's disk without involving
// the QUIC transport. Used by PutObject when this node is the destination for
// one of an object's shards (self-placement); avoids a loopback RPC.
// When an encryptor is configured, the shard is AES-256-GCM encrypted before writing.
// Writes are crash-safe: data goes to a .tmp file, fsync'd, then renamed.
func (s *ShardService) WriteLocalShard(bucket, key string, shardIdx int, data []byte) error {
	dir := filepath.Join(s.dataDir, bucket, key)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create shard dir: %w", err)
	}
	aad := []byte(bucket + "/" + key + "/" + strconv.Itoa(shardIdx))
	payload := data
	if s.encryptor != nil {
		var err error
		payload, err = s.encryptor.EncryptWithAAD(data, aad)
		if err != nil {
			return fmt.Errorf("encrypt shard: %w", err)
		}
	}
	path := filepath.Join(dir, fmt.Sprintf("shard_%d", shardIdx))
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create tmp shard: %w", err)
	}
	if _, err := f.Write(payload); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write tmp shard: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("sync tmp shard: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close tmp shard: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename shard: %w", err)
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// ReadLocalShard fetches a shard from the local node's disk.
// Decrypts the data if an encryptor is configured.
// Returns an error if the shard appears encrypted but no encryptor is set
// (downgrade guard).
func (s *ShardService) ReadLocalShard(bucket, key string, shardIdx int) ([]byte, error) {
	path := filepath.Join(s.dataDir, bucket, key, fmt.Sprintf("shard_%d", shardIdx))
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	aad := []byte(bucket + "/" + key + "/" + strconv.Itoa(shardIdx))
	if s.encryptor != nil {
		data, err = s.encryptor.DecryptWithAAD(data, aad)
		if err != nil {
			return nil, fmt.Errorf("decrypt shard: %w", err)
		}
		return data, nil
	}
	if encrypt.IsEncryptedBlob(data) {
		return nil, fmt.Errorf("shard is encrypted but encryption is disabled; start server with --encryption-key-file")
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
		Payload: marshalResponseDirect("OK", data),
	}
}

func (s *ShardService) errorResponse(msg string) *transport.Message {
	return &transport.Message{
		Type:    transport.StreamData,
		Payload: marshalResponseDirect("Error", []byte(msg)),
	}
}
