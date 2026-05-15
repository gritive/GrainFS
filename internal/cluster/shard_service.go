package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/pool"
	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage/directio"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
	"github.com/gritive/GrainFS/internal/transport"
)

var shardBuilderPool = pool.New(func() *flatbuffers.Builder { return flatbuffers.NewBuilder(512) })

const maxShardRangeReplyBytes = 64 << 10

type shardFileWriter func(path string, payload []byte) error

// ShardService handles remote shard storage via QUIC Data Streams.
// Each node runs a ShardService that stores/retrieves shard data locally.
type ShardService struct {
	dataDir      string
	transport    *transport.QUICTransport
	encryptor    *encrypt.Encryptor
	addrBook     NodeAddressBook
	directWriter shardFileWriter
	// directIO bypasses the kernel page cache for shard writes when true.
	// Linux uses O_DIRECT, macOS uses F_NOCACHE. Default false: enable via
	// WithDirectIO and the --direct-io flag once measurement on the target
	// filesystem confirms the win (see shardio_directio_bench_test.go).
	directIO bool
}

// ShardServiceOption is a functional option for ShardService.
type ShardServiceOption func(*ShardService)

// WithEncryptor wires an AES-256-GCM encryptor into the shard service so that
// all shards are encrypted at rest. Pass nil to disable encryption.
func WithEncryptor(enc *encrypt.Encryptor) ShardServiceOption {
	return func(s *ShardService) { s.encryptor = enc }
}

// WithDirectIO enables direct I/O (page-cache bypass) on the local shard
// write path. Beneficial for the typical EC shard size range (1-4 MB),
// neutral for larger shards. Off by default — opt in after measuring on the
// target filesystem (some overlayfs/tmpfs configs reject O_DIRECT).
func WithDirectIO() ShardServiceOption {
	return func(s *ShardService) { s.directIO = true }
}

// WithNodeAddressBook lets shard RPC callers keep nodeID membership lists while
// dialing the QUIC address stored in MetaFSM.
func WithNodeAddressBook(book NodeAddressBook) ShardServiceOption {
	return func(s *ShardService) { s.addrBook = book }
}

// NewShardService creates a shard service rooted at dataDir/shards/.
func NewShardService(dataDir string, tr *transport.QUICTransport, opts ...ShardServiceOption) *ShardService {
	s := &ShardService{
		dataDir:      filepath.Join(dataDir, "shards"),
		transport:    tr,
		directWriter: writeDirect,
	}
	for _, opt := range opts {
		opt(s)
	}
	if err := os.MkdirAll(s.dataDir, 0o755); err != nil {
		log.Error().Err(err).Str("dir", s.dataDir).Msg("create shard data directory")
	}
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
	var err error
	peerAddr, err = s.resolvePeerAddress(peerAddr)
	if err != nil {
		return nil, err
	}
	return s.transport.Call(ctx, peerAddr, msg)
}

// Ping verifies that the peer's QUIC shard service can accept a bidirectional
// RPC. The handler returns an application-level error for the synthetic RPC
// type, but a transport-level response still proves the peer process is alive.
func (s *ShardService) Ping(ctx context.Context, peer string) error {
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return err
	}
	if s.transport == nil {
		return fmt.Errorf("shard service: no transport")
	}
	fw := buildShardEnvelope("Ping", "_grainfs_health", "_ping", 0, nil)
	defer func() { fw.Builder.Reset(); shardBuilderPool.Put(fw.Builder) }()
	_, err = s.transport.CallFlatBuffer(ctx, peerAddr, fw)
	return err
}

func (s *ShardService) resolvePeerAddress(peer string) (string, error) {
	if s.addrBook == nil {
		return peer, nil
	}
	addr, ok := ResolveNodeAddress(s.addrBook, peer)
	if !ok {
		return "", fmt.Errorf("node %q not found in address book", peer)
	}
	return addr, nil
}

// RegisterHandler registers a per-type stream handler on the transport.
func (s *ShardService) RegisterHandler(st transport.StreamType, h func(*transport.Message) *transport.Message) {
	if s.transport == nil {
		return
	}
	s.transport.Handle(st, h)
}

// RegisterBodyHandler registers a per-type handler whose framed request is
// followed by raw bytes on the same bidirectional stream.
func (s *ShardService) RegisterBodyHandler(st transport.StreamType, h func(*transport.Message, io.Reader) *transport.Message) {
	if s.transport == nil {
		return
	}
	s.transport.HandleBody(st, h)
}

// RegisterReadHandler registers a per-type handler whose framed response is
// followed by raw bytes on the same bidirectional stream.
func (s *ShardService) RegisterReadHandler(st transport.StreamType, h func(*transport.Message) (*transport.Message, io.ReadCloser)) {
	if s.transport == nil {
		return
	}
	s.transport.HandleRead(st, h)
}

// WriteShard sends a shard to a remote node for storage.
//
// PutObject now routes through ecObjectWriter, which calls this with the real
// Reed-Solomon shard index per split. RepairReplica calls it with shardIdx=0
// when repairing replicated (pre-EC) objects.
func (s *ShardService) WriteShard(ctx context.Context, peer, bucket, key string, shardIdx int, data []byte) error {
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return err
	}
	if s.transport == nil {
		return fmt.Errorf("shard service: no transport")
	}
	buildStart := time.Now()
	fw := buildShardEnvelope("WriteShard", bucket, key, int32(shardIdx), data)
	defer func() { fw.Builder.Reset(); shardBuilderPool.Put(fw.Builder) }()
	ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteBuild, buildStart, PutTraceStageFields{
		Bytes:            int64(len(data)),
		ShardIndex:       shardIdx,
		ShardTarget:      peerAddr,
		ShardTargetClass: "remote",
	})
	callStart := time.Now()
	resp, err := s.transport.CallFlatBuffer(ctx, peerAddr, fw)
	if err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteCall, callStart, PutTraceStageFields{
			Bytes:            int64(len(data)),
			ShardIndex:       shardIdx,
			ShardTarget:      peerAddr,
			ShardTargetClass: "remote",
			Error:            err.Error(),
		})
		return fmt.Errorf("write shard to %s: %w", peerAddr, err)
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteCall, callStart, PutTraceStageFields{
		Bytes:            int64(len(data)),
		ShardIndex:       shardIdx,
		ShardTarget:      peerAddr,
		ShardTargetClass: "remote",
	})

	decodeStart := time.Now()
	rpcType, _, err := unmarshalEnvelope(resp.Payload)
	if err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteDecode, decodeStart, PutTraceStageFields{
			Bytes:            int64(len(data)),
			ShardIndex:       shardIdx,
			ShardTarget:      peerAddr,
			ShardTargetClass: "remote",
			Error:            err.Error(),
		})
		return fmt.Errorf("unmarshal response: %w", err)
	}
	if rpcType == "Error" {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteDecode, decodeStart, PutTraceStageFields{
			Bytes:            int64(len(data)),
			ShardIndex:       shardIdx,
			ShardTarget:      peerAddr,
			ShardTargetClass: "remote",
			Error:            "remote error",
		})
		return fmt.Errorf("remote error from %s", peer)
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteDecode, decodeStart, PutTraceStageFields{
		Bytes:            int64(len(data)),
		ShardIndex:       shardIdx,
		ShardTarget:      peerAddr,
		ShardTargetClass: "remote",
	})
	return nil
}

// WriteShardStream sends shard bytes to a remote node without buffering the
// shard into the request envelope.
func (s *ShardService) WriteShardStream(ctx context.Context, peer, bucket, key string, shardIdx int, body io.Reader) error {
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return err
	}
	if s.transport == nil {
		return fmt.Errorf("shard service: no transport")
	}
	fw := buildShardEnvelope("WriteShard", bucket, key, int32(shardIdx), nil)
	defer func() { fw.Builder.Reset(); shardBuilderPool.Put(fw.Builder) }()
	req := &transport.Message{Type: transport.StreamShardWriteBody, Payload: append([]byte(nil), fw.Builder.FinishedBytes()...)}
	resp, err := s.transport.CallWithBody(ctx, peerAddr, req, body)
	if err != nil {
		return fmt.Errorf("stream shard to %s: %w", peerAddr, err)
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
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return nil, err
	}
	if s.transport == nil {
		return nil, fmt.Errorf("shard service: no transport")
	}
	fw := buildShardEnvelope("ReadShard", bucket, key, int32(shardIdx), nil)
	defer func() { fw.Builder.Reset(); shardBuilderPool.Put(fw.Builder) }()
	resp, err := s.transport.CallFlatBuffer(ctx, peerAddr, fw)
	if err != nil {
		return nil, fmt.Errorf("read shard from %s: %w", peerAddr, err)
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

// ReadShardRange fetches a bounded byte range from a remote shard in one RPC.
func (s *ShardService) ReadShardRange(ctx context.Context, peer, bucket, key string, shardIdx int, offset int64, length int64) ([]byte, error) {
	if offset < 0 {
		return nil, fmt.Errorf("negative shard offset %d", offset)
	}
	if length < 0 {
		return nil, fmt.Errorf("negative shard length %d", length)
	}
	if length > maxShardRangeReplyBytes {
		return nil, fmt.Errorf("shard range length %d exceeds max %d", length, maxShardRangeReplyBytes)
	}
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return nil, err
	}
	if s.transport == nil {
		return nil, fmt.Errorf("shard service: no transport")
	}
	var rangePayload [16]byte
	binary.BigEndian.PutUint64(rangePayload[0:8], uint64(offset))
	binary.BigEndian.PutUint64(rangePayload[8:16], uint64(length))
	fw := buildShardEnvelope("ReadShardRange", bucket, key, int32(shardIdx), rangePayload[:])
	defer func() { fw.Builder.Reset(); shardBuilderPool.Put(fw.Builder) }()
	resp, err := s.transport.CallFlatBuffer(ctx, peerAddr, fw)
	if err != nil {
		return nil, fmt.Errorf("read shard range from %s: %w", peerAddr, err)
	}

	rpcType, data, err := unmarshalEnvelope(resp.Payload)
	if err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	if rpcType == "Error" {
		if len(data) > 0 {
			return nil, fmt.Errorf("remote error from %s: %s", peer, string(data))
		}
		return nil, fmt.Errorf("remote error from %s", peer)
	}
	if rpcType != "OK" {
		return nil, fmt.Errorf("unexpected shard range response from %s: %s", peer, rpcType)
	}
	if int64(len(data)) != length {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}

// ReadShardStream fetches a remote shard as a plaintext stream.
func (s *ShardService) ReadShardStream(ctx context.Context, peer, bucket, key string, shardIdx int) (io.ReadCloser, error) {
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return nil, err
	}
	if s.transport == nil {
		return nil, fmt.Errorf("shard service: no transport")
	}
	fw := buildShardEnvelope("ReadShard", bucket, key, int32(shardIdx), nil)
	payload := append([]byte(nil), fw.Builder.FinishedBytes()...)
	fw.Builder.Reset()
	shardBuilderPool.Put(fw.Builder)

	req := &transport.Message{Type: transport.StreamShardReadBody, Payload: payload}
	resp, body, err := s.transport.CallRead(ctx, peerAddr, req)
	if err != nil {
		return nil, fmt.Errorf("stream shard from %s: %w", peerAddr, err)
	}

	rpcType, data, err := unmarshalEnvelope(resp.Payload)
	if err != nil {
		_ = body.Close()
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	if rpcType == "Error" {
		_ = body.Close()
		if len(data) > 0 {
			return nil, fmt.Errorf("remote error from %s: %s", peer, string(data))
		}
		return nil, fmt.Errorf("remote error from %s", peer)
	}
	if rpcType != "OK" {
		_ = body.Close()
		return nil, fmt.Errorf("unexpected shard stream response from %s: %s", peer, rpcType)
	}
	return body, nil
}

func (s *ShardService) ReadShardRangeStream(ctx context.Context, peer, bucket, key string, shardIdx int, offset int64, length int64) (io.ReadCloser, error) {
	if offset < 0 {
		return nil, fmt.Errorf("negative shard offset %d", offset)
	}
	if length < 0 {
		return nil, fmt.Errorf("negative shard length %d", length)
	}
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return nil, err
	}
	if s.transport == nil {
		return nil, fmt.Errorf("shard service: no transport")
	}
	var rangePayload [16]byte
	binary.BigEndian.PutUint64(rangePayload[0:8], uint64(offset))
	binary.BigEndian.PutUint64(rangePayload[8:16], uint64(length))
	fw := buildShardEnvelope("ReadShardRange", bucket, key, int32(shardIdx), rangePayload[:])
	payload := append([]byte(nil), fw.Builder.FinishedBytes()...)
	fw.Builder.Reset()
	shardBuilderPool.Put(fw.Builder)

	req := &transport.Message{Type: transport.StreamShardReadBody, Payload: payload}
	resp, body, err := s.transport.CallRead(ctx, peerAddr, req)
	if err != nil {
		return nil, fmt.Errorf("stream shard range from %s: %w", peerAddr, err)
	}

	rpcType, data, err := unmarshalEnvelope(resp.Payload)
	if err != nil {
		_ = body.Close()
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	if rpcType == "Error" {
		_ = body.Close()
		if len(data) > 0 {
			return nil, fmt.Errorf("remote error from %s: %s", peer, string(data))
		}
		return nil, fmt.Errorf("remote error from %s", peer)
	}
	if rpcType != "OK" {
		_ = body.Close()
		return nil, fmt.Errorf("unexpected shard range stream response from %s: %s", peer, rpcType)
	}
	return body, nil
}

// DeleteShards removes all shards for a key from a remote node.
func (s *ShardService) DeleteShards(ctx context.Context, peer, bucket, key string) error {
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return err
	}
	if s.transport == nil {
		return fmt.Errorf("shard service: no transport")
	}
	fw := buildShardEnvelope("DeleteShards", bucket, key, 0, nil)
	defer func() { fw.Builder.Reset(); shardBuilderPool.Put(fw.Builder) }()
	_, err = s.transport.CallFlatBuffer(ctx, peerAddr, fw)
	return err
}

// buildShardEnvelope builds an RPCMessage FlatBuffer wrapping a ShardRequest without make+copy.
// Returns a FlatBuffersWriter whose Builder MUST be Reset()+Put() to shardBuilderPool after use.
func buildShardEnvelope(msgType, bucket, key string, shardIdx int32, data []byte) *transport.FlatBuffersWriter {
	// Build ShardRequest in b; b.FinishedBytes() points into b's internal buffer.
	b := shardBuilderPool.Get()
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
	b2 := shardBuilderPool.Get()
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
	case "ReadShardRange":
		return s.handleReadRange(sr)
	case "DeleteShards":
		return s.handleDelete(sr)
	default:
		return s.errorResponse("unknown shard RPC: " + rpcType)
	}
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

func (s *ShardService) handleReadRange(sr *shardRequest) *transport.Message {
	if len(sr.Data) != 16 {
		return s.errorResponse("invalid shard range payload")
	}
	offset := int64(binary.BigEndian.Uint64(sr.Data[0:8]))
	length := int64(binary.BigEndian.Uint64(sr.Data[8:16]))
	if offset < 0 || length < 0 {
		return s.errorResponse("invalid shard range")
	}
	if length > maxShardRangeReplyBytes {
		return s.errorResponse(fmt.Sprintf("shard range length %d exceeds max %d", length, maxShardRangeReplyBytes))
	}
	buf := make([]byte, int(length))
	n, err := s.ReadLocalShardAt(sr.Bucket, sr.Key, int(sr.ShardIdx), offset, buf)
	if err != nil && n != len(buf) {
		return s.errorResponse(err.Error())
	}
	if n != len(buf) {
		return s.errorResponse(io.ErrUnexpectedEOF.Error())
	}
	return s.okResponse(buf)
}

// HandleWriteBody returns the streamed shard write handler for StreamRouter.
func (s *ShardService) HandleWriteBody() func(*transport.Message, io.Reader) *transport.Message {
	return func(req *transport.Message, body io.Reader) *transport.Message {
		stageStart := time.Now()
		rpcType, srData, err := unmarshalEnvelope(req.Payload)
		if err != nil {
			return s.errorResponse("unmarshal request: " + err.Error())
		}
		if rpcType != "WriteShard" {
			return s.errorResponse("unexpected shard body RPC: " + rpcType)
		}
		sr, err := unmarshalShardRequest(srData)
		if err != nil {
			return s.errorResponse("decode request: " + err.Error())
		}
		observePutStage("shard_stream_server", "parse_request", stageStart)
		stageStart = time.Now()
		if err := s.WriteLocalShardStream(sr.Bucket, sr.Key, int(sr.ShardIdx), body); err != nil {
			return s.errorResponse(err.Error())
		}
		observePutStage("shard_stream_server", "write_local", stageStart)
		return s.okResponse(nil)
	}
}

// HandleReadBody returns the streamed shard read handler for StreamRouter.
func (s *ShardService) HandleReadBody() func(*transport.Message) (*transport.Message, io.ReadCloser) {
	return func(req *transport.Message) (*transport.Message, io.ReadCloser) {
		rpcType, srData, err := unmarshalEnvelope(req.Payload)
		if err != nil {
			return s.errorResponse("unmarshal request: " + err.Error()), nil
		}
		if rpcType != "ReadShard" && rpcType != "ReadShardRange" {
			return s.errorResponse("unexpected shard read RPC: " + rpcType), nil
		}
		sr, err := unmarshalShardRequest(srData)
		if err != nil {
			return s.errorResponse("decode request: " + err.Error()), nil
		}
		var r io.ReadCloser
		if rpcType == "ReadShardRange" {
			if len(sr.Data) != 16 {
				return s.errorResponse("invalid shard range payload"), nil
			}
			offset := int64(binary.BigEndian.Uint64(sr.Data[0:8]))
			length := int64(binary.BigEndian.Uint64(sr.Data[8:16]))
			r, err = s.OpenLocalShardRange(sr.Bucket, sr.Key, int(sr.ShardIdx), offset, length)
		} else {
			r, err = s.OpenLocalShard(sr.Bucket, sr.Key, int(sr.ShardIdx))
		}
		if err != nil {
			return s.errorResponse(err.Error()), nil
		}
		return s.okResponse(nil), r
	}
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
// New encrypted writes use eccodec's chunked AEAD envelope. Plain writes keep
// the CRC envelope while that compatibility path remains available.
func (s *ShardService) WriteLocalShard(bucket, key string, shardIdx int, data []byte) error {
	dir := filepath.Join(s.dataDir, bucket, key)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create shard dir: %w", err)
	}
	aad := []byte(bucket + "/" + key + "/" + strconv.Itoa(shardIdx))
	path := filepath.Join(dir, fmt.Sprintf("shard_%d", shardIdx))
	dirSynced := false
	if s.encryptor != nil {
		if err := eccodec.WriteEncryptedShardStreamAtomic(path, bytes.NewReader(data), s.encryptor, aad, eccodec.DefaultEncryptedChunkSize); err != nil {
			return err
		}
		dirSynced = true
	} else {
		if err := s.writeShardFile(path, eccodec.EncodeShard(data)); err != nil {
			return err
		}
	}
	if dirSynced {
		return nil
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return nil
}

// WriteLocalShardStream stores a shard from body without buffering plaintext.
func (s *ShardService) WriteLocalShardStream(bucket, key string, shardIdx int, body io.Reader) error {
	dir := filepath.Join(s.dataDir, bucket, key)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create shard dir: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("shard_%d", shardIdx))
	if s.encryptor != nil {
		aad := []byte(bucket + "/" + key + "/" + strconv.Itoa(shardIdx))
		if err := eccodec.WriteEncryptedShardStreamAtomic(path, body, s.encryptor, aad, eccodec.DefaultEncryptedChunkSize); err != nil {
			return err
		}
	} else {
		if err := eccodec.WriteShardStreamAtomic(path, body); err != nil {
			return err
		}
	}
	// The streaming atomic writers already fsync the parent directory after
	// rename, so there is no second ShardService-level directory sync here.
	return nil
}

// writeShardFile writes payload to path using the atomic
// (tmp + sync + rename) recipe. Branches on s.directIO: when true the tmp
// file is opened with platform-specific direct-I/O hints and the payload is
// padded to alignment + truncated; when false the standard buffered path
// runs unchanged. Errors at any step delete the tmp file before returning.
func (s *ShardService) writeShardFile(path string, payload []byte) error {
	tmp := fmt.Sprintf("%s.%d.%d.tmp", path, os.Getpid(), time.Now().UnixNano())
	if s.directIO {
		if err := s.directWriter(tmp, payload); err == nil {
			if err := os.Rename(tmp, path); err != nil {
				os.Remove(tmp)
				return fmt.Errorf("rename shard: %w", err)
			}
			return nil
		} else if isUnsupportedDirectIO(err) {
			// Some filesystems (overlayfs, certain tmpfs configs) reject
			// O_DIRECT with EINVAL. Fall back to the buffered path so
			// production stays up — log nothing here; the operator already
			// opted in and the tests cover both branches.
			os.Remove(tmp)
		} else {
			os.Remove(tmp)
			return err
		}
	}
	if err := writeBuffered(tmp, payload); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename shard: %w", err)
	}
	return nil
}

// writeBuffered is the historical write path: open + write + sync + close.
// Kept verbatim for the !directIO branch and the direct-I/O fallback path.
func writeBuffered(tmp string, payload []byte) error {
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
	return nil
}

// writeDirect uses directio.OpenFile + AlignedCopy to bypass the page cache.
// The payload is copied into an aligned buffer once; the file is truncated
// back to the payload's true length so readers see exactly the bytes the
// caller passed in. Sync is still required — direct I/O does not flush
// disk firmware caches.
func writeDirect(tmp string, payload []byte) error {
	f, err := directio.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create tmp shard (direct): %w", err)
	}
	buf, alignedLen := directio.AlignedCopy(payload)
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("write tmp shard (direct): %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync tmp shard (direct): %w", err)
	}
	if alignedLen != len(payload) {
		if err := f.Truncate(int64(len(payload))); err != nil {
			f.Close()
			return fmt.Errorf("truncate tmp shard (direct): %w", err)
		}
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close tmp shard (direct): %w", err)
	}
	return nil
}

// isUnsupportedDirectIO recognises filesystem-level rejections of O_DIRECT
// (EINVAL or "operation not supported") so the caller can fall back to the
// buffered path silently. Filesystems that reject direct I/O should degrade
// gracefully instead of crashing the server.
func isUnsupportedDirectIO(err error) bool {
	if err == nil {
		return false
	}
	es := err.Error()
	return strings.Contains(es, "invalid argument") ||
		strings.Contains(es, "operation not supported") ||
		strings.Contains(es, "not implemented")
}

// ReadLocalShard fetches a shard from the local node's disk.
// Decrypts the data if an encryptor is configured.
// Returns an error if the shard appears encrypted but no encryptor is set
// (downgrade guard).
func (s *ShardService) ReadLocalShard(bucket, key string, shardIdx int) ([]byte, error) {
	path := filepath.Join(s.dataDir, bucket, key, fmt.Sprintf("shard_%d", shardIdx))
	aad := []byte(bucket + "/" + key + "/" + strconv.Itoa(shardIdx))

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var prefix [8]byte
	_, peekErr := io.ReadFull(f, prefix[:])
	if peekErr == nil && eccodec.IsEncryptedShard(prefix[:]) {
		if s.encryptor == nil {
			_ = f.Close()
			return nil, fmt.Errorf("shard is encrypted but encryption is disabled; start server with --encryption-key-file")
		}
		info, statErr := f.Stat()
		if statErr != nil {
			_ = f.Close()
			return nil, statErr
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
		var decoded bytes.Buffer
		if size := info.Size(); size > 0 {
			maxInt := int(^uint(0) >> 1)
			if size <= int64(maxInt) {
				decoded.Grow(int(size))
			}
		}
		if err := eccodec.DecodeEncryptedShard(&decoded, f, s.encryptor, aad); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("decrypt shard: %w", err)
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
		return decoded.Bytes(), nil
	}
	_ = f.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	data := raw
	decodedEncoded := false
	if eccodec.IsEncodedShard(raw) {
		data, err = eccodec.DecodeShard(raw)
		if err != nil {
			return nil, err
		}
		decodedEncoded = true
	}
	if s.encryptor != nil {
		if !encrypt.IsEncryptedBlob(data) {
			if decodedEncoded {
				return data, nil
			}
			return nil, fmt.Errorf("decrypt shard: not an encrypted blob (missing magic header)")
		}
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

// OpenLocalShard opens a local shard as plaintext. New chunked encrypted shards
// are decrypted chunk-by-chunk; compatibility formats fall back to ReadLocalShard.
func (s *ShardService) OpenLocalShard(bucket, key string, shardIdx int) (io.ReadCloser, error) {
	path := filepath.Join(s.dataDir, bucket, key, fmt.Sprintf("shard_%d", shardIdx))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var prefix [8]byte
	_, peekErr := io.ReadFull(f, prefix[:])
	aad := []byte(bucket + "/" + key + "/" + strconv.Itoa(shardIdx))
	if peekErr == nil && eccodec.IsEncryptedShard(prefix[:]) {
		if s.encryptor == nil {
			_ = f.Close()
			return nil, fmt.Errorf("shard is encrypted but encryption is disabled; start server with --encryption-key-file")
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
		r, err := eccodec.NewEncryptedShardReader(f, s.encryptor, aad)
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("decrypt shard: %w", err)
		}
		return &multiReadCloser{Reader: r, close: f.Close}, nil
	}
	if peekErr == nil && eccodec.IsEncodedShard(prefix[:]) {
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		payloadLen := info.Size() - 8 - 4
		if payloadLen < 0 {
			_ = f.Close()
			return nil, eccodec.ErrCRCMismatch
		}
		r := eccodec.NewSizedShardReader(f, payloadLen)
		return &multiReadCloser{Reader: r, close: f.Close}, nil
	}
	_ = f.Close()
	data, err := s.ReadLocalShard(bucket, key, shardIdx)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (s *ShardService) ReadLocalShardAt(bucket, key string, shardIdx int, offset int64, buf []byte) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("negative shard offset %d", offset)
	}
	if len(buf) == 0 {
		return 0, nil
	}
	path := filepath.Join(s.dataDir, bucket, key, fmt.Sprintf("shard_%d", shardIdx))
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var prefix [8]byte
	_, peekErr := io.ReadFull(f, prefix[:])
	aad := []byte(bucket + "/" + key + "/" + strconv.Itoa(shardIdx))
	if peekErr == nil && eccodec.IsEncryptedShard(prefix[:]) {
		if s.encryptor == nil {
			return 0, fmt.Errorf("shard is encrypted but encryption is disabled; start server with --encryption-key-file")
		}
		n, err := eccodec.ReadEncryptedShardRangeAt(f, s.encryptor, aad, offset, buf)
		if err != nil {
			return n, fmt.Errorf("decrypt shard range: %w", err)
		}
		return n, nil
	}
	if peekErr == nil && eccodec.IsEncodedShard(prefix[:]) {
		info, err := f.Stat()
		if err != nil {
			return 0, err
		}
		payloadLen := info.Size() - 8 - 4
		if payloadLen < 0 {
			return 0, eccodec.ErrCRCMismatch
		}
		if offset >= payloadLen {
			return 0, io.EOF
		}
		if max := payloadLen - offset; int64(len(buf)) > max {
			buf = buf[:max]
		}
		return f.ReadAt(buf, 8+offset)
	}
	r, err := s.OpenLocalShard(bucket, key, shardIdx)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	if offset > 0 {
		if _, err := io.CopyN(io.Discard, r, offset); err != nil {
			return 0, err
		}
	}
	return io.ReadFull(r, buf)
}

func (s *ShardService) OpenLocalShardRange(bucket, key string, shardIdx int, offset int64, length int64) (io.ReadCloser, error) {
	if offset < 0 {
		return nil, fmt.Errorf("negative shard offset %d", offset)
	}
	if length < 0 {
		return nil, fmt.Errorf("negative shard length %d", length)
	}
	path := filepath.Join(s.dataDir, bucket, key, fmt.Sprintf("shard_%d", shardIdx))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	var prefix [8]byte
	_, peekErr := io.ReadFull(f, prefix[:])
	aad := []byte(bucket + "/" + key + "/" + strconv.Itoa(shardIdx))
	if peekErr == nil && eccodec.IsEncryptedShard(prefix[:]) {
		if s.encryptor == nil {
			_ = f.Close()
			return nil, fmt.Errorf("shard is encrypted but encryption is disabled; start server with --encryption-key-file")
		}
		r, err := eccodec.NewEncryptedShardRangeReader(f, s.encryptor, aad, offset, length)
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("decrypt shard range: %w", err)
		}
		return &multiReadCloser{Reader: r, close: f.Close}, nil
	}
	if peekErr == nil && eccodec.IsEncodedShard(prefix[:]) {
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		payloadLen := info.Size() - 8 - 4
		if payloadLen < 0 {
			_ = f.Close()
			return nil, eccodec.ErrCRCMismatch
		}
		if offset >= payloadLen {
			_ = f.Close()
			return nil, io.EOF
		}
		if max := payloadLen - offset; length > max {
			length = max
		}
		return &multiReadCloser{Reader: io.NewSectionReader(f, 8+offset, length), close: f.Close}, nil
	}
	_ = f.Close()

	r, err := s.OpenLocalShard(bucket, key, shardIdx)
	if err != nil {
		return nil, err
	}
	if offset == 0 {
		return &multiReadCloser{Reader: io.LimitReader(r, length), close: r.Close}, nil
	}
	return &multiReadCloser{Reader: &skipThenLimitReader{r: r, skip: offset, limit: length}, close: r.Close}, nil
}

type skipThenLimitReader struct {
	r       io.Reader
	skip    int64
	limit   int64
	skipped bool
}

func (r *skipThenLimitReader) Read(p []byte) (int, error) {
	if !r.skipped {
		if _, err := io.CopyN(io.Discard, r.r, r.skip); err != nil {
			return 0, err
		}
		r.skipped = true
	}
	if r.limit <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.limit {
		p = p[:r.limit]
	}
	n, err := r.r.Read(p)
	r.limit -= int64(n)
	return n, err
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
