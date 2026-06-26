package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/pool"
	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/datawal"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
	"github.com/gritive/GrainFS/internal/transport"
)

var shardBuilderPool = pool.New(func() *flatbuffers.Builder { return flatbuffers.NewBuilder(512) })

const maxShardRangeReplyBytes = 64 << 10

func getShardBuilder(minSize int) *flatbuffers.Builder {
	b := shardBuilderPool.Get()
	if cap(b.Bytes) >= minSize {
		return b
	}
	return flatbuffers.NewBuilder(minSize)
}

// ShardService handles remote shard storage via the cluster transport Data Streams.
// Each node runs a ShardService that stores/retrieves shard data locally.
//
// It is a facade (spine): the local-shard concern (blob I/O, seal,
// syncDirChain durability, staging/promote-local) lives in `local
// *LocalShardStore`, to which the exported and RPC-handler methods delegate. The
// remote peer-RPC half (WriteShard/ReadShard/...), quorum-meta orchestration,
// manifest RPC/fan-out orchestration, and the generic raft buffered-route
// transport stay here. dataDirs is shared config — the same resolved roots
// LocalShardStore, LocalManifestStore, and LocalQuorumMetaStore hold.
type ShardService struct {
	dataDirs  []string
	transport shardTransport
	local     *LocalShardStore // local-shard concern (delegation target)
	manifest  *LocalManifestStore
	qmeta     *LocalQuorumMetaStore
	addrBook  NodeAddressBook
}

// ShardServiceOption is a functional option for ShardService.
type ShardServiceOption func(*ShardService)

// WithShardDEKKeeper wires the generation-aware DEK keeper as the chunked
// EC-shard data-at-rest seam. clusterID MUST be 16 bytes and MUST equal the
// value the put pipeline binds (divergence fails every GET). nil keeper or
// non-16-byte clusterID is a no-op so callers can append the option before the
// keeper is available in narrowly-scoped tests. The DEK setup lives on the
// LocalShardStore, so this forwards to its WithLocalShardDEKKeeper option.
func WithShardDEKKeeper(keeper *encrypt.DEKKeeper, clusterID []byte) ShardServiceOption {
	return func(s *ShardService) {
		WithLocalShardDEKKeeper(keeper, clusterID)(s.local)
	}
}

// WithNodeAddressBook lets shard RPC callers keep nodeID membership lists while
// dialing the transport address stored in MetaFSM.
func WithNodeAddressBook(book NodeAddressBook) ShardServiceOption {
	return func(s *ShardService) { s.addrBook = book }
}

// WithNoRedundancy wires a provider reporting whether the deployment has no EC
// redundancy (ParityShards==0, single-node 1+0). When it returns true, a large
// metadata-only shard write is fsynced directly because EC reconstruction
// cannot rebuild a page-cache-lost shard with no parity and no peers. The
// provider is read live, so a later EC reconfig is honored. Forwards to the
// LocalShardStore (which owns the no-redundancy decision).
func WithNoRedundancy(fn func() bool) ShardServiceOption {
	return func(s *ShardService) { WithLocalNoRedundancy(fn)(s.local) }
}

func (s *ShardService) DEKKeeper() *encrypt.DEKKeeper { return s.local.DEKKeeper() }

func (s *ShardService) ClusterID() []byte { return s.local.ClusterID() }

// segEnc exposes the at-rest data sealer (owned by LocalShardStore) read-only to
// the in-package spool / multipart / object-get sites that previously read the
// raw field. Set once at construction; never mutated after.
func (s *ShardService) segEnc() storage.DataEncryptor { return s.local.segEnc }

// Close releases resources owned by the ShardService. Currently a no-op.
func (s *ShardService) Close() error {
	return nil
}

// NewShardService creates a shard service rooted at dataDir/shards/.
func NewShardService(dataDir string, tr shardTransport, opts ...ShardServiceOption) *ShardService {
	return NewMultiRootShardService([]string{dataDir}, tr, opts...)
}

// NewMultiRootShardService creates a shard service rooted at multiple dataDirs/shards/.
func NewMultiRootShardService(dataDirs []string, tr shardTransport, opts ...ShardServiceOption) *ShardService {
	resolvedDirs := make([]string, len(dataDirs))
	for i, dir := range dataDirs {
		resolvedDirs[i] = filepath.Join(dir, "shards")
		if err := os.MkdirAll(resolvedDirs[i], 0o755); err != nil {
			log.Error().Err(err).Str("dir", resolvedDirs[i]).Msg("create shard data directory")
		}
	}

	s := &ShardService{
		dataDirs:  resolvedDirs,
		transport: tr,
		// Construct the LocalShardStore bare (sharing the resolved dataDirs) BEFORE
		// applying options: the DEK/no-redundancy options write into s.local.
		local:    &LocalShardStore{dataDirs: resolvedDirs},
		manifest: NewLocalManifestStore(resolvedDirs),
		qmeta:    NewLocalQuorumMetaStore(resolvedDirs),
	}
	for _, opt := range opts {
		opt(s)
	}
	// segEnc is the chunked-EC data-at-rest seam (owned by LocalShardStore).
	// Production sets it from the generation-aware DEK keeper; absent it panics
	// with the historical message (single owner: requireAtRestSealer).
	s.local.requireAtRestSealer()
	return s
}

// DataDirs returns the active shard data directories.
func (s *ShardService) DataDirs() []string {
	return s.dataDirs
}

// getShardDir delegates to the LocalShardStore; it stays on the facade because
// production callers reach it via b.shardSvc.getShardDir (orphan walker, etc.).
func (s *ShardService) getShardDir(bucket, key string, shardIdx int) (string, error) {
	return s.local.getShardDir(bucket, key, shardIdx)
}

// getShardPath delegates to the LocalShardStore; it stays on the facade because
// production callers reach it via b.shardSvc.getShardPath (scrubber, rewrap).
func (s *ShardService) getShardPath(bucket, key string, shardIdx int) (string, error) {
	return s.local.getShardPath(bucket, key, shardIdx)
}

// ShardPathUnderDataDir delegates to the LocalShardStore (containment chokepoint).
func (s *ShardService) ShardPathUnderDataDir(bucket string, shardIdx int, p string) bool {
	return s.local.ShardPathUnderDataDir(bucket, shardIdx, p)
}

// isSafePathSegment reports whether name is a single, non-traversal path
// component — non-empty, not "." or "..", and free of any path separator. Used
// to keep a bucket from re-rooting the shard containment check.
func isSafePathSegment(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	return !strings.ContainsRune(name, '/') && !strings.ContainsRune(name, filepath.Separator)
}

// NativeRPCHandler returns the native /shard/rpc buffered-route handler
// (transport.RegisterBufferedRoute, Phase 8 N7-3). The payload is the family's
// own FB RPC envelope, and every outcome — including "Error" replies — is
// in-band in the reply envelope (handleRPC never returns nil or a non-OK
// status), exactly as the tunnel delivered it.
func (s *ShardService) NativeRPCHandler() transport.BufferedRouteHandler {
	return func(payload []byte) ([]byte, error) {
		return s.handleRPC(payload), nil
	}
}

// callShardRPC sends one buffered shard-family RPC over the native /shard/rpc
// route and returns the raw reply envelope (application status in-band, parsed
// by the caller via unmarshalEnvelope).
func (s *ShardService) callShardRPC(ctx context.Context, addr string, b *flatbuffers.Builder) ([]byte, error) {
	return s.transport.CallBuffered(ctx, addr, transport.RouteShardRPC, b.FinishedBytes())
}

// SendRequest sends one buffered request to a peer over the given native
// route and returns the raw reply payload (application status in-band). The
// peer address is resolved through the address book; pooled HTTP conns keep
// the bounded-backpressure property on this PUT-hot forward path.
func (s *ShardService) SendRequest(ctx context.Context, peerAddr, route string, payload []byte) ([]byte, error) {
	if s.transport == nil {
		return nil, fmt.Errorf("shard service: no transport")
	}
	var err error
	peerAddr, err = s.resolvePeerAddress(peerAddr)
	if err != nil {
		return nil, err
	}
	return s.transport.CallBuffered(ctx, peerAddr, route, payload)
}

// Ping verifies that the peer's transport shard service can accept a bidirectional
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
	b := buildShardEnvelope("Ping", "_grainfs_health", "_ping", 0, nil)
	defer func() { b.Reset(); shardBuilderPool.Put(b) }()
	_, err = s.callShardRPC(ctx, peerAddr, b)
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

// RegisterBufferedRoute registers a native buffered-route handler on the
// transport (Phase 8 N7-3).
func (s *ShardService) RegisterBufferedRoute(path string, h transport.BufferedRouteHandler) {
	if s.transport == nil {
		return
	}
	s.transport.RegisterBufferedRoute(path, h)
}

// WriteShard sends a shard to a remote node for storage.
//
// PutObject now routes through ecObjectWriter, which calls this with the real
// Reed-Solomon shard index per split.
func (s *ShardService) WriteShard(ctx context.Context, peer, bucket, key string, shardIdx int, data []byte) error {
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return err
	}
	if s.transport == nil {
		return fmt.Errorf("shard service: no transport")
	}
	buildStart := time.Now()
	envb := buildShardEnvelope("WriteShard", bucket, key, int32(shardIdx), data)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	ObservePutTraceStage(ctx, PutTraceStageShardWriteRemoteBuild, buildStart, PutTraceStageFields{
		Bytes:            int64(len(data)),
		ShardIndex:       shardIdx,
		ShardTarget:      peerAddr,
		ShardTargetClass: "remote",
	})
	callStart := time.Now()
	respEnvelope, err := s.callShardRPC(ctx, peerAddr, envb)
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
	rpcType, _, err := unmarshalEnvelope(respEnvelope)
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
// shard into the request envelope. Native /shard/write route (Phase 8 N6).
func (s *ShardService) WriteShardStream(ctx context.Context, peer, bucket, key string, shardIdx int, body io.Reader) error {
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return err
	}
	if s.transport == nil {
		return fmt.Errorf("shard service: no transport")
	}
	req := transport.ShardWriteRequest{Bucket: bucket, Key: key, ShardIdx: shardIdx, Sealed: false}
	if err := s.transport.ShardWrite(ctx, peerAddr, req, body); err != nil {
		return fmt.Errorf("stream shard to %s: %w", peerAddr, err)
	}
	return nil
}

// WriteShardStreamStaged streams a shard to a remote node's STAGING physical path
// (stagingKey) while the receiver seals it with finalKey as AAD (PR1 segment
// staging). The wire carries finalKey in the request Key — so a legacy/AAD-by-key
// receiver still derives the final AAD — and stagingKey as the StagingKey redirect.
func (s *ShardService) WriteShardStreamStaged(ctx context.Context, peer, bucket, stagingKey, finalKey string, shardIdx int, body io.Reader) error {
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return err
	}
	if s.transport == nil {
		return fmt.Errorf("shard service: no transport")
	}
	req := transport.ShardWriteRequest{Bucket: bucket, Key: finalKey, StagingKey: stagingKey, ShardIdx: shardIdx, Sealed: false}
	if err := s.transport.ShardWrite(ctx, peerAddr, req, body); err != nil {
		return fmt.Errorf("stream staged shard to %s: %w", peerAddr, err)
	}
	return nil
}

// SealedShardTrailerLen is the length of the completeness trailer appended to a
// streamed sealed-shard body: an 8-byte big-endian count of the payload bytes
// that precede it. The receiver requires it to reject a TRUNCATED body. Without
// it, a mid-stream abort is indistinguishable from a clean finish over the HTTP
// transport: when the sink's pipe reader errors, Hertz logs the body-reader
// error as a warning and ends the chunked request cleanly, so the server reads
// a short body as a normal EOF and would commit a truncated shard.
const SealedShardTrailerLen = 8

// AppendSealedShardTrailer appends the 8-byte big-endian completeness trailer
// encoding payloadLen to buf. The streaming sink writes it after the last shard
// chunk on a clean Finalize (never on Abort).
func AppendSealedShardTrailer(buf []byte, payloadLen int64) []byte {
	var t [SealedShardTrailerLen]byte
	binary.BigEndian.PutUint64(t[:], uint64(payloadLen))
	return append(buf, t[:]...)
}

// SplitSealedShardTrailer verifies and strips the completeness trailer from a
// received sealed-shard body, returning the payload. It errors if the body is
// shorter than the trailer or if the declared payload length does not match the
// bytes received (the signature of a mid-stream truncation).
func SplitSealedShardTrailer(body []byte) ([]byte, error) {
	if len(body) < SealedShardTrailerLen {
		return nil, fmt.Errorf("sealed shard body %d bytes shorter than completeness trailer: truncated", len(body))
	}
	payload := body[:len(body)-SealedShardTrailerLen]
	declared := binary.BigEndian.Uint64(body[len(body)-SealedShardTrailerLen:])
	if uint64(len(payload)) != declared {
		return nil, fmt.Errorf("sealed shard truncated: received %d payload bytes, sender declared %d", len(payload), declared)
	}
	return payload, nil
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
	envb := buildShardEnvelope("ReadShard", bucket, key, int32(shardIdx), nil)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, peerAddr, envb)
	if err != nil {
		return nil, fmt.Errorf("read shard from %s: %w", peerAddr, err)
	}

	rpcType, data, err := unmarshalEnvelope(respEnvelope)
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
	envb := buildShardEnvelope("ReadShardRange", bucket, key, int32(shardIdx), rangePayload[:])
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, peerAddr, envb)
	if err != nil {
		return nil, fmt.Errorf("read shard range from %s: %w", peerAddr, err)
	}

	rpcType, data, err := unmarshalEnvelope(respEnvelope)
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

// ReadShardStream fetches a remote shard as a plaintext stream. Native
// /shard/read route (Phase 8 N7-1).
func (s *ShardService) ReadShardStream(ctx context.Context, peer, bucket, key string, shardIdx int) (io.ReadCloser, error) {
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return nil, err
	}
	if s.transport == nil {
		return nil, fmt.Errorf("shard service: no transport")
	}
	body, err := s.transport.ShardRead(ctx, peerAddr, transport.ShardReadRequest{Bucket: bucket, Key: key, ShardIdx: shardIdx})
	if err != nil {
		return nil, fmt.Errorf("stream shard from %s: %w", peerAddr, err)
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
	body, err := s.transport.ShardRead(ctx, peerAddr, transport.ShardReadRequest{Bucket: bucket, Key: key, ShardIdx: shardIdx, Range: true, Offset: offset, Length: length})
	if err != nil {
		return nil, fmt.Errorf("stream shard range from %s: %w", peerAddr, err)
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
	envb := buildShardEnvelope("DeleteShards", bucket, key, 0, nil)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	_, err = s.callShardRPC(ctx, peerAddr, envb)
	return err
}

// RemoveBucketPhysicalTreesRPC asks a peer to remove its local bucket data and
// quorum-meta trees after DeleteBucket has committed.
func (s *ShardService) RemoveBucketPhysicalTreesRPC(ctx context.Context, peer, bucket string) error {
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return err
	}
	if s.transport == nil {
		return fmt.Errorf("shard service: no transport")
	}
	envb := buildShardEnvelope("RemoveBucketPhysicalTrees", bucket, "", 0, nil)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, peerAddr, envb)
	if err != nil {
		return fmt.Errorf("remove bucket physical trees on %s: %w", peerAddr, err)
	}
	rpcType, data, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return fmt.Errorf("remove bucket physical trees on %s: unmarshal response: %w", peerAddr, err)
	}
	if rpcType == "Error" {
		return fmt.Errorf("remove bucket physical trees on %s: %s", peerAddr, data)
	}
	return nil
}

// PromoteStagedShards renames a segment's staged shard dirs (stagingKey) to their final path
// (finalKey) on a remote node — the remote counterpart of PromoteLocalStagedShards. The single-Key
// envelope carries stagingKey in Key and finalKey in Data. PR1 segment staging.
func (s *ShardService) PromoteStagedShards(ctx context.Context, peer, bucket, stagingKey, finalKey string) error {
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return err
	}
	if s.transport == nil {
		return fmt.Errorf("shard service: no transport")
	}
	envb := buildShardEnvelope("PromoteStagedShards", bucket, stagingKey, 0, []byte(finalKey))
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, peerAddr, envb)
	if err != nil {
		return fmt.Errorf("promote staged shards on %s: %w", peerAddr, err)
	}
	// Promote sits on the commit critical path (all-or-fail, data-before-meta), so
	// the handler's in-band application error (rename/mkdir/path failure surfaces as
	// an "Error" reply envelope, NOT a transport error) MUST be parsed and treated
	// as a promote failure — otherwise the manifest could commit while a peer's
	// shard is still staged. This differs from DeleteShards, whose best-effort
	// cleanup can swallow the reply.
	rpcType, _, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return fmt.Errorf("promote staged shards on %s: unmarshal response: %w", peerAddr, err)
	}
	if rpcType == "Error" {
		return fmt.Errorf("promote staged shards on %s: remote error", peer)
	}
	return nil
}

// PromoteStagedShardsBatch renames multiple staged segment shard dirs on one
// remote node in one RPC. It is the batched counterpart of PromoteStagedShards.
func (s *ShardService) PromoteStagedShardsBatch(ctx context.Context, peer, bucket string, pairs []stagedPromotePair) error {
	if len(pairs) == 0 {
		return nil
	}
	peerAddr, err := s.resolvePeerAddress(peer)
	if err != nil {
		return err
	}
	if s.transport == nil {
		return fmt.Errorf("shard service: no transport")
	}
	data, err := encodeStagedPromotePairs(pairs)
	if err != nil {
		return err
	}
	envb := buildShardEnvelope("PromoteStagedShardsBatch", bucket, "", 0, data)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	respEnvelope, err := s.callShardRPC(ctx, peerAddr, envb)
	if err != nil {
		return fmt.Errorf("promote staged shard batch on %s: %w", peerAddr, err)
	}
	rpcType, body, err := unmarshalEnvelope(respEnvelope)
	if err != nil {
		return fmt.Errorf("promote staged shard batch on %s: unmarshal response: %w", peerAddr, err)
	}
	if rpcType == "Error" {
		return fmt.Errorf("promote staged shard batch on %s: %s", peer, body)
	}
	return nil
}

func encodeStagedPromotePairs(pairs []stagedPromotePair) ([]byte, error) {
	if uint64(len(pairs)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("too many staged promote pairs: %d", len(pairs))
	}
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(len(pairs)))
	for _, pair := range pairs {
		var err error
		out, err = appendPromoteString(out, pair.stagingKey)
		if err != nil {
			return nil, err
		}
		out, err = appendPromoteString(out, pair.finalKey)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func appendPromoteString(out []byte, s string) ([]byte, error) {
	if uint64(len(s)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("staged promote key too long: %d", len(s))
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(s)))
	out = append(out, lenBuf[:]...)
	return append(out, s...), nil
}

func decodeStagedPromotePairs(data []byte) ([]stagedPromotePair, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("staged promote batch: truncated pair count")
	}
	count := int(binary.BigEndian.Uint32(data[:4]))
	off := 4
	pairs := make([]stagedPromotePair, 0, count)
	for i := 0; i < count; i++ {
		stagingKey, next, err := readPromoteString(data, off)
		if err != nil {
			return nil, fmt.Errorf("staged promote batch pair %d staging key: %w", i, err)
		}
		finalKey, next, err := readPromoteString(data, next)
		if err != nil {
			return nil, fmt.Errorf("staged promote batch pair %d final key: %w", i, err)
		}
		pairs = append(pairs, stagedPromotePair{stagingKey: stagingKey, finalKey: finalKey})
		off = next
	}
	if off != len(data) {
		return nil, fmt.Errorf("staged promote batch: trailing bytes")
	}
	return pairs, nil
}

func readPromoteString(data []byte, off int) (string, int, error) {
	if off+4 > len(data) {
		return "", off, fmt.Errorf("truncated length")
	}
	n := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	if off+n > len(data) {
		return "", off, fmt.Errorf("truncated string")
	}
	return string(data[off : off+n]), off + n, nil
}

// buildShardEnvelope builds an RPCMessage FlatBuffer wrapping a ShardRequest without make+copy.
// Returns a Builder that MUST be Reset()+Put() to shardBuilderPool after use.
func buildShardEnvelope(msgType, bucket, key string, shardIdx int32, data []byte) *flatbuffers.Builder {
	// Build ShardRequest in b; b.FinishedBytes() points into b's internal buffer.
	requestSize := len(data) + len(bucket) + len(key) + 128
	b := getShardBuilder(requestSize)
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
	responseSize := len(srBytes) + len(msgType) + 128
	b2 := getShardBuilder(responseSize)
	typeOff := b2.CreateString(msgType)
	srVec := b2.CreateByteVector(srBytes) // srBytes copied — b can now be returned
	b.Reset()
	shardBuilderPool.Put(b)

	pb.RPCMessageStart(b2)
	pb.RPCMessageAddType(b2, typeOff)
	pb.RPCMessageAddData(b2, srVec)
	b2.Finish(pb.RPCMessageEnd(b2))

	return b2
}

// handleRPC processes incoming shard RPCs. Every outcome — including "Error"
// replies — is in-band in the returned reply envelope.
func (s *ShardService) handleRPC(payload []byte) []byte {
	rpcType, srData, err := unmarshalEnvelope(payload)
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
	case "RemoveBucketPhysicalTrees":
		return s.handleRemoveBucketPhysicalTrees(sr)
	case "PromoteStagedShards":
		return s.handlePromoteStaged(sr)
	case "PromoteStagedShardsBatch":
		return s.handlePromoteStagedBatch(sr)
	case "WriteQuorumMeta":
		return s.handleQuorumMetaWrite(sr)
	case "WriteQuorumMetaVersion":
		return s.handleQuorumMetaVersionWrite(sr)
	case "WriteQuorumMetaAppend":
		return s.handleQuorumMetaAppendWrite(sr)
	case "ReadQuorumMeta":
		return s.handleQuorumMetaRead(sr)
	case "ReadQuorumMetaBatch":
		return s.handleQuorumMetaBatchRead(sr)
	case "ReadQuorumMetaAppend":
		return s.handleQuorumMetaAppendRead(sr)
	case "ScanQuorumMeta":
		return s.handleScanQuorumMeta(sr)
	case "ScanQuorumMetaPage":
		return s.handleScanQuorumMetaPage(sr)
	case "ScanQuorumMetaVersions":
		return s.handleScanQuorumMetaVersions(sr)
	case "ScanQuorumMetaVersionsAll":
		return s.handleScanQuorumMetaVersionsAll(sr)
	case "ReadQuorumMetaVersions":
		return s.handleQuorumMetaVersionsRead(sr)
	case "ReadQuorumMetaVersionsRaw":
		return s.handleQuorumMetaVersionsReadRaw(sr)
	case "DeleteQuorumMeta":
		return s.handleQuorumMetaDelete(sr)
	case "DeleteQuorumMetaVersion":
		return s.handleQuorumMetaVersionDelete(sr)
	case "WriteManifestBlob":
		return s.handleManifestBlobWrite(sr)
	case "ReadManifestBlob":
		return s.handleManifestBlobRead(sr)
	case "DeleteManifestBlob":
		return s.handleManifestBlobDelete(sr)
	case "ScanManifestBlobs":
		return s.handleManifestBlobScan(sr)
	default:
		return s.errorResponse("unknown shard RPC: " + rpcType)
	}
}

// handleQuorumMetaWrite receives a Phase 3 primary quorum meta blob and
// durably writes it locally (write + fsync). Failures are reported to the
// caller so the PUT can fail the quorum check.
func (s *ShardService) handleQuorumMetaWrite(sr *shardRequest) []byte {
	if err := s.writeQuorumMetaLocal(sr.Bucket, sr.Key, sr.Data); err != nil {
		// Emit the CAS-reject wire code (not the free-text message) so the client
		// can reconstitute errQuorumMetaCASReject (BUG-1). Every other error keeps
		// its free-text body.
		return s.errorResponse(quorumMetaWriteErrorBody(err))
	}
	return s.okResponse(nil)
}

// handleQuorumMetaVersionWrite receives a per-version quorum-meta blob and
// durably writes it under the .quorum_meta_versions subtree. sr.Key carries
// path.Join(key, versionID).
func (s *ShardService) handleQuorumMetaVersionWrite(sr *shardRequest) []byte {
	if err := s.writeQuorumMetaVersionLocal(sr.Bucket, sr.Key, sr.Data); err != nil {
		// Mirror handleQuorumMetaWrite: surface a CAS reject as the stable wire code
		// so the client can map it back to errQuorumMetaCASReject (BUG-1).
		return s.errorResponse(quorumMetaWriteErrorBody(err))
	}
	return s.okResponse(nil)
}

func (s *ShardService) handleQuorumMetaAppendWrite(sr *shardRequest) []byte {
	if err := s.qmeta.writeQuorumMetaAppendLocal(sr.Bucket, sr.Key, sr.Data); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

// handleQuorumMetaRead serves a ReadQuorumMeta RPC: reads the local quorum
// meta file and returns its raw bytes, or OK with empty payload when absent.
func (s *ShardService) handleQuorumMetaRead(sr *shardRequest) []byte {
	data, err := s.readQuorumMetaRaw(sr.Bucket, sr.Key)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return s.okResponse(nil) // empty payload = not found on this node
		}
		return s.errorResponse(err.Error())
	}
	return s.okResponse(data)
}

func (s *ShardService) handleQuorumMetaBatchRead(sr *shardRequest) []byte {
	keys, err := unpackStringList(sr.Data)
	if err != nil {
		return s.errorResponse(err.Error())
	}
	blobs := make(map[string][]byte, len(keys))
	for _, key := range keys {
		data, rerr := s.readQuorumMetaRaw(sr.Bucket, key)
		if rerr != nil {
			if errors.Is(rerr, storage.ErrObjectNotFound) {
				continue
			}
			return s.errorResponse(rerr.Error())
		}
		if len(data) > 0 {
			blobs[key] = data
		}
	}
	return s.okResponse(packKeyBlobMap(blobs))
}

func (s *ShardService) handleQuorumMetaAppendRead(sr *shardRequest) []byte {
	data, err := s.qmeta.readQuorumMetaAppendRawLocal(sr.Bucket, sr.Key)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return s.okResponse(nil)
		}
		return s.errorResponse(err.Error())
	}
	return s.okResponse(data)
}

// handleScanQuorumMeta serves a ScanQuorumMeta RPC: scans the local quorum meta
// store for all entries in the given bucket (sr.Key = prefix) and returns them
// as a packBlobList-encoded payload.
func (s *ShardService) handleScanQuorumMeta(sr *shardRequest) []byte {
	entries, err := s.ScanQuorumMetaBucket(sr.Bucket, sr.Key) // Key field = prefix
	if err != nil {
		return s.errorResponse(err.Error())
	}
	blobs := make([][]byte, 0, len(entries))
	for i := range entries {
		blob, eerr := encodeQuorumMetaBlob(entries[i])
		if eerr == nil {
			blobs = append(blobs, blob)
		}
	}
	return s.okResponse(packBlobList(blobs))
}

// handleScanQuorumMetaPage serves one local latest-only quorum-meta page.
// sr.Key carries prefix and sr.Data carries marker/maxKeys.
func (s *ShardService) handleScanQuorumMetaPage(sr *shardRequest) []byte {
	marker, maxKeys, err := unpackScanQuorumMetaPageArgs(sr.Data)
	if err != nil {
		return s.errorResponse(err.Error())
	}
	entries, _, err := s.ScanQuorumMetaBucketPage(sr.Bucket, sr.Key, marker, maxKeys) // Key field = prefix
	if err != nil {
		return s.errorResponse(err.Error())
	}
	blobs := make([][]byte, 0, len(entries))
	for i := range entries {
		blob, eerr := encodeQuorumMetaBlob(entries[i])
		if eerr == nil {
			blobs = append(blobs, blob)
		}
	}
	return s.okResponse(packBlobList(blobs))
}

// handleScanQuorumMetaVersions serves a ScanQuorumMetaVersions RPC: walks the
// local per-version subtree for the bucket (sr.Key = prefix), groups by decoded
// key, and returns each per-key max-VersionID cmd as a packBlobList payload.
func (s *ShardService) handleScanQuorumMetaVersions(sr *shardRequest) []byte {
	entries, err := s.ScanQuorumMetaVersionsBucket(sr.Bucket, sr.Key) // Key field = prefix
	if err != nil {
		return s.errorResponse(err.Error())
	}
	blobs := make([][]byte, 0, len(entries))
	for i := range entries {
		if blob, eerr := encodeQuorumMetaBlob(entries[i]); eerr == nil {
			blobs = append(blobs, blob)
		}
	}
	return s.okResponse(packBlobList(blobs))
}

// handleScanQuorumMetaVersionsAll serves a ScanQuorumMetaVersionsAll RPC: walks
// the local per-version subtree for the bucket (sr.Key = prefix) and returns
// EVERY decoded per-version cmd (no max-per-key grouping) as a packBlobList
// payload. Mirrors handleScanQuorumMetaVersions but uses the FAIL-CLOSED
// all-version scan: a per-blob read/decode failure returns an "Error" reply
// (so the cluster-wide fan-in surfaces a non-nil error) instead of a
// silently-truncated list. An ABSENT bucket stays an empty success.
func (s *ShardService) handleScanQuorumMetaVersionsAll(sr *shardRequest) []byte {
	entries, err := s.scanQuorumMetaVersionsBucketAllStrict(sr.Bucket, sr.Key) // Key field = prefix
	if err != nil {
		return s.errorResponse(err.Error())
	}
	blobs := make([][]byte, 0, len(entries))
	for i := range entries {
		if blob, eerr := encodeQuorumMetaBlob(entries[i]); eerr == nil {
			blobs = append(blobs, blob)
		}
	}
	return s.okResponse(packBlobList(blobs))
}

// handleQuorumMetaVersionsReadRaw serves a ReadQuorumMetaVersionsRaw RPC: returns
// the RAW per-version blob bytes for (bucket, key) WITHOUT decoding, so the caller
// can decode-strict (a corrupt blob is served as-is, not dropped server-side — the
// difference from handleQuorumMetaVersionsRead, which decode-drops). A local read
// error → Error reply (the read1 decode-strict reader tolerates that as a peer
// availability skip; a corrupt blob is caught at the caller's strict decode).
func (s *ShardService) handleQuorumMetaVersionsReadRaw(sr *shardRequest) []byte {
	blobs, err := s.readQuorumMetaVersionsRawLocal(sr.Bucket, sr.Key)
	if err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(packBlobList(blobs))
}

// handleQuorumMetaVersionsRead serves a ReadQuorumMetaVersions RPC: lists the
// local per-version blobs for (bucket, key) and returns them as a packBlobList.
func (s *ShardService) handleQuorumMetaVersionsRead(sr *shardRequest) []byte {
	cmds, err := s.readQuorumMetaVersionsLocal(sr.Bucket, sr.Key)
	if err != nil {
		return s.errorResponse(err.Error())
	}
	blobs := make([][]byte, 0, len(cmds))
	for i := range cmds {
		if blob, eerr := encodeQuorumMetaBlob(cmds[i]); eerr == nil {
			blobs = append(blobs, blob)
		}
	}
	return s.okResponse(packBlobList(blobs))
}

// handleQuorumMetaVersionDelete serves a DeleteQuorumMetaVersion RPC: removes the
// local per-version blob. sr.Key carries path.Join(key, versionID); the trailing
// segment is the versionID. Absent file is not an error (idempotent).
func (s *ShardService) handleQuorumMetaVersionDelete(sr *shardRequest) []byte {
	key, versionID := splitVersionSubpath(sr.Key)
	if versionID == "" {
		return s.errorResponse("quorum meta version delete: missing version id")
	}
	if err := s.deleteQuorumMetaVersionLocal(sr.Bucket, key, versionID); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

// handleQuorumMetaDelete serves a DeleteQuorumMeta RPC: removes the local
// latest-only quorum-meta blob. sr.Key carries the object key directly (no
// versionID). Absent file is not an error (idempotent).
func (s *ShardService) handleQuorumMetaDelete(sr *shardRequest) []byte {
	if err := s.deleteQuorumMetaLocal(sr.Bucket, sr.Key); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

// splitVersionSubpath splits a path.Join(key, versionID) value into (key, vid)
// at the final slash. A value with no slash returns ("", value) (no key).
func splitVersionSubpath(subpath string) (string, string) {
	i := strings.LastIndex(subpath, "/")
	if i < 0 {
		return "", subpath
	}
	return subpath[:i], subpath[i+1:]
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

func (s *ShardService) handleWrite(sr *shardRequest) []byte {
	ctx := ContextWithPutTrace(context.Background(), PutTraceRequest{
		Bucket:      sr.Bucket,
		Key:         sr.Key,
		Ingress:     PutTraceIngressReceiver,
		SizeClass:   putTraceSizeClass(int64(len(sr.Data)), ecShardBufferedLimit),
		ForwardMode: PutTraceForwardNone,
	})
	if err := s.local.writeLocalShard(ctx, sr.Bucket, sr.Key, int(sr.ShardIdx), sr.Data); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

func (s *ShardService) handleReadRange(sr *shardRequest) []byte {
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

// NativeWriteHandler returns the native-route shard write handler
// (transport.RegisterShardWriteHandler). The metadata arrives already-parsed
// and the result is a plain error (the transport maps it to the HTTP status).
// Sealed=true is WriteSealedShard (verbatim GFSENC3 body + completeness
// trailer, the S2 streaming sender seals at the source — stored verbatim,
// never re-encrypted); Sealed=false is WriteShard (plaintext stream,
// destination seals).
func (s *ShardService) NativeWriteHandler() transport.ShardWriteHandler {
	return func(req transport.ShardWriteRequest, body io.Reader) error {
		stageStart := time.Now()
		if req.Sealed {
			// The body is the final encoded payload plus an 8-byte completeness
			// trailer, so the payload is bounded directly by the data WAL's
			// MaxPayloadBytes (no further encode grows it); read with room for the
			// trailer. Buffer is per-shard, not whole-object. A truncated body is
			// rejected: a mid-stream abort surfaces over HTTP as a clean EOF, so
			// the trailer's declared length is the only signal that distinguishes
			// a complete shard from a partial one.
			raw, rerr := readShardPayload(body, datawal.MaxPayloadBytes+SealedShardTrailerLen, -1, false)
			if rerr != nil {
				return rerr
			}
			sealed, terr := SplitSealedShardTrailer(raw)
			if terr != nil {
				return terr
			}
			if werr := s.local.writeLocalSealedShard(context.Background(), req.Bucket, req.Key, req.ShardIdx, sealed); werr != nil {
				return werr
			}
			observePutStage("shard_stream_server", "write_local_sealed", stageStart)
			return nil
		}
		// PR1 segment staging: a non-empty StagingKey redirects the bytes to the
		// staging physical path while Key stays the FINAL key used as AAD, so a
		// post-promote read of Key decrypts correctly.
		if req.StagingKey != "" {
			if err := s.WriteLocalShardStreamStagedContext(context.Background(), req.Bucket, req.StagingKey, req.Key, req.ShardIdx, body); err != nil {
				return err
			}
			observePutStage("shard_stream_server", "write_local_staged", stageStart)
			return nil
		}
		if err := s.WriteLocalShardStream(req.Bucket, req.Key, req.ShardIdx, body); err != nil {
			return err
		}
		observePutStage("shard_stream_server", "write_local", stageStart)
		return nil
	}
}

// NativeReadHandler returns the native-route shard read handler
// (transport.RegisterShardReadHandler). Metadata arrives parsed, errors
// surface as plain errors (the transport maps them to HTTP 500 + text).
func (s *ShardService) NativeReadHandler() transport.ShardReadHandler {
	return func(req transport.ShardReadRequest) (io.ReadCloser, error) {
		if req.Range {
			return s.OpenLocalShardRange(req.Bucket, req.Key, req.ShardIdx, req.Offset, req.Length)
		}
		return s.OpenLocalShard(req.Bucket, req.Key, req.ShardIdx)
	}
}

// --- LocalShardStore facade delegates -------------------------------------
//
// A named field does NOT promote methods, so the facade re-exposes every local
// method callers and interfaces depend on (the localShardStore interface, the
// ecObjectSizedShardStore type assertion, and the production b.shardSvc.<X>
// callers) by delegating to s.local. Behavior is unchanged.

// WriteLocalShard stores a shard on the local node's disk without involving
// the cluster transport. Used by PutObject when this node is the destination for
// one of an object's shards (self-placement); avoids a loopback RPC.
func (s *ShardService) WriteLocalShard(bucket, key string, shardIdx int, data []byte) error {
	return s.local.WriteLocalShard(bucket, key, shardIdx, data)
}

func (s *ShardService) WriteLocalShardContext(ctx context.Context, bucket, key string, shardIdx int, data []byte) error {
	return s.local.WriteLocalShardContext(ctx, bucket, key, shardIdx, data)
}

// WriteLocalShardStream stores a shard from body without buffering plaintext.
func (s *ShardService) WriteLocalShardStream(bucket, key string, shardIdx int, body io.Reader) error {
	return s.local.WriteLocalShardStream(bucket, key, shardIdx, body)
}

func (s *ShardService) WriteLocalShardStreamContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader) error {
	return s.local.WriteLocalShardStreamContext(ctx, bucket, key, shardIdx, body)
}

// WriteLocalShardStreamSizedContext stays on the facade: localShardEndpoint
// type-asserts *ShardService to ecObjectSizedShardStore to take the sized-write
// path (perf). Losing it silently changes that path.
func (s *ShardService) WriteLocalShardStreamSizedContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader, streamSize int64) error {
	return s.local.WriteLocalShardStreamSizedContext(ctx, bucket, key, shardIdx, body, streamSize)
}

// WriteLocalShardStreamStagedContext writes a shard to the staging physical path
// while sealing with the final logical key as AAD (PR1 segment staging).
func (s *ShardService) WriteLocalShardStreamStagedContext(ctx context.Context, bucket, stagingKey, finalKey string, shardIdx int, body io.Reader) error {
	return s.local.WriteLocalShardStreamStagedContext(ctx, bucket, stagingKey, finalKey, shardIdx, body)
}

// EncodeEncryptedShardBuffer seals data as a GFSENC3 chunked shard using the DEK
// seam (scrubber/EC-repair path).
func (s *ShardService) EncodeEncryptedShardBuffer(bucket, key string, shardIdx int, data []byte) ([]byte, error) {
	return s.local.EncodeEncryptedShardBuffer(bucket, key, shardIdx, data)
}

// ReadLocalShard fetches a shard from the local node's disk and decrypts it.
func (s *ShardService) ReadLocalShard(bucket, key string, shardIdx int) ([]byte, error) {
	return s.local.ReadLocalShard(bucket, key, shardIdx)
}

// ReadLocalShardAt reads len(buf) bytes at offset within the local shard.
func (s *ShardService) ReadLocalShardAt(bucket, key string, shardIdx int, offset int64, buf []byte) (int, error) {
	return s.local.ReadLocalShardAt(bucket, key, shardIdx, offset, buf)
}

// OpenLocalShard opens a local shard as a plaintext stream.
func (s *ShardService) OpenLocalShard(bucket, key string, shardIdx int) (io.ReadCloser, error) {
	return s.local.OpenLocalShard(bucket, key, shardIdx)
}

// OpenLocalShardRange opens a bounded byte range of a local shard as a stream.
func (s *ShardService) OpenLocalShardRange(bucket, key string, shardIdx int, offset int64, length int64) (io.ReadCloser, error) {
	return s.local.OpenLocalShardRange(bucket, key, shardIdx, offset, length)
}

// DeleteLocalShards removes every shard for key on the local node (all indices).
func (s *ShardService) DeleteLocalShards(bucket, key string) error {
	return s.local.DeleteLocalShards(bucket, key)
}

// PromoteLocalStagedShards renames a segment's staged shard dirs to their final
// path (PR1 segment staging).
func (s *ShardService) PromoteLocalStagedShards(bucket, stagingKey, finalKey string) error {
	return s.local.PromoteLocalStagedShards(bucket, stagingKey, finalKey)
}

// --- end facade delegates -------------------------------------------------

// syncDir fsyncs a directory so a create/rename inside it is durably linked
// into the namespace. A general durability primitive (also used by replica
// repair); it formerly lived in the now-removed shard_pack.go.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// encryptedShardEnvelopeOverhead is a conservative upper bound on the bytes
// the AEAD chunked encoder (eccodec.EncodeEncryptedShard) adds per
// DefaultEncryptedChunkSize chunk of plaintext: chunk header + GCM tag, with
// slack for the one-time shard header. 64 bytes/chunk leaves room without
// pinning to the exact eccodec internals.
const encryptedShardEnvelopeOverhead = 64

// maxRawShardPayload returns the largest raw plaintext shard size whose
// encoded payload is guaranteed to fit within datawal.MaxPayloadBytes. The
// encrypted path inflates input by chunked AEAD overhead; the plain path is
// only the small CRC envelope and is bounded by MaxPayloadBytes directly.
func maxRawShardPayload(encrypted bool) int64 {
	if !encrypted {
		return datawal.MaxPayloadBytes
	}
	chunks := int64(datawal.MaxPayloadBytes/eccodec.DefaultEncryptedChunkSize) + 1
	overhead := chunks * encryptedShardEnvelopeOverhead
	if overhead >= datawal.MaxPayloadBytes {
		return 0
	}
	return datawal.MaxPayloadBytes - overhead
}

// readShardPayload buffers body into a byte slice bounded by rawCap. When
// streamSize >= 0 the caller has committed to an exact length: we pre-allocate
// once with make + io.ReadFull, avoiding the bytes.Buffer doubling chain that
// io.ReadAll otherwise drives on every shard write. When streamSize < 0 the
// size is unknown and we fall back to io.ReadAll with the historical cap
// guard.
func readShardPayload(body io.Reader, rawCap, streamSize int64, encrypted bool) ([]byte, error) {
	overCapErr := func(n int64) error {
		if encrypted {
			return fmt.Errorf("shard payload too large after encryption: %d raw bytes exceeds %d cap", n, rawCap)
		}
		return fmt.Errorf("shard payload too large: %d", n)
	}
	if streamSize >= 0 {
		if streamSize > rawCap {
			return nil, overCapErr(streamSize)
		}
		data := make([]byte, streamSize)
		if _, err := io.ReadFull(body, data); err != nil {
			return nil, err
		}
		return data, nil
	}
	data, err := io.ReadAll(io.LimitReader(body, rawCap+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > rawCap {
		return nil, overCapErr(int64(len(data)))
	}
	return data, nil
}

// largeShardFsyncThreshold is the shard-payload size boundary between
// fsync-covered small shards and EC-covered large shards. Below the threshold a
// shard is always fsynced (file + dir chain) at write time; at or above it,
// durability comes from EC redundancy when present (ParityShards>0), else a
// direct fsync (no-redundancy).
const largeShardFsyncThreshold = 1 << 20

// crcShardMagicLen is the length of the eccodec CRC envelope magic
// ("GFSCRC1\x00"); the inner payload (and thus any inner blob magic) begins at
// this byte offset within a GFSCRC1-encoded shard file.
const crcShardMagicLen = 8

// rejectLegacyEncodedShardBlob peeks the 2 inner-payload bytes of a
// GFSCRC1-encoded shard (at file offset crcShardMagicLen) and returns a loud
// error if they are the exact pre-XAES EncryptWithAAD blob magic (0xAE 0xE1).
// The streaming/range fast-paths hand back a reader over the CRC payload
// without ever decrypting, so without this guard an old encrypted single-blob
// shard would be streamed out as raw "plaintext" (silent corruption). A short
// read (shard smaller than the 2-byte probe) is not legacy data, so it passes.
func rejectLegacyEncodedShardBlob(r io.ReaderAt) error {
	var inner [2]byte
	if _, err := r.ReadAt(inner[:], crcShardMagicLen); err != nil {
		return nil // too short to carry the 2-byte legacy magic → not legacy
	}
	if encrypt.IsLegacyEncryptedBlob(inner[:]) {
		return fmt.Errorf("shard carries an unsupported/old encrypted-blob format (pre-XAES); in-place upgrade unsupported")
	}
	return nil
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

func (s *ShardService) handleRead(sr *shardRequest) []byte {
	data, err := s.ReadLocalShard(sr.Bucket, sr.Key, int(sr.ShardIdx))
	if err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(data)
}

func (s *ShardService) handleDelete(sr *shardRequest) []byte {
	if err := s.DeleteLocalShards(sr.Bucket, sr.Key); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

func (s *ShardService) handleRemoveBucketPhysicalTrees(sr *shardRequest) []byte {
	if err := s.RemoveBucketPhysicalTrees(sr.Bucket); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

// handlePromoteStaged renames staged segment shard dirs (sr.Key) to the final path; the final logical
// key is carried in sr.Data (the single-Key envelope). PR1 segment staging.
func (s *ShardService) handlePromoteStaged(sr *shardRequest) []byte {
	if err := s.PromoteLocalStagedShards(sr.Bucket, sr.Key, string(sr.Data)); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

func (s *ShardService) handlePromoteStagedBatch(sr *shardRequest) []byte {
	pairs, err := decodeStagedPromotePairs(sr.Data)
	if err != nil {
		return s.errorResponse(err.Error())
	}
	for _, pair := range pairs {
		if err := s.PromoteLocalStagedShards(sr.Bucket, pair.stagingKey, pair.finalKey); err != nil {
			return s.errorResponse(err.Error())
		}
	}
	return s.okResponse(nil)
}

func (s *ShardService) okResponse(data []byte) []byte {
	return marshalResponseDirect("OK", data)
}

func (s *ShardService) errorResponse(msg string) []byte {
	return marshalResponseDirect("Error", []byte(msg))
}
