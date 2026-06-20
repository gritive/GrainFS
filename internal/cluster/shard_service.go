package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/pool"
	pb "github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/datawal"
	"github.com/gritive/GrainFS/internal/storage/directio"
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
type ShardService struct {
	dataDirs  []string
	transport shardTransport
	segEnc    storage.DataEncryptor // chunked EC-shard data-at-rest seam
	dekKeeper *encrypt.DEKKeeper
	clusterID [16]byte // zero sentinel in D-seg-ec-struct; real ID in slice C
	addrBook  NodeAddressBook
	dirCache  sync.Map
	// dirDurable records, per directory path, that the dir's entry in its PARENT
	// has been persisted by a COMPLETED fsync (Stored post-fsync, never before).
	// syncDirChain consults it to skip re-fsyncing ancestor shard directories
	// whose child link a prior completed write already made durable. Same
	// process-local coherence model as dirCache (no external-delete invalidation;
	// shard leaf paths are version/UUID-keyed and never reborn — see syncDirChain).
	dirDurable sync.Map
	// syncFileHook / syncDirHook are nil-default test seams. Production leaves
	// them nil → fsyncFile/fsyncDir call directio.Sync / syncDir directly. Tests
	// set them to assert the durability fsync call ORDER (DBV / locked-order).
	syncFileHook func(*os.File) error
	syncDirHook  func(string) error
	// noRedundancy, when set, reports whether the deployment has no EC parity
	// and no peers (single-node 1+0). In that case a large metadata-only WAL
	// record cannot be recovered via EC reconstruction, so the shard file must
	// be fsynced directly. Read live so a later EC reconfig is reflected. Nil
	// (legacy callers / tests) never forces a direct fsync.
	noRedundancy func() bool
	// quorumMetaTargetLocks serializes the (LWW guard-read + os.Rename) critical
	// section in writeQuorumMetaLocal and writeQuorumMetaVersionLocal on a
	// per-target-path basis. Without this lock two concurrent writers to the same
	// target can both observe the existing blob, both decide their candidate wins,
	// and then race on the rename — allowing a lower-priority blob to land last
	// and clobber the true LWW winner. The temp-create/write/fsync/close steps
	// remain outside the lock (each writer uses a unique temp name; no contention).
	quorumMetaTargetLocks pool.SyncMap[string, *sync.Mutex]
	// bucketSoleAuthLocks holds the per-bucket fence RWMutex. The flip
	// (applySetBucketSoleAuthority) takes the bucket WRITE-lock; later the
	// quorum-meta leaves take the bucket READ-lock so an in-flight flip fences
	// concurrent writes. DORMANT until the leaves are wired (Task 4).
	bucketSoleAuthLocks pool.SyncMap[string, *sync.RWMutex]
	// soleAuthEpochFn returns the live per-bucket committed sole-authority epoch
	// (0 when never flipped). The quorum-meta leaves consult it under the bucket
	// RLock to reject a stale wire epoch. Set ONCE at startup by SetShardService
	// (via SetSoleAuthEpochFn) before the service serves any RPC/coordinator
	// traffic, so the plain-field read on the apply-independent leaf goroutines
	// is safe by happens-before: the wiring write precedes every leaf read.
	soleAuthEpochFn func(string) uint32
}

// SetSoleAuthEpochFn wires the live committed-epoch reader the quorum-meta
// leaves consult under the bucket RLock. Set once at startup (SetShardService)
// before traffic; the happens-before of that single wiring write makes the
// plain-field read on later leaf goroutines race-free.
func (s *ShardService) SetSoleAuthEpochFn(fn func(string) uint32) { s.soleAuthEpochFn = fn }

// ShardServiceOption is a functional option for ShardService.
type ShardServiceOption func(*ShardService)

// WithShardDEKKeeper wires the generation-aware DEK keeper as the chunked
// EC-shard data-at-rest seam. clusterID MUST be 16 bytes and MUST equal the
// value the put pipeline binds (divergence fails every GET). nil keeper or
// non-16-byte clusterID is a no-op so callers can append the option before the
// keeper is available in narrowly-scoped tests.
func WithShardDEKKeeper(keeper *encrypt.DEKKeeper, clusterID []byte) ShardServiceOption {
	return func(s *ShardService) {
		if keeper == nil || len(clusterID) != 16 {
			return
		}
		copy(s.clusterID[:], clusterID)
		s.dekKeeper = keeper
		s.segEnc = storage.NewDEKKeeperAdapter(keeper, s.clusterID[:])
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
// provider is read live, so a later EC reconfig is honored.
func WithNoRedundancy(fn func() bool) ShardServiceOption {
	return func(s *ShardService) { s.noRedundancy = fn }
}

func (s *ShardService) DEKKeeper() *encrypt.DEKKeeper { return s.dekKeeper }

func (s *ShardService) ClusterID() []byte {
	out := make([]byte, len(s.clusterID))
	copy(out, s.clusterID[:])
	return out
}

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
	}
	for _, opt := range opts {
		opt(s)
	}
	// segEnc is the chunked-EC data-at-rest seam. Production sets it from the
	// generation-aware DEK keeper.
	if s.segEnc == nil {
		panic("cluster.NewShardService: at-rest sealer is mandatory; use WithShardDEKKeeper")
	}
	return s
}

// DataDirs returns the active shard data directories.
func (s *ShardService) DataDirs() []string {
	return s.dataDirs
}

// quorumMetaTargetLock returns the per-target Mutex that serializes the
// (LWW guard-read + os.Rename) critical section in the leaf writers. Mirrors
// DistributedBackend.objectMetaRMWLock: LoadOrStore is atomic so concurrent
// callers always converge on a single Mutex per target path.
func (s *ShardService) quorumMetaTargetLock(target string) *sync.Mutex {
	v, _ := s.quorumMetaTargetLocks.LoadOrStore(target, &sync.Mutex{})
	return v
}

// bucketSoleAuthLock returns the per-bucket fence RWMutex that the soleauth
// flip write-locks (and, once wired, the quorum-meta leaves read-lock). Mirrors
// quorumMetaTargetLock: LoadOrStore is atomic so concurrent callers always
// converge on a single RWMutex per bucket.
func (s *ShardService) bucketSoleAuthLock(bucket string) *sync.RWMutex {
	v, _ := s.bucketSoleAuthLocks.LoadOrStore(bucket, &sync.RWMutex{})
	return v
}

// getShardDir resolves the on-disk directory for an object's shard and rejects
// any key whose ".." segments would escape the {dataDir}/{bucket} root. This is
// the single containment chokepoint for every shard path consumer (S3 writes,
// peer shard RPC, record-driven mover/repair, reads) — see ShardPathUnderDataDir.
func (s *ShardService) getShardDir(bucket, key string, shardIdx int) (string, error) {
	targetDir := s.dataDirs[shardIdx%len(s.dataDirs)]
	dir := filepath.Join(targetDir, bucket, key)
	if !s.ShardPathUnderDataDir(bucket, shardIdx, dir) {
		return "", fmt.Errorf("shard path for object key %q escapes the shard root", key)
	}
	return dir, nil
}

func (s *ShardService) getShardPath(bucket, key string, shardIdx int) (string, error) {
	dir, err := s.getShardDir(bucket, key, shardIdx)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, fmt.Sprintf("shard_%d", shardIdx)), nil
}

// ShardPathUnderDataDir reports whether p resolves inside the {dataDir}/{bucket}
// subtree for the given shard index — i.e. no "../" traversal escapes the shard
// root. The scrubber repair read/write paths derive the on-disk location from an
// object key via getShardPath; a key containing enough ".." would otherwise let
// that path resolve outside the shard root. Callers MUST reject when this returns
// false (containment guard previously provided by shardServiceKeyFromPath).
func (s *ShardService) ShardPathUnderDataDir(bucket string, shardIdx int, p string) bool {
	if len(s.dataDirs) == 0 {
		return false
	}
	// The candidate path AND the containment root are both derived from bucket,
	// so a bucket of ".." (or one carrying a separator) would move both up
	// together and slip past the per-bucket Rel check while physically escaping
	// the shard data dir. Require bucket to be a single clean path segment. S3
	// ingress already rejects these (ValidBucketName); this guards the trusted
	// peer shard-RPC / mover paths that reach the chokepoint directly.
	if !isSafePathSegment(bucket) {
		return false
	}
	root := filepath.Join(s.dataDirs[shardIdx%len(s.dataDirs)], bucket)
	rel, err := filepath.Rel(root, filepath.Clean(p))
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
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
	b := buildShardEnvelope("Ping", "_grainfs_health", "_ping", 0, nil, 0)
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
	envb := buildShardEnvelope("WriteShard", bucket, key, int32(shardIdx), data, 0)
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
	envb := buildShardEnvelope("ReadShard", bucket, key, int32(shardIdx), nil, 0)
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
	envb := buildShardEnvelope("ReadShardRange", bucket, key, int32(shardIdx), rangePayload[:], 0)
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
	envb := buildShardEnvelope("DeleteShards", bucket, key, 0, nil, 0)
	defer func() { envb.Reset(); shardBuilderPool.Put(envb) }()
	_, err = s.callShardRPC(ctx, peerAddr, envb)
	return err
}

// buildShardEnvelope builds an RPCMessage FlatBuffer wrapping a ShardRequest without make+copy.
// Returns a Builder that MUST be Reset()+Put() to shardBuilderPool after use.
func buildShardEnvelope(msgType, bucket, key string, shardIdx int32, data []byte, admittedEpoch uint32) *flatbuffers.Builder {
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
	pb.ShardRequestAddAdmittedSoleauthEpoch(b, admittedEpoch)
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
	case "WriteShadowMeta":
		return s.handleShadowMeta(sr)
	case "WriteQuorumMeta":
		return s.handleQuorumMetaWrite(sr)
	case "WriteQuorumMetaVersion":
		return s.handleQuorumMetaVersionWrite(sr)
	case "DeleteQuorumMeta":
		return s.handleQuorumMetaDelete(sr)
	case "ReadQuorumMeta":
		return s.handleQuorumMetaRead(sr)
	case "ScanQuorumMeta":
		return s.handleScanQuorumMeta(sr)
	case "ScanQuorumMetaVersions":
		return s.handleScanQuorumMetaVersions(sr)
	case "ScanQuorumMetaVersionsAll":
		return s.handleScanQuorumMetaVersionsAll(sr)
	case "ReadQuorumMetaVersions":
		return s.handleQuorumMetaVersionsRead(sr)
	case "ReadQuorumMetaVersionsRaw":
		return s.handleQuorumMetaVersionsReadRaw(sr)
	case "DeleteQuorumMetaVersion":
		return s.handleQuorumMetaVersionDelete(sr)
	default:
		return s.errorResponse("unknown shard RPC: " + rpcType)
	}
}

// handleQuorumMetaWrite receives a Phase 3 primary quorum meta blob and
// durably writes it locally (write + fsync). Failures are reported to the
// caller so the PUT can fail the quorum check.
func (s *ShardService) handleQuorumMetaWrite(sr *shardRequest) []byte {
	if err := s.writeQuorumMetaLocal(sr.Bucket, sr.Key, sr.Data, sr.AdmittedSoleAuthEpoch); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

// handleQuorumMetaVersionWrite receives a per-version quorum-meta blob and
// durably writes it under the .quorum_meta_versions subtree. sr.Key carries
// path.Join(key, versionID).
func (s *ShardService) handleQuorumMetaVersionWrite(sr *shardRequest) []byte {
	if err := s.writeQuorumMetaVersionLocal(sr.Bucket, sr.Key, sr.Data, sr.AdmittedSoleAuthEpoch); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

// handleQuorumMetaDelete serves a DeleteQuorumMeta RPC: removes the local quorum
// meta file for (bucket, key). Absent file is not an error (idempotent cleanup).
func (s *ShardService) handleQuorumMetaDelete(sr *shardRequest) []byte {
	if err := s.deleteQuorumMetaLocal(sr.Bucket, sr.Key, sr.AdmittedSoleAuthEpoch); err != nil {
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
		blob, eerr := EncodeCommand(CmdPutObjectMeta, entries[i])
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
		if blob, eerr := EncodeCommand(CmdPutObjectMeta, entries[i]); eerr == nil {
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
		if blob, eerr := EncodeCommand(CmdPutObjectMeta, entries[i]); eerr == nil {
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
		if blob, eerr := EncodeCommand(CmdPutObjectMeta, cmds[i]); eerr == nil {
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
	if err := s.deleteQuorumMetaVersionLocal(sr.Bucket, key, versionID, sr.AdmittedSoleAuthEpoch); err != nil {
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
		Bucket:                string(t.Bucket()),
		Key:                   string(t.Key()),
		ShardIdx:              t.ShardIdx(),
		Data:                  t.DataBytes(),
		AdmittedSoleAuthEpoch: t.AdmittedSoleauthEpoch(),
	}, nil
}

// shardRequest is the in-memory representation of a shard RPC request.
type shardRequest struct {
	Bucket   string
	Key      string
	ShardIdx int32
	Data     []byte
	// AdmittedSoleAuthEpoch carries the originating coordinator's sole-authority
	// epoch on a fenced quorum-meta write/delete. DORMANT (S4c-a2-A T3): every
	// caller currently passes 0; Task 4 supplies the real epoch at fenced sites.
	AdmittedSoleAuthEpoch uint32
}

func (s *ShardService) handleWrite(sr *shardRequest) []byte {
	ctx := ContextWithPutTrace(context.Background(), PutTraceRequest{
		Bucket:      sr.Bucket,
		Key:         sr.Key,
		Ingress:     PutTraceIngressReceiver,
		SizeClass:   putTraceSizeClass(int64(len(sr.Data)), ecShardBufferedLimit),
		ForwardMode: PutTraceForwardNone,
	})
	if err := s.writeLocalShard(ctx, sr.Bucket, sr.Key, int(sr.ShardIdx), sr.Data); err != nil {
		return s.errorResponse(err.Error())
	}
	return s.okResponse(nil)
}

// handleShadowMeta receives a Phase 0 shadow object-meta blob and durably
// writes it locally (write + fsync). Measurement only — not load-bearing.
func (s *ShardService) handleShadowMeta(sr *shardRequest) []byte {
	if err := s.writeShadowMetaLocal(sr.Bucket, sr.Key, sr.Data); err != nil {
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
			if werr := s.writeLocalSealedShard(context.Background(), req.Bucket, req.Key, req.ShardIdx, sealed); werr != nil {
				return werr
			}
			observePutStage("shard_stream_server", "write_local_sealed", stageStart)
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

// WriteLocalShard stores a shard on the local node's disk without involving
// the cluster transport. Used by PutObject when this node is the destination for
// one of an object's shards (self-placement); avoids a loopback RPC.
// The shard is sealed via the DEK keeper (GFSENC3) before writing.
// Writes are crash-safe via tmp + rename for atomic visibility. Durability is
// established at write time (no WAL since S4): small / no-redundancy shards
// fsync the file + parent-dir chain; large redundant shards rely on EC
// reconstruction + the background scrubber.
// New encrypted writes use eccodec's chunked AEAD envelope. Plain writes keep
// the CRC envelope while that compatibility path remains available.
func (s *ShardService) WriteLocalShard(bucket, key string, shardIdx int, data []byte) error {
	return s.writeLocalShard(context.Background(), bucket, key, shardIdx, data)
}

func (s *ShardService) WriteLocalShardContext(ctx context.Context, bucket, key string, shardIdx int, data []byte) error {
	return s.writeLocalShard(ctx, bucket, key, shardIdx, data)
}

// EncodeEncryptedShardBuffer seals data as a GFSENC3 chunked shard using the
// DEK seam, identical to the normal writeLocalShard format. Used by the
// scrubber/EC-repair path (DistributedBackend.WriteShard) so repaired shards
// are DEK-encrypted at rest like normally-written shards. The at-rest sealer
// (segEnc) is mandatory (NewShardService panics if absent), so there is no
// plaintext branch.
func (s *ShardService) EncodeEncryptedShardBuffer(bucket, key string, shardIdx int, data []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := eccodec.EncodeEncryptedShard(&buf, bytes.NewReader(data), s.segEnc, ShardAADFields(bucket, key, shardIdx), eccodec.DefaultEncryptedChunkSize); err != nil {
		return nil, fmt.Errorf("encode encrypted shard: %w", err)
	}
	return buf.Bytes(), nil
}

func (s *ShardService) writeLocalShard(ctx context.Context, bucket, key string, shardIdx int, data []byte) error {
	dir, err := s.getShardDir(bucket, key, shardIdx)
	if err != nil {
		return err
	}
	mkdirStart := time.Now()
	if err := s.ensureShardDir(dir); err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalMkdir, mkdirStart, PutTraceStageFields{
			Bytes:            int64(len(data)),
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
			Error:            err.Error(),
		})
		return fmt.Errorf("create shard dir: %w", err)
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalMkdir, mkdirStart, PutTraceStageFields{
		Bytes:            int64(len(data)),
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})
	path := filepath.Join(dir, fmt.Sprintf("shard_%d", shardIdx))
	encodeStart := time.Now()
	var encoded bytes.Buffer
	if err := eccodec.EncodeEncryptedShard(&encoded, bytes.NewReader(data), s.segEnc, ShardAADFields(bucket, key, shardIdx), eccodec.DefaultEncryptedChunkSize); err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncode, encodeStart, PutTraceStageFields{
			Bytes:            int64(len(data)),
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
			Error:            err.Error(),
		})
		return err
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncode, encodeStart, PutTraceStageFields{
		Bytes:            int64(len(data)),
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})
	payload := encoded.Bytes()
	requireFsync := s.shardWriteRequiresFsync(len(payload))
	fileStart := time.Now()
	if err := s.writeEncryptedShardFile(ctx, dir, path, payload, shardIdx, requireFsync); err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalFile, fileStart, PutTraceStageFields{
			Bytes:            int64(len(payload)),
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
			Error:            err.Error(),
		})
		return err
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalFile, fileStart, PutTraceStageFields{
		Bytes:            int64(len(payload)),
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})
	// Directory durability (the DirSync trace stage) now runs inside
	// writeEncryptedShardFile, where requireFsync is known and the real
	// syncDirChain fsync happens for the fsync classes (S2).
	return nil
}

// writeLocalSealedShard stores an ALREADY-SEALED shard payload verbatim — no
// re-encryption at the destination. Used by the S2 streaming path: the
// coordinator's CPUPool seals the shard at the source (seal-at-source), so the
// bytes on the wire and on disk are identical GFSENC3 ciphertext. This mirrors
// writeLocalShard's tail (mkdir → tmp+rename → fsync/EC durability) MINUS the
// encode step. Packing is intentionally bypassed: the
// streaming path is the large-object band (shard ≫ pack threshold), so a
// sealed-stream shard always materializes as a shard_N file. `sealed` must be
// EncodeEncryptedShard output for ShardAADFields(bucket,key,shardIdx) (the same
// identity this node will use on read), or the read-side AEAD/AAD check fails.
func (s *ShardService) writeLocalSealedShard(ctx context.Context, bucket, key string, shardIdx int, sealed []byte) error {
	dir, err := s.getShardDir(bucket, key, shardIdx)
	if err != nil {
		return err
	}
	if err := s.ensureShardDir(dir); err != nil {
		return fmt.Errorf("create shard dir: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("shard_%d", shardIdx))
	requireFsync := s.shardWriteRequiresFsync(len(sealed))
	return s.writeEncryptedShardFile(ctx, dir, path, sealed, shardIdx, requireFsync)
}

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

// fsyncFile fsyncs an open shard tmp file (or the test seam when set).
func (s *ShardService) fsyncFile(f *os.File) error {
	if s.syncFileHook != nil {
		return s.syncFileHook(f)
	}
	return directio.Sync(f)
}

// fsyncDir fsyncs one directory so a rename (the durable link of a shard file
// into the namespace) survives a crash (or the test seam).
func (s *ShardService) fsyncDir(dir string) error {
	if s.syncDirHook != nil {
		return s.syncDirHook(dir)
	}
	return syncDir(dir)
}

// syncDirChain durably LINKS a freshly written shard into the namespace: it
// fsyncs the leaf (its new shard-file entry) and persists each ancestor's entry
// in its parent up to (exclusive) stop. getShardDir builds dir = dataDir/bucket/key
// (version-keyed key dir created per PUT), so fsyncing only the leaf would leave
// the leaf's own entry in its parent non-durable (a crash could lose the whole
// shard dir after the quorum-meta commit). stop is the shard's data dir; the
// bucket dir's own entry in dataDir is out of scope (a pre-existing gap — see
// below — that this function never closed and CreateBucket does not either).
//
// Dedup: the leaf is fsynced every write (a new shard FILE lands there). An
// ancestor only needs fsyncing the first time its child entry is created+
// persisted; once a COMPLETED prior write made that link durable, the chain stays
// durable across later writes (which only add shard FILES to leaves, never
// restructure ancestors). dirDurable(d) records that d's entry in its parent was
// persisted; the mark is Stored only AFTER the persisting fsync(parent(d))
// returns, so any skip is backed by durable on-disk data (race-free under the
// concurrent same-leaf shard writes the errgroup data-shard path issues). A
// newly-created dir is absent from the map (== not durable) → its parent IS
// fsynced, persisting the new entry; so a new sibling is never skipped.
//
// Coherence: dirDurable is process-local with no external-delete invalidation —
// the same model as dirCache. Every shard-leaf path is version/UUID-keyed and the
// delete paths (DeleteLocalShards, orphan RemoveAll) operate on those unique
// leaves, never reborn, so a stale mark is never consulted again.
func (s *ShardService) syncDirChain(leaf, stop string) error {
	stop = filepath.Clean(stop)
	leaf = filepath.Clean(leaf)
	if leaf == stop {
		// Degenerate (unreachable for real shards: dir = dataDir/bucket/key is
		// always strictly below stop = dataDir). Preserve the pre-dedup contract,
		// whose `for d := leaf; d != stop` loop fsynced nothing here.
		return nil
	}
	// The leaf always receives a fresh shard-file entry on this write → fsync its
	// CONTENTS every time, independent of whether its own dir entry is durable.
	if err := s.fsyncDir(leaf); err != nil {
		return err
	}
	for d := leaf; ; {
		if _, durable := s.dirDurable.Load(d); durable {
			return nil // d's entry — and the whole chain above it — already durable
		}
		parent := filepath.Dir(d)
		if parent == stop {
			// d is the shard-tree bucket dir; stop is the data dir. Match the
			// pre-dedup contract exactly: never fsync stop (its bucket-dir entry is
			// a pre-existing, out-of-scope durability gap). Record d so future
			// writes can stop here too.
			s.dirDurable.Store(d, struct{}{})
			return nil
		}
		if parent == d { // reached filesystem root WITHOUT hitting stop
			// stop must be an ancestor of leaf; if not, the caller passed a wrong
			// data-dir root and we'd otherwise silently report success (masking a
			// placement/path bug) — fail loudly instead.
			return fmt.Errorf("syncDirChain: stop %q is not an ancestor of %q", stop, leaf)
		}
		if err := s.fsyncDir(parent); err != nil {
			return err // d NOT marked durable: its entry isn't persisted yet
		}
		s.dirDurable.Store(d, struct{}{})
		d = parent
	}
}

// writeEncryptedShardFile materializes pre-encoded (chunked AEAD) shard bytes
// to disk using the atomic tmp + rename recipe. Encoding happens in the
// caller so the encrypted payload is computed before any shard file mutation;
// this function only handles the on-disk I/O.
//
// Post-S2 durability is established at WRITE TIME for the fsync classes (small /
// no-redundancy): when requireFsync the EncSync stage fsyncs the shard file and
// the DirSync stage fsyncs the shard's directory CHAIN (leaf shard dir + each
// newly-created ancestor up to the data dir) so a crash cannot lose the file or
// its namespace link. Large redundant shards (requireFsync=false) skip both —
// EC reconstruction + the scrubber own their durability (S1). Trace stages:
//   - PutTraceStageShardWriteLocalEncWrite: file write of the encoded payload.
//   - PutTraceStageShardWriteLocalEncSync: shard-file fsync (requireFsync only).
//   - PutTraceStageShardWriteLocalDirSync: directory-chain fsync (requireFsync only).
func (s *ShardService) writeEncryptedShardFile(ctx context.Context, dir, path string, payload []byte, shardIdx int, requireFsync bool) error {
	tmp := fmt.Sprintf("%s.%d.%d.tmp", path, os.Getpid(), time.Now().UnixNano())
	openStart := time.Now()
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncOpen, openStart, PutTraceStageFields{
			Bytes:            int64(len(payload)),
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
			Error:            err.Error(),
		})
		return fmt.Errorf("create tmp shard: %w", err)
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncOpen, openStart, PutTraceStageFields{
		Bytes:            int64(len(payload)),
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(tmp)
	}

	writeStart := time.Now()
	if _, err := f.Write(payload); err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncWrite, writeStart, PutTraceStageFields{
			Bytes:            int64(len(payload)),
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
			Error:            err.Error(),
		})
		cleanup()
		return fmt.Errorf("write tmp shard: %w", err)
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncWrite, writeStart, PutTraceStageFields{
		Bytes:            int64(len(payload)),
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})

	// EncSync fsyncs the shard file only when requireFsync is set: for small
	// shards and for large no-redundancy shards (no parity to reconstruct from).
	// It is skipped for large redundant shards, which rely on EC reconstruction
	// + the scrubber
	// (S1 — no WAL record, no fsync).
	encSyncStart := time.Now()
	if requireFsync {
		if err := s.fsyncFile(f); err != nil {
			ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncSync, encSyncStart, PutTraceStageFields{
				Bytes:            int64(len(payload)),
				ShardIndex:       shardIdx,
				ShardTargetClass: "local",
				Error:            err.Error(),
			})
			cleanup()
			return fmt.Errorf("fsync tmp shard: %w", err)
		}
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncSync, encSyncStart, PutTraceStageFields{
		Bytes:            int64(len(payload)),
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})

	closeStart := time.Now()
	if err := f.Close(); err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncClose, closeStart, PutTraceStageFields{
			Bytes:            int64(len(payload)),
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
			Error:            err.Error(),
		})
		_ = os.Remove(tmp)
		return fmt.Errorf("close tmp shard: %w", err)
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncClose, closeStart, PutTraceStageFields{
		Bytes:            int64(len(payload)),
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})

	renameStart := time.Now()
	if err := os.Rename(tmp, path); err != nil {
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncRename, renameStart, PutTraceStageFields{
			Bytes:            int64(len(payload)),
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
			Error:            err.Error(),
		})
		_ = os.Remove(tmp)
		return fmt.Errorf("rename shard: %w", err)
	}
	ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalEncRename, renameStart, PutTraceStageFields{
		Bytes:            int64(len(payload)),
		ShardIndex:       shardIdx,
		ShardTargetClass: "local",
	})

	// D2 (DBV): after the rename, fsync the shard's directory CHAIN (leaf shard
	// dir + each newly-created ancestor up to the data dir) so the durable link
	// of the shard file into the namespace cannot be lost on crash. Gated on
	// requireFsync — large redundant shards (S1) own durability via EC
	// reconstruction and skip both the file and dir fsync (a vanished dir there
	// is just a "missing shard" rebuilt lazily). Locked order:
	// write(tmp) → Sync(tmp) → rename → syncDirChain(dir) → return.
	if requireFsync {
		dirSyncStart := time.Now()
		stop := s.dataDirs[shardIdx%len(s.dataDirs)]
		if err := s.syncDirChain(dir, stop); err != nil {
			ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalDirSync, dirSyncStart, PutTraceStageFields{
				Bytes:            int64(len(payload)),
				ShardIndex:       shardIdx,
				ShardTargetClass: "local",
				Error:            err.Error(),
			})
			return fmt.Errorf("fsync shard dir chain: %w", err)
		}
		ObservePutTraceStage(ctx, PutTraceStageShardWriteLocalDirSync, dirSyncStart, PutTraceStageFields{
			Bytes:            int64(len(payload)),
			ShardIndex:       shardIdx,
			ShardTargetClass: "local",
		})
	}

	return nil
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

// shardWriteRequiresFsync reports whether a freshly written shard file (and its
// parent directory chain) must be fsynced for durability, by shard class.
// Post-S2 the data WAL is no longer written on the shard PUT path — durability
// is established at write time:
//
//   - Small (< largeShardFsyncThreshold) OR large with NO EC redundancy
//     (ParityShards==0, i.e. noRedundancy()): fsync the shard file + parent dir
//     chain (there is no parity to reconstruct from, or the shard is too small
//     to be worth an EC stripe) → returns true.
//   - Large WITH EC redundancy (ParityShards>0, i.e. !noRedundancy): EC
//     reconstruction + the background scrubber (S0/S1) own durability; the file
//     is NOT fsynced → returns false. A nil noRedundancy provider counts as
//     redundant (matches production wiring; nil only occurs in tests).
//
// S4 removed the data WAL entirely, so there is no replay path to special-case.
func (s *ShardService) shardWriteRequiresFsync(payloadLen int) bool {
	large := payloadLen >= largeShardFsyncThreshold
	if large && (s.noRedundancy == nil || !s.noRedundancy()) {
		return false // large + redundant: EC durability, no fsync (S1)
	}
	return true // small, or large + no-redundancy: write-time fsync
}

// WriteLocalShardStream stores a shard from body without buffering plaintext.
func (s *ShardService) WriteLocalShardStream(bucket, key string, shardIdx int, body io.Reader) error {
	return s.WriteLocalShardStreamContext(context.Background(), bucket, key, shardIdx, body)
}

func (s *ShardService) WriteLocalShardStreamContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader) error {
	return s.writeLocalShardStreamContext(ctx, bucket, key, shardIdx, body, -1)
}

func (s *ShardService) WriteLocalShardStreamSizedContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader, streamSize int64) error {
	return s.writeLocalShardStreamContext(ctx, bucket, key, shardIdx, body, streamSize)
}

func (s *ShardService) writeLocalShardStreamContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader, streamSize int64) error {
	// Buffer the body bounded by a 64 MiB memory cap (derived from
	// datawal.MaxPayloadBytes; the value is retained purely as a memory bound,
	// not a WAL record limit), then route through writeLocalShard. Durability no
	// longer involves a WAL (S4): writeLocalShard fsyncs small / no-redundancy
	// shards directly and relies on EC for large redundant shards.
	rawCap := maxRawShardPayload(false)
	data, err := readShardPayload(body, rawCap, streamSize, false)
	if err != nil {
		return err
	}
	return s.writeLocalShard(ctx, bucket, key, shardIdx, data)
}

func (s *ShardService) ensureShardDir(dir string) error {
	if _, ok := s.dirCache.Load(dir); ok {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		s.dirCache.Delete(dir)
		s.dirDurable.Delete(dir)
		return err
	}
	s.dirCache.Store(dir, struct{}{})
	return nil
}

// ReadLocalShard fetches a shard from the local node's disk and decrypts it via
// the DEK keeper. Returns an error if the shard appears encrypted but at-rest
// encryption is disabled (downgrade guard).
func (s *ShardService) ReadLocalShard(bucket, key string, shardIdx int) ([]byte, error) {
	path, err := s.getShardPath(bucket, key, shardIdx)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var prefix [8]byte
	_, peekErr := io.ReadFull(f, prefix[:])
	if peekErr == nil && eccodec.IsEncryptedShard(prefix[:]) {
		if s.segEnc == nil {
			_ = f.Close()
			return nil, fmt.Errorf("shard is encrypted but encryption is disabled; start with DEK-backed at-rest encryption enabled")
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
		if err := eccodec.DecodeEncryptedShard(&decoded, f, s.segEnc, ShardAADFields(bucket, key, shardIdx)); err != nil {
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
	if eccodec.IsEncodedShard(raw) {
		data, err = eccodec.DecodeShard(raw)
		if err != nil {
			return nil, err
		}
	}
	if encrypt.IsEncryptedBlob(data) {
		return nil, fmt.Errorf("shard is encrypted but encryption is disabled; start with DEK-backed at-rest encryption enabled")
	}
	if encrypt.IsLegacyEncryptedBlob(data) {
		return nil, fmt.Errorf("shard carries an unsupported/old encrypted-blob format (pre-XAES); in-place upgrade unsupported")
	}
	// DEK-only service: every shard is GFSENC3-sealed (handled above). A payload
	// that reaches here carries no envelope — reject plaintext fail-closed.
	return nil, fmt.Errorf("%w: shard carries no GFSENC3 envelope and at-rest encryption is DEK-only (plaintext rejected)", eccodec.ErrShardCorrupt)
}

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

// OpenLocalShard opens a local shard as plaintext. New chunked encrypted shards
// are decrypted chunk-by-chunk; compatibility formats fall back to ReadLocalShard.
func (s *ShardService) OpenLocalShard(bucket, key string, shardIdx int) (io.ReadCloser, error) {
	path, err := s.getShardPath(bucket, key, shardIdx)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var prefix [8]byte
	_, peekErr := io.ReadFull(f, prefix[:])
	if peekErr == nil && eccodec.IsEncryptedShard(prefix[:]) {
		if s.segEnc == nil {
			_ = f.Close()
			return nil, fmt.Errorf("shard is encrypted but encryption is disabled; start with DEK-backed at-rest encryption enabled")
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
		r, err := eccodec.NewEncryptedShardReader(f, s.segEnc, ShardAADFields(bucket, key, shardIdx))
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("decrypt shard: %w", err)
		}
		return &multiReadCloser{Reader: r, close: func() error {
			var closeErr error
			if closer, ok := r.(io.Closer); ok {
				closeErr = closer.Close()
			}
			if err := f.Close(); closeErr == nil {
				closeErr = err
			}
			return closeErr
		}}, nil
	}
	if peekErr == nil && eccodec.IsEncodedShard(prefix[:]) {
		if err := rejectLegacyEncodedShardBlob(f); err != nil {
			_ = f.Close()
			return nil, err
		}
		// GFSCRC1-wrapped shard: may be an encrypted single-blob (XAES) that
		// cannot be streamed without decrypting. Delegate to ReadLocalShard to
		// buffer-decrypt; reject plain shards as corruption.
		_ = f.Close()
		data, err := s.ReadLocalShard(bucket, key, shardIdx)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(data)), nil
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
	path, err := s.getShardPath(bucket, key, shardIdx)
	if err != nil {
		return 0, err
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var prefix [8]byte
	_, peekErr := io.ReadFull(f, prefix[:])
	if peekErr == nil && eccodec.IsEncryptedShard(prefix[:]) {
		if s.segEnc == nil {
			return 0, fmt.Errorf("shard is encrypted but encryption is disabled; start with DEK-backed at-rest encryption enabled")
		}
		n, err := eccodec.ReadEncryptedShardRangeAt(f, s.segEnc, ShardAADFields(bucket, key, shardIdx), offset, buf)
		if err != nil {
			return n, fmt.Errorf("decrypt shard range: %w", err)
		}
		return n, nil
	}
	if peekErr == nil && eccodec.IsEncodedShard(prefix[:]) {
		if err := rejectLegacyEncodedShardBlob(f); err != nil {
			_ = f.Close()
			return 0, err
		}
		// GFSCRC1-wrapped shard: may be an encrypted single-blob (XAES) that
		// cannot be range-read without decrypting. Fall through to OpenLocalShard.
		_ = f.Close()
	} else {
		_ = f.Close()
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
	path, err := s.getShardPath(bucket, key, shardIdx)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	var prefix [8]byte
	_, peekErr := io.ReadFull(f, prefix[:])
	if peekErr == nil && eccodec.IsEncryptedShard(prefix[:]) {
		if s.segEnc == nil {
			_ = f.Close()
			return nil, fmt.Errorf("shard is encrypted but encryption is disabled; start with DEK-backed at-rest encryption enabled")
		}
		r, err := eccodec.NewEncryptedShardRangeReader(f, s.segEnc, ShardAADFields(bucket, key, shardIdx), offset, length)
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("decrypt shard range: %w", err)
		}
		closeFn := f.Close
		if closer, ok := r.(io.Closer); ok {
			closeFn = func() error {
				rerr := closer.Close()
				ferr := f.Close()
				if rerr != nil {
					return rerr
				}
				return ferr
			}
		}
		return &multiReadCloser{Reader: r, close: closeFn}, nil
	}
	if peekErr == nil && eccodec.IsEncodedShard(prefix[:]) {
		// DEK-only service: every shard is GFSENC3-sealed, so a GFSCRC1 shard here
		// is plaintext (or legacy). Reject fail-closed rather than leak it.
		_ = f.Close()
		return nil, fmt.Errorf("%w: shard carries no GFSENC3 envelope and at-rest encryption is DEK-only (plaintext rejected)", eccodec.ErrShardCorrupt)
	}
	// A plain/short shard: route through OpenLocalShard for proper decode (rejects
	// plaintext) rather than streaming the raw payload — keeps OpenLocalShardRange
	// consistent with ReadLocalShard/ReadLocalShardAt.
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
	for _, dataDir := range s.dataDirs {
		dir := filepath.Join(dataDir, bucket, key)
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
	}
	return nil
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

func (s *ShardService) okResponse(data []byte) []byte {
	return marshalResponseDirect("OK", data)
}

func (s *ShardService) errorResponse(msg string) []byte {
	return marshalResponseDirect("Error", []byte(msg))
}
