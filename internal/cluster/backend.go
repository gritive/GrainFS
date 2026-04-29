package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/metrics/readamp"
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
)

// shardRPCTimeout is the per-shard RPC deadline for remote writes/reads.
// QUIC max-idle is 10 s; 3 s leaves margin for retries before the stream times out.
const shardRPCTimeout = 3 * time.Second

// newVersionID returns a fresh UUIDv7 string for use as an object VersionID.
// UUIDv7 is k-sortable by millisecond timestamp; ListObjectVersions reverses to DESC.
func newVersionID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}

// OnApplyFunc is called after FSM.Apply() with the command type, bucket, and key.
// Used for cache invalidation and metrics updates.
type OnApplyFunc func(cmdType CommandType, bucket, key string)

// BucketAssigner proposes a bucket→group assignment to the meta-Raft cluster.
// Implemented by *MetaRaft; nil = no persistence (single-node legacy mode).
type BucketAssigner interface {
	ProposeBucketAssignment(ctx context.Context, bucket, groupID string) error
}

// DistributedBackend implements storage.Backend with Raft-replicated metadata
// and local file storage for data. Metadata mutations go through Raft;
// reads are served from the local BadgerDB (kept in sync by the FSM).
type DistributedBackend struct {
	root        string
	db          *badger.DB
	node        *raft.Node
	fsm         *FSM
	logger      zerolog.Logger
	lastApplied atomic.Uint64
	onApply     OnApplyFunc
	snapMgr     *raft.SnapshotManager
	snapNode    *raft.Node // node for CompactLog after snapshot
	shardSvc    *ShardService
	allNodes    []string // all node addresses (including self) for shard placement
	selfAddr    string   // this node's raft address (matches entries in allNodes)
	peerHealth  *PeerHealth
	registry    *Registry                           // cache invalidators (VFS instances)
	ecConfig    ECConfig                            // Phase 18: erasure coding config (k+m shard parameters)
	shardLocks  pool.SyncMap[string, *sync.RWMutex] // scrubbable.go: per-(bucket,key) RWMutex for ReadShard/WriteShard

	// shardCache caches reconstructed/fetched EC shards. Sits in front of
	// getObjectEC's per-shard fan-out: a full hit (every needed shard
	// resident) skips disk and network entirely. Nil disables caching.
	// See internal/cache/shardcache for the rationale (sharded LRU,
	// lock-free counters, why we do not use an actor pattern here).
	shardCache *shardcache.Cache

	assigner BucketAssigner // PR-D: MetaRaft proposer; nil = no-op (single-node legacy)
	router   *Router        // PR-D: bucket→group routing; nil = no routing

	// vfsFixedVersion controls VFS-internal-bucket behavior:
	// true (default) → PutObject for "__grainfs_vfs_*" uses fixed versionID
	//   "current" so on-disk usage stays bounded to one copy per key.
	// false → legacy behavior (fresh ULID per PUT). Operators can flip via
	//   --backend-vfs-fixed-version=false to roll back without rebuild.
	vfsFixedVersion atomic.Bool
}

// NewDistributedBackend creates a new distributed storage backend.
// The FSM apply loop must be started separately via RunApplyLoop.
func NewDistributedBackend(root string, db *badger.DB, node *raft.Node) (*DistributedBackend, error) {
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	fsm := NewFSM(db)

	if noOp, err := EncodeNoOpCommand(); err == nil {
		node.SetNoOpCommand(noOp)
	}

	b := &DistributedBackend{
		root:     root,
		db:       db,
		node:     node,
		fsm:      fsm,
		logger:   log.With().Str("component", "distributed-backend").Logger(),
		registry: NewRegistry(),
	}
	b.vfsFixedVersion.Store(true) // default on; toggle via --backend-vfs-fixed-version=false
	return b, nil
}

// SetVFSFixedVersionEnabled toggles the fixed-versionID behavior for
// __grainfs_vfs_* buckets. See vfsFixedVersion field comment.
func (b *DistributedBackend) SetVFSFixedVersionEnabled(on bool) {
	b.vfsFixedVersion.Store(on)
}

// VFSFixedVersionEnabled reports the current toggle state.
func (b *DistributedBackend) VFSFixedVersionEnabled() bool {
	return b.vfsFixedVersion.Load()
}

// SetShardCache configures the EC shard cache. Pass a cache built with
// shardcache.New(byteBudget). Pass nil (or shardcache.New(0)) to leave
// caching disabled. Must be called before serving traffic.
func (b *DistributedBackend) SetShardCache(c *shardcache.Cache) {
	b.shardCache = c
}

// shardCacheKey is the canonical cache key for a single EC shard. Must
// match the readamp tracker key (backend.go:getObjectEC RecordECShard
// call) so simulator and real cache share the same identity.
func shardCacheKey(bucket, shardKey string, idx int) string {
	return fmt.Sprintf("%s/%s/%d", bucket, shardKey, idx)
}

// invalidateShardCache drops every shard slot for one shardKey. Used by
// PutObject overwrite, DeleteObject, and repairShardEC so a subsequent
// read sees post-write state. nShards covers the full k+m fan-out.
func (b *DistributedBackend) invalidateShardCache(bucket, shardKey string, nShards int) {
	if b.shardCache == nil {
		return
	}
	for i := 0; i < nShards; i++ {
		b.shardCache.Invalidate(shardCacheKey(bucket, shardKey, i))
	}
}

// SetECConfig configures erasure-coding shard parameters (k, m) for
// PutObject/GetObject. Phase 18. Call before serving traffic. EC activates
// whenever the cluster has at least MinECNodes nodes.
func (b *DistributedBackend) SetECConfig(cfg ECConfig) {
	b.ecConfig = cfg
}

// SetShardService configures the distributed shard service for fan-out.
// allNodes includes all cluster node addresses for placement (self first is
// expected so the self address can be cached before the slice is sorted).
func (b *DistributedBackend) SetShardService(svc *ShardService, allNodes []string) {
	b.shardSvc = svc
	// Cache self address BEFORE sorting so per-request self-skip checks can
	// compare raft addresses (node.ID() returns a UUID, not the address).
	if len(allNodes) > 0 {
		b.selfAddr = allNodes[0]
	}
	b.allNodes = append([]string(nil), allNodes...)
	sort.Strings(b.allNodes)
	// Build peer list (excluding self) for health tracking
	var peers []string
	for _, n := range allNodes {
		if n != b.selfAddr {
			peers = append(peers, n)
		}
	}
	b.peerHealth = NewPeerHealth(peers, 10*time.Second)
}

// liveNodes returns the current cluster node list for placement decisions.
// When the Raft node has configured peers (normal operation), the list is
// built from the Raft peer set plus selfAddr so it reflects membership
// changes without requiring a restart. Falls back to the statically-cached
// allNodes when the node has no peers (unit tests, single-node deploy).
func (b *DistributedBackend) liveNodes() []string {
	if b.node != nil {
		if peers := b.node.Peers(); len(peers) > 0 {
			nodes := make([]string, 0, len(peers)+1)
			if b.selfAddr != "" {
				nodes = append(nodes, b.selfAddr)
			}
			for _, p := range peers {
				if b.peerHealth == nil || b.peerHealth.IsHealthy(p) {
					nodes = append(nodes, p)
				}
			}
			sort.Strings(nodes)
			return nodes
		}
	}
	return b.allNodes
}

// SetSnapshotManager configures automatic snapshot creation after N applied entries.
// Must be called before RunApplyLoop.
func (b *DistributedBackend) SetSnapshotManager(mgr *raft.SnapshotManager, node *raft.Node) {
	b.snapMgr = mgr
	b.snapNode = node
}

// SetOnApply sets the callback invoked after each FSM apply.
// Must be called before RunApplyLoop.
func (b *DistributedBackend) SetOnApply(fn OnApplyFunc) {
	b.onApply = fn
}

// RunApplyLoop consumes committed entries from the Raft node and applies them to the FSM.
// This must run in a goroutine.
func (b *DistributedBackend) RunApplyLoop(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case entry := <-b.node.ApplyCh():
			if err := b.fsm.Apply(entry.Command); err != nil {
				b.logger.Error().Uint64("index", entry.Index).Err(err).Msg("fsm apply error")
			}
			b.lastApplied.Store(entry.Index)

			// Notify cache/metrics callback
			if b.onApply != nil {
				b.notifyOnApply(entry.Command)
			}

			// Check if snapshot should be taken
			if b.snapMgr != nil {
				if b.snapMgr.MaybeTrigger(entry.Index, entry.Term) {
					b.logger.Info().Uint64("index", entry.Index).Uint64("term", entry.Term).Msg("snapshot taken")
					if b.snapNode != nil {
						b.snapNode.CompactLog(entry.Index)
					}
				}
			}
		}
	}
}

// notifyOnApply extracts bucket/key from a committed command and calls the callback.
func (b *DistributedBackend) notifyOnApply(raw []byte) {
	cmd, err := DecodeCommand(raw)
	if err != nil {
		return
	}

	var bucket, key string
	switch cmd.Type {
	case CmdPutObjectMeta:
		c, err := decodePutObjectMetaCmd(cmd.Data)
		if err == nil {
			bucket, key = c.Bucket, c.Key
		}
	case CmdDeleteObject:
		c, err := decodeDeleteObjectCmd(cmd.Data)
		if err == nil {
			bucket, key = c.Bucket, c.Key
		}
	case CmdCompleteMultipart:
		c, err := decodeCompleteMultipartCmd(cmd.Data)
		if err == nil {
			bucket, key = c.Bucket, c.Key
		}
	default:
		// Other commands don't affect object cache
		bucket = ""
	}

	if bucket != "" {
		// Invalidate all registered caches (VFS, NFS, etc.)
		b.registry.InvalidateAll(bucket, key)

		// Call legacy callback for CachedBackend
		if b.onApply != nil {
			b.onApply(cmd.Type, bucket, key)
		}
	}
}

// forwardPropose는 팔로워에서 리더로 propose 요청을 QUIC RPC로 전달한다.
// 응답 형식: [8B index big-endian][4B errLen big-endian][errBytes...]
func (b *DistributedBackend) forwardPropose(ctx context.Context, leaderAddr string, data []byte) (uint64, error) {
	if b.shardSvc == nil {
		return 0, fmt.Errorf("forwardPropose: no transport available")
	}
	resp, err := b.shardSvc.SendRequest(ctx, leaderAddr, &transport.Message{
		Type:    transport.StreamProposeForward,
		Payload: data,
	})
	if err != nil {
		return 0, fmt.Errorf("forwardPropose: send: %w", err)
	}
	if len(resp.Payload) < 12 {
		return 0, fmt.Errorf("forwardPropose: response too short: %d bytes", len(resp.Payload))
	}
	index := binary.BigEndian.Uint64(resp.Payload[0:8])
	errLen := binary.BigEndian.Uint32(resp.Payload[8:12])
	if errLen > 0 && len(resp.Payload) >= 12+int(errLen) {
		return 0, fmt.Errorf("forwardPropose: leader error: %s", string(resp.Payload[12:12+int(errLen)]))
	}
	return index, nil
}

// RegisterProposeForwardHandler는 StreamProposeForward 핸들러를 QUIC 라우터에 등록한다.
// 리더 노드에서 호출해야 하며, 팔로워의 propose를 대신 처리한다.
func (b *DistributedBackend) RegisterProposeForwardHandler() {
	if b.shardSvc == nil {
		return
	}
	b.shardSvc.RegisterHandler(transport.StreamProposeForward, func(req *transport.Message) *transport.Message {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		idx, err := b.node.ProposeWait(ctx, req.Payload)
		resp := make([]byte, 12)
		if err != nil {
			errBytes := []byte(err.Error())
			binary.BigEndian.PutUint64(resp[0:8], 0)
			binary.BigEndian.PutUint32(resp[8:12], uint32(len(errBytes)))
			resp = append(resp, errBytes...)
		} else {
			binary.BigEndian.PutUint64(resp[0:8], idx)
			binary.BigEndian.PutUint32(resp[8:12], 0)
		}
		return &transport.Message{Type: transport.StreamProposeForward, Payload: resp}
	})
}

func (b *DistributedBackend) propose(ctx context.Context, cmdType CommandType, payload any) error {
	data, err := EncodeCommand(cmdType, payload)
	if err != nil {
		return fmt.Errorf("encode command: %w", err)
	}

	if b.node.IsLeader() {
		proposeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		idx, err := b.node.ProposeWait(proposeCtx, data)
		if err != nil {
			return err
		}
		for b.lastApplied.Load() < idx {
			select {
			case <-proposeCtx.Done():
				return proposeCtx.Err()
			default:
				time.Sleep(time.Millisecond)
			}
		}
		return nil
	}

	// 팔로워: 리더에게 포워딩
	leaderID := b.node.LeaderID()
	if leaderID == "" {
		return raft.ErrNotLeader
	}
	proposeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = b.forwardPropose(proposeCtx, leaderID, data)
	return err
}

// Close closes the metadata database.
func (b *DistributedBackend) Close() error {
	return b.db.Close()
}

// GetRegistry returns the cache invalidator registry for registering VFS instances.
func (b *DistributedBackend) GetRegistry() *Registry {
	return b.registry
}

// SetBucketAssigner injects the MetaRaft proposer for bucket assignment persistence.
// Must be called before CreateBucket. Nil disables persistence (single-node legacy mode).
func (b *DistributedBackend) SetBucketAssigner(a BucketAssigner) { b.assigner = a }

// SetRouter wires a Router for bucket→group routing used by CreateBucket.
func (b *DistributedBackend) SetRouter(r *Router) { b.router = r }

// --- Bucket operations ---

func (b *DistributedBackend) CreateBucket(bucket string) error {
	// Check if already exists (read local)
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey(bucket))
		return err
	})
	if err == nil {
		return storage.ErrBucketAlreadyExists
	}
	if err != badger.ErrKeyNotFound {
		return err
	}

	if err := os.MkdirAll(b.bucketDir(bucket), 0o755); err != nil {
		return fmt.Errorf("create bucket dir: %w", err)
	}

	// PR-D: persist bucket→group assignment in meta-Raft before data-Raft create.
	// assigner nil means single-node or not-yet-wired (legacy skip).
	// If ProposeBucketAssignment succeeds but b.propose(CmdCreateBucket) below fails,
	// the assignment is durable but the bucket key won't exist yet. A retry will
	// re-propose (idempotent overwrite) and re-create — safe by design.
	if b.assigner != nil {
		if b.router == nil {
			return fmt.Errorf("create bucket %q: router not configured", bucket)
		}
		g, routeErr := b.router.RouteKey(bucket, "")
		if routeErr != nil {
			return fmt.Errorf("route bucket %q: %w", bucket, routeErr)
		}
		if propErr := b.assigner.ProposeBucketAssignment(context.Background(), bucket, g.ID()); propErr != nil {
			return fmt.Errorf("propose bucket assignment: %w", propErr)
		}
	}

	return b.propose(context.Background(), CmdCreateBucket, CreateBucketCmd{Bucket: bucket})
}

func (b *DistributedBackend) HeadBucket(bucket string) error {
	return b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey(bucket))
		if err == badger.ErrKeyNotFound {
			return storage.ErrBucketNotFound
		}
		return err
	})
}

func (b *DistributedBackend) DeleteBucket(bucket string) error {
	// Check existence and emptiness
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(bucketKey(bucket))
		if err == badger.ErrKeyNotFound {
			return storage.ErrBucketNotFound
		}
		if err != nil {
			return err
		}

		prefix := []byte("obj:" + bucket + "/")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(prefix)
		if it.ValidForPrefix(prefix) {
			return storage.ErrBucketNotEmpty
		}
		return nil
	})
	if err != nil {
		return err
	}

	if err := os.RemoveAll(b.bucketDir(bucket)); err != nil {
		return fmt.Errorf("remove bucket dir: %w", err)
	}

	return b.propose(context.Background(), CmdDeleteBucket, DeleteBucketCmd{Bucket: bucket})
}

// SetBucketVersioning satisfies server.BucketVersioner. Replicates the
// versioning state change through Raft so all cluster nodes apply it atomically.
func (b *DistributedBackend) SetBucketVersioning(bucket, state string) error {
	// Pre-check: verify bucket exists locally before proposing. The FSM also
	// checks, but propose() does not propagate FSM errors back to the caller.
	if err := b.HeadBucket(bucket); err != nil {
		return err
	}
	return b.propose(context.Background(), CmdSetBucketVersioning, SetBucketVersioningCmd{
		Bucket: bucket,
		State:  state,
	})
}

// SetObjectACL satisfies storage.ACLSetter. Replicates the ACL change through
// Raft and updates the stored objectMeta on every node.
func (b *DistributedBackend) SetObjectACL(bucket, key string, acl uint8) error {
	// Pre-check: verify object exists locally before proposing.
	if _, err := b.HeadObject(bucket, key); err != nil {
		return err
	}
	return b.propose(context.Background(), CmdSetObjectACL, SetObjectACLCmd{
		Bucket: bucket,
		Key:    key,
		ACL:    acl,
	})
}

// GetBucketVersioning satisfies server.BucketVersioner. Returns "Unversioned"
// when no state has been set so the S3 semantic matches ECBackend's default.
func (b *DistributedBackend) GetBucketVersioning(bucket string) (string, error) {
	var state string
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("bucketver:" + bucket))
		if err == badger.ErrKeyNotFound {
			state = "Unversioned"
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			state = string(v)
			return nil
		})
	})
	return state, err
}

func (b *DistributedBackend) ListBuckets() ([]string, error) {
	var buckets []string
	err := b.db.View(func(txn *badger.Txn) error {
		prefix := []byte("bucket:")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := string(it.Item().Key())
			name := strings.TrimPrefix(key, "bucket:")
			buckets = append(buckets, name)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(buckets)
	return buckets, nil
}

// --- Object operations ---

func (b *DistributedBackend) PutObject(bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	// Read all data into memory for replication (or EC split).
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read object data: %w", err)
	}

	// VFS internal buckets ("__grainfs_vfs_*") are owned exclusively by the
	// VFS layer; multi-versioning has no meaning there and accumulates disk
	// usage proportional to NFS WRITE RPC count (see docs/superpowers/specs/
	// 2026-04-28-vfs-write-amp-design.md). Use a fixed versionID so the on-
	// disk path is overwritten in place. EC is also disabled for these
	// buckets: a fixed versionID combined with EC's RingVersion-keyed shard
	// placement would leak stale shards on ring topology changes.
	versionID := newVersionID()
	useEC := b.ecConfig.IsActive(len(b.liveNodes())) && b.shardSvc != nil
	if storage.IsVFSBucket(bucket) && b.vfsFixedVersion.Load() {
		versionID = "current"
		useEC = false
	}

	if useEC {
		return b.putObjectEC(bucket, key, versionID, data, contentType)
	}
	return b.putObjectNx(bucket, key, versionID, data, contentType)
}

// putObjectNx is the legacy N× full-replication path. Every peer receives
// a copy of the full object at shardIdx=0. Preserved for small clusters
// (< k+m) and for backward compatibility with pre-Phase-18 deployments.
//
// Version-addressable storage: the local full-object file goes under
// {root}/data/{bucket}/{key}/.v/{versionID} so prior versions coexist with the
// latest. Peer replicas are addressed via ShardService with key+"/"+versionID
// so ShardService's API stays frozen (ShardService sees a longer "key").
// clusterTraceEnabled activates per-stage putObjectNx latency logging.
// Enable with GRAINFS_VOLUME_TRACE=1.
var clusterTraceEnabled = os.Getenv("GRAINFS_VOLUME_TRACE") == "1"

func (b *DistributedBackend) putObjectNx(bucket, key, versionID string, data []byte, contentType string) (*storage.Object, error) {
	var tStart, tStage time.Time
	if clusterTraceEnabled {
		tStart = time.Now()
		tStage = tStart
	}

	// Write data locally at the versioned path.
	objPath := b.objectPathV(bucket, key, versionID)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return nil, fmt.Errorf("create object dir: %w", err)
	}
	if err := writeFileAtomic(objPath, data); err != nil {
		return nil, fmt.Errorf("write object: %w", err)
	}

	if clusterTraceEnabled {
		log.Debug().Dur("write_file_atomic", time.Since(tStage)).Str("bucket", bucket).Int("bytes", len(data)).Msg("putObjectNx trace")
		tStage = time.Now()
	}

	// Replicate data to healthy peer nodes via ShardService. We encode the
	// version into the ShardService "key" parameter (key+"/"+versionID) so the
	// ShardService signature stays unchanged for this slice.
	shardKey := key + "/" + versionID
	if b.shardSvc != nil {
		ctx := context.Background()
		for _, peer := range b.liveNodes() {
			if peer == b.selfAddr {
				continue
			}
			if b.peerHealth != nil && !b.peerHealth.IsHealthy(peer) {
				b.logger.Debug().Str("peer", peer).Msg("skipping unhealthy peer for replication")
				continue
			}
			if err := b.shardSvc.WriteShard(ctx, peer, bucket, shardKey, 0, data); err != nil {
				b.logger.Warn().Str("peer", peer).Str("bucket", bucket).Str("key", key).Err(err).Msg("data replication failed")
				if b.peerHealth != nil {
					b.peerHealth.MarkUnhealthy(peer)
				}
			} else if b.peerHealth != nil {
				b.peerHealth.MarkHealthy(peer)
			}
		}
	}

	var etag string
	if !storage.IsInternalBucket(bucket) {
		h := md5.Sum(data)
		etag = hex.EncodeToString(h[:])
	}
	now := time.Now().Unix()

	// Replicate metadata through Raft
	err := b.propose(context.Background(), CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      bucket,
		Key:         key,
		Size:        int64(len(data)),
		ContentType: contentType,
		ETag:        etag,
		ModTime:     now,
		VersionID:   versionID,
	})
	if err != nil {
		os.Remove(objPath)
		return nil, err
	}

	if clusterTraceEnabled {
		log.Debug().Dur("raft_propose", time.Since(tStage)).Dur("total", time.Since(tStart)).Str("bucket", bucket).Msg("putObjectNx trace")
	}

	return &storage.Object{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
		VersionID:    versionID,
	}, nil
}

// PutObjectAsync is the write-back variant of PutObject.
// It writes data locally and replicates to peers (fast path ~0.3ms), then
// returns a commitFn that defers the Raft metadata proposal (~2ms).
// On flush the caller runs all commitFns concurrently so the Raft batcher
// coalesces them into a single fdatasync.
func (b *DistributedBackend) PutObjectAsync(bucket, key string, r io.Reader, contentType string) (*storage.Object, func() error, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, nil, err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, fmt.Errorf("read object data: %w", err)
	}
	versionID := newVersionID()
	useEC := b.ecConfig.IsActive(len(b.liveNodes())) && b.shardSvc != nil
	if storage.IsVFSBucket(bucket) && b.vfsFixedVersion.Load() {
		versionID = "current"
		useEC = false
	}
	if useEC {
		obj, err := b.putObjectEC(bucket, key, versionID, data, contentType)
		return obj, func() error { return nil }, err
	}
	return b.putObjectNxAsync(bucket, key, versionID, data, contentType)
}

// putObjectNxAsync splits putObjectNx into fast (local write + peer replication)
// and slow (Raft propose) parts. The caller must invoke commitFn before flush.
func (b *DistributedBackend) putObjectNxAsync(bucket, key, versionID string, data []byte, contentType string) (*storage.Object, func() error, error) {
	t0 := time.Now()
	objPath := b.objectPathV(bucket, key, versionID)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return nil, nil, fmt.Errorf("create object dir: %w", err)
	}
	if err := writeFileAtomic(objPath, data); err != nil {
		return nil, nil, fmt.Errorf("write object: %w", err)
	}
	wfaDur := time.Since(t0)

	shardKey := key + "/" + versionID
	if b.shardSvc != nil {
		ctx := context.Background()
		for _, peer := range b.liveNodes() {
			if peer == b.selfAddr {
				continue
			}
			if b.peerHealth != nil && !b.peerHealth.IsHealthy(peer) {
				continue
			}
			if err := b.shardSvc.WriteShard(ctx, peer, bucket, shardKey, 0, data); err != nil {
				b.logger.Warn().Str("peer", peer).Str("bucket", bucket).Str("key", key).Err(err).Msg("data replication failed")
				if b.peerHealth != nil {
					b.peerHealth.MarkUnhealthy(peer)
				}
			} else if b.peerHealth != nil {
				b.peerHealth.MarkHealthy(peer)
			}
		}
	}

	var etag string
	if !storage.IsInternalBucket(bucket) {
		h := md5.Sum(data)
		etag = hex.EncodeToString(h[:])
	}
	now := time.Now().Unix()

	obj := &storage.Object{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
		VersionID:    versionID,
	}
	if os.Getenv("GRAINFS_VOLUME_TRACE") == "1" {
		b.logger.Debug().
			Str("bucket", bucket).Str("key", key).
			Dur("write_file_atomic", wfaDur).
			Msg("putObjectNxAsync trace")
	}
	commitFn := func() error {
		t1 := time.Now()
		err := b.propose(context.Background(), CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket:      bucket,
			Key:         key,
			Size:        int64(len(data)),
			ContentType: contentType,
			ETag:        etag,
			ModTime:     now,
			VersionID:   versionID,
		})
		if err != nil {
			os.Remove(objPath)
			return err
		}
		if os.Getenv("GRAINFS_VOLUME_TRACE") == "1" {
			b.logger.Debug().
				Str("bucket", bucket).Str("key", key).
				Dur("raft_propose", time.Since(t1)).
				Msg("putObjectNxAsync commit trace")
		}
		return nil
	}
	return obj, commitFn, nil
}

// putObjectEC is the Phase 18 Cluster EC path: Reed-Solomon split into
// cfg.NumShards() shards, fan-out each to its placed node (self or peer),
// then commit metadata (with RingVersion) through Raft.
//
// Consistency: write-all. Any shard write failure → cleanup + error.
// Placement is derived deterministically from the ring (if available) or
// via PlacementForNodes (legacy). The RingVersion is stored in object metadata
// so reads can recompute the same placement without a separate Raft record.
func (b *DistributedBackend) putObjectEC(bucket, key, versionID string, data []byte, contentType string) (*storage.Object, error) {
	ctx := context.Background()

	liveNodes := b.liveNodes()
	effectiveCfg := EffectiveConfig(len(liveNodes), b.ecConfig)
	shards, err := ECSplit(effectiveCfg, data)
	if err != nil {
		return nil, fmt.Errorf("ec split: %w", err)
	}

	// ShardService's key parameter carries the versionID as a suffix so shards
	// for different versions land at different paths without changing the API.
	shardKey := key + "/" + versionID

	// 링이 있으면 결정론적 배치 사용, 없으면 기존 PlacementForNodes 사용
	var placement []string
	var ringVer RingVersion
	if currentRing, ringErr := b.fsm.GetRingStore().GetCurrentRing(); ringErr == nil {
		placement = currentRing.PlacementForKey(effectiveCfg, shardKey)
		ringVer = currentRing.Version
	} else {
		placement = PlacementForNodes(effectiveCfg, liveNodes, shardKey)
	}
	if len(placement) != effectiveCfg.NumShards() {
		return nil, fmt.Errorf("putObjectEC: placement has %d nodes, need %d (k=%d m=%d)",
			len(placement), effectiveCfg.NumShards(), effectiveCfg.DataShards, effectiveCfg.ParityShards)
	}
	selfID := b.selfAddr

	// Track nodes we wrote to so cleanup can target them precisely.
	// writtenMu: concurrent goroutines append to written simultaneously.
	var (
		writtenMu sync.Mutex
		written   []string
	)
	cleanup := func() {
		// Called after g.Wait() — single goroutine, no mutex needed here.
		for _, n := range written {
			if n == selfID {
				_ = b.shardSvc.DeleteLocalShards(bucket, shardKey)
				continue
			}
			_ = b.shardSvc.DeleteShards(ctx, n, bucket, shardKey)
		}
	}

	// Fan-out: write all shards in parallel. Write-all consistency.
	// Total latency = max(per-shard latency) instead of Σ(per-shard latency).
	// Each remote write gets a 3s deadline so a dead peer fails fast.
	g, gctx := errgroup.WithContext(ctx)
	for i, node := range placement {
		i, node := i, node
		g.Go(func() error {
			var werr error
			if node == selfID {
				werr = b.shardSvc.WriteLocalShard(bucket, shardKey, i, shards[i])
			} else {
				writeCtx, writeCancel := context.WithTimeout(gctx, shardRPCTimeout)
				defer writeCancel()
				werr = b.shardSvc.WriteShard(writeCtx, node, bucket, shardKey, i, shards[i])
				if b.peerHealth != nil {
					if werr != nil {
						b.peerHealth.MarkUnhealthy(node)
					} else {
						b.peerHealth.MarkHealthy(node)
					}
				}
			}
			if werr != nil {
				return fmt.Errorf("ec write shard %d to %s: %w", i, node, werr)
			}
			writtenMu.Lock()
			written = append(written, node)
			writtenMu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		cleanup()
		return nil, err
	}

	h := md5.Sum(data)
	etag := hex.EncodeToString(h[:])
	now := time.Now().Unix()

	// Commit metadata. RingVersion + ECData/ECParity + NodeIDs stored so reads
	// can reconstruct shards without a separate placement record (NodeIDs fallback
	// is used when RingVersion==0 and no placement record exists).
	// On failure, best-effort cleanup of orphaned shards.
	if merr := b.propose(ctx, CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      bucket,
		Key:         key,
		Size:        int64(len(data)),
		ContentType: contentType,
		ETag:        etag,
		ModTime:     now,
		VersionID:   versionID,
		RingVersion: ringVer,
		ECData:      uint8(effectiveCfg.DataShards),
		ECParity:    uint8(effectiveCfg.ParityShards),
		NodeIDs:     placement,
	}); merr != nil {
		go b.deleteShardsAsync(bucket, placement, shardKey)
		return nil, merr
	}

	return &storage.Object{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
		VersionID:    versionID,
	}, nil
}

// deleteShardsAsync는 propose 실패 시 고아 샤드를 백그라운드에서 삭제한다.
// best-effort: 실패는 무시하고 scrubber fallback에 위임한다.
func (b *DistributedBackend) deleteShardsAsync(bucket string, placement []string, shardKey string) {
	// Drop any cached entries for this shardKey before/after the disk
	// delete. Reads after this point must miss the cache so they can
	// learn the object is gone (or at least re-fetch fresh placement).
	b.invalidateShardCache(bucket, shardKey, len(placement))
	for _, node := range placement {
		if node == b.selfAddr {
			_ = b.shardSvc.DeleteLocalShards(bucket, shardKey)
		} else {
			_ = b.shardSvc.DeleteShards(context.Background(), node, bucket, shardKey)
		}
	}
}

func (b *DistributedBackend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	obj, err := b.HeadObject(bucket, key)
	if err != nil {
		return nil, nil, err
	}
	// HeadObject already rejects tombstones with ErrObjectNotFound, so obj here
	// is a real version. VersionID is non-empty for versioned writes and empty
	// for legacy log replay.

	// EC path: shardKey = key+"/"+versionID for versioned objects.
	shardKey := key
	if obj.VersionID != "" {
		shardKey = key + "/" + obj.VersionID
	}

	if b.shardSvc != nil {
		// 신 경로: 오브젝트 메타에서 RingVersion을 읽어 배치를 재계산한다.
		var metaRingVersion RingVersion
		var metaECData, metaECParity uint8
		var metaNodeIDs []string
		if viewErr := b.db.View(func(txn *badger.Txn) error {
			var dbKey []byte
			if obj.VersionID == "" {
				dbKey = objectMetaKey(bucket, obj.Key)
			} else {
				dbKey = objectMetaKeyV(bucket, obj.Key, obj.VersionID)
			}
			item, err := txn.Get(dbKey)
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				m, err := unmarshalObjectMeta(val)
				if err != nil {
					return err
				}
				metaRingVersion = RingVersion(m.RingVersion)
				metaECData = m.ECData
				metaECParity = m.ECParity
				metaNodeIDs = m.NodeIDs
				return nil
			})
		}); viewErr != nil && viewErr != badger.ErrKeyNotFound {
			b.logger.Warn().Str("bucket", bucket).Str("key", key).Err(viewErr).Msg("GetObject: failed to read EC metadata, falling back")
		}

		if metaRingVersion > 0 {
			if ring, ringErr := b.fsm.GetRingStore().GetRing(metaRingVersion); ringErr == nil {
				cfg := ECConfig{DataShards: int(metaECData), ParityShards: int(metaECParity)}
				placement := ring.PlacementForKey(cfg, shardKey)
				rec := PlacementRecord{Nodes: placement, K: int(metaECData), M: int(metaECParity)}
				data, ecErr := b.getObjectEC(context.Background(), bucket, key, obj.VersionID, rec)
				if ecErr == nil {
					return io.NopCloser(bytes.NewReader(data)), obj, nil
				}
				b.logger.Warn().Str("bucket", bucket).Str("key", key).Err(ecErr).Msg("ring-based ec reconstruct failed, falling back")
			}
		}

		// 구 경로 (RingVersion==0): LookupShardPlacement fallback
		if ecRec, lookupErr := b.fsm.LookupShardPlacement(bucket, shardKey); lookupErr == nil && len(ecRec.Nodes) > 0 {
			data, ecErr := b.getObjectEC(context.Background(), bucket, key, obj.VersionID, ecRec)
			if ecErr == nil {
				return io.NopCloser(bytes.NewReader(data)), obj, nil
			}
			b.logger.Warn().Str("bucket", bucket).Str("key", key).Err(ecErr).Msg("ec reconstruct failed, trying NodeIDs-from-metadata path")
		}

		// NodeIDs-from-metadata fallback: placement stored in PutObjectMetaCmd
		// when CmdPutShardPlacement is a no-op (no ring yet).
		if metaECData > 0 && len(metaNodeIDs) > 0 {
			rec := PlacementRecord{Nodes: metaNodeIDs, K: int(metaECData), M: int(metaECParity)}
			data, ecErr := b.getObjectEC(context.Background(), bucket, key, obj.VersionID, rec)
			if ecErr == nil {
				return io.NopCloser(bytes.NewReader(data)), obj, nil
			}
			b.logger.Warn().Str("bucket", bucket).Str("key", key).Err(ecErr).Msg("NodeIDs-from-metadata ec reconstruct failed, falling back to N× path")
		}
	}

	// Try the version-addressable local path first (new writers), then the
	// legacy unversioned path (pre-versioning replay).
	if obj.VersionID != "" {
		if f, oerr := os.Open(b.objectPathV(bucket, key, obj.VersionID)); oerr == nil {
			return f, obj, nil
		}
	}
	f, err := os.Open(b.objectPath(bucket, key))
	if err == nil {
		return f, obj, nil
	}

	// Local file not found — try fetching from peer nodes (healthy first, then all).
	// Peers store under shardKey (key+"/"+versionID) when the write was versioned.
	if b.shardSvc != nil && os.IsNotExist(err) {
		ctx := context.Background()
		// Try healthy peers first
		for _, peer := range b.liveNodes() {
			if peer == b.selfAddr {
				continue
			}
			if b.peerHealth != nil && !b.peerHealth.IsHealthy(peer) {
				continue
			}
			data, fetchErr := b.shardSvc.ReadShard(ctx, peer, bucket, shardKey, 0)
			if fetchErr == nil && data != nil {
				if b.peerHealth != nil {
					b.peerHealth.MarkHealthy(peer)
				}
				return io.NopCloser(bytes.NewReader(data)), obj, nil
			}
			if fetchErr != nil && b.peerHealth != nil {
				b.peerHealth.MarkUnhealthy(peer)
			}
		}
		// Fallback: try unhealthy peers (they may have recovered)
		if b.peerHealth != nil {
			for _, peer := range b.liveNodes() {
				if peer == b.selfAddr {
					continue
				}
				if b.peerHealth.IsHealthy(peer) {
					continue // already tried
				}
				data, fetchErr := b.shardSvc.ReadShard(ctx, peer, bucket, shardKey, 0)
				if fetchErr == nil && data != nil {
					b.peerHealth.MarkHealthy(peer)
					return io.NopCloser(bytes.NewReader(data)), obj, nil
				}
			}
		}
	}

	return nil, nil, fmt.Errorf("open object: %w", err)
}

// RepairShard rebuilds a single missing shard by reading the other shards from
// the cluster and writing the reconstructed shardIdx back to its placement
// node. Phase 18 Slice 6: the primitive that ShardPlacementMonitor.onMissing
// plugs into, and that an admin endpoint can trigger on demand.
//
// Preconditions: the object must already have a placement record (created by
// putObjectEC or ConvertObjectToEC). shardIdx must be in [0, k+m). At least k
// of the other shards must be readable or reconstruction fails.
//
// versionID identifies the physical shard files on disk: putObjectEC writes
// shards under `{key}/{versionID}/shard_{N}` via ShardService, and this
// routine must read/write the same layout. When versionID is empty, the
// latest pointer from the FSM is consulted.
//
// Write target: the repaired shard goes back to placement[shardIdx]. When
// that node is this node, we use WriteLocalShard; otherwise WriteShard.
func (b *DistributedBackend) RepairShard(ctx context.Context, bucket, key, versionID string, shardIdx int) error {
	if b.shardSvc == nil {
		return fmt.Errorf("shard service not configured")
	}
	// Resolve to the latest version when caller doesn't know it (monitor
	// callback path). Empty latest means pre-versioned legacy EC; fall back
	// to bare-key layout, preserving pre-Slice-3 behaviour.
	if versionID == "" {
		latest, lerr := b.fsm.LookupLatestVersion(bucket, key)
		if lerr != nil {
			return fmt.Errorf("resolve version for repair %s/%s: %w", bucket, key, lerr)
		}
		versionID = latest
	}
	// Placement must be looked up AFTER resolving versionID so shardKey
	// matches the key written by putObjectEC (key+"/"+versionID).
	shardKey := key
	if versionID != "" {
		shardKey = key + "/" + versionID
	}

	ecRec, lookupErr := b.lookupPlacementWithFallback(bucket, shardKey, versionID)
	if lookupErr != nil {
		return fmt.Errorf("lookup shard placement: %w", lookupErr)
	}
	if len(ecRec.Nodes) == 0 {
		return fmt.Errorf("no placement for %s/%s — object is not EC-managed", bucket, key)
	}
	recCfg := ecRec.ECConfigOrFallback(b.ecConfig)
	if shardIdx < 0 || shardIdx >= len(ecRec.Nodes) {
		return fmt.Errorf("shardIdx %d out of range [0,%d)", shardIdx, len(ecRec.Nodes))
	}
	if len(ecRec.Nodes) != recCfg.NumShards() {
		return fmt.Errorf("placement length %d != k+m %d", len(ecRec.Nodes), recCfg.NumShards())
	}

	selfID := b.selfAddr
	shards := make([][]byte, len(ecRec.Nodes))
	available := 0

	// Pull every OTHER shard. We intentionally skip shardIdx to avoid pulling
	// the corrupt/missing copy into the reconstruction.
	for i, node := range ecRec.Nodes {
		if i == shardIdx {
			continue
		}
		var data []byte
		var rerr error
		if node == selfID {
			data, rerr = b.shardSvc.ReadLocalShard(bucket, shardKey, i)
		} else {
			data, rerr = b.shardSvc.ReadShard(ctx, node, bucket, shardKey, i)
		}
		if rerr == nil && data != nil {
			shards[i] = data
			available++
		}
	}
	if available < recCfg.DataShards {
		return fmt.Errorf("repair: only %d/%d other shards readable, need %d",
			available, len(ecRec.Nodes)-1, recCfg.DataShards)
	}

	// ECReconstruct rebuilds the whole object; we then re-split to get the
	// canonical byte layout of each shard (including the missing one).
	data, rerr := ECReconstruct(recCfg, shards)
	if rerr != nil {
		return fmt.Errorf("repair reconstruct: %w", rerr)
	}
	freshShards, serr := ECSplit(recCfg, data)
	if serr != nil {
		return fmt.Errorf("repair re-split: %w", serr)
	}

	// Write just the missing shard back to its placement node.
	target := ecRec.Nodes[shardIdx]
	var werr error
	if target == selfID {
		werr = b.shardSvc.WriteLocalShard(bucket, shardKey, shardIdx, freshShards[shardIdx])
	} else {
		werr = b.shardSvc.WriteShard(ctx, target, bucket, shardKey, shardIdx, freshShards[shardIdx])
	}
	if werr == nil && b.shardCache != nil {
		// Repaired shard bytes may differ from any cached copy of the
		// corrupted slot. Drop the cache entry so subsequent reads pull
		// fresh data — repaint > stale.
		b.shardCache.Invalidate(shardCacheKey(bucket, shardKey, shardIdx))
	}
	return werr
}

// FSMRef returns the underlying FSM so reshard / monitor code can iterate
// placements + object metas without reaching through the backend's private fields.
func (b *DistributedBackend) FSMRef() *FSM { return b.fsm }

// FSMDB returns the underlying FSM BadgerDB handle.
// Used by lifecycle.NewStore and other components needing shared metadata storage.
// Lifecycle keys ("lifecycle:{bucket}") share the DB with FSM keys ("obj:", "lat:",
// "bucket:", etc.); the prefixes are disjoint so they coexist safely.
func (b *DistributedBackend) FSMDB() *badger.DB { return b.db }

// LiveNodes returns the list of cluster nodes currently considered reachable.
// This is the public counterpart of the internal liveNodes() method.
func (b *DistributedBackend) LiveNodes() []string { return b.liveNodes() }

// ECActive reports whether Phase 18 cluster EC will be applied to the next
// PutObject call (EC enabled + enough nodes for k+m split).
func (b *DistributedBackend) ECActive() bool { return b.ecConfig.IsActive(len(b.liveNodes())) }

// EffectiveECConfig returns the ECConfig proportionally scaled to the current
// cluster size. Used by ReshardManager to determine the target k,m for upgrades.
func (b *DistributedBackend) EffectiveECConfig() ECConfig {
	return EffectiveConfig(len(b.liveNodes()), b.ecConfig)
}

// CurrentRingVersion returns the version of the current ring (0 if none).
func (b *DistributedBackend) CurrentRingVersion() RingVersion {
	ring, err := b.fsm.GetRingStore().GetCurrentRing()
	if err != nil {
		return 0
	}
	return ring.Version
}

// ReshardToRing reshards an object from oldRingVer's placement to the current
// ring's placement. It reconstructs the object data from the old layout and
// re-fans it out using putObjectEC (which will use the current ring).
func (b *DistributedBackend) ReshardToRing(ctx context.Context, bucket, key string, oldRingVer RingVersion) error {
	obj, err := b.HeadObject(bucket, key)
	if err != nil {
		return err
	}
	shardKey := key
	if obj.VersionID != "" {
		shardKey = key + "/" + obj.VersionID
	}

	currentRing, err := b.fsm.GetRingStore().GetCurrentRing()
	if err != nil {
		return fmt.Errorf("reshard: no current ring: %w", err)
	}
	if currentRing.Version == oldRingVer {
		return nil // already up to date
	}

	cfg := EffectiveConfig(len(b.liveNodes()), b.ecConfig)

	var oldData []byte
	if oldRingVer > 0 {
		oldRing, rerr := b.fsm.GetRingStore().GetRing(oldRingVer)
		if rerr != nil {
			return fmt.Errorf("reshard: old ring %d not found: %w", oldRingVer, rerr)
		}
		oldPlacement := oldRing.PlacementForKey(cfg, shardKey)
		rec := PlacementRecord{Nodes: oldPlacement, K: cfg.DataShards, M: cfg.ParityShards}
		oldData, err = b.getObjectEC(ctx, bucket, key, obj.VersionID, rec)
		if err != nil {
			return fmt.Errorf("reshard: reconstruct from ring %d: %w", oldRingVer, err)
		}
	} else {
		// RingVersion==0: fall back to placement record
		ecRec, lerr := b.fsm.LookupShardPlacement(bucket, shardKey)
		if lerr != nil || len(ecRec.Nodes) == 0 {
			return fmt.Errorf("reshard: no placement for ring-v0 object %s/%s", bucket, key)
		}
		oldData, err = b.getObjectEC(ctx, bucket, key, obj.VersionID, ecRec)
		if err != nil {
			return fmt.Errorf("reshard: reconstruct: %w", err)
		}
	}

	// EC 디코딩 결과가 원본과 일치하는지 검증 (Reed-Solomon은 무손실이어야 함).
	h := md5.Sum(oldData)
	if computedETag := hex.EncodeToString(h[:]); computedETag != obj.ETag {
		return fmt.Errorf("reshard: ETag mismatch after EC reconstruction for %s/%s: got %s, want %s",
			bucket, key, computedETag, obj.ETag)
	}

	_, err = b.putObjectEC(bucket, key, obj.VersionID, oldData, obj.ContentType)
	return err
}

// ConvertObjectToEC migrates an existing N×-replicated object to Phase 18
// EC placement. Used by the background re-placement manager (Slice 5).
// Idempotent: if the object already has a placement record, returns nil
// immediately. If the object meta changes mid-conversion (detected via ETag),
// rolls back and returns a retry-able error.
//
// Consistency: etag-check-before-commit. PUT races that land between read and
// commit overwrite both the N× copy and (post-commit) the EC shards, so the
// last writer wins per normal PUT semantics.
func (b *DistributedBackend) ConvertObjectToEC(ctx context.Context, bucket, key string) error {
	liveNodes := b.liveNodes()
	effectiveCfg := EffectiveConfig(len(liveNodes), b.ecConfig)
	if !effectiveCfg.IsActive(len(liveNodes)) || b.shardSvc == nil {
		return fmt.Errorf("ec not active: cluster_size=%d shard_svc=%v",
			len(liveNodes), b.shardSvc != nil)
	}
	existing, lookupErr := b.fsm.LookupShardPlacement(bucket, key)
	if lookupErr != nil {
		return fmt.Errorf("lookup shard placement: %w", lookupErr)
	}
	if len(existing.Nodes) > 0 {
		return nil // already converted
	}

	// Snapshot meta before reading data so we can detect concurrent writes.
	metaBefore, err := b.HeadObject(bucket, key)
	if err != nil {
		return fmt.Errorf("head before convert: %w", err)
	}

	// Read the full object via the legacy N× path. GetObject will fall through
	// to local or peer full-replica fetch because placement is still absent.
	rc, _, err := b.GetObject(bucket, key)
	if err != nil {
		return fmt.Errorf("read for convert: %w", err)
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return fmt.Errorf("drain for convert: %w", err)
	}

	// Split + fan-out shards. Mirrors putObjectEC's write-all semantics.
	shards, err := ECSplit(effectiveCfg, data)
	if err != nil {
		return fmt.Errorf("ec split for convert: %w", err)
	}
	// ConvertObjectToEC is a legacy-to-EC migration path for pre-versioned objects,
	// so placement uses bare key (no versionID suffix).
	placement := PlacementForNodes(effectiveCfg, liveNodes, key)
	selfID := b.selfAddr
	written := make([]string, 0, len(shards))
	rollbackShards := func() {
		for _, n := range written {
			if n == selfID {
				_ = b.shardSvc.DeleteLocalShards(bucket, key)
				continue
			}
			_ = b.shardSvc.DeleteShards(ctx, n, bucket, key)
		}
	}
	for i, node := range placement {
		if node == selfID {
			if werr := b.shardSvc.WriteLocalShard(bucket, key, i, shards[i]); werr != nil {
				rollbackShards()
				return fmt.Errorf("convert write local shard %d: %w", i, werr)
			}
		} else {
			if werr := b.shardSvc.WriteShard(ctx, node, bucket, key, i, shards[i]); werr != nil {
				rollbackShards()
				return fmt.Errorf("convert write shard %d to %s: %w", i, node, werr)
			}
		}
		written = append(written, node)
	}

	// Re-check meta: did a PUT race us while we were writing shards?
	metaAfter, err := b.HeadObject(bucket, key)
	if err != nil || metaAfter.ETag != metaBefore.ETag {
		rollbackShards()
		return fmt.Errorf("convert aborted: meta changed mid-conversion (etag %q → %q)",
			metaBefore.ETag, func() string {
				if metaAfter != nil {
					return metaAfter.ETag
				}
				return ""
			}())
	}

	// Commit placement. A concurrent PUT between here and commit will also
	// propose a placement (putObjectEC), and Raft serializes — whoever lands
	// first wins. Idempotent applyPutShardPlacement tolerates either order.
	if perr := b.propose(ctx, CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket:  bucket,
		Key:     key,
		NodeIDs: placement,
		K:       effectiveCfg.DataShards,
		M:       effectiveCfg.ParityShards,
	}); perr != nil {
		rollbackShards()
		return fmt.Errorf("convert propose placement: %w", perr)
	}

	// Cleanup legacy N× replicas on nodes NOT in the placement. The local full-
	// object file is always deleted (whether or not self is a placement node,
	// the full file is now redundant). Best-effort — failures just leave stale
	// N× copies that a future sweep can reclaim.
	_ = os.Remove(b.objectPath(bucket, key))
	placementSet := make(map[string]bool, len(placement))
	for _, n := range placement {
		placementSet[n] = true
	}
	for _, peer := range b.liveNodes() {
		if peer == selfID {
			continue
		}
		if placementSet[peer] {
			continue // this peer legitimately holds a shard now
		}
		// Peer only had the old full-object N× copy at shardIdx=0; DeleteShards
		// wipes the whole <bucket>/<key>/ dir on that peer.
		_ = b.shardSvc.DeleteShards(ctx, peer, bucket, key)
	}
	return nil
}

// getObjectEC reads shards from the placed nodes and reconstructs the object.
// rec.Nodes[i] is the nodeID holding shardIdx i. rec.K and rec.M are the EC
// parameters used when the object was written. Tolerates up to M unreachable nodes.
func (b *DistributedBackend) getObjectEC(ctx context.Context, bucket, key, versionID string, rec PlacementRecord) ([]byte, error) {
	recCfg := rec.ECConfigOrFallback(b.ecConfig)
	if len(rec.Nodes) != recCfg.NumShards() {
		return nil, fmt.Errorf("placement length %d != expected %d", len(rec.Nodes), recCfg.NumShards())
	}
	// putObjectEC writes shards under shardKey = key + "/" + versionID so
	// concurrent versions don't clobber one another on disk. Reads have to
	// target the same path. Empty versionID preserves the pre-Slice-1 layout
	// for log replay of legacy EC objects.
	shardKey := key
	if versionID != "" {
		shardKey = key + "/" + versionID
	}
	// k-of-n fast path: read all shards in parallel, stop once k succeed.
	// cancel() signals remaining goroutines to abort after k shards received.
	// resultCh is buffered(len(nodes)) so goroutines never block on send.
	type shardResult struct {
		idx       int
		data      []byte
		err       error
		fromCache bool
	}

	// Cache pre-pass: try to satisfy from cache first. A full hit means
	// we never touch disk or the network. Partial hit narrows the
	// fan-out to just the missing slots.
	shards := make([][]byte, len(rec.Nodes))
	available := 0
	cached := make([]bool, len(rec.Nodes))
	if b.shardCache != nil {
		for i := range rec.Nodes {
			// readamp records every read intent (cache + miss) so the
			// simulator hit-rate curve stays comparable to runs that
			// disable the real cache.
			readamp.RecordECShard(shardCacheKey(bucket, shardKey, i))
			if data, ok := b.shardCache.Get(shardCacheKey(bucket, shardKey, i)); ok {
				shards[i] = data
				cached[i] = true
				available++
				if available == recCfg.DataShards {
					return ECReconstruct(recCfg, shards)
				}
			}
		}
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	selfID := b.selfAddr
	dispatched := 0
	resultCh := make(chan shardResult, len(rec.Nodes))
	for i, node := range rec.Nodes {
		if cached[i] {
			continue
		}
		i, node := i, node
		// readamp recording was done in the cache pre-pass when the
		// cache is enabled; keep parity for the disabled path so the
		// readamp histogram covers every fetch attempt.
		if b.shardCache == nil {
			readamp.RecordECShard(shardCacheKey(bucket, shardKey, i))
		}
		dispatched++
		go func() {
			var data []byte
			var err error
			if node == selfID {
				data, err = b.shardSvc.ReadLocalShard(bucket, shardKey, i)
			} else {
				if b.peerHealth != nil && !b.peerHealth.IsHealthy(node) {
					resultCh <- shardResult{idx: i, err: fmt.Errorf("node %s unhealthy", node)}
					return
				}
				shardCtx, shardCancel := context.WithTimeout(readCtx, shardRPCTimeout)
				defer shardCancel()
				data, err = b.shardSvc.ReadShard(shardCtx, node, bucket, shardKey, i)
				if b.peerHealth != nil {
					if err != nil {
						if errors.Is(err, context.Canceled) && readCtx.Err() != nil {
							// k-of-n early exit cancelled this shard — not a peer failure
						} else {
							b.peerHealth.MarkUnhealthy(node)
						}
					} else {
						b.peerHealth.MarkHealthy(node)
					}
				}
			}
			resultCh <- shardResult{idx: i, data: data, err: err}
		}()
	}

	// We drain ALL dispatched responses, not just the first k. The
	// extra m responses no longer block reconstruction — cancel()
	// already signaled them to abort — but any that already received
	// bytes before cancel arrived populate the cache. Without this the
	// next read would always miss the m-th shard slot, ceiling the
	// real hit rate at k/(k+m). With it, repeat reads of the same
	// object hit fully and skip the fan-out entirely.
	for r := 0; r < dispatched; r++ {
		res := <-resultCh
		if res.err != nil || res.data == nil {
			continue
		}
		if available < recCfg.DataShards {
			shards[res.idx] = res.data
		}
		if b.shardCache != nil {
			b.shardCache.Put(shardCacheKey(bucket, shardKey, res.idx), res.data)
		}
		available++
		if available == recCfg.DataShards {
			cancel() // signal remaining in-flight goroutines to abort
		}
	}
	if available < recCfg.DataShards {
		return nil, fmt.Errorf("ec get: only %d/%d shards available, need %d",
			available, len(rec.Nodes), recCfg.DataShards)
	}
	return ECReconstruct(recCfg, shards)
}

// upgradeObjectEC re-encodes an EC object from oldRec's (k1,m1) to newCfg's (k2,m2).
// Called by ReshardManager when the cluster grows and the effective EC config changes.
// Sequence: reconstruct with old config → re-encode with new config → fan-out new shards
// → propose updated placement → delete old shards (best-effort).
func (b *DistributedBackend) upgradeObjectEC(ctx context.Context, bucket, key string, oldRec PlacementRecord, newCfg ECConfig) error {
	if b.shardSvc == nil {
		return fmt.Errorf("shard service unavailable")
	}
	oldCfg := oldRec.ECConfigOrFallback(b.ecConfig)

	// Reconstruct original data from old shards.
	data, err := b.getObjectEC(ctx, bucket, key, "", oldRec)
	if err != nil {
		return fmt.Errorf("upgrade reconstruct: %w", err)
	}

	// Re-encode with new config.
	liveNodes := b.liveNodes()
	newShards, err := ECSplit(newCfg, data)
	if err != nil {
		return fmt.Errorf("upgrade re-split: %w", err)
	}
	newPlacement := PlacementForNodes(newCfg, liveNodes, key)
	selfID := b.selfAddr

	var (
		writtenMu sync.Mutex
		written   []string
	)
	cleanup := func() {
		for _, n := range written {
			if n == selfID {
				_ = b.shardSvc.DeleteLocalShards(bucket, key)
			} else {
				_ = b.shardSvc.DeleteShards(ctx, n, bucket, key)
			}
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	for i, node := range newPlacement {
		i, node := i, node
		g.Go(func() error {
			var werr error
			if node == selfID {
				werr = b.shardSvc.WriteLocalShard(bucket, key, i, newShards[i])
			} else {
				writeCtx, writeCancel := context.WithTimeout(gctx, shardRPCTimeout)
				defer writeCancel()
				werr = b.shardSvc.WriteShard(writeCtx, node, bucket, key, i, newShards[i])
				if b.peerHealth != nil {
					if werr != nil {
						b.peerHealth.MarkUnhealthy(node)
					} else {
						b.peerHealth.MarkHealthy(node)
					}
				}
			}
			if werr != nil {
				return fmt.Errorf("upgrade write shard %d to %s: %w", i, node, werr)
			}
			writtenMu.Lock()
			written = append(written, node)
			writtenMu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		cleanup()
		return err
	}

	// Commit updated placement via Raft.
	if perr := b.propose(ctx, CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket:  bucket,
		Key:     key,
		NodeIDs: newPlacement,
		K:       newCfg.DataShards,
		M:       newCfg.ParityShards,
	}); perr != nil {
		cleanup()
		return fmt.Errorf("upgrade propose placement: %w", perr)
	}

	// Best-effort deletion of old shards from nodes no longer in the new placement.
	newSet := make(map[string]struct{}, len(newPlacement))
	for _, n := range newPlacement {
		newSet[n] = struct{}{}
	}
	for _, n := range oldRec.Nodes {
		if _, inNew := newSet[n]; inNew {
			continue
		}
		_ = oldCfg.NumShards() // reference to suppress unused warning
		if n == selfID {
			_ = b.shardSvc.DeleteLocalShards(bucket, key)
		} else {
			_ = b.shardSvc.DeleteShards(ctx, n, bucket, key)
		}
	}
	return nil
}

func (b *DistributedBackend) HeadObject(bucket, key string) (*storage.Object, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	var obj storage.Object
	err := b.db.View(func(txn *badger.Txn) error {
		// Resolve via latest-version pointer when present so callers see the
		// most recent version. Falls back to the legacy single-key read when
		// no lat: pointer exists (e.g., legacy replay).
		metaKeyBytes := objectMetaKey(bucket, key)
		versionID := ""
		if latItem, lerr := txn.Get(latestKey(bucket, key)); lerr == nil {
			_ = latItem.Value(func(v []byte) error {
				versionID = string(v)
				return nil
			})
			if versionID != "" {
				metaKeyBytes = objectMetaKeyV(bucket, key, versionID)
			}
		} else if lerr != badger.ErrKeyNotFound {
			return lerr
		}

		item, err := txn.Get(metaKeyBytes)
		if err == badger.ErrKeyNotFound {
			return storage.ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			// Tombstone markers aren't observable via HeadObject — callers use
			// HeadObjectVersion / ListObjectVersions to see them explicitly.
			if m.ETag == deleteMarkerETag {
				return storage.ErrObjectNotFound
			}
			obj = storage.Object{
				Key:          m.Key,
				Size:         m.Size,
				ContentType:  m.ContentType,
				ETag:         m.ETag,
				LastModified: m.LastModified,
				VersionID:    versionID,
				ACL:          m.ACL,
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

func (b *DistributedBackend) DeleteObject(bucket, key string) error {
	_, err := b.DeleteObjectReturningMarker(bucket, key)
	return err
}

// DeleteObjectReturningMarker satisfies server.VersionedSoftDeleter. Same
// tombstone semantics as DeleteObject but returns the delete marker's
// VersionID so the S3 handler can surface it in the response header.
func (b *DistributedBackend) DeleteObjectReturningMarker(bucket, key string) (string, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return "", err
	}

	// Tombstone semantics: DeleteObject creates a delete marker as a new
	// version. Prior version data remains addressable via GetObjectVersion and
	// is NOT physically removed here. Hard-delete of a specific version goes
	// through DeleteObjectVersion (used by lifecycle/scrubber).
	//
	// For backward compatibility with the legacy N× on-disk layout, we also
	// remove the unversioned local object file if present — it's guaranteed to
	// be stale (superseded by a versioned path) and keeping it risks GetObject
	// serving it as a fallback read.
	os.Remove(b.objectPath(bucket, key))

	markerID := newVersionID()
	err := b.propose(context.Background(), CmdDeleteObject, DeleteObjectCmd{
		Bucket:    bucket,
		Key:       key,
		VersionID: markerID,
	})
	if err != nil {
		return "", err
	}
	return markerID, nil
}

func (b *DistributedBackend) ListObjects(bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	var objects []*storage.Object
	err := b.db.View(func(txn *badger.Txn) error {
		// Load latest-version pointers for this bucket so we can dedupe versioned
		// entries down to a single row per base key (skipping delete markers).
		latMap := make(map[string]string) // base key → latest versionID
		latPrefix := []byte("lat:" + bucket + "/")
		itLat := txn.NewIterator(badger.DefaultIteratorOptions)
		for itLat.Seek(latPrefix); itLat.ValidForPrefix(latPrefix); itLat.Next() {
			baseKey := string(itLat.Item().Key()[len(latPrefix):])
			_ = itLat.Item().Value(func(v []byte) error {
				latMap[baseKey] = string(v)
				return nil
			})
		}
		itLat.Close()

		// Prefixed scan on obj:{bucket}/{prefix}. For base keys that appear in
		// latMap we emit exactly the version that's current, skipping all other
		// versioned entries. For keys not in latMap (legacy non-versioned data)
		// we emit the single entry we find.
		emitted := make(map[string]bool)
		pfx := []byte("obj:" + bucket + "/" + prefix)
		bucketPfx := []byte("obj:" + bucket + "/")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		count := 0
		for it.Seek(pfx); it.ValidForPrefix(bucketPfx); it.Next() {
			if !it.ValidForPrefix(pfx) {
				break
			}
			if count >= maxKeys {
				break
			}
			k := string(it.Item().Key())
			rest := k[len(bucketPfx):]

			// Derive the base key. A versioned key is "{baseKey}/{versionID}";
			// a legacy key is just "{baseKey}" with no trailing segment.
			baseKey := rest
			isVersioned := false
			if slash := strings.LastIndex(rest, "/"); slash >= 0 {
				candidateBase := rest[:slash]
				candidateVID := rest[slash+1:]
				if lat, ok := latMap[candidateBase]; ok && lat == candidateVID {
					baseKey = candidateBase
					isVersioned = true
				} else if _, baseInLat := latMap[candidateBase]; baseInLat {
					// This is a non-latest version of a versioned key — skip.
					continue
				}
			}
			// A versioned entry "foo/{versionID}" deduces to baseKey="foo", but if
			// the caller asked for prefix "foo/", we must NOT return "foo" — that
			// would cause isDir("foo") to return true for a regular file.
			if !strings.HasPrefix(baseKey, prefix) {
				continue
			}
			if emitted[baseKey] {
				continue
			}

			// If the base key has a lat: pointer but this iteration hit the
			// legacy unversioned `obj:{bucket}/{baseKey}` entry first, we
			// should wait and emit the versioned one. Skip this legacy entry.
			if !isVersioned {
				if _, inLat := latMap[baseKey]; inLat {
					continue
				}
			}

			var obj storage.Object
			err := it.Item().Value(func(val []byte) error {
				m, err := unmarshalObjectMeta(val)
				if err != nil {
					return err
				}
				if m.ETag == deleteMarkerETag {
					return nil // tombstone — don't emit
				}
				obj = storage.Object{
					Key:          m.Key,
					Size:         m.Size,
					ContentType:  m.ContentType,
					ETag:         m.ETag,
					LastModified: m.LastModified,
					ACL:          m.ACL,
				}
				if isVersioned {
					obj.VersionID = latMap[baseKey]
				}
				return nil
			})
			if err != nil {
				return err
			}
			if obj.Key == "" {
				// Skipped (tombstone or empty meta).
				continue
			}
			objects = append(objects, &obj)
			emitted[baseKey] = true
			count++
		}
		return nil
	})
	return objects, err
}

func (b *DistributedBackend) WalkObjects(bucket, prefix string, fn func(*storage.Object) error) error {
	if err := b.HeadBucket(bucket); err != nil {
		return err
	}
	return b.db.View(func(txn *badger.Txn) error {
		latMap := make(map[string]string)
		latPrefix := []byte("lat:" + bucket + "/")
		itLat := txn.NewIterator(badger.DefaultIteratorOptions)
		for itLat.Seek(latPrefix); itLat.ValidForPrefix(latPrefix); itLat.Next() {
			baseKey := string(itLat.Item().Key()[len(latPrefix):])
			_ = itLat.Item().Value(func(v []byte) error {
				latMap[baseKey] = string(v)
				return nil
			})
		}
		itLat.Close()

		emitted := make(map[string]bool)
		pfx := []byte("obj:" + bucket + "/" + prefix)
		bucketPfx := []byte("obj:" + bucket + "/")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			k := string(it.Item().Key())
			rest := k[len(bucketPfx):]

			baseKey := rest
			isVersioned := false
			if slash := strings.LastIndex(rest, "/"); slash >= 0 {
				candidateBase := rest[:slash]
				candidateVID := rest[slash+1:]
				if lat, ok := latMap[candidateBase]; ok && lat == candidateVID {
					baseKey = candidateBase
					isVersioned = true
				} else if _, baseInLat := latMap[candidateBase]; baseInLat {
					continue
				}
			}
			if !strings.HasPrefix(baseKey, prefix) {
				continue
			}
			if emitted[baseKey] {
				continue
			}
			if !isVersioned {
				if _, inLat := latMap[baseKey]; inLat {
					continue
				}
			}

			var obj storage.Object
			if err := it.Item().Value(func(val []byte) error {
				m, err := unmarshalObjectMeta(val)
				if err != nil {
					return err
				}
				if m.ETag == deleteMarkerETag {
					return nil
				}
				obj = storage.Object{
					Key:          m.Key,
					Size:         m.Size,
					ContentType:  m.ContentType,
					ETag:         m.ETag,
					LastModified: m.LastModified,
					ACL:          m.ACL,
				}
				if isVersioned {
					obj.VersionID = latMap[baseKey]
				}
				return nil
			}); err != nil {
				return err
			}
			if obj.Key == "" {
				continue
			}
			emitted[baseKey] = true
			if err := fn(&obj); err != nil {
				return err
			}
		}
		return nil
	})
}

// --- Multipart operations ---

func (b *DistributedBackend) CreateMultipartUpload(bucket, key, contentType string) (*storage.MultipartUpload, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}

	uploadID := uuid.New().String()
	if err := os.MkdirAll(b.partDir(uploadID), 0o755); err != nil {
		return nil, fmt.Errorf("create part dir: %w", err)
	}

	now := time.Now().Unix()

	err := b.propose(context.Background(), CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		CreatedAt:   now,
	})
	if err != nil {
		os.RemoveAll(b.partDir(uploadID))
		return nil, err
	}

	return &storage.MultipartUpload{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		CreatedAt:   now,
	}, nil
}

func (b *DistributedBackend) UploadPart(bucket, key, uploadID string, partNumber int, r io.Reader) (*storage.Part, error) {
	// Verify upload exists (read local metadata)
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(multipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return storage.ErrUploadNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	// Data write is local — no Raft needed for part data
	partFile := b.partPath(uploadID, partNumber)
	f, err := os.Create(partFile)
	if err != nil {
		return nil, fmt.Errorf("create part file: %w", err)
	}

	h := md5.New()
	w := io.MultiWriter(f, h)
	size, err := io.Copy(w, r)
	f.Close()
	if err != nil {
		os.Remove(partFile)
		return nil, fmt.Errorf("write part: %w", err)
	}

	return &storage.Part{
		PartNumber: partNumber,
		ETag:       hex.EncodeToString(h.Sum(nil)),
		Size:       size,
	}, nil
}

func (b *DistributedBackend) CompleteMultipartUpload(bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	// Read upload metadata
	var meta clusterMultipartMeta
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(multipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return storage.ErrUploadNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			m, err := unmarshalClusterMultipartMeta(val)
			if err != nil {
				return err
			}
			meta = m
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// Sort parts and assemble locally
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	versionID := newVersionID()
	objPath := b.objectPathV(bucket, key, versionID)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return nil, fmt.Errorf("create object dir: %w", err)
	}

	out, err := os.Create(objPath)
	if err != nil {
		return nil, fmt.Errorf("create final object: %w", err)
	}

	h := md5.New()
	mw := io.MultiWriter(out, h)
	var totalSize int64

	for _, p := range parts {
		partFile := b.partPath(uploadID, p.PartNumber)
		f, err := os.Open(partFile)
		if err != nil {
			out.Close()
			os.Remove(objPath)
			return nil, fmt.Errorf("open part %d: %w", p.PartNumber, err)
		}
		n, err := io.Copy(mw, f)
		f.Close()
		if err != nil {
			out.Close()
			os.Remove(objPath)
			return nil, fmt.Errorf("copy part %d: %w", p.PartNumber, err)
		}
		totalSize += n
	}
	out.Close()

	etag := hex.EncodeToString(h.Sum(nil))
	now := time.Now().Unix()

	err = b.propose(context.Background(), CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket:      bucket,
		Key:         key,
		UploadID:    uploadID,
		Size:        totalSize,
		ContentType: meta.ContentType,
		ETag:        etag,
		ModTime:     now,
		VersionID:   versionID,
	})
	if err != nil {
		return nil, err
	}

	os.RemoveAll(b.partDir(uploadID))

	return &storage.Object{
		Key:          key,
		Size:         totalSize,
		ContentType:  meta.ContentType,
		ETag:         etag,
		LastModified: now,
		VersionID:    versionID,
	}, nil
}

func (b *DistributedBackend) AbortMultipartUpload(bucket, key, uploadID string) error {
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(multipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return storage.ErrUploadNotFound
		}
		return err
	})
	if err != nil {
		return err
	}

	os.RemoveAll(b.partDir(uploadID))

	return b.propose(context.Background(), CmdAbortMultipart, AbortMultipartCmd{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	})
}

// --- Versioning ---

// HeadObjectVersion returns metadata for a specific version. Returns
// storage.ErrObjectNotFound if the version doesn't exist or is a delete marker.
func (b *DistributedBackend) HeadObjectVersion(bucket, key, versionID string) (*storage.Object, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}
	var obj storage.Object
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(objectMetaKeyV(bucket, key, versionID))
		if err == badger.ErrKeyNotFound {
			return storage.ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			if m.ETag == deleteMarkerETag {
				// S3 semantics: HEAD on a delete marker version returns 405
				// MethodNotAllowed. storage.ErrMethodNotAllowed is the sentinel
				// the server handler maps to that response, including the
				// x-amz-delete-marker: true header.
				return storage.ErrMethodNotAllowed
			}
			obj = storage.Object{
				Key:          m.Key,
				Size:         m.Size,
				ContentType:  m.ContentType,
				ETag:         m.ETag,
				LastModified: m.LastModified,
				VersionID:    versionID,
				ACL:          m.ACL,
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

// GetObjectVersion reads a specific version's data. Returns
// storage.ErrObjectNotFound if the version doesn't exist. For delete markers,
// returns ErrMethodNotAllowed to mirror the erasure backend's behavior.
func (b *DistributedBackend) GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, *storage.Object, error) {
	obj, err := b.HeadObjectVersion(bucket, key, versionID)
	if err != nil {
		return nil, nil, err
	}
	if obj.IsDeleteMarker {
		return nil, nil, storage.ErrMethodNotAllowed
	}
	// Prefer the versioned local file; fall back to legacy unversioned path if
	// the version happens to be the legacy latest (uncommon mid-transition case).
	if f, oerr := os.Open(b.objectPathV(bucket, key, versionID)); oerr == nil {
		return f, obj, nil
	}
	f, err := os.Open(b.objectPath(bucket, key))
	if err != nil {
		return nil, nil, fmt.Errorf("open versioned object: %w", err)
	}
	return f, obj, nil
}

// DeleteObjectVersion hard-deletes a specific version (no tombstone).
// Used by lifecycle/scrubber to reclaim expired versions.
func (b *DistributedBackend) DeleteObjectVersion(bucket, key, versionID string) error {
	if err := b.HeadBucket(bucket); err != nil {
		return err
	}
	// Local data cleanup: best-effort (ENOENT is fine — FSM apply is the source of truth).
	_ = os.Remove(b.objectPathV(bucket, key, versionID))
	return b.propose(context.Background(), CmdDeleteObjectVersion, DeleteObjectVersionCmd{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
	})
}

// ListObjectVersions returns every version (including delete markers) under
// the given prefix, sorted newest-first. When maxKeys > 0 the result is
// truncated. VersionIDs are UUIDv7 (k-sortable ASC by ms timestamp), so we
// sort DESC to get newest-first. Matches server.ObjectVersionLister.
func (b *DistributedBackend) ListObjectVersions(bucket, prefix string, maxKeys int) ([]*storage.ObjectVersion, error) {
	if err := b.HeadBucket(bucket); err != nil {
		return nil, err
	}
	var versions []*storage.ObjectVersion
	latestMap := map[string]string{} // key → latestVID
	err := b.db.View(func(txn *badger.Txn) error {
		// Pre-scan latest pointers for the prefix so each version can tag IsLatest.
		latPrefix := []byte("lat:" + bucket + "/" + prefix)
		latIt := txn.NewIterator(badger.DefaultIteratorOptions)
		for latIt.Seek(latPrefix); latIt.ValidForPrefix(latPrefix); latIt.Next() {
			k := string(latIt.Item().Key())
			key := strings.TrimPrefix(k, "lat:"+bucket+"/")
			_ = latIt.Item().Value(func(v []byte) error { latestMap[key] = string(v); return nil })
		}
		latIt.Close()

		// Match any object key starting with `prefix` — iterate the per-bucket
		// versioned store and filter in-memory. The version ID is the last
		// path segment after the final `/`; everything before is the S3 key.
		objPrefix := []byte("obj:" + bucket + "/")
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(objPrefix); it.ValidForPrefix(objPrefix); it.Next() {
			k := string(it.Item().Key())
			rest := strings.TrimPrefix(k, "obj:"+bucket+"/")
			// Versioned format: {key}/{versionID}. Unversioned legacy: {key}.
			slash := strings.LastIndex(rest, "/")
			if slash < 0 {
				continue // legacy unversioned entry, no per-version record
			}
			key := rest[:slash]
			vid := rest[slash+1:]
			if vid == "" || !strings.HasPrefix(key, prefix) {
				continue
			}
			latestVID := latestMap[key]
			var v storage.ObjectVersion
			if err := it.Item().Value(func(val []byte) error {
				m, err := unmarshalObjectMeta(val)
				if err != nil {
					return err
				}
				v = storage.ObjectVersion{
					Key:            key,
					VersionID:      vid,
					IsLatest:       vid == latestVID,
					IsDeleteMarker: m.ETag == deleteMarkerETag,
					LastModified:   m.LastModified,
					ETag:           m.ETag,
					Size:           m.Size,
				}
				return nil
			}); err != nil {
				return err
			}
			versions = append(versions, &v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Sort DESC by VersionID (UUIDv7 is lex-ASC-by-time, so reverse = newest-first).
	sort.Slice(versions, func(i, j int) bool {
		if versions[i].Key != versions[j].Key {
			return versions[i].Key < versions[j].Key
		}
		return versions[i].VersionID > versions[j].VersionID
	})
	if maxKeys > 0 && len(versions) > maxKeys {
		versions = versions[:maxKeys]
	}
	return versions, nil
}

// --- Path helpers ---

func (b *DistributedBackend) bucketDir(bucket string) string {
	return filepath.Join(b.root, "data", bucket)
}

// objectPath returns the legacy-unversioned local path for a full-object copy.
// Kept as a read fallback only — writers go through objectPathV. New keys never
// collide with objectPathV because the versioned namespace lives under a
// sibling ".obj" directory.
func (b *DistributedBackend) objectPath(bucket, key string) string {
	return filepath.Join(b.root, "data", bucket, key)
}

// writeFileAtomic writes data to path using a temp+rename recipe. The temp
// file lives in the same directory as the target so rename is a metadata-
// only operation on the same filesystem. Caller must have ensured the
// parent directory exists (MkdirAll). On error the temp file is best-effort
// removed.
//
// This is the lock-free serialization primitive for VFS bucket fixed-
// versionID writes: concurrent writers race their renames; POSIX rename
// atomicity yields last-writer-wins semantics with no torn intermediate
// state visible to readers.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-")
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename tmp→target: %w", err)
	}
	return nil
}

// objectPathV returns the version-addressable local path for a full-object copy
// in the N× path: {root}/data/{bucket}/.obj/{key}/{versionID}.
//
// The ".obj/" sibling namespace was adopted in v0.0.4.0 because the previous
// scheme ({bucket}/{key}/.v/{vid}) collided with the unversioned path — if a
// caller like NFS wrote "foo.txt" as a plain file via an older code path, a
// subsequent versioned write couldn't MkdirAll("foo.txt/.v/"). Splitting
// versioned writes into a separate ".obj" root resolves that; it's at most
// the bucket name that's reserved, which S3 already forbids keys from.
func (b *DistributedBackend) objectPathV(bucket, key, versionID string) string {
	return filepath.Join(b.root, "data", bucket, ".obj", key, versionID)
}

func (b *DistributedBackend) partDir(uploadID string) string {
	return filepath.Join(b.root, "parts", uploadID)
}

func (b *DistributedBackend) partPath(uploadID string, partNumber int) string {
	return filepath.Join(b.partDir(uploadID), fmt.Sprintf("%05d", partNumber))
}
