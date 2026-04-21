package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

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

// DistributedBackend implements storage.Backend with Raft-replicated metadata
// and local file storage for data. Metadata mutations go through Raft;
// reads are served from the local BadgerDB (kept in sync by the FSM).
type DistributedBackend struct {
	root        string
	db          *badger.DB
	node        *raft.Node
	fsm         *FSM
	logger      *slog.Logger
	lastApplied atomic.Uint64
	onApply     OnApplyFunc
	snapMgr     *raft.SnapshotManager
	snapNode    *raft.Node // node for CompactLog after snapshot
	shardSvc    *ShardService
	allNodes    []string // all node addresses (including self) for shard placement
	selfAddr    string   // this node's raft address (matches entries in allNodes)
	peerHealth  *PeerHealth
	registry    *Registry // cache invalidators (VFS instances)
	ecConfig    ECConfig  // Phase 18: erasure coding config (disabled = legacy N× path)
	shardLocks  sync.Map  // scrubbable.go: per-(bucket,key) RWMutex for ReadShard/WriteShard
}

// NewDistributedBackend creates a new distributed storage backend.
// The FSM apply loop must be started separately via RunApplyLoop.
func NewDistributedBackend(root string, db *badger.DB, node *raft.Node) (*DistributedBackend, error) {
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	fsm := NewFSM(db)
	return &DistributedBackend{
		root:     root,
		db:       db,
		node:     node,
		fsm:      fsm,
		logger:   slog.With("component", "distributed-backend"),
		registry: NewRegistry(),
	}, nil
}

// SetECConfig enables erasure coding for PutObject/GetObject when the cluster
// is large enough. Phase 18. Call before serving traffic. Zero-value = disabled.
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
				b.logger.Error("fsm apply error", "index", entry.Index, "error", err)
			}
			b.lastApplied.Store(entry.Index)

			// Notify cache/metrics callback
			if b.onApply != nil {
				b.notifyOnApply(entry.Command)
			}

			// Check if snapshot should be taken
			if b.snapMgr != nil {
				if b.snapMgr.MaybeTrigger(entry.Index, entry.Term) {
					b.logger.Info("snapshot taken", "index", entry.Index, "term", entry.Term)
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

func (b *DistributedBackend) propose(ctx context.Context, cmdType CommandType, payload any) error {
	data, err := EncodeCommand(cmdType, payload)
	if err != nil {
		return fmt.Errorf("encode command: %w", err)
	}

	proposeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	idx, err := b.node.ProposeWait(proposeCtx, data)
	if err != nil {
		return err
	}

	// Wait until the FSM has applied this entry
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

// Close closes the metadata database.
func (b *DistributedBackend) Close() error {
	return b.db.Close()
}

// GetRegistry returns the cache invalidator registry for registering VFS instances.
func (b *DistributedBackend) GetRegistry() *Registry {
	return b.registry
}

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

// SetBucketVersioning satisfies server.BucketVersioner. Persists the S3
// versioning state ("Enabled"/"Suspended") under key bucketver:{bucket} in
// the FSM BadgerDB. NOTE: writes bypass Raft in this slice — consistent on a
// singleton, eventually consistent across a cluster until follow-up work
// wires a SetBucketVersioning FSM command. Object versioning itself (via
// UUIDv7 VersionID) remains always-on regardless of this flag.
func (b *DistributedBackend) SetBucketVersioning(bucket, state string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		if _, err := txn.Get(bucketKey(bucket)); err != nil {
			if err == badger.ErrKeyNotFound {
				return storage.ErrBucketNotFound
			}
			return err
		}
		return txn.Set([]byte("bucketver:"+bucket), []byte(state))
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

	// Versioning is unconditional: every PUT gets a fresh ULID so prior versions
	// remain addressable via GetObjectVersion / ListObjectVersions. Bucket-level
	// versioning-state gating is a later slice.
	versionID := newVersionID()

	// Phase 18 Cluster EC: split across k+m nodes when enabled and cluster is large enough.
	if b.ecConfig.IsActive(len(b.allNodes)) && b.shardSvc != nil {
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
func (b *DistributedBackend) putObjectNx(bucket, key, versionID string, data []byte, contentType string) (*storage.Object, error) {
	// Write data locally at the versioned path.
	objPath := b.objectPathV(bucket, key, versionID)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return nil, fmt.Errorf("create object dir: %w", err)
	}
	if err := os.WriteFile(objPath, data, 0o644); err != nil {
		return nil, fmt.Errorf("write object: %w", err)
	}

	// Replicate data to healthy peer nodes via ShardService. We encode the
	// version into the ShardService "key" parameter (key+"/"+versionID) so the
	// ShardService signature stays unchanged for this slice.
	shardKey := key + "/" + versionID
	if b.shardSvc != nil {
		ctx := context.Background()
		for _, peer := range b.allNodes {
			if peer == b.selfAddr {
				continue
			}
			if b.peerHealth != nil && !b.peerHealth.IsHealthy(peer) {
				b.logger.Debug("skipping unhealthy peer for replication", "peer", peer)
				continue
			}
			if err := b.shardSvc.WriteShard(ctx, peer, bucket, shardKey, 0, data); err != nil {
				b.logger.Warn("data replication failed", "peer", peer, "bucket", bucket, "key", key, "error", err)
				if b.peerHealth != nil {
					b.peerHealth.MarkUnhealthy(peer)
				}
			} else if b.peerHealth != nil {
				b.peerHealth.MarkHealthy(peer)
			}
		}
	}

	h := md5.Sum(data)
	etag := hex.EncodeToString(h[:])
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

	return &storage.Object{
		Key:          key,
		Size:         int64(len(data)),
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
		VersionID:    versionID,
	}, nil
}

// putObjectEC is the Phase 18 Cluster EC path: Reed-Solomon split into
// cfg.NumShards() shards, fan-out each to its placed node (self or peer),
// then commit placement + meta through Raft.
//
// Consistency: write-all. Any shard write failure → cleanup + error.
// Raft commit order: CmdPutShardPlacement first (so a crash after this step
// leaves the placement record as the source of truth for Slice 4 repair),
// then CmdPutObjectMeta. Rollback on meta failure deletes all shards and
// removes the placement record.
func (b *DistributedBackend) putObjectEC(bucket, key, versionID string, data []byte, contentType string) (*storage.Object, error) {
	ctx := context.Background()

	shards, err := ECSplit(b.ecConfig, data)
	if err != nil {
		return nil, fmt.Errorf("ec split: %w", err)
	}

	// ShardService's key parameter carries the versionID as a suffix so shards
	// for different versions land at different paths without changing the API.
	shardKey := key + "/" + versionID

	placement := PlacementForNodes(b.ecConfig, b.allNodes, shardKey)
	selfID := b.node.ID()

	// Track nodes we wrote to so cleanup can target them precisely.
	written := make([]string, 0, len(shards))
	cleanup := func() {
		for _, n := range written {
			if n == selfID {
				_ = b.shardSvc.DeleteLocalShards(bucket, shardKey)
				continue
			}
			_ = b.shardSvc.DeleteShards(ctx, n, bucket, shardKey)
		}
	}

	// Fan-out: write each shard to its placed node. Write-all consistency.
	for i, node := range placement {
		if node == selfID {
			if werr := b.shardSvc.WriteLocalShard(bucket, shardKey, i, shards[i]); werr != nil {
				cleanup()
				return nil, fmt.Errorf("ec write local shard %d: %w", i, werr)
			}
		} else {
			if werr := b.shardSvc.WriteShard(ctx, node, bucket, shardKey, i, shards[i]); werr != nil {
				if b.peerHealth != nil {
					b.peerHealth.MarkUnhealthy(node)
				}
				cleanup()
				return nil, fmt.Errorf("ec write shard %d to %s: %w", i, node, werr)
			}
			if b.peerHealth != nil {
				b.peerHealth.MarkHealthy(node)
			}
		}
		written = append(written, node)
	}

	// Commit placement through Raft.
	if perr := b.propose(ctx, CmdPutShardPlacement, PutShardPlacementCmd{
		Bucket:  bucket,
		Key:     key,
		NodeIDs: placement,
	}); perr != nil {
		cleanup()
		return nil, fmt.Errorf("ec propose placement: %w", perr)
	}

	h := md5.Sum(data)
	etag := hex.EncodeToString(h[:])
	now := time.Now().Unix()

	// Commit metadata. On failure, roll back placement + shards.
	if merr := b.propose(ctx, CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:      bucket,
		Key:         key,
		Size:        int64(len(data)),
		ContentType: contentType,
		ETag:        etag,
		ModTime:     now,
		VersionID:   versionID,
	}); merr != nil {
		_ = b.propose(ctx, CmdDeleteShardPlacement, DeleteShardPlacementCmd{Bucket: bucket, Key: key})
		cleanup()
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

func (b *DistributedBackend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	obj, err := b.HeadObject(bucket, key)
	if err != nil {
		return nil, nil, err
	}
	// HeadObject already rejects tombstones with ErrObjectNotFound, so obj here
	// is a real version. VersionID is non-empty for versioned writes and empty
	// for legacy log replay.

	// Phase 18: EC placement takes precedence. Absent placement falls through
	// to the legacy N×-replicated single-shard path below.
	if nodes, ok := b.fsm.LookupShardPlacement(bucket, key); ok && b.shardSvc != nil {
		data, ecErr := b.getObjectEC(context.Background(), bucket, key, obj.VersionID, nodes)
		if ecErr == nil {
			return io.NopCloser(bytes.NewReader(data)), obj, nil
		}
		// Reconstruction failed — log and fall through to any legacy local/peer
		// full-object copy that may still exist (e.g. mid-migration state).
		b.logger.Warn("ec reconstruct failed, falling back to N× path",
			"bucket", bucket, "key", key, "error", ecErr)
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
	// Peers store under key+"/"+versionID when the write was versioned.
	shardKey := key
	if obj.VersionID != "" {
		shardKey = key + "/" + obj.VersionID
	}
	if b.shardSvc != nil && os.IsNotExist(err) {
		ctx := context.Background()
		// Try healthy peers first
		for _, peer := range b.allNodes {
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
			for _, peer := range b.allNodes {
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
	placement, ok := b.fsm.LookupShardPlacement(bucket, key)
	if !ok {
		return fmt.Errorf("no placement for %s/%s — object is not EC-managed", bucket, key)
	}
	if shardIdx < 0 || shardIdx >= len(placement) {
		return fmt.Errorf("shardIdx %d out of range [0,%d)", shardIdx, len(placement))
	}
	if len(placement) != b.ecConfig.NumShards() {
		return fmt.Errorf("placement length %d != k+m %d", len(placement), b.ecConfig.NumShards())
	}

	// Resolve to the latest version when caller doesn't know it (monitor
	// callback path). Empty latest means pre-versioned legacy EC; fall back
	// to bare-key layout, preserving pre-Slice-3 behaviour.
	if versionID == "" {
		if latest, lerr := b.fsm.LookupLatestVersion(bucket, key); lerr == nil {
			versionID = latest
		}
	}
	shardKey := key
	if versionID != "" {
		shardKey = key + "/" + versionID
	}

	selfID := b.node.ID()
	shards := make([][]byte, len(placement))
	available := 0

	// Pull every OTHER shard. We intentionally skip shardIdx to avoid pulling
	// the corrupt/missing copy into the reconstruction.
	for i, node := range placement {
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
	if available < b.ecConfig.DataShards {
		return fmt.Errorf("repair: only %d/%d other shards readable, need %d",
			available, len(placement)-1, b.ecConfig.DataShards)
	}

	// ECReconstruct rebuilds the whole object; we then re-split to get the
	// canonical byte layout of each shard (including the missing one).
	data, rerr := ECReconstruct(b.ecConfig, shards)
	if rerr != nil {
		return fmt.Errorf("repair reconstruct: %w", rerr)
	}
	freshShards, serr := ECSplit(b.ecConfig, data)
	if serr != nil {
		return fmt.Errorf("repair re-split: %w", serr)
	}

	// Write just the missing shard back to its placement node.
	target := placement[shardIdx]
	if target == selfID {
		return b.shardSvc.WriteLocalShard(bucket, shardKey, shardIdx, freshShards[shardIdx])
	}
	return b.shardSvc.WriteShard(ctx, target, bucket, shardKey, shardIdx, freshShards[shardIdx])
}

// FSMRef returns the underlying FSM so reshard / monitor code can iterate
// placements + object metas without reaching through the backend's private fields.
func (b *DistributedBackend) FSMRef() *FSM { return b.fsm }

// FSMDB returns the underlying FSM BadgerDB handle.
// Used by lifecycle.NewStore and other components needing shared metadata storage.
// Lifecycle keys ("lifecycle:{bucket}") share the DB with FSM keys ("obj:", "lat:",
// "bucket:", etc.); the prefixes are disjoint so they coexist safely.
func (b *DistributedBackend) FSMDB() *badger.DB { return b.db }

// ECActive reports whether Phase 18 cluster EC will be applied to the next
// PutObject call (EC enabled + enough nodes for k+m split).
func (b *DistributedBackend) ECActive() bool { return b.ecConfig.IsActive(len(b.allNodes)) }

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
	if !b.ecConfig.IsActive(len(b.allNodes)) || b.shardSvc == nil {
		return fmt.Errorf("ec not active: enabled=%v cluster_size=%d k+m=%d",
			b.ecConfig.Enabled, len(b.allNodes), b.ecConfig.NumShards())
	}
	if _, ok := b.fsm.LookupShardPlacement(bucket, key); ok {
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
	shards, err := ECSplit(b.ecConfig, data)
	if err != nil {
		return fmt.Errorf("ec split for convert: %w", err)
	}
	placement := PlacementForNodes(b.ecConfig, b.allNodes, key)
	selfID := b.node.ID()
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
	for _, peer := range b.allNodes {
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
// placement[i] is the nodeID holding shardIdx i. Tolerates up to ParityShards
// unreachable nodes (read-k). The placement slice must have length NumShards().
func (b *DistributedBackend) getObjectEC(ctx context.Context, bucket, key, versionID string, placement []string) ([]byte, error) {
	if len(placement) != b.ecConfig.NumShards() {
		return nil, fmt.Errorf("placement length %d != expected %d", len(placement), b.ecConfig.NumShards())
	}
	// putObjectEC writes shards under shardKey = key + "/" + versionID so
	// concurrent versions don't clobber one another on disk. Reads have to
	// target the same path. Empty versionID preserves the pre-Slice-1 layout
	// for log replay of legacy EC objects.
	shardKey := key
	if versionID != "" {
		shardKey = key + "/" + versionID
	}
	selfID := b.node.ID()
	shards := make([][]byte, len(placement))
	available := 0
	for i, node := range placement {
		var data []byte
		var err error
		if node == selfID {
			data, err = b.shardSvc.ReadLocalShard(bucket, shardKey, i)
		} else {
			data, err = b.shardSvc.ReadShard(ctx, node, bucket, shardKey, i)
			if err != nil && b.peerHealth != nil {
				b.peerHealth.MarkUnhealthy(node)
			} else if err == nil && b.peerHealth != nil {
				b.peerHealth.MarkHealthy(node)
			}
		}
		if err == nil && data != nil {
			shards[i] = data
			available++
		}
	}
	if available < b.ecConfig.DataShards {
		return nil, fmt.Errorf("ec get: only %d/%d shards available, need %d", available, len(placement), b.ecConfig.DataShards)
	}
	return ECReconstruct(b.ecConfig, shards)
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

func (b *DistributedBackend) objectPath(bucket, key string) string {
	return filepath.Join(b.root, "data", bucket, key)
}

// objectPathV returns the version-addressable local path for a full-object copy
// in the N× path: {root}/data/{bucket}/{key}/.v/{versionID}. The ".v/" segment
// namespaces versioned files away from any sub-prefix that happens to share the
// key's name, so keys like "a" and "a/b" can coexist.
func (b *DistributedBackend) objectPathV(bucket, key, versionID string) string {
	return filepath.Join(b.root, "data", bucket, key, ".v", versionID)
}

func (b *DistributedBackend) partDir(uploadID string) string {
	return filepath.Join(b.root, "parts", uploadID)
}

func (b *DistributedBackend) partPath(uploadID string, partNumber int) string {
	return filepath.Join(b.partDir(uploadID), fmt.Sprintf("%05d", partNumber))
}
