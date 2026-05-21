package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
)

// shardRPCTimeout is the per-shard RPC deadline for remote writes/reads.
// EC PUTs stream shard bodies during the caller's write path; cold multi-raft
// startup can legitimately spend more than a few seconds opening and draining
// QUIC shard streams before the metadata propose completes.
const shardRPCTimeout = 2 * time.Minute

const maxSingleLocalShardMemoryFastPathBytes = 16 << 20

// EC in-memory shard fast path size caps. Replication (parity == 0) keeps the
// original 16 MiB cap; parity EC gets a lower 1 MiB cap so concurrent small
// PUTs cannot stack into a multi-hundred-megabyte burst (the rationale for
// commit 8d0ecccd #411). Within the cap, parity EC bypasses both the body
// spool-to-disk and the EC shard spool-to-disk, dropping ~30% CPU on small
// PUTs that dominate the warp s3 workload.
const (
	maxECMemoryShardFastPathBytesReplicated = 16 << 20
	maxECMemoryShardFastPathBytesParity     = 1 << 20
)

func maxECMemoryShardFastPathBytesForCfg(cfg ECConfig) int64 {
	if cfg.ParityShards == 0 {
		return maxECMemoryShardFastPathBytesReplicated
	}
	return maxECMemoryShardFastPathBytesParity
}

const (
	ecShardBufferedLimit = 256 * 1024
	ecShardWriteAttempts = 3
	ecShardWriteBackoff  = 250 * time.Millisecond
)

var distributedWriteAtTraceEnabled = os.Getenv("GRAINFS_VOLUME_TRACE") == "1"

type readerWithoutWriterTo struct {
	io.Reader
}

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

type raftSnapshotRequest struct {
	ctx  context.Context
	resp chan raftSnapshotResponse
}

type raftSnapshotResponse struct {
	result raft.SnapshotResult
	err    error
}

// BucketAssigner proposes a bucket→group assignment to the meta-Raft cluster.
// Implemented by *MetaRaft; nil = no persistence (single-node legacy mode).
type BucketAssigner interface {
	ProposeBucketAssignment(ctx context.Context, bucket, groupID string) error
}

// DistributedBackend implements storage.Backend with Raft-replicated metadata
// and local file storage for data. Metadata mutations go through Raft;
// reads are served from the local BadgerDB (kept in sync by the FSM).
type DistributedBackend struct {
	root                             string
	db                               *badger.DB
	node                             RaftNode
	fsm                              *FSM
	keys                             *stateKeyspace
	groupID                          string // non-empty when constructed via NewDistributedBackendForGroup; used by Phase B1 append-segment peer-fetch
	shared                           bool
	logger                           zerolog.Logger
	lastApplied                      atomic.Uint64
	lastAppliedTerm                  atomic.Uint64
	snapRequests                     chan raftSnapshotRequest
	onApply                          OnApplyFunc
	shardSvc                         *ShardService
	allNodes                         []string // all node addresses (including self) for shard placement
	selfAddr                         string   // this node's raft address (matches entries in allNodes)
	peerHealth                       *PeerHealth
	topologySnapshot                 atomic.Pointer[backendTopology]
	registry                         *Registry // cache invalidators (VFS instances)
	ecConfig                         ECConfig  // Phase 18: erasure coding config (k+m shard parameters)
	ecConfigSnapshot                 atomic.Pointer[ECConfig]
	runtimeSnapshot                  atomic.Pointer[backendRuntimeSnapshot]
	shardLocks                       pool.SyncMap[string, *sync.RWMutex] // scrubbable.go: per-(bucket,key) RWMutex for ReadShard/WriteShard
	multipartLocks                   sync.Map                            // map[uploadID]*sync.RWMutex; serializes part writes against complete/abort cleanup
	appendLocks                      [appendLockStripeCount]sync.Mutex   // striped owner-side admission locks for same-object AppendObject
	incidentRecorder                 IncidentRecorder                    // nil disables zero-ops incident recording
	testBeforeChunkedMultipartCommit func() error                        // test-only hook for chunked multipart commit preflight
	testBeforeAppendSegmentWrite     func()                              // test-only hook after append pre-check before segment write

	// shardCache caches reconstructed/fetched EC shards. Sits in front of
	// getObjectEC's per-shard fan-out: a full hit (every needed shard
	// resident) skips disk and network entirely. Nil disables caching.
	// See internal/cache/shardcache for the rationale (sharded LRU,
	// lock-free counters, why we do not use an actor pattern here).
	shardCache *shardcache.Cache

	assigner   BucketAssigner   // PR-D: MetaRaft proposer; nil = no-op (single-node legacy)
	router     *Router          // PR-D: bucket→group routing; nil = no routing
	shardGroup ShardGroupSource // v0.0.7.0: query active groups for hash assignment; nil = legacy single-group path
	// chunkedPutChunkSize is a test seam; zero keeps the production default.
	chunkedPutChunkSize int

	// bypassBucketCheck skips the HeadBucket pre-check in PutObject. Set by
	// GroupBackend: bucket existence is guaranteed by the router (design doc
	// invariant 5), so the per-group DB need not duplicate the META-DB check.
	bypassBucketCheck bool

	internalPathCache sync.Map // map[internalObjectCacheKey]internalObjectPath
	internalDirCache  sync.Map // map[string]struct{}
	internalSizeCache sync.Map // map[internalObjectCacheKey]int64

	// Phase A: FSM apply error propagation. Mirrors MetaRaft.applyErrs
	// (meta_raft.go:797). applyErrs keys are Raft log indices; readers consume
	// entries via ApplyError exactly once per ProposeWait.
	applyResultMu sync.Mutex
	applyErrs     map[uint64]error

	// Phase B2 coalesce: lifecycle context + worker + first-seen tracker.
	// coalesceCfg holds trigger thresholds (count / size / idle / cleanup).
	// Stored as atomic.Pointer so SetCoalesceConfig and the backstop-scan
	// goroutine can access it without a mutex.
	// coalesceCancel is invoked from Close to stop both the worker goroutine
	// and the periodic backstop scanner.
	coalesceCfg       atomic.Pointer[CoalesceConfig]
	coalesce          *coalesceWorker
	coalesceCtx       context.Context
	coalesceCancel    context.CancelFunc
	coalesceFirstSeen sync.Map // key="<bucket>\x00<key>" → time.Time
	// coalesceFaultAfterECWrite is a test-only hook: when set to a non-nil
	// function returning an error, processCoalesceJobB3 calls it after the
	// EC write but before propose, allowing tests to simulate a crash that
	// leaves orphan EC shards but no metadata commit. Production builds
	// leave this nil.
	coalesceFaultAfterECWrite func() error

	// scrubOrphanAge is the age gate for WalkOrphanSegments. Set via SetScrubOrphanAge.
	scrubOrphanAge time.Duration
}

type backendTopology struct {
	allNodes   []string
	selfAddr   string
	peerHealth *PeerHealth
}

type backendRuntimeSnapshot struct {
	topology backendTopology
	ecConfig ECConfig
}

type internalObjectCacheKey struct {
	bucket string
	key    string
}

type internalObjectPath struct {
	path    string
	dir     string
	metaKey []byte
}

// NewDistributedBackend creates a new distributed storage backend.
// The FSM apply loop must be started separately via RunApplyLoop.
// keys may be nil (uses an identity keyspace); shared controls whether Close
// skips closing the BadgerDB (for shared-DB mode where the caller owns the DB lifecycle).
func NewDistributedBackend(root string, db *badger.DB, node RaftNode, keys *stateKeyspace, shared bool) (*DistributedBackend, error) {
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	if keys == nil {
		keys = newStateKeyspaceEmpty()
	}

	fsm := NewFSM(db, keys)

	if noOp, err := EncodeNoOpCommand(); err == nil {
		node.SetNoOpCommand(noOp)
	}

	b := &DistributedBackend{
		root:         root,
		db:           db,
		node:         node,
		fsm:          fsm,
		keys:         keys,
		shared:       shared,
		logger:       log.With().Str("component", "distributed-backend").Logger(),
		registry:     NewRegistry(),
		snapRequests: make(chan raftSnapshotRequest),
	}
	// Phase B2: wire the in-process coalesce worker + periodic backstop scan.
	// Lifecycle is bound to Close() via coalesceCancel.
	defCfg := DefaultCoalesceConfig()
	b.coalesceCfg.Store(&defCfg)
	b.fsm.SetCoalesceCfg(defCfg)
	b.coalesceCtx, b.coalesceCancel = context.WithCancel(context.Background())
	b.coalesce = newCoalesceWorker(256, b.processCoalesceJobB3)
	b.coalesce.Start(b.coalesceCtx)
	go b.coalesceBackstopScan(b.coalesceCtx)
	if b.scrubOrphanAge == 0 {
		b.scrubOrphanAge = 5 * time.Minute
	}
	return b, nil
}

// NewDistributedBackendForGroup builds a DistributedBackend whose FSM-state
// keys carry groupID's keyspace prefix and which opens in shared-DB mode
// (Close does NOT close db — the caller owns the shared DB's lifecycle).
// serveruntime uses this for the group-0 main backend over the per-node
// shared FSM-state DB (C2 P3). groupID must be non-empty.
func NewDistributedBackendForGroup(root string, db *badger.DB, node RaftNode, groupID string) (*DistributedBackend, error) {
	keys, err := newStateKeyspace(groupID)
	if err != nil {
		return nil, fmt.Errorf("group %s: keyspace: %w", groupID, err)
	}
	b, err := NewDistributedBackend(root, db, node, keys, true)
	if err != nil {
		return nil, err
	}
	b.groupID = groupID
	return b, nil
}

// GroupID returns the placement group this backend serves, or empty for
// legacy single-group test/tooling constructions.
func (b *DistributedBackend) GroupID() string { return b.groupID }

// SegmentBlobPath exposes the on-disk path for an append-segment blob so
// the node-level Phase B1 peer-fetch handler can open it through the
// right group backend.
func (b *DistributedBackend) SegmentBlobPath(bucket, key, blobID string) string {
	return b.segmentBlobPath(bucket, key, blobID)
}

// ks returns the effective stateKeyspace for this backend. When b.keys is nil
// (backend constructed via struct literal in tests) it falls back to the identity
// keyspace so all methods work correctly without requiring the constructor.
func (b *DistributedBackend) ks() *stateKeyspace {
	if b.keys == nil {
		return newStateKeyspaceEmpty()
	}
	return b.keys
}

// SetShardCache configures the EC shard cache. Pass a cache built with
// shardcache.New(byteBudget). Pass nil (or shardcache.New(0)) to leave
// caching disabled. Must be called before serving traffic.
func (b *DistributedBackend) SetShardCache(c *shardcache.Cache) {
	b.shardCache = c
}

// SetScrubOrphanAge configures the age gate used by WalkOrphanSegments.
// 0 value is treated as "use default" (5m).
func (b *DistributedBackend) SetScrubOrphanAge(d time.Duration) {
	if d > 0 {
		b.scrubOrphanAge = d
	}
}

// shardCacheKey is the canonical cache key for a single EC shard. Must
// match the readamp tracker key used by ecObjectReader so the simulator
// and real cache share the same identity.
func shardCacheKey(bucket, shardKey string, idx int) string {
	return fmt.Sprintf("%s/%s/%d", bucket, shardKey, idx)
}

func shardRangeCacheKey(bucket, shardKey string, idx int, offset, length int64) string {
	return fmt.Sprintf("%s/%s/%d:%d:%d", bucket, shardKey, idx, offset, length)
}

func shardRangeCachePrefix(bucket, shardKey string, idx int) string {
	return fmt.Sprintf("%s/%s/%d:", bucket, shardKey, idx)
}

// invalidateShardCache drops every shard slot for one shardKey. Used by
// PutObject overwrite, DeleteObject, and repairShardEC so a subsequent
// read sees post-write state. nShards covers the full k+m fan-out.
func (b *DistributedBackend) invalidateShardCache(bucket, shardKey string, nShards int) {
	if b.shardCache == nil {
		return
	}
	b.shardCache.InvalidatePrefix(fmt.Sprintf("%s/%s/", bucket, shardKey))
	for i := 0; i < nShards; i++ {
		b.shardCache.Invalidate(shardCacheKey(bucket, shardKey, i))
	}
}

// SetECConfig configures erasure-coding shard parameters (k, m) for
// PutObject/GetObject. Call before serving traffic. The configured profile must
// fit the active write node set; invalid profiles make EC writes fail fast.
// SetCoalesceConfig updates the coalesce thresholds at runtime.
// Propagates to the FSM so the apply loop uses the new SizeCapBytes
// immediately on the next committed entry.
func (b *DistributedBackend) SetCoalesceConfig(cfg CoalesceConfig) {
	cfgCopy := cfg
	b.coalesceCfg.Store(&cfgCopy)
	if b.fsm != nil {
		b.fsm.SetCoalesceCfg(cfg)
	}
}

func (b *DistributedBackend) SetECConfig(cfg ECConfig) {
	if b.ecConfigSnapshot.Load() == nil {
		b.ecConfig = cfg
	}
	cfgCopy := cfg
	b.ecConfigSnapshot.Store(&cfgCopy)
	topology := b.currentTopology()
	b.publishRuntimeSnapshot(topology, cfg)
}

// SetShardService configures the distributed shard service for fan-out.
// allNodes includes all cluster node addresses for placement (self first is
// expected so the self address can be cached before the slice is sorted).
func (b *DistributedBackend) SetShardService(svc *ShardService, allNodes []string) {
	b.shardSvc = svc
	if b.fsm != nil && svc != nil {
		b.fsm.SetEncryptor(svc.encryptor)
	}
	topology := newBackendTopology(allNodes)
	b.selfAddr = topology.selfAddr
	b.allNodes = append([]string(nil), topology.allNodes...)
	b.peerHealth = topology.peerHealth
	b.topologySnapshot.Store(topology)
	b.publishRuntimeSnapshot(*topology, b.currentECConfig())
}

// SetClusterNodes refreshes the configured placement node set without
// replacing the ShardService. Runtime join paths use this after meta-raft
// membership grows so new writes do not stay pinned to boot-time topology.
func (b *DistributedBackend) SetClusterNodes(allNodes []string) {
	topology := newBackendTopology(allNodes)
	b.topologySnapshot.Store(topology)
	b.publishRuntimeSnapshot(*topology, b.currentECConfig())
}

// SetClusterTopology publishes membership and EC config as one immutable
// snapshot. Runtime join paths use this so a request never observes a widened
// placement set with the old shard profile, or vice versa.
func (b *DistributedBackend) SetClusterTopology(allNodes []string, cfg ECConfig) {
	topology := newBackendTopology(allNodes)
	cfgCopy := cfg
	b.topologySnapshot.Store(topology)
	b.ecConfigSnapshot.Store(&cfgCopy)
	b.publishRuntimeSnapshot(*topology, cfg)
}

func newBackendTopology(allNodes []string) *backendTopology {
	// Cache self address BEFORE sorting so per-request self-skip checks can
	// compare raft addresses (node.ID() returns a UUID, not the address).
	selfAddr := ""
	if len(allNodes) > 0 {
		selfAddr = allNodes[0]
	}
	sortedNodes := append([]string(nil), allNodes...)
	sort.Strings(sortedNodes)
	// Build peer list (excluding self) for health tracking
	var peers []string
	for _, n := range allNodes {
		if n != selfAddr {
			peers = append(peers, n)
		}
	}
	peerHealth := NewPeerHealth(peers, 10*time.Second)

	return &backendTopology{
		allNodes:   sortedNodes,
		selfAddr:   selfAddr,
		peerHealth: peerHealth,
	}
}

func (b *DistributedBackend) currentTopology() backendTopology {
	if snapshot := b.runtimeSnapshot.Load(); snapshot != nil {
		if legacy, ok := b.legacyTopologyOverride(snapshot.topology); ok {
			return legacy
		}
		return snapshot.topology
	}
	if snapshot := b.topologySnapshot.Load(); snapshot != nil {
		// Some older unit tests configure private topology fields directly.
		// Production runtime updates only publish snapshots, so prefer the
		// legacy fields only when they clearly carry extra test topology.
		if legacy, ok := b.legacyTopologyOverride(*snapshot); ok {
			return legacy
		}
		return *snapshot
	}
	return backendTopology{
		allNodes:   append([]string(nil), b.allNodes...),
		selfAddr:   b.selfAddr,
		peerHealth: b.peerHealth,
	}
}

func (b *DistributedBackend) legacyTopologyOverride(snapshot backendTopology) (backendTopology, bool) {
	if len(b.allNodes) <= len(snapshot.allNodes) && !(snapshot.selfAddr == "" && b.selfAddr != "") {
		return backendTopology{}, false
	}
	return backendTopology{
		allNodes:   append([]string(nil), b.allNodes...),
		selfAddr:   b.selfAddr,
		peerHealth: b.peerHealth,
	}, true
}

func (b *DistributedBackend) currentSelfAddr() string {
	return b.currentTopology().selfAddr
}

func (b *DistributedBackend) currentPeerHealth() *PeerHealth {
	return b.currentTopology().peerHealth
}

func (b *DistributedBackend) configuredNodeList() []string {
	return append([]string(nil), b.currentTopology().allNodes...)
}

func (b *DistributedBackend) currentECConfig() ECConfig {
	if snapshot := b.runtimeSnapshot.Load(); snapshot != nil {
		return snapshot.ecConfig
	}
	if cfg := b.ecConfigSnapshot.Load(); cfg != nil {
		return *cfg
	}
	return b.ecConfig
}

func (b *DistributedBackend) publishRuntimeSnapshot(topology backendTopology, cfg ECConfig) {
	b.runtimeSnapshot.Store(&backendRuntimeSnapshot{
		topology: topology,
		ecConfig: cfg,
	})
}

// PeerHealth returns the backend's peer-health tracker, or nil if SetShardService
// has not been called yet. Exposed so admin endpoints can surface peer state to
// operators without reaching into private fields.
func (b *DistributedBackend) PeerHealth() *PeerHealth {
	return b.currentPeerHealth()
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
			if b.currentSelfAddr() != "" {
				nodes = append(nodes, b.currentSelfAddr())
			}
			for _, p := range peers {
				if b.currentPeerHealth() == nil || b.currentPeerHealth().IsHealthy(p) {
					nodes = append(nodes, p)
				}
			}
			sort.Strings(nodes)
			return nodes
		}
	}
	return b.configuredNodeList()
}

// clusterNodes returns the configured membership without peer-health filtering.
// EC write activation and placement must be based on membership: transient
// startup probes can mark peers unhealthy before their services are ready, but
// silently falling back to N× for user writes would make later EC reads fail.
func (b *DistributedBackend) clusterNodes() []string {
	if b.node != nil {
		if peers := b.node.Peers(); len(peers) > 0 {
			nodes := make([]string, 0, len(peers)+1)
			if b.currentSelfAddr() != "" {
				nodes = append(nodes, b.currentSelfAddr())
			}
			nodes = append(nodes, peers...)
			sort.Strings(nodes)
			return nodes
		}
	}
	return append([]string(nil), b.configuredNodeList()...)
}

// ecWriteNodes returns the node set used for new EC object placement.
//
// Prefer health-filtered live membership when it can still form an EC stripe:
// after a real node failure, new writes must avoid the dead node and place
// shards across the surviving nodes. If health-filtering drops below the EC
// activation threshold, fall back to configured membership so transient startup
// peerHealth misses do not silently shrink the EC stripe below the configured width.
func (b *DistributedBackend) ecWriteNodes() []string {
	nodes := b.liveNodes()
	if b.currentECConfig().IsActive(len(nodes)) {
		return nodes
	}
	return b.clusterNodes()
}

// TriggerRaftSnapshot forces a Raft FSM snapshot on the current leader.
// As of M5 PR 29 v2 owns snapshot lifecycle exclusively; the apply loop
// forwards through RaftNode.CreateSnapshot (formerly the RaftV2Snapshotter
// interface, folded into RaftNode).
func (b *DistributedBackend) TriggerRaftSnapshot(ctx context.Context) (raft.SnapshotResult, error) {
	if err := ctx.Err(); err != nil {
		return raft.SnapshotResult{}, err
	}
	if !b.node.IsLeader() {
		return raft.SnapshotResult{}, raft.ErrNotLeader
	}
	if b.snapRequests == nil {
		return raft.SnapshotResult{}, fmt.Errorf("raft snapshot apply loop unavailable")
	}
	req := raftSnapshotRequest{
		ctx:  ctx,
		resp: make(chan raftSnapshotResponse, 1),
	}
	select {
	case b.snapRequests <- req:
	case <-ctx.Done():
		return raft.SnapshotResult{}, ctx.Err()
	}
	select {
	case resp := <-req.resp:
		return resp.result, resp.err
	case <-ctx.Done():
		return raft.SnapshotResult{}, ctx.Err()
	}
}

func (b *DistributedBackend) completeRaftSnapshotRequest(req raftSnapshotRequest) {
	result, err := b.triggerRaftSnapshotInApplyLoop(req.ctx)
	select {
	case req.resp <- raftSnapshotResponse{result: result, err: err}:
	case <-req.ctx.Done():
	}
}

// triggerRaftSnapshotInApplyLoop runs inside RunApplyLoop so lastApplied is
// stable. Captures FSM bytes via FSM.Snapshot, then forwards through
// RaftNode.CreateSnapshot which serializes inside v2's actor (compacting the
// log).
func (b *DistributedBackend) triggerRaftSnapshotInApplyLoop(ctx context.Context) (raft.SnapshotResult, error) {
	if err := ctx.Err(); err != nil {
		return raft.SnapshotResult{}, err
	}
	if !b.node.IsLeader() {
		return raft.SnapshotResult{}, raft.ErrNotLeader
	}
	idx := b.lastApplied.Load()
	term := b.lastAppliedTerm.Load()
	if idx == 0 {
		return raft.SnapshotResult{}, fmt.Errorf("raft snapshot unavailable: no applied entries")
	}
	data, err := b.fsm.Snapshot()
	if err != nil {
		return raft.SnapshotResult{}, fmt.Errorf("raft snapshot: %w", err)
	}
	if err := b.node.CreateSnapshot(idx, data); err != nil {
		return raft.SnapshotResult{}, fmt.Errorf("raft create snapshot: %w", err)
	}
	return raft.SnapshotResult{Index: idx, Term: term, SizeBytes: len(data)}, nil
}

// RaftSnapshotStatus reports the latest persisted Raft FSM snapshot via the
// RaftNode interface (folded from the former RaftV2Snapshotter in PR 29).
func (b *DistributedBackend) RaftSnapshotStatus() (raft.SnapshotStatus, error) {
	return b.node.SnapshotStatus()
}

// RegisterCacheInvalidator adds a cache invalidator for committed object mutations.
func (b *DistributedBackend) RegisterCacheInvalidator(id string, inv CacheInvalidator) {
	b.registry.Register(id, inv)
}

// UnregisterCacheInvalidator removes a previously registered cache invalidator.
func (b *DistributedBackend) UnregisterCacheInvalidator(id string) {
	b.registry.Unregister(id)
}

// SetOnApply sets the legacy callback invoked after each FSM apply.
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
		case req := <-b.snapRequests:
			b.completeRaftSnapshotRequest(req)
		case entry, ok := <-b.node.ApplyCh():
			if !ok {
				// ApplyCh closed (Raft node stopped). Exit cleanly so the
				// loop does not spin on zero-value reads. Closing surfaces
				// from the v2 adapter when the underlying actor terminates.
				b.logger.Debug().Msg("apply loop: ApplyCh closed; exiting")
				return
			}
			switch entry.Type {
			case raft.LogEntryCommand:
				applyErr := b.fsm.Apply(entry.Command)
				// Record apply result BEFORE lastApplied.Store so propose
				// loop sees ApplyError(idx) set by the time it observes
				// lastApplied >= idx (race-free linearization).
				b.recordApplyResult(entry.Index, applyErr)
				if applyErr != nil {
					b.logger.Error().Uint64("index", entry.Index).Err(applyErr).Msg("fsm apply error")
				}
				// Notify cache invalidators and legacy metrics callback.
				b.notifyOnApply(entry.Command)
			case raft.LogEntrySnapshot:
				meta := raft.SnapshotMeta{
					Index:         entry.Index,
					Term:          entry.Term,
					Servers:       b.node.Configuration().Servers,
					FormatVersion: raft.FSMSnapshotFormatVersion,
				}
				if err := b.fsm.Restore(meta, entry.Command); err != nil {
					b.logger.Error().Uint64("index", entry.Index).Err(err).Msg("fsm restore snapshot error")
				}
			default:
				continue
			}
			b.lastApplied.Store(entry.Index)
			b.lastAppliedTerm.Store(entry.Term)

		}
	}
}

// notifyOnApply extracts bucket/key from a committed command and invalidates caches.
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
	streamType := transport.StreamProposeForward
	payload := data
	if groupID, ok := PlacementGroupFromContext(ctx); ok {
		streamType = transport.StreamDataGroupProposeForward
		payload = encodeGroupForwardPayload(groupID, data)
	}
	resp, err := b.shardSvc.SendRequest(ctx, leaderAddr, &transport.Message{
		Type:    streamType,
		Payload: payload,
	})
	if err != nil {
		return 0, fmt.Errorf("forwardPropose: send: %w", err)
	}
	index, applyErr, transportErr := decodeProposeForwardReply(resp.Payload)
	if transportErr != nil {
		return 0, fmt.Errorf("forwardPropose: %w", transportErr)
	}
	if applyErr != nil {
		// raft.ErrNotLeader is a propose-time signal — keep the legacy
		// string match so callers can errors.Is() the canonical sentinel.
		if applyErr.Error() == raft.ErrNotLeader.Error() {
			return 0, raft.ErrNotLeader
		}
		return 0, applyErr
	}
	return index, nil
}

// RegisterProposeForwardHandler는 StreamProposeForward 핸들러를 QUIC 라우터에 등록한다.
// 리더 노드에서 호출해야 하며, 팔로워의 propose를 대신 처리한다.
//
// Phase A (Task 16): the leader also waits for the entry to be applied locally
// and harvests any FSM apply error via ApplyError(idx), encoding it on the wire
// as a stable code so the follower can reconstruct the original sentinel via
// decodeApplyError.
func (b *DistributedBackend) RegisterProposeForwardHandler() {
	if b.shardSvc == nil {
		return
	}
	b.shardSvc.RegisterHandler(transport.StreamProposeForward, func(req *transport.Message) *transport.Message {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		idx, err := b.node.ProposeWait(ctx, req.Payload)
		if err == nil {
			// Wait for apply, then surface FSM apply error (if any).
			for b.lastApplied.Load() < idx {
				select {
				case <-ctx.Done():
					err = ctx.Err()
				default:
				}
				if err != nil {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if err == nil {
				if applyErr := b.ApplyError(idx); applyErr != nil {
					err = applyErr
				}
			}
		}
		return &transport.Message{
			Type:    transport.StreamProposeForward,
			Payload: encodeProposeForwardReply(idx, err),
		}
	})
}

// forwardReadIndex sends a StreamReadIndex RPC to leaderAddr and returns the leader's commitIndex.
// Wire: request=[empty], response=[8B commitIndex BE][4B errLen BE][errBytes...]
func (b *DistributedBackend) forwardReadIndex(ctx context.Context, leaderAddr string) (uint64, error) {
	if b.shardSvc == nil {
		return 0, fmt.Errorf("forwardReadIndex: no transport available")
	}
	resp, err := b.shardSvc.SendRequest(ctx, leaderAddr, &transport.Message{
		Type:    transport.StreamReadIndex,
		Payload: []byte{},
	})
	if err != nil {
		return 0, fmt.Errorf("forwardReadIndex: %w", err)
	}
	if len(resp.Payload) < 12 {
		return 0, fmt.Errorf("forwardReadIndex: short response: %d bytes", len(resp.Payload))
	}
	idx := binary.BigEndian.Uint64(resp.Payload[0:8])
	errLen := binary.BigEndian.Uint32(resp.Payload[8:12])
	if errLen > 0 && len(resp.Payload) >= 12+int(errLen) {
		msg := string(resp.Payload[12 : 12+int(errLen)])
		if msg == raft.ErrNotLeader.Error() {
			return 0, raft.ErrNotLeader
		}
		return 0, fmt.Errorf("forwardReadIndex: leader: %s", msg)
	}
	return idx, nil
}

// RegisterReadIndexHandler registers the StreamReadIndex handler on this (leader) node.
// Incoming requests call node.ReadIndex and return the commitIndex to the follower.
func (b *DistributedBackend) RegisterReadIndexHandler() {
	if b.shardSvc == nil {
		return
	}
	b.shardSvc.RegisterHandler(transport.StreamReadIndex, func(req *transport.Message) *transport.Message {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp := make([]byte, 12)
		idx, err := b.node.ReadIndex(ctx)
		if err != nil {
			errBytes := []byte(err.Error())
			binary.BigEndian.PutUint64(resp[0:8], 0)
			binary.BigEndian.PutUint32(resp[8:12], uint32(len(errBytes)))
			resp = append(resp, errBytes...)
		} else {
			binary.BigEndian.PutUint64(resp[0:8], idx)
			binary.BigEndian.PutUint32(resp[8:12], 0)
		}
		return &transport.Message{Type: transport.StreamReadIndex, Payload: resp}
	})
}

// ReadIndex returns a linearizable read fence index.
// On the leader it confirms leadership via heartbeat quorum.
// On a follower it forwards to the leader via StreamReadIndex QUIC RPC.
func (b *DistributedBackend) ReadIndex(ctx context.Context) (uint64, error) {
	var lastErr error
	for {
		idx, err := b.node.ReadIndex(ctx)
		if err == nil {
			return idx, nil
		}
		if !errors.Is(err, raft.ErrNotLeader) {
			return 0, err
		}

		peers := b.node.Peers()
		if len(peers) == 0 {
			return 0, raft.ErrNotLeader
		}
		lastErr = err
		for _, peer := range peers {
			var ci uint64
			ci, lastErr = b.forwardReadIndex(ctx, peer)
			if lastErr == nil {
				return ci, nil
			}
			// A stale leader hint or killed peer can fail with a transport error.
			// Try the rest of the voter set before waiting for the next local
			// ReadIndex/leader-observation cycle.
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			if lastErr != nil {
				return 0, lastErr
			}
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
}

// WaitApplied blocks until this backend's FSM has applied at least index or ctx is done.
func (b *DistributedBackend) WaitApplied(ctx context.Context, index uint64) error {
	if index == 0 {
		return nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if b.lastApplied.Load() >= index {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *DistributedBackend) propose(ctx context.Context, cmdType CommandType, payload any) error {
	if b.groupID != "" {
		if _, ok := PlacementGroupFromContext(ctx); !ok {
			ctx = ContextWithPlacementGroup(ctx, b.groupID)
		}
	}
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
		// Phase A: surface FSM apply errors to the caller. recordApplyResult
		// runs before lastApplied.Store in the apply loop, so by the time we
		// observe lastApplied >= idx the entry (if any) is already set.
		if applyErr := b.ApplyError(idx); applyErr != nil {
			return applyErr
		}
		return nil
	}

	// Follower / edge node: forward to the data-group leader. When the
	// leader hint is known we forward only there; otherwise we fan out to
	// the configured peer set (covers dynamic-join edge nodes that don't
	// know the raft topology yet).
	//
	// A freshly-instantiated multi-voter data group may not have completed
	// its first election by the time a write arrives (raft tick + heartbeat
	// race the very first request to land on a non-leader). Retry on
	// ErrNotLeader with bounded backoff so the propose converges as soon
	// as the election settles rather than failing with a 500.
	proposeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	const retryInterval = 50 * time.Millisecond
	var lastErr error
	for {
		if b.node.IsLeader() {
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
			if applyErr := b.ApplyError(idx); applyErr != nil {
				return applyErr
			}
			return nil
		}
		peers := b.forwardPeersForPropose()
		if len(peers) == 0 {
			lastErr = raft.ErrNotLeader
		} else {
			lastErr = nil
			allNotLeader := true
			for _, peer := range peers {
				idx, err := b.forwardPropose(proposeCtx, peer, data)
				if err == nil {
					_ = idx
					return nil
				}
				lastErr = err
				if !errors.Is(err, raft.ErrNotLeader) {
					allNotLeader = false
				}
			}
			// Preserve the original try-all-peers semantics: a non-ErrNotLeader
			// error from any peer is surfaced only after the full peer list has
			// been attempted, so a transient transport error on peer #1 doesn't
			// mask peer #2 being the actual leader.
			if !allNotLeader {
				return lastErr
			}
		}
		select {
		case <-proposeCtx.Done():
			if lastErr != nil {
				return lastErr
			}
			return proposeCtx.Err()
		case <-time.After(retryInterval):
		}
	}
}

// forwardPeersForPropose returns the preferred set of peers for forwarding a
// follower propose. When the raft layer knows the leader (LeaderID non-empty),
// only the leader is returned to avoid futile round-robin to other followers.
// Otherwise falls back to the full peer set (raft membership when available,
// configured node list otherwise).
func (b *DistributedBackend) forwardPeersForPropose() []string {
	selfAddr := b.currentSelfAddr()
	if leader := b.node.LeaderID(); leader != "" && leader != selfAddr {
		return []string{leader}
	}
	return proposalForwardPeers(b.node.Peers(), b.configuredNodeList(), selfAddr)
}

func proposalForwardPeers(raftPeers, allNodes []string, selfAddr string) []string {
	if len(raftPeers) > 0 {
		return append([]string(nil), raftPeers...)
	}
	if len(allNodes) == 0 {
		return nil
	}
	peers := make([]string, 0, len(allNodes))
	for _, node := range allNodes {
		if node == "" || node == selfAddr {
			continue
		}
		peers = append(peers, node)
	}
	return peers
}

// Close closes the metadata database. When shared is true the DB is owned by
// the caller and Close is a no-op for the DB (only internal state is released).
func (b *DistributedBackend) Close() error {
	// Stop coalesce worker + backstop scanner before tearing down the DB so
	// neither outlives the BadgerDB (would panic on closed-DB reads).
	if b.coalesceCancel != nil {
		b.coalesceCancel()
	}
	if b.coalesce != nil {
		b.coalesce.Stop()
	}
	if b.shared {
		return nil
	}
	return b.db.Close()
}

// GetRegistry returns the cache invalidator registry for registering VFS instances.
func (b *DistributedBackend) GetRegistry() *Registry {
	return b.registry
}

var (
	_ storage.Backend     = (*DistributedBackend)(nil)
	_ storage.PartialIO   = (*DistributedBackend)(nil)
	_ storage.Truncatable = (*DistributedBackend)(nil)
)

// SetBucketAssigner injects the MetaRaft proposer for bucket assignment persistence.
// Must be called before CreateBucket. Nil disables persistence (single-node legacy mode).
func (b *DistributedBackend) SetBucketAssigner(a BucketAssigner) { b.assigner = a }

// SetRouter wires a Router for bucket→group routing used by CreateBucket.
func (b *DistributedBackend) SetRouter(r *Router) { b.router = r }

// SetShardGroupSource wires a ShardGroupSource (typically *MetaFSM) so
// CreateBucket can query the active group list for hash-based assignment.
// Must be called before serving traffic. Nil falls back to Router.RouteKey
// only (legacy default-group behavior).
func (b *DistributedBackend) SetShardGroupSource(s ShardGroupSource) { b.shardGroup = s }

// --- Bucket operations ---

func (b *DistributedBackend) CreateBucket(ctx context.Context, bucket string) error {
	return b.createBucketInternal(ctx, bucket, false)
}

// CreateBucketBypassReserved creates a bucket even when its name is reserved.
// Use only from the bootstrap/seed path. Public API callers must use CreateBucket.
func (b *DistributedBackend) CreateBucketBypassReserved(ctx context.Context, bucket string) error {
	return b.createBucketInternal(ctx, bucket, true)
}

func (b *DistributedBackend) createBucketInternal(ctx context.Context, bucket string, bypassReserved bool) error {
	// Check if already exists (read local)
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(b.ks().BucketKey(bucket))
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
		// Determine target group:
		//   1. If meta-FSM has an explicit assignment (e.g., from rebalance), preserve it.
		//   2. Else, hash-assign across active groups (if shardGroup wired).
		//   3. Else, fall back to the router's default group (single-group / test deployments).
		groupID := ""
		if gid, ok := b.router.ExplicitGroup(bucket); ok {
			groupID = gid
		}
		if groupID == "" && b.shardGroup != nil {
			entries := b.shardGroup.ShardGroups()
			if group, selErr := SelectObjectPlacementGroup(bucket, "", entries, b.currentECConfig()); selErr == nil {
				groupID = group.ID
			} else {
				ids := make([]string, 0, len(entries))
				for _, e := range entries {
					ids = append(ids, e.ID)
				}
				sort.Strings(ids) // deterministic legacy fallback
				groupID = HashAssign(bucket, ids)
			}
		}
		if groupID == "" {
			if dg, routeErr := b.router.RouteKey(bucket, ""); routeErr == nil {
				groupID = dg.ID()
			}
		}
		if groupID == "" {
			return fmt.Errorf("create bucket %q: no active groups for assignment", bucket)
		}
		if propErr := b.assigner.ProposeBucketAssignment(ctx, bucket, groupID); propErr != nil {
			return fmt.Errorf("propose bucket assignment: %w", propErr)
		}
	}

	return b.propose(ctx, CmdCreateBucket, CreateBucketCmd{Bucket: bucket, BypassReserved: bypassReserved})
}

func (b *DistributedBackend) HeadBucket(ctx context.Context, bucket string) error {
	if b.bypassBucketCheck {
		return nil
	}
	return b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(b.ks().BucketKey(bucket))
		if err == badger.ErrKeyNotFound {
			return storage.ErrBucketNotFound
		}
		return err
	})
}

func (b *DistributedBackend) DeleteBucket(ctx context.Context, bucket string) error {
	// Check existence and emptiness
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(b.ks().BucketKey(bucket))
		if err == badger.ErrKeyNotFound {
			return storage.ErrBucketNotFound
		}
		if err != nil {
			return err
		}

		prefix := b.ks().Prefix([]byte("obj:" + bucket + "/"))
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

	return b.propose(ctx, CmdDeleteBucket, DeleteBucketCmd{Bucket: bucket})
}

// ForceDeleteBucket deletes all objects in the bucket and then removes it.
// Unlike DeleteBucket, it does not fail when the bucket is non-empty.
//
// Scans all obj:<bucket>/ keys directly (not via WalkObjects) so that older
// versions of multi-version objects are collected too. WalkObjects only returns
// the latest version per key; skipping older versions would leave their Badger
// keys behind, causing DeleteBucket to still see them and return ErrBucketNotEmpty.
func (b *DistributedBackend) ForceDeleteBucket(ctx context.Context, bucket string) error {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	// Collect all obj: refs first so the Badger View is closed before any
	// Raft propose. Calling propose inside db.View holds the MVCC snapshot for
	// N×RTT and blocks Badger GC.
	type objRef struct {
		key       string
		versionID string // empty for legacy unversioned keys
	}
	var refs []objRef
	if err := b.db.View(func(txn *badger.Txn) error {
		// Build latMap so we can distinguish versioned sub-keys from unversioned
		// legacy keys. A key of the form obj:<bucket>/<base>/<vid> is a versioned
		// object iff <base> appears in latMap (i.e. lat:<bucket>/<base> exists)
		// AND <vid> is a valid UUID (all version IDs are UUID v4/v7). The UUID
		// check prevents misclassifying a legacy key like "a/b" as key="a"
		// versionID="b" when a versioned key "a" happens to share its prefix.
		latMap := make(map[string]struct{})
		rawLatPfx := []byte("lat:" + bucket + "/")
		latPfx := b.ks().Prefix(rawLatPfx)
		itLat := txn.NewIterator(badger.DefaultIteratorOptions)
		for itLat.Seek(latPfx); itLat.ValidForPrefix(latPfx); itLat.Next() {
			rawK := b.ks().MustStrip(itLat.Item().Key())
			latMap[string(rawK[len(rawLatPfx):])] = struct{}{}
		}
		itLat.Close()

		rawBucketPfx := []byte("obj:" + bucket + "/")
		pfx := b.ks().Prefix(rawBucketPfx)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			rawK := b.ks().MustStrip(it.Item().Key())
			rest := string(rawK[len(rawBucketPfx):])
			key, versionID := rest, ""
			if slash := strings.LastIndex(rest, "/"); slash >= 0 {
				candidateBase := rest[:slash]
				candidateVID := rest[slash+1:]
				if _, inLat := latMap[candidateBase]; inLat {
					if _, err := uuid.Parse(candidateVID); err == nil {
						key, versionID = candidateBase, candidateVID
					}
				}
			}
			refs = append(refs, objRef{key: key, versionID: versionID})
		}
		return nil
	}); err != nil {
		return fmt.Errorf("force delete: scan objects: %w", err)
	}
	// Two-pass deletion to prevent ring refcount double-decRef:
	//
	// Pass 1 — versioned refs first. applyDeleteObjectVersion calls decRef(rv)
	// for each version's ring. When the last versioned ref for a key is removed,
	// applyDeleteObjectVersion also deletes the unversioned ObjectMetaKey, so
	// Pass 2 finds it absent and skips decRef.
	//
	// Pass 2 — unversioned refs. applyDeleteObject("") only calls decRef if
	// ObjectMetaKey still exists. If Pass 1 already removed it, rv stays 0 and
	// decRef is not called, preventing a double-decRef of the ring refcount.
	for _, ref := range refs {
		if ref.versionID == "" {
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := b.forceDeleteObject(ctx, bucket, ref.key, ref.versionID); err != nil {
			return fmt.Errorf("force delete: %q: %w", ref.key, err)
		}
	}
	for _, ref := range refs {
		if ref.versionID != "" {
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := b.forceDeleteObject(ctx, bucket, ref.key, ref.versionID); err != nil {
			return fmt.Errorf("force delete: %q: %w", ref.key, err)
		}
	}
	return b.DeleteBucket(ctx, bucket)
}

// forceDeleteObject hard-deletes one Badger record for a single object without
// creating a tombstone. Used only by ForceDeleteBucket.
//
// For versioned objects (versionID != ""): removes the versioned obj: key via
// CmdDeleteObjectVersion. applyDeleteObjectVersion promotes the next-oldest
// version to latest, or removes lat:/legacy obj: keys when the last version is
// gone — so the final CmdDeleteObjectVersion call on each key leaves no traces.
// For legacy unversioned objects (versionID == ""): CmdDeleteObject with empty
// VersionID hard-deletes the unversioned obj: key (no tombstone written).
func (b *DistributedBackend) forceDeleteObject(ctx context.Context, bucket, key, versionID string) error {
	if versionID != "" {
		_ = os.Remove(b.objectPathV(bucket, key, versionID))
		return b.propose(ctx, CmdDeleteObjectVersion, DeleteObjectVersionCmd{
			Bucket:    bucket,
			Key:       key,
			VersionID: versionID,
		})
	}
	// Legacy unversioned key: hard-delete, no tombstone.
	_ = os.Remove(b.objectPath(bucket, key))
	return b.propose(ctx, CmdDeleteObject, DeleteObjectCmd{
		Bucket:    bucket,
		Key:       key,
		VersionID: "", // empty = legacy hard delete, no tombstone
	})
}

// SetBucketVersioning satisfies server.BucketVersioner. Replicates the
// versioning state change through Raft so all cluster nodes apply it atomically.
func (b *DistributedBackend) SetBucketVersioning(bucket, state string) error {
	ctx := context.Background()
	// Pre-check: verify bucket exists locally before proposing. The FSM also
	// checks, but propose() does not propagate FSM errors back to the caller.
	// This is the single-DistributedBackend path (tests, single-node EC
	// setups). The coordinator-driven cluster path uses
	// SetBucketVersioningPropose to bypass this local check after running its
	// own cluster-aware HeadBucket.
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	return b.SetBucketVersioningPropose(bucket, state)
}

// SetBucketVersioningPropose is the coordinator-facing entrypoint: it skips
// the local bucket-existence pre-check because the coordinator has already
// run a cluster-aware HeadBucket. On a freshly bootstrapped cluster a
// follower may have the meta-Raft bucket assignment without having applied
// the data-Raft CmdCreateBucket entry locally; calling SetBucketVersioning
// from that follower would falsely reject the request with NoSuchBucket.
func (b *DistributedBackend) SetBucketVersioningPropose(bucket, state string) error {
	return b.propose(context.Background(), CmdSetBucketVersioning, SetBucketVersioningCmd{
		Bucket: bucket,
		State:  state,
	})
}

// SetBucketPolicy satisfies storage.PolicyBackend. The policy document is
// replicated through Raft so every node observes the same bucket policy.
func (b *DistributedBackend) SetBucketPolicy(bucket string, policyJSON []byte) error {
	ctx := context.Background()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	return b.SetBucketPolicyPropose(bucket, policyJSON)
}

// SetBucketPolicyPropose is the coordinator-facing entrypoint: it skips the
// local bucket-existence pre-check after the coordinator has run a
// cluster-aware HeadBucket.
func (b *DistributedBackend) SetBucketPolicyPropose(bucket string, policyJSON []byte) error {
	ctx := context.Background()
	return b.propose(ctx, CmdSetBucketPolicy, SetBucketPolicyCmd{
		Bucket:     bucket,
		PolicyJSON: append([]byte(nil), policyJSON...),
	})
}

// GetBucketPolicy satisfies storage.PolicyBackend. Reads use the local
// FSM-consistent view; writes flow through Raft.
func (b *DistributedBackend) GetBucketPolicy(bucket string) ([]byte, error) {
	var data []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.ks().BucketPolicyKey(bucket))
		if err == badger.ErrKeyNotFound {
			return storage.ErrBucketNotFound
		}
		if err != nil {
			return err
		}
		data, err = b.itemValueCopy(item)
		return err
	})
	return data, err
}

// DeleteBucketPolicy satisfies storage.PolicyBackend.
func (b *DistributedBackend) DeleteBucketPolicy(bucket string) error {
	ctx := context.Background()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	return b.DeleteBucketPolicyPropose(bucket)
}

// DeleteBucketPolicyPropose is the coordinator-facing entrypoint.
func (b *DistributedBackend) DeleteBucketPolicyPropose(bucket string) error {
	ctx := context.Background()
	return b.propose(ctx, CmdDeleteBucketPolicy, DeleteBucketPolicyCmd{Bucket: bucket})
}

// SetObjectACL satisfies storage.ACLSetter. Replicates the ACL change through
// Raft and updates the stored objectMeta on every node.
func (b *DistributedBackend) SetObjectACL(bucket, key string, acl uint8) error {
	ctx := context.Background()
	// Pre-check: verify object exists locally before proposing.
	if _, err := b.HeadObject(ctx, bucket, key); err != nil {
		return err
	}
	return b.propose(ctx, CmdSetObjectACL, SetObjectACLCmd{
		Bucket: bucket,
		Key:    key,
		ACL:    acl,
	})
}

// SetObjectTags satisfies storage.ObjectTagsSetter. Replicates the tag
// mutation through Raft so every replica converges on the same tag set.
// VersionID="" targets the current version; VersionID!="" targets a
// specific version. Passing nil tags clears the tag set. Does not modify
// ETag, LastModified, ACL, or blob bytes.
func (b *DistributedBackend) SetObjectTags(bucket, key, versionID string, tags []storage.Tag) error {
	ctx := context.Background()
	// Pre-check: object must exist locally before we propose. Mirrors SetObjectACL.
	if _, err := b.HeadObject(ctx, bucket, key); err != nil {
		return err
	}
	return b.propose(ctx, CmdSetObjectTags, SetObjectTagsCmd{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
		Tags:      tags,
	})
}

// GetObjectTags satisfies storage.ObjectTagsGetter. Reads from the local
// FSM-consistent view; writes flow through Raft and replicate to every
// node, so the local view is always current modulo replication lag.
func (b *DistributedBackend) GetObjectTags(bucket, key, versionID string) ([]storage.Tag, error) {
	var result []storage.Tag
	err := b.db.View(func(txn *badger.Txn) error {
		dbKey := b.ks().ObjectMetaKey(bucket, key)
		if versionID != "" {
			dbKey = b.ks().ObjectMetaKeyV(bucket, key, versionID)
		}
		item, err := txn.Get(dbKey)
		if err == badger.ErrKeyNotFound {
			return storage.ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		val, err := b.itemValueCopy(item)
		if err != nil {
			return err
		}
		m, err := unmarshalObjectMeta(val)
		if err != nil {
			return err
		}
		if len(m.Tags) > 0 {
			result = append([]storage.Tag(nil), m.Tags...)
		}
		return nil
	})
	return result, err
}

// GetBucketVersioning satisfies server.BucketVersioner. Returns "Unversioned"
// when no state has been set so the S3 semantic matches ECBackend's default.
func (b *DistributedBackend) GetBucketVersioning(bucket string) (string, error) {
	var state string
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.ks().BucketVerKey(bucket))
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

func (b *DistributedBackend) ListBuckets(ctx context.Context) ([]string, error) {
	var buckets []string
	err := b.db.View(func(txn *badger.Txn) error {
		return b.ks().scanGroupPrefix(txn, []byte("bucket:"), func(rawKey []byte, item *badger.Item) error {
			name := strings.TrimPrefix(string(rawKey), "bucket:")
			buckets = append(buckets, name)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(buckets)
	return buckets, nil
}

// --- Object operations ---

func (b *DistributedBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	return b.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, nil)
}

func (b *DistributedBackend) PutObjectWithUserMetadata(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*storage.Object, error) {
	return b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:       bucket,
		Key:          key,
		Body:         r,
		ContentType:  contentType,
		UserMetadata: userMetadata,
	})
}

func (b *DistributedBackend) PutObjectWithRequest(ctx context.Context, req storage.PutObjectRequest) (*storage.Object, error) {
	bucket, key, r, contentType := req.Bucket, req.Key, req.Body, req.ContentType
	userMetadata := req.UserMetadata
	sseAlgorithm := req.SystemMetadata.SSEAlgorithm
	stageStart := time.Now()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	observePutStage("distributed", "head_bucket", stageStart)

	stageStart = time.Now()
	if blocked, q, qerr := b.isObjectQuarantined(bucket, key, ""); qerr != nil {
		return nil, fmt.Errorf("check quarantine: %w", qerr)
	} else if blocked {
		return nil, objectQuarantinedError(bucket, key, q)
	}
	observePutStage("distributed", "quarantine_check", stageStart)

	versionID := newVersionID()
	if b.currentECConfig().NumShards() != 0 && b.shardSvc != nil {
		obj, handled, err := b.tryPutObjectSingleLocalShardInMemory(ctx, bucket, key, versionID, r, contentType, userMetadata, sseAlgorithm)
		if handled || err != nil {
			return obj, err
		}
		obj, handled, err = b.tryPutObjectSingleLocalShardKnownSize(ctx, bucket, key, versionID, r, req.SizeHint, contentType, userMetadata, sseAlgorithm)
		if handled || err != nil {
			return obj, err
		}
		obj, handled, err = b.tryPutObjectECDataInMemory(ctx, bucket, key, versionID, r, contentType, userMetadata, sseAlgorithm)
		if handled || err != nil {
			return obj, err
		}
	}

	stageStart = time.Now()
	sp, err := b.spoolPutObject(ctx, bucket, r)
	if err != nil {
		return nil, err
	}
	observePutStage("distributed", "spool_object", stageStart)
	defer sp.Cleanup()

	if b.currentECConfig().NumShards() == 0 || b.shardSvc == nil {
		return nil, fmt.Errorf("put object: EC storage is required")
	}
	return b.putObjectECSpooled(ctx, bucket, key, versionID, sp, contentType, userMetadata, sseAlgorithm)
}

func (b *DistributedBackend) tryPutObjectSingleLocalShardInMemory(
	ctx context.Context,
	bucket, key, versionID string,
	r io.Reader,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
) (*storage.Object, bool, error) {
	placementGroupID, ok := PlacementGroupFromContext(ctx)
	if !ok {
		if b.bypassBucketCheck {
			return nil, false, nil
		}
		placementGroupID = "group-0"
	}
	liveNodes := b.ecWriteNodes()
	effectiveCfg := EffectiveConfig(len(liveNodes), b.currentECConfig())
	if effectiveCfg.NumShards() == 0 && !b.bypassBucketCheck {
		effectiveCfg = AutoECConfigForClusterSize(len(liveNodes))
	}
	shardKey := ecObjectShardKey(key, versionID)
	placement := selectECPlacement(effectiveCfg, liveNodes, shardKey)
	if group, cfg, err := placementTargetsFromContext(ctx, "put_object"); err == nil {
		placementGroupID = group.ID
		effectiveCfg = cfg
		placement = cloneStringSlice(group.PeerIDs[:cfg.NumShards()])
	} else if PlacementGroupHasFullEntry(ctx) {
		return nil, false, err
	}
	if effectiveCfg.DataShards != 1 || effectiveCfg.ParityShards != 0 {
		return nil, false, nil
	}
	if len(placement) != 1 || placement[0] != b.currentSelfAddr() {
		return nil, false, nil
	}

	stageStart := time.Now()
	body, size, ok := sizedReaderAtSection(r, maxSingleLocalShardMemoryFastPathBytes)
	if !ok {
		return nil, false, nil
	}
	observePutStage("ec_single_memory", "read_body", stageStart)

	sp := &spooledObject{
		Size: size,
	}
	h := md5.New()
	obj, err := b.putObjectSingleLocalShardFromReader(
		ctx,
		bucket,
		key,
		versionID,
		placementGroupID,
		placement,
		sp,
		body,
		contentType,
		userMetadata,
		sseAlgorithm,
		"ec_single_memory",
		h,
		nil,
		nil,
		"",
	)
	return obj, true, err
}

func (b *DistributedBackend) tryPutObjectSingleLocalShardKnownSize(
	ctx context.Context,
	bucket, key, versionID string,
	r io.Reader,
	sizeHint *int64,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
) (*storage.Object, bool, error) {
	if sizeHint == nil || *sizeHint < 0 {
		return nil, false, nil
	}
	placementGroupID, ok := PlacementGroupFromContext(ctx)
	if !ok {
		if b.bypassBucketCheck {
			return nil, false, nil
		}
		placementGroupID = "group-0"
	}
	liveNodes := b.ecWriteNodes()
	effectiveCfg := EffectiveConfig(len(liveNodes), b.currentECConfig())
	if effectiveCfg.NumShards() == 0 && !b.bypassBucketCheck {
		effectiveCfg = AutoECConfigForClusterSize(len(liveNodes))
	}
	shardKey := ecObjectShardKey(key, versionID)
	placement := selectECPlacement(effectiveCfg, liveNodes, shardKey)
	if group, cfg, err := placementTargetsFromContext(ctx, "put_object"); err == nil {
		placementGroupID = group.ID
		effectiveCfg = cfg
		placement = cloneStringSlice(group.PeerIDs[:cfg.NumShards()])
	} else if PlacementGroupHasFullEntry(ctx) {
		return nil, false, err
	}
	if effectiveCfg.DataShards != 1 || effectiveCfg.ParityShards != 0 {
		return nil, false, nil
	}
	if len(placement) != 1 || placement[0] != b.currentSelfAddr() {
		return nil, false, nil
	}

	sp := &spooledObject{
		Size: *sizeHint,
	}
	obj, err := b.putObjectSingleLocalShardFromReader(
		ctx,
		bucket,
		key,
		versionID,
		placementGroupID,
		placement,
		sp,
		r,
		contentType,
		userMetadata,
		sseAlgorithm,
		"ec_single_stream",
		md5.New(),
		nil,
		nil,
		"",
	)
	return obj, true, err
}

type sizedReaderAt interface {
	io.ReaderAt
	Len() int
	Size() int64
}

func sizedReaderAtSection(r io.Reader, maxBytes int64) (io.Reader, int64, bool) {
	sr, ok := r.(sizedReaderAt)
	if !ok {
		return nil, 0, false
	}
	remaining := int64(sr.Len())
	if remaining < 0 || remaining > maxBytes {
		return nil, 0, false
	}
	offset := sr.Size() - remaining
	return io.NewSectionReader(sr, offset, remaining), remaining, true
}

func (b *DistributedBackend) tryPutObjectECDataInMemory(
	ctx context.Context,
	bucket, key, versionID string,
	r io.Reader,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
) (*storage.Object, bool, error) {
	limit, ok := b.ecMemoryShardFastPathLimit(ctx)
	if !ok {
		return nil, false, nil
	}
	body, size, ok := sizedReaderAtSection(r, limit)
	if !ok {
		return nil, false, nil
	}
	// Pre-size the buffer from the known body length: avoids the geometric
	// growth churn of io.ReadAll (top alloc_space contributor for this path
	// in iter1 pprof) and skips zeroing the unused tail.
	data := make([]byte, size)
	if _, err := io.ReadFull(body, data); err != nil {
		return nil, true, fmt.Errorf("read small EC object: %w", err)
	}

	obj, err := b.putObjectECData(ctx, bucket, key, versionID, data, contentType, userMetadata, sseAlgorithm)
	clear(data)
	return obj, true, err
}

// ecMemoryShardFastPathLimit returns the maximum body size (in bytes) for which
// the in-memory EC fast path may be used in this request, or ok=false when the
// fast path is disabled for this placement.
func (b *DistributedBackend) ecMemoryShardFastPathLimit(ctx context.Context) (int64, bool) {
	if _, cfg, err := placementTargetsFromContext(ctx, "put_object"); err == nil {
		if !ecMemoryShardFastPathEnabled(cfg) {
			return 0, false
		}
		return maxECMemoryShardFastPathBytesForCfg(cfg), true
	} else if PlacementGroupHasFullEntry(ctx) {
		return 0, false
	}
	liveNodes := b.ecWriteNodes()
	cfg := EffectiveConfig(len(liveNodes), b.currentECConfig())
	if cfg.NumShards() == 0 && !b.bypassBucketCheck {
		cfg = AutoECConfigForClusterSize(len(liveNodes))
	}
	if !ecMemoryShardFastPathEnabled(cfg) {
		return 0, false
	}
	return maxECMemoryShardFastPathBytesForCfg(cfg), true
}

func ecMemoryShardFastPathEnabled(cfg ECConfig) bool {
	return cfg.NumShards() != 0
}

// PutObjectAsync is the write-back variant of PutObject.
// It delegates to putObjectECSpooled and returns a no-op commitFn for API
// compatibility with callers that batch commitFns (e.g., block_io_executor).
func (b *DistributedBackend) PutObjectAsync(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, func() error, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, nil, err
	}
	sp, err := b.spoolPutObject(ctx, bucket, r)
	if err != nil {
		return nil, nil, err
	}
	defer sp.Cleanup()
	versionID := newVersionID()
	if b.currentECConfig().NumShards() == 0 || b.shardSvc == nil {
		return nil, nil, fmt.Errorf("put object async: EC storage is required")
	}
	obj, err := b.putObjectECSpooled(ctx, bucket, key, versionID, sp, contentType, nil, "")
	return obj, func() error { return nil }, err
}

// WriteAt implements a pwrite-based fast path for internal buckets (NFS4 and
// VFS). It avoids the full-file RMW cost by calling pwrite(2) directly on the
// versioned path, then updating BadgerDB metadata without a Raft propose.
//
// Constraints: single-node only — no peer replication, no EC. Callers MUST
// check IsInternalBucket before relying on this; ErrNotSupported is returned
// for user-facing buckets.
func (b *DistributedBackend) WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*storage.Object, error) {
	if !storage.IsInternalBucket(bucket) {
		return nil, fmt.Errorf("WriteAt not supported for user bucket %q", bucket)
	}
	if b.encryptedShardStorage() {
		return nil, fmt.Errorf("WriteAt not supported for encrypted shard storage")
	}

	var tStart, tStage time.Time
	if distributedWriteAtTraceEnabled {
		tStart = time.Now()
		tStage = tStart
	}

	cacheKey := internalObjectCacheKey{bucket: bucket, key: key}
	objPath := b.currentInternalObjectPath(bucket, key)
	if err := b.ensureInternalObjectDir(objPath.dir); err != nil {
		return nil, fmt.Errorf("create object dir: %w", err)
	}
	if distributedWriteAtTraceEnabled {
		log.Debug().Dur("ensure_dir", time.Since(tStage)).Str("bucket", bucket).Msg("Distributed WriteAt trace")
		tStage = time.Now()
	}

	f, err := os.OpenFile(objPath.path, os.O_CREATE|os.O_RDWR, 0o644)
	if errors.Is(err, os.ErrNotExist) {
		b.internalDirCache.Delete(objPath.dir)
		if derr := b.ensureInternalObjectDir(objPath.dir); derr != nil {
			return nil, fmt.Errorf("create object dir: %w", derr)
		}
		f, err = os.OpenFile(objPath.path, os.O_CREATE|os.O_RDWR, 0o644)
	}
	if err != nil {
		return nil, fmt.Errorf("open object: %w", err)
	}
	defer f.Close()
	if distributedWriteAtTraceEnabled {
		log.Debug().Dur("open", time.Since(tStage)).Msg("Distributed WriteAt trace")
		tStage = time.Now()
	}

	if _, err = f.WriteAt(data, int64(offset)); err != nil {
		return nil, fmt.Errorf("pwrite object: %w", err)
	}
	if distributedWriteAtTraceEnabled {
		log.Debug().Dur("pwrite", time.Since(tStage)).Int("bytes", len(data)).Msg("Distributed WriteAt trace")
		tStage = time.Now()
	}

	newSize := int64(offset) + int64(len(data))
	if cached, ok := b.internalSizeCache.Load(cacheKey); ok {
		if size := cached.(int64); size > newSize {
			newSize = size
		}
	} else {
		fi, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat object: %w", err)
		}
		newSize = fi.Size()
	}
	if distributedWriteAtTraceEnabled {
		log.Debug().Dur("size_lookup", time.Since(tStage)).Int64("object_size", newSize).Msg("Distributed WriteAt trace")
		tStage = time.Now()
	}
	b.internalSizeCache.Store(cacheKey, newSize)
	now := time.Now().Unix()

	// Full-file MD5 after every pwrite makes NFS/NBD append workloads O(n^2).
	// Keep an ETag oracle only when this call overwrites the full object from
	// byte zero; scrubber treats empty ETags as "no oracle" and skips them.
	etag := writeAtETag(data, offset, newSize)

	meta, err := marshalObjectMeta(objectMeta{
		Key:          key,
		Size:         newSize,
		ContentType:  "application/octet-stream",
		ETag:         etag,
		LastModified: now,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal object meta: %w", err)
	}
	if err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(objPath.metaKey, meta)
	}); err != nil {
		return nil, fmt.Errorf("update object meta: %w", err)
	}
	if distributedWriteAtTraceEnabled {
		log.Debug().Dur("badger_update", time.Since(tStage)).Dur("total", time.Since(tStart)).Msg("Distributed WriteAt trace")
	}

	return &storage.Object{
		Key:          key,
		Size:         newSize,
		ContentType:  "application/octet-stream",
		ETag:         etag,
		LastModified: now,
		VersionID:    "current",
	}, nil
}

// writeAtETag computes an xxhash3 ETag when a WriteAt call overwrites the
// whole object. Partial writes return an empty ETag so scrubber skips the
// object instead of forcing every NFS/NBD write to re-read the full file.
//
// Empty ETag contract: scrubber's ReplicationVerifier reports such blocks as
// Skipped (not Corrupt) — see internal/scrubber/replication.go and
// TestReplicationVerifier_LegacyETagSkipped. Trade-off: files that have only
// ever been touched via partial writes lose the per-block oracle; EC
// parity still detects shard-level corruption on read.
func writeAtETag(data []byte, offset uint64, size int64) string {
	if offset == 0 && int64(len(data)) == size {
		return storage.InternalETag(data)
	}
	return ""
}

// ReadAt implements partial object reads. Internal buckets use the historical
// direct pread path. EC user buckets read only the data shard segments that
// overlap the requested byte range when those data shards are available.
func (b *DistributedBackend) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("ReadAt negative offset %d", offset)
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if storage.IsInternalBucket(bucket) {
		f, err := os.Open(b.currentInternalObjectPath(bucket, key).path)
		if err == nil {
			defer f.Close()
			return f.ReadAt(buf, offset)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
	}

	obj, placementMeta, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return 0, err
	}
	if blocked, q, qerr := b.isObjectQuarantined(bucket, key, obj.VersionID); qerr != nil {
		return 0, fmt.Errorf("check quarantine: %w", qerr)
	} else if blocked {
		return 0, objectQuarantinedError(bucket, key, q)
	}
	return b.readAtPreparedObject(ctx, bucket, key, obj, placementMeta, offset, buf)
}

func (b *DistributedBackend) ReadAtObject(ctx context.Context, bucket, key string, obj *storage.Object, offset int64, buf []byte) (int, error) {
	if obj == nil {
		return b.ReadAt(ctx, bucket, key, offset, buf)
	}
	if obj.Key != "" && obj.Key != key {
		return 0, fmt.Errorf("ReadAt object key mismatch: got %q, want %q", obj.Key, key)
	}
	if offset < 0 {
		return 0, fmt.Errorf("ReadAt negative offset %d", offset)
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if storage.IsInternalBucket(bucket) {
		return b.ReadAt(ctx, bucket, key, offset, buf)
	}
	placementMeta := PlacementMeta{
		VersionID:        obj.VersionID,
		ECData:           obj.ECData,
		ECParity:         obj.ECParity,
		NodeIDs:          obj.NodeIDs,
		PlacementGroupID: obj.PlacementGroupID,
	}
	if !obj.IsAppendable && len(obj.Segments) == 0 && placementMeta.ECData == 0 && len(placementMeta.NodeIDs) == 0 {
		return b.ReadAt(ctx, bucket, key, offset, buf)
	}
	if blocked, q, qerr := b.isObjectQuarantined(bucket, key, obj.VersionID); qerr != nil {
		return 0, fmt.Errorf("check quarantine: %w", qerr)
	} else if blocked {
		return 0, objectQuarantinedError(bucket, key, q)
	}
	return b.readAtPreparedObject(ctx, bucket, key, obj, placementMeta, offset, buf)
}

func (b *DistributedBackend) readAtPreparedObject(ctx context.Context, bucket, key string, obj *storage.Object, placementMeta PlacementMeta, offset int64, buf []byte) (int, error) {
	if offset >= obj.Size {
		return 0, io.EOF
	}
	if max := obj.Size - offset; int64(len(buf)) > max {
		buf = buf[:max]
	}

	// Appendable objects: dispatch via the stitched reader's range path so we
	// only read the chunks that intersect [offset, offset+len(buf)). The
	// generic EC ResolvePlacement returns ErrNotEC for appendables (no
	// placement record); without this fast path ReadAt falls back to a full
	// GET + discard which negates range-read efficiency.
	if obj.IsAppendable && (len(obj.Segments) > 0 || len(obj.Coalesced) > 0) && obj.Size > 0 {
		return b.readAtAppendable(ctx, bucket, key, obj, offset, buf)
	}
	if !obj.IsAppendable && len(obj.Segments) > 0 && obj.Size > 0 {
		store := &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}
		return readAtChunkedSegments(ctx, store, obj.Segments, offset, buf)
	}

	if b.shardSvc != nil {
		resolved, rerr := b.ResolvePlacement(ctx, bucket, key, placementMeta)
		if rerr == nil {
			n, ecErr := b.readObjectECAtShardKey(ctx, bucket, resolved.ShardKey, resolved.Record, obj.Size, offset, buf)
			if ecErr == nil {
				return n, nil
			}
			return b.readAtViaGetObject(ctx, bucket, key, offset, buf)
		}
		if !errors.Is(rerr, ErrNotEC) {
			return 0, fmt.Errorf("resolve placement for %s/%s: %w", bucket, key, rerr)
		}
	}

	if obj.VersionID != "" {
		if f, oerr := b.openObjectIfSizeMatches(b.objectPathV(bucket, key, obj.VersionID), obj); oerr == nil {
			defer f.Close()
			return f.ReadAt(buf, offset)
		}
	}
	if f, oerr := b.openObjectIfSizeMatches(b.objectPath(bucket, key), obj); oerr == nil {
		defer f.Close()
		return f.ReadAt(buf, offset)
	}
	return b.readAtViaGetObject(ctx, bucket, key, offset, buf)
}

func (b *DistributedBackend) PreferReadAt(bucket string) bool {
	return true
}

func (b *DistributedBackend) PreferWriteAt(bucket string) bool {
	if !storage.IsInternalBucket(bucket) {
		return false
	}
	if b.encryptedShardStorage() {
		return false
	}
	nodes := b.liveNodes()
	if len(nodes) <= 1 {
		return true
	}
	unique := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		unique[node] = struct{}{}
	}
	return len(unique) <= 1
}

// Truncate implements the internal-bucket fast path used by NFS SETATTR size.
// It updates the fixed "current" object and metadata in place, avoiding the
// full-object read/append/write fallback used by generic object stores.
func (b *DistributedBackend) Truncate(ctx context.Context, bucket, key string, size int64) error {
	if !storage.IsInternalBucket(bucket) {
		return fmt.Errorf("Truncate not supported for user bucket %q", bucket)
	}
	if b.encryptedShardStorage() {
		return fmt.Errorf("Truncate not supported for encrypted shard storage")
	}
	if size < 0 {
		return fmt.Errorf("truncate: negative size %d", size)
	}
	cacheKey := internalObjectCacheKey{bucket: bucket, key: key}
	objPath := b.currentInternalObjectPath(bucket, key)
	if err := b.ensureInternalObjectDir(objPath.dir); err != nil {
		return fmt.Errorf("create object dir: %w", err)
	}
	f, err := os.OpenFile(objPath.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open object: %w", err)
	}
	if err := f.Truncate(size); err != nil {
		_ = f.Close()
		return fmt.Errorf("truncate object: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close object: %w", err)
	}
	b.internalSizeCache.Store(cacheKey, size)

	now := time.Now().Unix()
	meta, err := marshalObjectMeta(objectMeta{
		Key:          key,
		Size:         size,
		ContentType:  "application/octet-stream",
		LastModified: now,
	})
	if err != nil {
		return fmt.Errorf("marshal object meta: %w", err)
	}
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(objPath.metaKey, meta)
	})
}

func (b *DistributedBackend) encryptedShardStorage() bool {
	return b.shardSvc != nil && b.shardSvc.encryptor != nil
}

// selectECPlacement selects shard placement for shardKey using rendezvous hashing over liveNodes.
func selectECPlacement(cfg ECConfig, liveNodes []string, shardKey string) []string {
	return PlaceShards(shardKey, liveNodes, nil, cfg.NumShards())
}

func placementTargetsFromContext(ctx context.Context, operation string) (ShardGroupEntry, ECConfig, error) {
	group, ok := PlacementGroupEntryFromContext(ctx)
	if !ok {
		groupID, _ := PlacementGroupFromContext(ctx)
		return ShardGroupEntry{}, ECConfig{}, &ErrInsufficientPlacementTargets{
			Operation:     operation,
			GroupID:       groupID,
			FailureReason: "full placement group not present in write context",
		}
	}
	cfg := DesiredECConfigForGroup(group)
	if cfg.NumShards() == 0 {
		return ShardGroupEntry{}, ECConfig{}, &ErrInsufficientPlacementTargets{
			Operation:     operation,
			GroupID:       group.ID,
			Configured:    cloneStringSlice(group.PeerIDs),
			FailureReason: "placement group has no zero-config EC profile",
		}
	}
	if len(group.PeerIDs) < cfg.NumShards() {
		return ShardGroupEntry{}, ECConfig{}, &ErrInsufficientPlacementTargets{
			Operation:     operation,
			GroupID:       group.ID,
			Desired:       cfg,
			Configured:    cloneStringSlice(group.PeerIDs),
			Unavailable:   cloneStringSlice(group.PeerIDs),
			FailureReason: "configured placement group is narrower than desired profile",
		}
	}
	group.PeerIDs = cloneStringSlice(group.PeerIDs)
	return group, cfg, nil
}

func PlacementGroupHasFullEntry(ctx context.Context) bool {
	_, ok := PlacementGroupEntryFromContext(ctx)
	return ok
}

// putObjectECData is the Phase 18 Cluster EC path: Reed-Solomon split into
// cfg.NumShards() shards, fan-out each to its placed node (self or peer),
// then commit metadata through Raft.
//
// Consistency: write-all. Any shard write failure → cleanup + error.
// Placement is derived deterministically via PlaceShards (HRW).
// ECData/ECParity + NodeIDs are stored in object metadata so reads can
// reconstruct shards without a separate Raft record.
func (b *DistributedBackend) putObjectECData(ctx context.Context, bucket, key, versionID string, data []byte, contentType string, userMetadata map[string]string, sseAlgorithm string) (*storage.Object, error) {
	placementGroupID, ok := PlacementGroupFromContext(ctx)
	if !ok {
		if b.bypassBucketCheck {
			return nil, fmt.Errorf("putObjectEC: missing placement_group_id")
		}
		placementGroupID = "group-0"
	}
	liveNodes := b.ecWriteNodes()
	effectiveCfg := EffectiveConfig(len(liveNodes), b.currentECConfig())
	if effectiveCfg.NumShards() == 0 && !b.bypassBucketCheck {
		effectiveCfg = AutoECConfigForClusterSize(len(liveNodes))
	}
	if effectiveCfg.NumShards() == 0 {
		return nil, fmt.Errorf("putObjectEC: EC profile cannot place on %d nodes", len(liveNodes))
	}

	// ShardService's key parameter carries the versionID as a suffix so shards
	// for different versions land at different paths without changing the API.
	shardKey := ecObjectShardKey(key, versionID)

	placement := selectECPlacement(effectiveCfg, liveNodes, shardKey)
	topologyWrite := false
	topologyGroup := ShardGroupEntry{}
	if group, cfg, err := placementTargetsFromContext(ctx, "put_object"); err == nil {
		topologyWrite = true
		topologyGroup = group
		placementGroupID = group.ID
		effectiveCfg = cfg
		placement = cloneStringSlice(group.PeerIDs[:cfg.NumShards()])
	} else if PlacementGroupHasFullEntry(ctx) {
		return nil, err
	}
	if len(placement) != effectiveCfg.NumShards() {
		return nil, fmt.Errorf("putObjectEC: placement has %d nodes, need %d (k=%d m=%d)",
			len(placement), effectiveCfg.NumShards(), effectiveCfg.DataShards, effectiveCfg.ParityShards)
	}
	if topologyWrite {
		if err := b.checkTopologyPlacementHealth(topologyGroup, effectiveCfg, placement); err != nil {
			return nil, err
		}
	}

	// Commit metadata. ECData/ECParity + NodeIDs stored so reads can
	// reconstruct shards without a separate placement record.
	// On failure, best-effort cleanup of orphaned shards.
	plan := ecObjectWritePlan{
		Bucket:           bucket,
		Key:              key,
		VersionID:        versionID,
		PlacementGroupID: placementGroupID,
		Config:           effectiveCfg,
		Placement:        placement,
		ContentType:      contentType,
		UserMetadata:     cloneStringMap(userMetadata),
		SSEAlgorithm:     sseAlgorithm,
	}
	writer := newECObjectWriter(b.currentSelfAddr(), b.shardSvc, b.currentPeerHealth())
	result, err := writer.writeDataShards(ctx, plan, data)
	if err != nil {
		if topologyWrite {
			return nil, topologyShardWriteError(topologyGroup, effectiveCfg, err)
		}
		return nil, err
	}
	return b.commitECObjectWriteResult(ctx, plan, result, "ec")
}

func (b *DistributedBackend) putObjectECSpooled(ctx context.Context, bucket, key, versionID string, sp *spooledObject, contentType string, userMetadata map[string]string, sseAlgorithm string) (*storage.Object, error) {
	return b.putObjectECSpooledWithOptionalModTime(ctx, bucket, key, versionID, sp, contentType, userMetadata, sseAlgorithm, 0, false, "", nil, nil, nil, "")
}

func (b *DistributedBackend) putObjectECSpooledWithOptionalModTime(ctx context.Context, bucket, key, versionID string, sp *spooledObject, contentType string, userMetadata map[string]string, sseAlgorithm string, modTime int64, preserveModTime bool, expectedETag string, beforeCommit func() error, parts []storage.MultipartPartEntry, tags []storage.Tag, multipartUploadID string) (*storage.Object, error) {
	stageStart := time.Now()
	placementGroupID, ok := PlacementGroupFromContext(ctx)
	if !ok {
		if b.bypassBucketCheck {
			return nil, fmt.Errorf("putObjectEC: missing placement_group_id")
		}
		placementGroupID = "group-0"
	}
	liveNodes := b.ecWriteNodes()
	effectiveCfg := EffectiveConfig(len(liveNodes), b.currentECConfig())
	if effectiveCfg.NumShards() == 0 && !b.bypassBucketCheck {
		effectiveCfg = AutoECConfigForClusterSize(len(liveNodes))
	}
	if effectiveCfg.NumShards() == 0 {
		return nil, fmt.Errorf("putObjectEC: EC profile cannot place on %d nodes", len(liveNodes))
	}

	// Phase 2 chunked PUT routing: objects larger than DefaultChunkSize
	// (16 MiB) take the segmented path — N×16 MiB segments fanned across
	// placement groups, single atomic metadata commit, best-effort blob
	// fanout with defer cleanup on error. <= 16 MiB stays on the existing
	// single-segment EC path.
	//
	// Chunked PUT requires a ShardGroupSource for SelectSegmentPlacementGroup;
	// legacy single-group setups (b.shardGroup == nil) fall back to the
	// existing single-blob EC path even for large objects. Once a cluster
	// wires SetShardGroupSource, large objects automatically segment.
	if b.chunkedPathThresholdMet(sp.Size) && b.shardGroup != nil {
		return b.putObjectChunked(
			ctx, bucket, key, versionID, sp,
			contentType, userMetadata, sseAlgorithm,
			modTime, preserveModTime, expectedETag, beforeCommit, parts, tags,
		)
	}

	shardKey := ecObjectShardKey(key, versionID)
	placement := selectECPlacement(effectiveCfg, liveNodes, shardKey)
	topologyWrite := false
	topologyGroup := ShardGroupEntry{}
	if group, cfg, err := placementTargetsFromContext(ctx, "put_object"); err == nil {
		topologyWrite = true
		topologyGroup = group
		placementGroupID = group.ID
		effectiveCfg = cfg
		placement = cloneStringSlice(group.PeerIDs[:cfg.NumShards()])
	}
	if len(placement) != effectiveCfg.NumShards() {
		return nil, fmt.Errorf("putObjectEC: placement has %d nodes, need %d (k=%d m=%d)",
			len(placement), effectiveCfg.NumShards(), effectiveCfg.DataShards, effectiveCfg.ParityShards)
	}
	if topologyWrite {
		if err := b.checkTopologyPlacementHealth(topologyGroup, effectiveCfg, placement); err != nil {
			return nil, err
		}
	}
	observePutStage("ec", "placement", stageStart)

	selfID := b.currentSelfAddr()
	if beforeCommit == nil && effectiveCfg.DataShards == 1 && effectiveCfg.ParityShards == 0 && len(placement) == 1 && placement[0] == selfID {
		return b.putObjectSingleLocalShardSpooled(ctx, bucket, key, versionID, placementGroupID, placement, sp, contentType, userMetadata, sseAlgorithm, parts, tags, multipartUploadID)
	}

	if beforeCommit == nil && ecMemoryShardFastPathEnabled(effectiveCfg) && sp.Size <= maxECMemoryShardFastPathBytesForCfg(effectiveCfg) {
		obj, handled, err := b.tryPutObjectECMemoryShards(ctx, bucket, key, versionID, placementGroupID, placement, effectiveCfg, sp, contentType, userMetadata, sseAlgorithm, parts, tags, multipartUploadID)
		if err != nil && topologyWrite {
			return nil, topologyShardWriteError(topologyGroup, effectiveCfg, err)
		}
		if handled || err != nil {
			return obj, err
		}
	}

	plan := ecObjectWritePlan{
		Bucket:           bucket,
		Key:              key,
		VersionID:        versionID,
		PlacementGroupID: placementGroupID,
		ModTime:          modTime,
		PreserveModTime:  preserveModTime,
		ExpectedETag:     expectedETag,
		Config:           effectiveCfg,
		Placement:        placement,
		ContentType:      contentType,
		UserMetadata:     cloneStringMap(userMetadata),
		SSEAlgorithm:     sseAlgorithm,
	}
	writer := newECObjectWriter(b.currentSelfAddr(), b.shardSvc, b.currentPeerHealth())
	result, err := writer.writeSpooledShards(ctx, plan, b.ecSpoolDir(), sp)
	if err != nil {
		if topologyWrite {
			return nil, topologyShardWriteError(topologyGroup, effectiveCfg, err)
		}
		return nil, err
	}
	if beforeCommit != nil {
		if err := beforeCommit(); err != nil {
			b.deleteShardsAsync(plan.Bucket, result.Placement, result.ShardKey)
			return nil, err
		}
	}

	result.Parts = parts
	result.Tags = tags
	if multipartUploadID != "" {
		return b.commitCompleteMultipartObjectWriteResult(ctx, multipartUploadID, plan, result, "ec")
	}
	return b.commitECObjectWriteResult(ctx, plan, result, "ec")
}

func topologyShardWriteError(group ShardGroupEntry, cfg ECConfig, err error) error {
	var shardErr *ecObjectShardWriteError
	if !errors.As(err, &shardErr) {
		return err
	}
	return &ErrInsufficientPlacementTargets{
		Operation:     "put_object",
		GroupID:       group.ID,
		Desired:       cfg,
		Configured:    cloneStringSlice(group.PeerIDs),
		Unavailable:   []string{shardErr.node},
		FailureReason: fmt.Sprintf("ec write shard %d failed: %v", shardErr.shardIdx, shardErr.err),
	}
}

func (b *DistributedBackend) checkTopologyPlacementHealth(group ShardGroupEntry, cfg ECConfig, placement []string) error {
	if b.currentPeerHealth() == nil {
		return nil
	}
	for _, node := range placement {
		if node == b.currentSelfAddr() {
			continue
		}
		if !b.currentPeerHealth().IsHealthy(node) {
			return &ErrInsufficientPlacementTargets{
				Operation:     "put_object",
				GroupID:       group.ID,
				Desired:       cfg,
				Configured:    cloneStringSlice(group.PeerIDs),
				Unavailable:   []string{node},
				FailureReason: "known unhealthy placement target",
			}
		}
	}
	return nil
}

func (b *DistributedBackend) tryPutObjectECMemoryShards(
	ctx context.Context,
	bucket, key, versionID, placementGroupID string,
	placement []string,
	cfg ECConfig,
	sp *spooledObject,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	parts []storage.MultipartPartEntry,
	tags []storage.Tag,
	multipartUploadID string,
) (*storage.Object, bool, error) {
	writer := newECObjectWriter(b.currentSelfAddr(), b.shardSvc, b.currentPeerHealth())
	result, err := writer.writeMemoryShards(ctx, ecObjectWritePlan{
		Bucket:           bucket,
		Key:              key,
		VersionID:        versionID,
		PlacementGroupID: placementGroupID,
		Config:           cfg,
		Placement:        placement,
		ContentType:      contentType,
		UserMetadata:     cloneStringMap(userMetadata),
		SSEAlgorithm:     sseAlgorithm,
	}, sp)
	if err != nil {
		return nil, true, err
	}

	result.Parts = parts
	result.Tags = tags
	if multipartUploadID != "" {
		obj, err := b.commitCompleteMultipartObjectWriteResult(
			ctx,
			multipartUploadID,
			ecObjectWritePlan{
				Bucket:           bucket,
				Key:              key,
				VersionID:        versionID,
				PlacementGroupID: placementGroupID,
				Config:           cfg,
				Placement:        placement,
				ContentType:      contentType,
				UserMetadata:     cloneStringMap(userMetadata),
				SSEAlgorithm:     sseAlgorithm,
			},
			result,
			"ec_memory",
		)
		return obj, true, err
	}
	obj, err := b.commitECObjectWriteResult(
		ctx,
		ecObjectWritePlan{
			Bucket:           bucket,
			Key:              key,
			VersionID:        versionID,
			PlacementGroupID: placementGroupID,
			Config:           cfg,
			Placement:        placement,
			ContentType:      contentType,
			UserMetadata:     cloneStringMap(userMetadata),
			SSEAlgorithm:     sseAlgorithm,
		},
		result,
		"ec_memory",
	)
	return obj, true, err
}

func (b *DistributedBackend) commitECObjectWriteResult(
	ctx context.Context,
	plan ecObjectWritePlan,
	result ecObjectWriteResult,
	metricPath string,
) (*storage.Object, error) {
	stageStart := time.Now()
	modTime := result.ModTime
	if plan.PreserveModTime {
		modTime = plan.ModTime
	}
	if merr := b.propose(ctx, CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:           plan.Bucket,
		Key:              plan.Key,
		Size:             result.Size,
		ContentType:      plan.ContentType,
		ETag:             result.ETag,
		ModTime:          modTime,
		VersionID:        plan.VersionID,
		PlacementGroupID: plan.PlacementGroupID,
		ECData:           result.ECData,
		ECParity:         result.ECParity,
		NodeIDs:          result.Placement,
		UserMetadata:     cloneStringMap(plan.UserMetadata),
		SSEAlgorithm:     plan.SSEAlgorithm,
		ExpectedETag:     plan.ExpectedETag,
		Parts:            result.Parts,
		Tags:             result.Tags,
	}); merr != nil {
		ObservePutTraceStage(ctx, PutTraceStageDataRaftProposeMeta, stageStart, PutTraceStageFields{Error: merr.Error()})
		go b.deleteShardsAsync(plan.Bucket, result.Placement, result.ShardKey)
		return nil, merr
	}
	ObservePutTraceStage(ctx, PutTraceStageDataRaftProposeMeta, stageStart, PutTraceStageFields{})
	observePutStage(metricPath, "propose_meta", stageStart)

	// result.Tags aliases the caller's slice; do not introduce concurrent
	// readers/writers on result after this point.
	return &storage.Object{
		Key:              plan.Key,
		Size:             result.Size,
		ContentType:      plan.ContentType,
		ETag:             result.ETag,
		LastModified:     modTime,
		VersionID:        plan.VersionID,
		UserMetadata:     cloneStringMap(plan.UserMetadata),
		SSEAlgorithm:     plan.SSEAlgorithm,
		PlacementGroupID: plan.PlacementGroupID,
		ECData:           result.ECData,
		ECParity:         result.ECParity,
		NodeIDs:          cloneStringSlice(result.Placement),
		Parts:            result.Parts,
		Tags:             result.Tags,
	}, nil
}

func (b *DistributedBackend) commitCompleteMultipartObjectWriteResult(
	ctx context.Context,
	uploadID string,
	plan ecObjectWritePlan,
	result ecObjectWriteResult,
	metricPath string,
) (*storage.Object, error) {
	stageStart := time.Now()
	modTime := result.ModTime
	if plan.PreserveModTime {
		modTime = plan.ModTime
	}
	if merr := b.propose(ctx, CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket:           plan.Bucket,
		Key:              plan.Key,
		UploadID:         uploadID,
		Size:             result.Size,
		ContentType:      plan.ContentType,
		ETag:             result.ETag,
		ModTime:          modTime,
		VersionID:        plan.VersionID,
		PlacementGroupID: plan.PlacementGroupID,
		ECData:           result.ECData,
		ECParity:         result.ECParity,
		NodeIDs:          result.Placement,
		Parts:            result.Parts,
		Tags:             result.Tags,
	}); merr != nil {
		ObservePutTraceStage(ctx, PutTraceStageDataRaftProposeMeta, stageStart, PutTraceStageFields{Error: merr.Error()})
		go b.deleteShardsAsync(plan.Bucket, result.Placement, result.ShardKey)
		return nil, merr
	}
	ObservePutTraceStage(ctx, PutTraceStageDataRaftProposeMeta, stageStart, PutTraceStageFields{})
	observePutStage(metricPath, "propose_meta", stageStart)

	return &storage.Object{
		Key:              plan.Key,
		Size:             result.Size,
		ContentType:      plan.ContentType,
		ETag:             result.ETag,
		LastModified:     modTime,
		VersionID:        plan.VersionID,
		UserMetadata:     cloneStringMap(plan.UserMetadata),
		SSEAlgorithm:     plan.SSEAlgorithm,
		PlacementGroupID: plan.PlacementGroupID,
		ECData:           result.ECData,
		ECParity:         result.ECParity,
		NodeIDs:          cloneStringSlice(result.Placement),
		Parts:            result.Parts,
		Tags:             result.Tags,
	}, nil
}

func (b *DistributedBackend) putObjectSingleLocalShardSpooled(
	ctx context.Context,
	bucket, key, versionID, placementGroupID string,
	placement []string,
	sp *spooledObject,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	parts []storage.MultipartPartEntry,
	tags []storage.Tag,
	multipartUploadID string,
) (*storage.Object, error) {
	shardKey := ecObjectShardKey(key, versionID)
	stageStart := time.Now()
	body, err := sp.Open()
	if err != nil {
		return nil, fmt.Errorf("open single shard body: %w", err)
	}
	obj, writeErr := b.putObjectSingleLocalShardFromReader(
		ctx,
		bucket,
		key,
		versionID,
		placementGroupID,
		placement,
		sp,
		body,
		contentType,
		userMetadata,
		sseAlgorithm,
		"ec_single",
		nil,
		parts,
		tags,
		multipartUploadID,
	)
	closeErr := body.Close()
	if writeErr != nil {
		return nil, writeErr
	}
	if closeErr != nil {
		_ = b.shardSvc.DeleteLocalShards(bucket, shardKey)
		return nil, fmt.Errorf("close single shard body: %w", closeErr)
	}
	observePutStage("ec_single", "total_with_open_close", stageStart)
	return obj, nil
}

func (b *DistributedBackend) putObjectSingleLocalShardFromReader(
	ctx context.Context,
	bucket, key, versionID, placementGroupID string,
	placement []string,
	sp *spooledObject,
	body io.Reader,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	metricPath string,
	bodyHash hash.Hash,
	parts []storage.MultipartPartEntry,
	tags []storage.Tag,
	multipartUploadID string,
) (*storage.Object, error) {
	plan := ecObjectWritePlan{
		Bucket:           bucket,
		Key:              key,
		VersionID:        versionID,
		PlacementGroupID: placementGroupID,
		Config:           ECConfig{DataShards: 1, ParityShards: 0},
		Placement:        placement,
		ContentType:      contentType,
		UserMetadata:     cloneStringMap(userMetadata),
		SSEAlgorithm:     sseAlgorithm,
	}
	writer := newECObjectWriter(b.currentSelfAddr(), b.shardSvc, b.currentPeerHealth())
	result, err := writer.writeSingleLocalReader(ctx, plan, sp, body, metricPath, bodyHash)
	if err != nil {
		return nil, err
	}
	result.Parts = parts
	result.Tags = tags

	if multipartUploadID != "" {
		return b.commitCompleteMultipartObjectWriteResult(ctx, multipartUploadID, plan, result, "ec_single")
	}

	stageStart := time.Now()
	if merr := b.propose(ctx, CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:           bucket,
		Key:              key,
		Size:             result.Size,
		ContentType:      contentType,
		ETag:             result.ETag,
		ModTime:          result.ModTime,
		VersionID:        versionID,
		PlacementGroupID: placementGroupID,
		ECData:           result.ECData,
		ECParity:         result.ECParity,
		NodeIDs:          result.Placement,
		UserMetadata:     cloneStringMap(userMetadata),
		SSEAlgorithm:     sseAlgorithm,
		Parts:            parts,
		Tags:             tags,
	}); merr != nil {
		ObservePutTraceStage(ctx, PutTraceStageDataRaftProposeMeta, stageStart, PutTraceStageFields{Error: merr.Error()})
		_ = b.shardSvc.DeleteLocalShards(bucket, result.ShardKey)
		return nil, merr
	}
	ObservePutTraceStage(ctx, PutTraceStageDataRaftProposeMeta, stageStart, PutTraceStageFields{})
	observePutStage("ec_single", "propose_meta", stageStart)

	return &storage.Object{
		Key:              key,
		Size:             result.Size,
		ContentType:      contentType,
		ETag:             result.ETag,
		LastModified:     result.ModTime,
		VersionID:        versionID,
		UserMetadata:     cloneStringMap(userMetadata),
		SSEAlgorithm:     sseAlgorithm,
		PlacementGroupID: placementGroupID,
		ECData:           result.ECData,
		ECParity:         result.ECParity,
		NodeIDs:          cloneStringSlice(result.Placement),
		Parts:            parts,
		Tags:             tags,
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
		if node == b.currentSelfAddr() {
			_ = b.shardSvc.DeleteLocalShards(bucket, shardKey)
		} else {
			_ = b.shardSvc.DeleteShards(context.Background(), node, bucket, shardKey)
		}
	}
}

func (b *DistributedBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	obj, placementMeta, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, nil, err
	}
	if blocked, q, qerr := b.isObjectQuarantined(bucket, key, obj.VersionID); qerr != nil {
		return nil, nil, fmt.Errorf("check quarantine: %w", qerr)
	} else if blocked {
		return nil, nil, objectQuarantinedError(bucket, key, q)
	}
	// HeadObject already rejects tombstones with ErrObjectNotFound, so obj here
	// is a real version. VersionID is non-empty for versioned writes and empty
	// for legacy log replay.

	// Appendable objects store bytes across per-segment blobs under
	// <objectPath>_segments/<blobID> (see writeSegmentBlobForAppend). Stitch
	// them with a multi-segment reader instead of trying to open a single
	// objectPath file (which never exists for appendables).
	if obj.IsAppendable && (len(obj.Segments) > 0 || len(obj.Coalesced) > 0) && obj.Size > 0 {
		return b.openAppendableSegments(bucket, key, obj), obj, nil
	}
	if !obj.IsAppendable && len(obj.Segments) > 0 && obj.Size > 0 {
		store := &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}
		return storage.NewSegmentReaderCtx(ctx, store, obj.Segments), obj, nil
	}

	// EC path: shardKey = key+"/"+versionID for versioned objects.
	shardKey := key
	if obj.VersionID != "" {
		shardKey = key + "/" + obj.VersionID
	}

	if b.shardSvc != nil {
		resolved, rerr := b.ResolvePlacement(ctx, bucket, key, placementMeta)
		if rerr == nil {
			rc, ecErr := b.getObjectECReaderAtShardKey(ctx, bucket, resolved.ShardKey, resolved.Record, obj.Size)
			if ecErr != nil {
				return nil, nil, fmt.Errorf("ec reconstruct %s/%s: %w", bucket, key, ecErr)
			}
			return rc, obj, nil
		}
		if !errors.Is(rerr, ErrNotEC) {
			return nil, nil, fmt.Errorf("resolve placement for %s/%s: %w", bucket, key, rerr)
		}
	}

	// Try the version-addressable local path first (new writers), then the
	// legacy unversioned path (pre-versioning replay). A stale or partial
	// local file must not satisfy metadata that names a larger object; under
	// MultiRaft follower reads that would otherwise surface as a successful
	// GET with an empty body.
	var localErr error
	if obj.VersionID != "" {
		if f, oerr := b.openObjectIfSizeMatches(b.objectPathV(bucket, key, obj.VersionID), obj); oerr == nil {
			return f, obj, nil
		} else if !os.IsNotExist(oerr) {
			localErr = oerr
		}
	}
	f, err := b.openObjectIfSizeMatches(b.objectPath(bucket, key), obj)
	if err == nil {
		return f, obj, nil
	}
	if !os.IsNotExist(err) {
		localErr = err
	}

	// Local file not found — try fetching from peer nodes (healthy first, then all).
	// Peers store under shardKey (key+"/"+versionID) when the write was versioned.
	if b.shardSvc != nil {

		// Try healthy peers first
		for _, peer := range b.liveNodes() {
			if peer == b.currentSelfAddr() {
				continue
			}
			if b.currentPeerHealth() != nil && !b.currentPeerHealth().IsHealthy(peer) {
				continue
			}
			data, fetchErr := b.shardSvc.ReadShard(ctx, peer, bucket, shardKey, 0)
			if fetchErr == nil && data != nil {
				if b.currentPeerHealth() != nil {
					b.currentPeerHealth().MarkHealthy(peer)
				}
				return io.NopCloser(bytes.NewReader(data)), obj, nil
			}
			if fetchErr != nil && b.currentPeerHealth() != nil {
				b.currentPeerHealth().MarkUnhealthy(peer)
			}
		}
		// Fallback: try unhealthy peers (they may have recovered)
		if b.currentPeerHealth() != nil {
			for _, peer := range b.clusterNodes() {
				if peer == b.currentSelfAddr() {
					continue
				}
				if b.currentPeerHealth().IsHealthy(peer) {
					continue // already tried
				}
				data, fetchErr := b.shardSvc.ReadShard(ctx, peer, bucket, shardKey, 0)
				if fetchErr == nil && data != nil {
					b.currentPeerHealth().MarkHealthy(peer)
					return io.NopCloser(bytes.NewReader(data)), obj, nil
				}
			}
		}
	}

	if localErr != nil {
		return nil, nil, fmt.Errorf("open object: %w", localErr)
	}
	return nil, nil, fmt.Errorf("open object: %w", err)
}

func (b *DistributedBackend) openObjectIfSizeMatches(path string, obj *storage.Object) (*os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if st.Size() != obj.Size {
		_ = f.Close()
		return nil, fmt.Errorf("local object size mismatch for %s: metadata=%d file=%d", path, obj.Size, st.Size())
	}
	return f, nil
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
	resolved, lookupErr := b.ResolvePlacement(ctx, bucket, key, b.readPlacementMeta(bucket, key, versionID))
	if errors.Is(lookupErr, ErrNotEC) {
		return fmt.Errorf("no placement for %s/%s — object is not EC-managed", bucket, key)
	}
	if lookupErr != nil {
		return fmt.Errorf("lookup shard placement: %w", lookupErr)
	}
	ecRec := resolved.Record
	shardKey := resolved.ShardKey
	recCfg := ecRec.ECConfigOrFallback(b.currentECConfig())
	if shardIdx < 0 || shardIdx >= len(ecRec.Nodes) {
		return fmt.Errorf("shardIdx %d out of range [0,%d)", shardIdx, len(ecRec.Nodes))
	}
	if len(ecRec.Nodes) != recCfg.NumShards() {
		return fmt.Errorf("placement length %d != k+m %d", len(ecRec.Nodes), recCfg.NumShards())
	}

	selfID := b.currentSelfAddr()
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
		b.shardCache.InvalidatePrefix(shardRangeCachePrefix(bucket, shardKey, shardIdx))
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

func (b *DistributedBackend) SetIncidentRecorder(rec IncidentRecorder) {
	b.incidentRecorder = rec
}

// LiveNodes returns the list of cluster nodes currently considered reachable.
// This is the public counterpart of the internal liveNodes() method.
func (b *DistributedBackend) LiveNodes() []string { return b.liveNodes() }

// ECActive reports whether Phase 18 cluster EC will be applied to the next
// PutObject call (EC enabled + enough nodes for k+m split).
func (b *DistributedBackend) ECActive() bool {
	return b.currentECConfig().IsActive(len(b.ecWriteNodes()))
}

// EffectiveECConfig returns the ECConfig proportionally scaled to the current
// cluster size. Used by ReshardManager to determine the target k,m for upgrades.
func (b *DistributedBackend) EffectiveECConfig() ECConfig {
	return EffectiveConfig(len(b.ecWriteNodes()), b.currentECConfig())
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
	liveNodes := b.ecWriteNodes()
	effectiveCfg := EffectiveConfig(len(liveNodes), b.currentECConfig())
	if !effectiveCfg.IsActive(len(liveNodes)) || b.shardSvc == nil {
		return fmt.Errorf("ec not active: cluster_size=%d shard_svc=%v",
			len(liveNodes), b.shardSvc != nil)
	}
	// Snapshot meta before reading data so we can detect concurrent writes.
	metaBefore, placementMeta, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("head before convert: %w", err)
	}
	if _, rerr := b.ResolvePlacement(ctx, bucket, key, placementMeta); rerr == nil {
		return nil // already converted
	} else if !errors.Is(rerr, ErrNotEC) {
		return fmt.Errorf("resolve existing placement before convert: %w", rerr)
	}

	// Read the full object via the legacy N× path. GetObject will fall through
	// to local or peer full-replica fetch because placement is still absent.
	rc, _, err := b.GetObject(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("read for convert: %w", err)
	}
	sp, spoolErr := b.spoolPutObject(ctx, bucket, rc)
	closeErr := rc.Close()
	if spoolErr != nil {
		return fmt.Errorf("spool for convert: %w", spoolErr)
	}
	if closeErr != nil {
		sp.Cleanup()
		return fmt.Errorf("close convert source: %w", closeErr)
	}
	defer sp.Cleanup()
	if sp.ETag != metaBefore.ETag {
		return fmt.Errorf("convert aborted: spool ETag mismatch for %s/%s: got %s, want %s",
			bucket, key, sp.ETag, metaBefore.ETag)
	}

	// Re-check meta: did a PUT race us while we were spooling the source?
	metaAfter, err := b.HeadObject(ctx, bucket, key)
	if err != nil || metaAfter.ETag != metaBefore.ETag {
		return fmt.Errorf("convert aborted: meta changed mid-conversion (etag %q → %q)",
			metaBefore.ETag, func() string {
				if metaAfter != nil {
					return metaAfter.ETag
				}
				return ""
			}())
	}

	beforeCommit := func() error {
		metaAfter, err := b.HeadObject(ctx, bucket, key)
		if err != nil || metaAfter.ETag != metaBefore.ETag {
			return fmt.Errorf("convert aborted: meta changed mid-conversion (etag %q → %q)",
				metaBefore.ETag, func() string {
					if metaAfter != nil {
						return metaAfter.ETag
					}
					return ""
				}())
		}
		return nil
	}
	// applyPutObjectMeta writes Tags unconditionally; forward metaBefore.Tags
	// so a legacy N×→EC conversion doesn't clobber existing user tags.
	if _, err := b.putObjectECSpooledWithOptionalModTime(ctx, bucket, key, metaBefore.VersionID, sp, metaBefore.ContentType, metaBefore.UserMetadata, metaBefore.SSEAlgorithm, metaBefore.LastModified, true, metaBefore.ETag, beforeCommit, nil, metaBefore.Tags, ""); err != nil {
		return fmt.Errorf("convert write ec shards: %w", err)
	}
	_, convertedMeta, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("head after convert: %w", err)
	}
	resolved, err := b.ResolvePlacement(ctx, bucket, key, convertedMeta)
	if err != nil {
		return fmt.Errorf("resolve converted placement: %w", err)
	}

	// Cleanup legacy N× replicas on nodes NOT in the placement. The local full-
	// object file is always deleted (whether or not self is a placement node,
	// the full file is now redundant). Best-effort — failures just leave stale
	// N× copies that a future sweep can reclaim.
	_ = os.Remove(b.objectPath(bucket, key))
	if metaBefore.VersionID != "" {
		_ = os.Remove(b.objectPathV(bucket, key, metaBefore.VersionID))
	}
	selfID := b.currentSelfAddr()
	placementSet := make(map[string]bool, len(resolved.Record.Nodes))
	for _, n := range resolved.Record.Nodes {
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

func (b *DistributedBackend) newECObjectReader() ecObjectReader {
	r := ecObjectReader{selfID: b.currentSelfAddr(), shards: b.shardSvc, ecConfig: b.currentECConfig()}
	if b.shardCache != nil {
		r.cache = b.shardCache
	}
	if b.currentPeerHealth() != nil {
		r.peerHealth = b.currentPeerHealth()
	}
	return r
}

func (b *DistributedBackend) getObjectECReaderAtShardKey(ctx context.Context, bucket, shardKey string, rec PlacementRecord, objectSize int64) (io.ReadCloser, error) {
	return b.newECObjectReader().OpenObject(ctx, bucket, shardKey, rec, objectSize)
}

func (b *DistributedBackend) readObjectECAtShardKey(ctx context.Context, bucket, shardKey string, rec PlacementRecord, objectSize int64, offset int64, buf []byte) (int, error) {
	if b.shardSvc == nil {
		return 0, fmt.Errorf("shard service unavailable")
	}
	return b.newECObjectReader().ReadAt(ctx, bucket, shardKey, rec, objectSize, offset, buf)
}

func (b *DistributedBackend) readAtViaGetObject(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	rc, _, err := b.GetObject(ctx, bucket, key)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	if _, err := io.CopyN(io.Discard, rc, offset); err != nil {
		return 0, err
	}
	return io.ReadFull(rc, buf)
}

// upgradeObjectEC re-encodes an EC object from oldRec's (k1,m1) to newCfg's (k2,m2).
// Called by ReshardManager when the cluster grows and the effective EC config changes.
// Sequence: reconstruct with old config → re-encode with new config → fan-out new shards
// → propose updated placement → delete old shards (best-effort).
func (b *DistributedBackend) upgradeObjectEC(ctx context.Context, bucket, key string, oldRec PlacementRecord, newCfg ECConfig) error {
	if b.shardSvc == nil {
		return fmt.Errorf("shard service unavailable")
	}

	obj, meta, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return fmt.Errorf("upgrade head object: %w", err)
	}
	resolved, err := b.ResolvePlacement(ctx, bucket, key, meta)
	if err != nil {
		return fmt.Errorf("upgrade resolve placement: %w", err)
	}
	if len(resolved.Record.Nodes) > 0 {
		oldRec = resolved.Record
	}
	shardKey := resolved.ShardKey

	// Reconstruct original data from old shards.
	data, err := b.newECObjectReader().ReadObject(ctx, bucket, shardKey, oldRec)
	if err != nil {
		return fmt.Errorf("upgrade reconstruct: %w", err)
	}

	// Re-encode with new config.
	liveNodes := b.ecWriteNodes()
	newShards, err := ECSplit(newCfg, data)
	if err != nil {
		return fmt.Errorf("upgrade re-split: %w", err)
	}
	newPlacement := PlacementForNodes(newCfg, liveNodes, shardKey)
	selfID := b.currentSelfAddr()

	var (
		writtenMu sync.Mutex
		written   []string
	)
	cleanup := func() {
		for _, n := range written {
			if n == selfID {
				_ = b.shardSvc.DeleteLocalShards(bucket, shardKey)
			} else {
				_ = b.shardSvc.DeleteShards(ctx, n, bucket, shardKey)
			}
		}
	}

	g, gctx := errgroup.WithContext(ctx)
	for i, node := range newPlacement {
		i, node := i, node
		g.Go(func() error {
			var werr error
			if node == selfID {
				werr = b.shardSvc.WriteLocalShard(bucket, shardKey, i, newShards[i])
			} else {
				writeCtx, writeCancel := context.WithTimeout(gctx, shardRPCTimeout)
				defer writeCancel()
				werr = b.shardSvc.WriteShard(writeCtx, node, bucket, shardKey, i, newShards[i])
				if b.currentPeerHealth() != nil {
					if werr != nil {
						b.currentPeerHealth().MarkUnhealthy(node)
					} else {
						b.currentPeerHealth().MarkHealthy(node)
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

	// Commit updated EC placement via ObjectMeta. CmdPutShardPlacement is
	// retained only for legacy decode compatibility and no longer stores rows.
	if perr := b.propose(ctx, CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:           bucket,
		Key:              key,
		Size:             obj.Size,
		ContentType:      obj.ContentType,
		ETag:             obj.ETag,
		ModTime:          obj.LastModified,
		VersionID:        obj.VersionID,
		PlacementGroupID: meta.PlacementGroupID,
		ECData:           uint8(newCfg.DataShards),
		ECParity:         uint8(newCfg.ParityShards),
		NodeIDs:          newPlacement,
		UserMetadata:     cloneStringMap(obj.UserMetadata),
		// applyPutObjectMeta writes Tags unconditionally; forward the existing
		// tags so an EC config upgrade doesn't clobber them to nil.
		Tags: obj.Tags,
	}); perr != nil {
		cleanup()
		return fmt.Errorf("upgrade propose object meta: %w", perr)
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
		if n == selfID {
			_ = b.shardSvc.DeleteLocalShards(bucket, shardKey)
		} else {
			_ = b.shardSvc.DeleteShards(ctx, n, bucket, shardKey)
		}
	}
	return nil
}

func (b *DistributedBackend) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	obj, _, err := b.headObjectMeta(ctx, bucket, key)
	return obj, err
}

func (b *DistributedBackend) headObjectMeta(ctx context.Context, bucket, key string) (*storage.Object, PlacementMeta, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, PlacementMeta{}, err
	}

	var obj storage.Object
	var placement PlacementMeta
	err := b.db.View(func(txn *badger.Txn) error {
		decodeMeta := func(item *badger.Item, versionID string) error {
			val, err := b.itemValueCopy(item)
			if err != nil {
				return err
			}
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
				Key:              m.Key,
				Size:             m.Size,
				ContentType:      m.ContentType,
				ETag:             m.ETag,
				LastModified:     m.LastModified,
				VersionID:        versionID,
				ACL:              m.ACL,
				UserMetadata:     cloneStringMap(m.UserMetadata),
				SSEAlgorithm:     m.SSEAlgorithm,
				PlacementGroupID: m.PlacementGroupID,
				ECData:           m.ECData,
				ECParity:         m.ECParity,
				NodeIDs:          cloneStringSlice(m.NodeIDs),
				Segments:         m.Segments,
				Parts:            m.Parts,
				Coalesced:        coalescedRefsToStorage(m.Coalesced),
				IsAppendable:     m.IsAppendable,
				// Tags copied (not aliased) — m's backing bytes are reused by
				// badger once the View tx returns.
				Tags: append([]storage.Tag(nil), m.Tags...),
			}
			placement = PlacementMeta{
				VersionID:        versionID,
				ECData:           m.ECData,
				ECParity:         m.ECParity,
				NodeIDs:          m.NodeIDs,
				PlacementGroupID: m.PlacementGroupID,
			}
			return nil
		}

		// Resolve via latest-version pointer when present so callers see the
		// most recent version. Falls back to the legacy single-key read when
		// no lat: pointer exists (e.g., legacy replay).
		if storage.IsInternalBucket(bucket) {
			versionID := ""
			metaKeyBytes := b.internalObjectPath(bucket, key).metaKey
			if latItem, lerr := txn.Get(b.ks().LatestKey(bucket, key)); lerr == nil {
				_ = latItem.Value(func(v []byte) error {
					versionID = string(v)
					return nil
				})
				if versionID != "" {
					metaKeyBytes = b.ks().ObjectMetaKeyV(bucket, key, versionID)
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
			if versionID == "" {
				versionID = "current"
			}
			return decodeMeta(item, versionID)
		}

		metaKeyBytes := b.ks().ObjectMetaKey(bucket, key)
		versionID := ""
		if latItem, lerr := txn.Get(b.ks().LatestKey(bucket, key)); lerr == nil {
			_ = latItem.Value(func(v []byte) error {
				versionID = string(v)
				return nil
			})
			if versionID != "" {
				metaKeyBytes = b.ks().ObjectMetaKeyV(bucket, key, versionID)
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
		return decodeMeta(item, versionID)
	})
	if err != nil {
		return nil, PlacementMeta{}, err
	}
	return &obj, placement, nil
}

func (b *DistributedBackend) readPlacementMeta(bucket, key, versionID string) PlacementMeta {
	meta := PlacementMeta{VersionID: versionID}
	_ = b.db.View(func(txn *badger.Txn) error {
		dbKey := b.ks().ObjectMetaKey(bucket, key)
		if versionID != "" {
			dbKey = b.ks().ObjectMetaKeyV(bucket, key, versionID)
		}
		item, err := txn.Get(dbKey)
		if err != nil {
			return err
		}
		val, err := b.itemValueCopy(item)
		if err != nil {
			return err
		}
		m, err := unmarshalObjectMeta(val)
		if err != nil {
			return err
		}
		meta.ECData = m.ECData
		meta.ECParity = m.ECParity
		meta.NodeIDs = m.NodeIDs
		return nil
	})
	return meta
}

func (b *DistributedBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	return b.deleteObjectCtx(ctx, bucket, key)
}

// DeleteObjectReturningMarker satisfies server.VersionedSoftDeleter. Same
// tombstone semantics as DeleteObject but returns the delete marker's
// VersionID so the S3 handler can surface it in the response header.
func (b *DistributedBackend) DeleteObjectReturningMarker(bucket, key string) (string, error) {
	return b.deleteObjectWithMarker(context.Background(), bucket, key)
}

// deleteObjectCtx is the ctx-aware core used by DeleteObject so that HTTP
// request cancellation propagates into the Raft propose call.
func (b *DistributedBackend) deleteObjectCtx(ctx context.Context, bucket, key string) error {
	_, err := b.deleteObjectWithMarker(ctx, bucket, key)
	return err
}

// deleteObjectWithMarker is the single implementation shared by DeleteObject
// and DeleteObjectReturningMarker.
//
// Tombstone semantics: creates a delete marker as a new version. Prior version
// data remains addressable via GetObjectVersion and is NOT physically removed
// here. Hard-delete of a specific version goes through DeleteObjectVersion
// (used by lifecycle/scrubber).
//
// For backward compatibility with the legacy N× on-disk layout, we also
// remove the unversioned local object file if present — it's guaranteed to
// be stale (superseded by a versioned path) and keeping it risks GetObject
// serving it as a fallback read.
func (b *DistributedBackend) deleteObjectWithMarker(ctx context.Context, bucket, key string) (string, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return "", err
	}
	if storage.IsInternalBucket(bucket) {
		return "", b.deleteInternalObject(bucket, key)
	}
	os.Remove(b.objectPath(bucket, key))
	markerID := newVersionID()
	if err := b.propose(ctx, CmdDeleteObject, DeleteObjectCmd{
		Bucket:    bucket,
		Key:       key,
		VersionID: markerID,
	}); err != nil {
		return "", err
	}
	return markerID, nil
}

func (b *DistributedBackend) deleteInternalObject(bucket, key string) error {
	objPath := b.internalObjectPath(bucket, key)
	_ = os.Remove(objPath.path)
	b.internalPathCache.Delete(internalObjectCacheKey{bucket: bucket, key: key})
	b.internalSizeCache.Delete(internalObjectCacheKey{bucket: bucket, key: key})
	return b.db.Update(func(txn *badger.Txn) error {
		if item, err := txn.Get(b.ks().LatestKey(bucket, key)); err == nil {
			if err := item.Value(func(v []byte) error {
				versionID := string(v)
				if versionID == "" {
					return nil
				}
				_ = os.Remove(b.objectPathV(bucket, key, versionID))
				if err := txn.Delete(b.ks().ObjectMetaKeyV(bucket, key, versionID)); err != nil && err != badger.ErrKeyNotFound {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		for _, dbKey := range [][]byte{
			b.ks().LatestKey(bucket, key),
			b.ks().ObjectMetaKey(bucket, key),
		} {
			if err := txn.Delete(dbKey); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		return nil
	})
}

func (b *DistributedBackend) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}

	var objects []*storage.Object
	err := b.db.View(func(txn *badger.Txn) error {
		// Load latest-version pointers for this bucket so we can dedupe versioned
		// entries down to a single row per base key (skipping delete markers).
		latMap := make(map[string]string) // base key → latest versionID
		rawLatPrefix := []byte("lat:" + bucket + "/")
		latPrefix := b.ks().Prefix(rawLatPrefix)
		itLat := txn.NewIterator(badger.DefaultIteratorOptions)
		for itLat.Seek(latPrefix); itLat.ValidForPrefix(latPrefix); itLat.Next() {
			rawKey := b.ks().MustStrip(itLat.Item().Key())
			baseKey := string(rawKey[len(rawLatPrefix):])
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
		rawBucketPfx := []byte("obj:" + bucket + "/")
		pfx := b.ks().Prefix([]byte("obj:" + bucket + "/" + prefix))
		bucketPfx := b.ks().Prefix(rawBucketPfx)
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
			rawKey := b.ks().MustStrip(it.Item().Key())
			rest := string(rawKey[len(rawBucketPfx):])

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
			val, err := b.itemValueCopy(it.Item())
			if err != nil {
				return err
			}
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			if m.ETag != deleteMarkerETag {
				obj = storage.Object{
					Key:          m.Key,
					Size:         m.Size,
					ContentType:  m.ContentType,
					ETag:         m.ETag,
					LastModified: m.LastModified,
					ACL:          m.ACL,
					UserMetadata: cloneStringMap(m.UserMetadata),
					SSEAlgorithm: m.SSEAlgorithm,
					Parts:        m.Parts,
					// Tags copied (not aliased) — m's backing bytes are reused
					// by badger once the View tx returns.
					Tags: append([]storage.Tag(nil), m.Tags...),
				}
				if isVersioned {
					obj.VersionID = latMap[baseKey]
				}
			}
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

// ListObjectsPage returns one S3 ListObjects page from this DistributedBackend's
// local meta store, honoring marker. truncated is true when the iterator
// stopped because maxKeys was reached and at least one more matching base
// key would have followed. Used by GroupBackend on forwarded reads and by
// ClusterCoordinator's fallback path when bucket has no FSM object-index
// source; without this the fallback silently truncated pages whose marker
// fell past the first maxKeys+1 prefix-matching keys.
func (b *DistributedBackend) ListObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]*storage.Object, bool, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, false, err
	}
	var (
		objects   []*storage.Object
		truncated bool
	)
	err := b.db.View(func(txn *badger.Txn) error {
		latMap := make(map[string]string)
		rawLatPrefix := []byte("lat:" + bucket + "/")
		latPrefix := b.ks().Prefix(rawLatPrefix)
		itLat := txn.NewIterator(badger.DefaultIteratorOptions)
		for itLat.Seek(latPrefix); itLat.ValidForPrefix(latPrefix); itLat.Next() {
			rawKey := b.ks().MustStrip(itLat.Item().Key())
			baseKey := string(rawKey[len(rawLatPrefix):])
			_ = itLat.Item().Value(func(v []byte) error {
				latMap[baseKey] = string(v)
				return nil
			})
		}
		itLat.Close()

		emitted := make(map[string]bool)
		rawBucketPfx := []byte("obj:" + bucket + "/")
		pfx := b.ks().Prefix([]byte("obj:" + bucket + "/" + prefix))
		bucketPfx := b.ks().Prefix(rawBucketPfx)
		seek := pfx
		if marker != "" {
			// Resume strictly after `marker`. Append NUL so we land on the
			// first key whose suffix sorts past marker — versions of marker
			// itself ("marker/<vid>") also sort past "marker\x00", so they
			// remain reachable and the dedupe logic below filters them
			// against `emitted` and `latMap`.
			seek = b.ks().Prefix(append([]byte("obj:"+bucket+"/"+marker), 0))
		}
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(seek); it.ValidForPrefix(bucketPfx); it.Next() {
			if !it.ValidForPrefix(pfx) {
				break
			}
			rawKey := b.ks().MustStrip(it.Item().Key())
			rest := string(rawKey[len(rawBucketPfx):])

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
			if marker != "" && baseKey <= marker {
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
			if len(objects) >= maxKeys {
				truncated = true
				break
			}

			var obj storage.Object
			val, err := b.itemValueCopy(it.Item())
			if err != nil {
				return err
			}
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			if m.ETag == deleteMarkerETag {
				continue
			}
			obj = storage.Object{
				Key:          m.Key,
				Size:         m.Size,
				ContentType:  m.ContentType,
				ETag:         m.ETag,
				LastModified: m.LastModified,
				ACL:          m.ACL,
				UserMetadata: cloneStringMap(m.UserMetadata),
				SSEAlgorithm: m.SSEAlgorithm,
				Parts:        m.Parts,
				// Tags copied (not aliased) — m's backing bytes are reused by
				// badger once the View tx returns.
				Tags: append([]storage.Tag(nil), m.Tags...),
			}
			if isVersioned {
				obj.VersionID = latMap[baseKey]
			}
			if obj.Key == "" {
				continue
			}
			objects = append(objects, &obj)
			emitted[baseKey] = true
		}
		return nil
	})
	return objects, truncated, err
}

func (b *DistributedBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	return b.db.View(func(txn *badger.Txn) error {
		latMap := make(map[string]string)
		rawLatPrefix := []byte("lat:" + bucket + "/")
		latPrefix := b.ks().Prefix(rawLatPrefix)
		itLat := txn.NewIterator(badger.DefaultIteratorOptions)
		for itLat.Seek(latPrefix); itLat.ValidForPrefix(latPrefix); itLat.Next() {
			rawKey := b.ks().MustStrip(itLat.Item().Key())
			baseKey := string(rawKey[len(rawLatPrefix):])
			_ = itLat.Item().Value(func(v []byte) error {
				latMap[baseKey] = string(v)
				return nil
			})
		}
		itLat.Close()

		emitted := make(map[string]bool)
		rawBucketPfx := []byte("obj:" + bucket + "/")
		pfx := b.ks().Prefix([]byte("obj:" + bucket + "/" + prefix))
		bucketPfx := b.ks().Prefix(rawBucketPfx)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(pfx); it.ValidForPrefix(bucketPfx); it.Next() {
			if !it.ValidForPrefix(pfx) {
				break
			}
			rawKey := b.ks().MustStrip(it.Item().Key())
			rest := string(rawKey[len(rawBucketPfx):])

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
			val, err := b.itemValueCopy(it.Item())
			if err != nil {
				return err
			}
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			if m.ETag != deleteMarkerETag {
				obj = storage.Object{
					Key:          m.Key,
					Size:         m.Size,
					ContentType:  m.ContentType,
					ETag:         m.ETag,
					LastModified: m.LastModified,
					ACL:          m.ACL,
					UserMetadata: cloneStringMap(m.UserMetadata),
					SSEAlgorithm: m.SSEAlgorithm,
					Parts:        m.Parts,
					// Tags copied (not aliased) — m's backing bytes are reused
					// by badger once the View tx returns.
					Tags: append([]storage.Tag(nil), m.Tags...),
				}
				if isVersioned {
					obj.VersionID = latMap[baseKey]
				}
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

func (b *DistributedBackend) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*storage.MultipartUpload, error) {
	uploadID, createdAt, err := b.createMultipartUploadInternal(ctx, bucket, key, contentType, nil)
	if err != nil {
		return nil, err
	}
	return &storage.MultipartUpload{
		UploadID:    uploadID,
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		CreatedAt:   createdAt,
	}, nil
}

// CreateMultipartUploadWithTags creates a multipart upload with tags in cluster mode.
// Tags travel through CreateMultipartUploadCmd (Raft replicated) and live on
// clusterMultipartMeta until CompleteMultipartUpload, where they are materialised
// onto the finalised object's objectMeta.Tags via the same Raft path that
// materialises content (CmdPutObjectMeta carries the Tags vector — no separate
// SetObjectTags proposal).
func (b *DistributedBackend) CreateMultipartUploadWithTags(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (string, error) {
	uploadID, _, err := b.createMultipartUploadInternal(ctx, bucket, key, contentType, tags)
	if err != nil {
		return "", err
	}
	return uploadID, nil
}

// createMultipartUploadInternal is the shared body for CreateMultipartUpload and
// CreateMultipartUploadWithTags. If tags is non-empty it is defensively copied at
// this cluster API boundary so the propose path can safely retain it (propose is
// async); downstream code must not introduce further copies.
func (b *DistributedBackend) createMultipartUploadInternal(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (string, int64, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return "", 0, err
	}

	uploadID := uuid.New().String()
	if err := os.MkdirAll(b.partDir(uploadID), 0o755); err != nil {
		return "", 0, fmt.Errorf("create part dir: %w", err)
	}

	now := time.Now().Unix()
	placementGroupID, ok := PlacementGroupFromContext(ctx)
	// GroupBackend (bypassBucketCheck=true) always injects a placement-group ID via
	// context; missing one there is a programming error. Direct DistributedBackend
	// callers (bypassBucketCheck=false) may omit it — putObjectECSpooled resolves
	// placement from the stored empty string using the object's bucket assignment.
	if !ok && b.shardSvc != nil && b.bypassBucketCheck {
		return "", 0, fmt.Errorf("create multipart: missing placement_group_id")
	}

	var tagsCopy []storage.Tag
	if len(tags) > 0 {
		tagsCopy = make([]storage.Tag, len(tags))
		copy(tagsCopy, tags)
	}

	err := b.propose(ctx, CmdCreateMultipartUpload, CreateMultipartUploadCmd{
		UploadID:         uploadID,
		Bucket:           bucket,
		Key:              key,
		ContentType:      contentType,
		CreatedAt:        now,
		PlacementGroupID: placementGroupID,
		Tags:             tagsCopy,
	})
	if err != nil {
		os.RemoveAll(b.partDir(uploadID))
		return "", 0, err
	}
	return uploadID, now, nil
}

func (b *DistributedBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader) (*storage.Part, error) {
	lifeMu := b.multipartLifecycleLock(uploadID)
	lifeMu.RLock()
	defer lifeMu.RUnlock()

	// Verify upload exists (read local metadata)
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(b.ks().MultipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return storage.ErrUploadNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
	}

	// Data write is local — no Raft needed for part data
	if err := os.MkdirAll(b.partDir(uploadID), 0o755); err != nil {
		return nil, fmt.Errorf("create part dir: %w", err)
	}
	partFile := b.partPath(uploadID, partNumber)
	f, err := os.Create(partFile)
	if err != nil {
		return nil, fmt.Errorf("create part file: %w", err)
	}

	h := md5.New()
	var partWriter io.Writer = f
	if b.encryptedShardStorage() {
		partWriter = &encryptedSpoolRecordWriter{
			w:      f,
			enc:    b.shardSvc.encryptor,
			domain: clusterMultipartPartDomain(uploadID, partNumber),
		}
	}
	w := io.MultiWriter(partWriter, h)
	size, err := copyToSpoolChunked(w, r)
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

func (b *DistributedBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	lifeMu := b.multipartLifecycleLock(uploadID)
	lifeMu.Lock()
	defer func() {
		lifeMu.Unlock()
		b.multipartLocks.Delete(uploadID)
	}()

	// Read upload metadata
	var meta clusterMultipartMeta
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.ks().MultipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return storage.ErrUploadNotFound
		}
		if err != nil {
			return err
		}
		val, err := b.itemValueCopy(item)
		if err != nil {
			return err
		}
		m, err := unmarshalClusterMultipartMeta(val)
		if err != nil {
			return err
		}
		meta = m
		return nil
	})
	if err != nil {
		return nil, err
	}
	if meta.PlacementGroupID != "" {
		var ctxErr error
		ctx, ctxErr = contextForMultipartComplete(ctx, meta, func(id string) (ShardGroupEntry, bool) {
			group, ok := PlacementGroupEntryFromContext(ctx)
			if !ok || group.ID != id {
				return ShardGroupEntry{}, false
			}
			return group, true
		})
		if ctxErr != nil {
			return nil, ctxErr
		}
	}

	manifest, err := b.buildMultipartCompleteManifest(uploadID, parts)
	if err != nil {
		return nil, err
	}

	versionID := newVersionID()
	var obj *storage.Object
	if b.currentECConfig().NumShards() > 0 && b.shardSvc != nil {
		if b.chunkedPathThresholdMet(manifest.TotalSize) && b.shardGroup != nil {
			beforeCommit := b.testBeforeChunkedMultipartCommit
			obj, err = b.putMultipartObjectChunked(ctx, bucket, key, versionID, uploadID, manifest, meta.ContentType, nil, "", 0, false, "", beforeCommit, meta.Tags)
		} else {
			var sp *spooledObject
			cleanupSpool := true
			if len(manifest.Parts) == 1 {
				sp = b.multipartPartSpooledObject(uploadID, manifest.Parts[0])
				cleanupSpool = false
			} else {
				var spoolErr error
				sp, spoolErr = b.spoolMultipartCompleteManifest(ctx, uploadID, versionID, bucket, manifest)
				if spoolErr != nil {
					return nil, spoolErr
				}
			}
			if cleanupSpool {
				defer sp.Cleanup()
			}
			obj, err = b.putObjectECSpooledWithOptionalModTime(ctx, bucket, key, versionID, sp, meta.ContentType, nil, "", 0, false, "", nil, manifest.Parts, meta.Tags, uploadID)
		}
	} else {
		err = fmt.Errorf("complete multipart: EC storage is required")
	}
	if err != nil {
		return nil, err
	}
	if err := os.RemoveAll(b.partDir(uploadID)); err != nil {
		b.logger.Debug().Err(err).Str("upload_id", uploadID).Msg("multipart part cleanup after complete failed")
	}
	return obj, nil
}

func (b *DistributedBackend) multipartPartSpooledObject(uploadID string, part storage.MultipartPartEntry) *spooledObject {
	sp := &spooledObject{
		Path: b.partPath(uploadID, part.PartNumber),
		Size: part.Size,
		ETag: part.ETag,
	}
	if b.encryptedShardStorage() {
		sp.encrypted = true
		sp.encryptor = b.shardSvc.encryptor
		sp.domain = clusterMultipartPartDomain(uploadID, part.PartNumber)
	}
	return sp
}

func (b *DistributedBackend) spoolMultipartCompleteManifest(ctx context.Context, uploadID, versionID, bucket string, manifest multipartCompleteManifest) (*spooledObject, error) {
	body, err := manifest.Open()
	if err != nil {
		return nil, fmt.Errorf("open multipart manifest: %w", err)
	}
	defer body.Close()
	if b.encryptedShardStorage() {
		return spoolObjectEncrypted(ctx, b.spoolDir(), body, bucket, b.shardSvc.encryptor, clusterMultipartSpoolDomain(uploadID, versionID))
	}
	return spoolObject(ctx, b.spoolDir(), body, bucket)
}

func contextForMultipartComplete(
	ctx context.Context,
	meta clusterMultipartMeta,
	lookup func(string) (ShardGroupEntry, bool),
) (context.Context, error) {
	if meta.PlacementGroupID == "" {
		return ctx, nil
	}
	if group, ok := PlacementGroupEntryFromContext(ctx); ok && group.ID == meta.PlacementGroupID {
		return ctx, nil
	}
	group, ok := lookup(meta.PlacementGroupID)
	if !ok {
		return ctx, &ErrInsufficientPlacementTargets{
			Operation:     "complete_multipart",
			GroupID:       meta.PlacementGroupID,
			FailureReason: "multipart placement group is missing",
		}
	}
	return ContextWithPlacementGroupEntry(ctx, group), nil
}

func (b *DistributedBackend) ListMultipartUploads(ctx context.Context, bucket, prefix string, maxUploads int) ([]*storage.MultipartUpload, error) {
	_ = ctx
	var uploads []*storage.MultipartUpload
	bucketBytes := []byte(bucket)
	prefixBytes := []byte(prefix)
	err := b.db.View(func(txn *badger.Txn) error {
		return b.ks().scanGroupPrefix(txn, []byte("mpu:"), func(rawKey []byte, item *badger.Item) error {
			raw, err := b.itemValueCopy(item)
			if err != nil {
				return err
			}
			meta, err := fbSafe(raw, func(d []byte) *clusterpb.MultipartMeta {
				return clusterpb.GetRootAsMultipartMeta(d, 0)
			})
			if err != nil {
				return fmt.Errorf("unmarshal MultipartMeta: %w", err)
			}
			metaBucket := meta.Bucket()
			metaKey := meta.Key()
			if len(metaBucket) == 0 || len(metaKey) == 0 {
				return nil
			}
			if !bytes.Equal(metaBucket, bucketBytes) || !bytes.HasPrefix(metaKey, prefixBytes) {
				return nil
			}
			uploadID := strings.TrimPrefix(string(rawKey), "mpu:")
			uploads = append(uploads, &storage.MultipartUpload{
				UploadID:    uploadID,
				Bucket:      string(metaBucket),
				Key:         string(metaKey),
				ContentType: string(meta.ContentType()),
				CreatedAt:   meta.CreatedAt(),
			})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(uploads, func(i, j int) bool {
		return multipartUploadLess(*uploads[i], *uploads[j])
	})
	if maxUploads > 0 && len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
	}
	return uploads, nil
}

func multipartUploadLess(a, b storage.MultipartUpload) bool {
	if a.CreatedAt != b.CreatedAt {
		return a.CreatedAt < b.CreatedAt
	}
	if a.Key != b.Key {
		return a.Key < b.Key
	}
	return a.UploadID < b.UploadID
}

// ListParts walks the local node's partDir for the given uploadID. Parts
// uploaded against another node's routed group are not visible from here;
// callers that need the full set should target the routed group directly
// (see ClusterCoordinator routing). uploadID existence is checked against
// the FSM-replicated multipart record so a missing uploadID returns
// ErrUploadNotFound consistently across nodes even when no parts landed
// locally.
func (b *DistributedBackend) ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]storage.Part, error) {
	_ = ctx
	_ = bucket
	_ = key
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(b.ks().MultipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return storage.ErrUploadNotFound
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(b.partDir(uploadID))
	if os.IsNotExist(err) {
		return []storage.Part{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read part dir: %w", err)
	}
	out := make([]storage.Part, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		partNumber, parseErr := strconv.Atoi(entry.Name())
		if parseErr != nil || partNumber <= 0 {
			continue
		}
		f, err := b.openMultipartPart(uploadID, partNumber)
		if err != nil {
			return nil, fmt.Errorf("open part %d: %w", partNumber, err)
		}
		h := md5.New()
		size, err := io.Copy(h, f)
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("hash part %d: %w", partNumber, err)
		}
		out = append(out, storage.Part{
			PartNumber: partNumber,
			ETag:       hex.EncodeToString(h.Sum(nil)),
			Size:       size,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PartNumber < out[j].PartNumber })
	if maxParts > 0 && len(out) > maxParts {
		out = out[:maxParts]
	}
	return out, nil
}

func (b *DistributedBackend) openMultipartPart(uploadID string, partNumber int) (io.ReadCloser, error) {
	full := b.partPath(uploadID, partNumber)
	if b.encryptedShardStorage() {
		return openSpoolEncryptedRecordFile(full, b.shardSvc.encryptor, clusterMultipartPartDomain(uploadID, partNumber))
	}
	return os.Open(full)
}

func clusterMultipartPartDomain(uploadID string, partNumber int) string {
	return fmt.Sprintf("cluster-multipart-part:%s:%d", uploadID, partNumber)
}

func clusterMultipartSpoolDomain(uploadID, versionID string) string {
	return fmt.Sprintf("cluster-multipart-spool:%s:%s", uploadID, versionID)
}

func (b *DistributedBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	lifeMu := b.multipartLifecycleLock(uploadID)
	lifeMu.Lock()
	defer func() {
		lifeMu.Unlock()
		b.multipartLocks.Delete(uploadID)
	}()

	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(b.ks().MultipartKey(uploadID))
		if err == badger.ErrKeyNotFound {
			return storage.ErrUploadNotFound
		}
		return err
	})
	if err != nil {
		return err
	}

	if err := b.propose(ctx, CmdAbortMultipart, AbortMultipartCmd{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}); err != nil {
		return err
	}

	os.RemoveAll(b.partDir(uploadID))
	return nil
}

// --- Versioning ---

// HeadObjectVersion returns metadata for a specific version. Returns
// storage.ErrObjectNotFound if the version doesn't exist or is a delete marker.
func (b *DistributedBackend) HeadObjectVersion(bucket, key, versionID string) (*storage.Object, error) {
	obj, _, err := b.headObjectMetaV(bucket, key, versionID)
	return obj, err
}

// headObjectMetaV reads a specific version's metadata and its EC placement
// fields in one transaction. Returns storage.ErrObjectNotFound for a missing
// version and storage.ErrMethodNotAllowed for a delete-marker version (S3
// semantics — the server handler maps that to a 405 with x-amz-delete-marker).
// Parallels headObjectMeta, which does the same for the latest version.
func (b *DistributedBackend) headObjectMetaV(bucket, key, versionID string) (*storage.Object, PlacementMeta, error) {
	ctx := context.Background()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, PlacementMeta{}, err
	}
	var obj storage.Object
	var placement PlacementMeta
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.ks().ObjectMetaKeyV(bucket, key, versionID))
		if err == badger.ErrKeyNotFound {
			return storage.ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		val, err := b.itemValueCopy(item)
		if err != nil {
			return err
		}
		m, err := unmarshalObjectMeta(val)
		if err != nil {
			return err
		}
		if m.ETag == deleteMarkerETag {
			return storage.ErrMethodNotAllowed
		}
		obj = storage.Object{
			Key:              m.Key,
			Size:             m.Size,
			ContentType:      m.ContentType,
			ETag:             m.ETag,
			LastModified:     m.LastModified,
			VersionID:        versionID,
			ACL:              m.ACL,
			UserMetadata:     cloneStringMap(m.UserMetadata),
			SSEAlgorithm:     m.SSEAlgorithm,
			PlacementGroupID: m.PlacementGroupID,
			ECData:           m.ECData,
			ECParity:         m.ECParity,
			NodeIDs:          cloneStringSlice(m.NodeIDs),
			Segments:         m.Segments,
			Parts:            m.Parts,
			Coalesced:        coalescedRefsToStorage(m.Coalesced),
			IsAppendable:     m.IsAppendable,
			// Tags copied (not aliased) — m's backing bytes are reused by
			// badger once the View tx returns. Mirror of headObjectMeta.
			Tags: append([]storage.Tag(nil), m.Tags...),
		}
		placement = PlacementMeta{
			VersionID:        versionID,
			ECData:           m.ECData,
			ECParity:         m.ECParity,
			NodeIDs:          m.NodeIDs,
			PlacementGroupID: m.PlacementGroupID,
		}
		return nil
	})
	if err != nil {
		return nil, PlacementMeta{}, err
	}
	return &obj, placement, nil
}

// GetObjectVersion reads a specific version's data. Returns
// storage.ErrObjectNotFound if the version doesn't exist. For delete markers,
// returns ErrMethodNotAllowed to mirror the erasure backend's behavior.
func (b *DistributedBackend) GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, *storage.Object, error) {
	ctx := context.Background()
	obj, meta, err := b.headObjectMetaV(bucket, key, versionID)
	if err != nil {
		return nil, nil, err
	}
	if obj.IsDeleteMarker {
		return nil, nil, storage.ErrMethodNotAllowed
	}
	if blocked, q, qerr := b.isObjectQuarantined(bucket, key, versionID); qerr != nil {
		return nil, nil, fmt.Errorf("check quarantine: %w", qerr)
	} else if blocked {
		return nil, nil, objectQuarantinedError(bucket, key, q)
	}
	if obj.IsAppendable && (len(obj.Segments) > 0 || len(obj.Coalesced) > 0) && obj.Size > 0 {
		return b.openAppendableSegments(bucket, key, obj), obj, nil
	}
	if !obj.IsAppendable && len(obj.Segments) > 0 && obj.Size > 0 {
		store := &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}
		return storage.NewSegmentReaderCtx(ctx, store, obj.Segments), obj, nil
	}
	// EC path: reconstruct from shards when the bucket is erasure-coded.
	// Mirrors GetObject — versioned objects use shardKey = key+"/"+versionID,
	// which ResolvePlacement derives from PlacementMeta.VersionID. Non-EC and
	// legacy objects fall through to the plain-file path (ResolvePlacement → ErrNotEC).
	if b.shardSvc != nil {
		resolved, rerr := b.ResolvePlacement(ctx, bucket, key, meta)
		if rerr == nil {
			rc, ecErr := b.getObjectECReaderAtShardKey(ctx, bucket, resolved.ShardKey, resolved.Record, obj.Size)
			if ecErr != nil {
				return nil, nil, fmt.Errorf("ec reconstruct %s/%s@%s: %w", bucket, key, versionID, ecErr)
			}
			return rc, obj, nil
		}
		if !errors.Is(rerr, ErrNotEC) {
			return nil, nil, fmt.Errorf("resolve placement for %s/%s@%s: %w", bucket, key, versionID, rerr)
		}
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
	ctx := context.Background()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	// Local data cleanup: best-effort (ENOENT is fine — FSM apply is the source of truth).
	_ = os.Remove(b.objectPathV(bucket, key, versionID))
	return b.propose(ctx, CmdDeleteObjectVersion, DeleteObjectVersionCmd{
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
	ctx := context.Background()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	var versions []*storage.ObjectVersion
	latestMap := map[string]string{} // key → latestVID
	err := b.db.View(func(txn *badger.Txn) error {
		// Pre-scan latest pointers for the prefix so each version can tag IsLatest.
		rawLatSemanticPfx := []byte("lat:" + bucket + "/" + prefix)
		latPrefix := b.ks().Prefix(rawLatSemanticPfx)
		latIt := txn.NewIterator(badger.DefaultIteratorOptions)
		for latIt.Seek(latPrefix); latIt.ValidForPrefix(latPrefix); latIt.Next() {
			rawKey := b.ks().MustStrip(latIt.Item().Key())
			key := strings.TrimPrefix(string(rawKey), "lat:"+bucket+"/")
			_ = latIt.Item().Value(func(v []byte) error { latestMap[key] = string(v); return nil })
		}
		latIt.Close()

		// Match any object key starting with `prefix` — iterate the per-bucket
		// versioned store and filter in-memory. The version ID is the last
		// path segment after the final `/`; everything before is the S3 key.
		rawObjBucketPfx := []byte("obj:" + bucket + "/")
		objPrefix := b.ks().Prefix(rawObjBucketPfx)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(objPrefix); it.ValidForPrefix(objPrefix); it.Next() {
			rawKey := b.ks().MustStrip(it.Item().Key())
			rest := strings.TrimPrefix(string(rawKey), "obj:"+bucket+"/")
			// Versioned format: {key}/{versionID}. Unversioned legacy: {key}.
			slash := strings.LastIndex(rest, "/")
			if slash < 0 {
				if _, hasVersionedRecord := latestMap[rest]; hasVersionedRecord {
					continue
				}
				val, err := b.itemValueCopy(it.Item())
				if err != nil {
					return err
				}
				m, err := unmarshalObjectMeta(val)
				if err != nil {
					return err
				}
				v := storage.ObjectVersion{
					Key:            rest,
					VersionID:      "",
					IsLatest:       true,
					IsDeleteMarker: m.ETag == deleteMarkerETag,
					LastModified:   m.LastModified,
					ETag:           m.ETag,
					Size:           m.Size,
					Tags:           append([]storage.Tag(nil), m.Tags...),
				}
				versions = append(versions, &v)
				continue
			}
			key := rest[:slash]
			vid := rest[slash+1:]
			latestVID, hasVersionedRecord := latestMap[key]
			if !hasVersionedRecord {
				if !strings.HasPrefix(rest, prefix) {
					continue
				}
				val, err := b.itemValueCopy(it.Item())
				if err != nil {
					return err
				}
				m, err := unmarshalObjectMeta(val)
				if err != nil {
					return err
				}
				v := storage.ObjectVersion{
					Key:            rest,
					VersionID:      "",
					IsLatest:       true,
					IsDeleteMarker: m.ETag == deleteMarkerETag,
					LastModified:   m.LastModified,
					ETag:           m.ETag,
					Size:           m.Size,
					Tags:           append([]storage.Tag(nil), m.Tags...),
				}
				versions = append(versions, &v)
				continue
			}
			if vid == "" || !strings.HasPrefix(key, prefix) {
				continue
			}
			val, err := b.itemValueCopy(it.Item())
			if err != nil {
				return err
			}
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			v := storage.ObjectVersion{
				Key:            key,
				VersionID:      vid,
				IsLatest:       vid == latestVID,
				IsDeleteMarker: m.ETag == deleteMarkerETag,
				LastModified:   m.LastModified,
				ETag:           m.ETag,
				Size:           m.Size,
				Tags:           append([]storage.Tag(nil), m.Tags...), // Task 7 carry-over
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

func (b *DistributedBackend) internalObjectPath(bucket, key string) internalObjectPath {
	cacheKey := internalObjectCacheKey{bucket: bucket, key: key}
	if cached, ok := b.internalPathCache.Load(cacheKey); ok {
		return cached.(internalObjectPath)
	}
	path := b.objectPathV(bucket, key, "current")
	candidate := internalObjectPath{path: path, dir: filepath.Dir(path), metaKey: b.ks().ObjectMetaKey(bucket, key)}
	actual, _ := b.internalPathCache.LoadOrStore(cacheKey, candidate)
	return actual.(internalObjectPath)
}

func (b *DistributedBackend) currentInternalObjectPath(bucket, key string) internalObjectPath {
	objPath := b.internalObjectPath(bucket, key)
	_ = b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.ks().LatestKey(bucket, key))
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			versionID := string(v)
			if versionID == "" {
				return nil
			}
			objPath = internalObjectPath{
				path:    objPath.path,
				dir:     objPath.dir,
				metaKey: b.ks().ObjectMetaKeyV(bucket, key, versionID),
			}
			return nil
		})
	})
	return objPath
}

func (b *DistributedBackend) ensureInternalObjectDir(dir string) error {
	if _, ok := b.internalDirCache.Load(dir); ok {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b.internalDirCache.Store(dir, struct{}{})
	return nil
}

// objectPath returns the legacy-unversioned local path for a full-object copy.
// Kept as a read fallback only — writers go through objectPathV. New keys never
// collide with objectPathV because the versioned namespace lives under a
// sibling ".obj" directory.
func (b *DistributedBackend) objectPath(bucket, key string) string {
	return filepath.Join(b.root, "data", bucket, key)
}

// OpenLocalReplica returns a ReadCloser for the locally-stored copy of a
// non-EC (replicated) object. It does NOT fall back to peers — that path
// belongs to RepairReplica. For internal buckets the file lives at
// objectPathV(bucket, key, "current"); for legacy unversioned buckets the
// caller should use objectPath. Volume blocks are internal, so "current"
// is the right version to look up; if the metadata says otherwise the
// caller should use a more specific path resolution.
func (b *DistributedBackend) OpenLocalReplica(bucket, key string) (io.ReadCloser, error) {
	return os.Open(b.objectPathV(bucket, key, "current"))
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

func (b *DistributedBackend) multipartLifecycleLock(uploadID string) *sync.RWMutex {
	v, _ := b.multipartLocks.LoadOrStore(uploadID, &sync.RWMutex{})
	return v.(*sync.RWMutex)
}

// Node returns the RaftNode interface for leadership and raft control.
func (b *DistributedBackend) Node() RaftNode { return b.node }

// recordApplyResult records an FSM apply error for the given Raft log index.
// Mirrors MetaRaft.recordApplyResult (meta_raft.go:797): only non-nil errors
// are stored, and a 1024-index lookback window self-trims to prevent unbounded
// growth.
func (b *DistributedBackend) recordApplyResult(index uint64, err error) {
	if err == nil {
		return
	}
	b.applyResultMu.Lock()
	if b.applyErrs == nil {
		b.applyErrs = make(map[uint64]error)
	}
	b.applyErrs[index] = err
	for old := range b.applyErrs {
		if old+1024 < index {
			delete(b.applyErrs, old)
		}
	}
	b.applyResultMu.Unlock()
}

// ApplyError returns the FSM apply error for the given Raft log index, or nil
// if no error was recorded. Reading consumes the entry — callers must read
// exactly once per ProposeWait.
func (b *DistributedBackend) ApplyError(index uint64) error {
	b.applyResultMu.Lock()
	err := b.applyErrs[index]
	delete(b.applyErrs, index)
	b.applyResultMu.Unlock()
	return err
}
