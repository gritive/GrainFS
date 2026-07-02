package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/gossip"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
)

// shardRPCTimeout is the per-shard RPC deadline for remote writes/reads.
// EC PUTs stream shard bodies during the caller's write path; cold multi-raft
// startup can legitimately spend more than a few seconds opening and draining
// transport shard streams before the metadata propose completes.
const shardRPCTimeout = 2 * time.Minute

// proposeForwardTimeout bounds the leader-side raft commit for a forwarded
// propose/read-index RPC. The receiver handlers carry no caller ctx (the
// transport handler signature is `func(*Message) *Message`), so they cannot
// honor the originator's budget without a wire change — this generous bound
// replaces a hardcoded 5s that aborted commits the caller was still willing to
// wait for (the dominant CompleteMultipartUpload-under-load 500 mode). 30s sits
// above burst raft-commit p99 and below typical S3 client timeouts, so the
// phantom-commit window (a commit landing after the caller gave up — already
// possible at 5s, since ProposeWait cancellation does not un-propose) stays
// bounded. TODO: wire-propagate the caller's exact deadline (needs a payload
// field) to align with the originator's budget instead of a fixed bound.
const proposeForwardTimeout = 30 * time.Second

// ErrProposeTimeout marks a propose that exhausted its server-side deadline
// (the raft commit could not complete in time under load). The S3 layer maps it
// to a retryable 503 SlowDown rather than a fatal 500, so clients auto-retry.
// It is surfaced only when the propose context expired by DeadlineExceeded —
// never on client cancellation, and it masks the transient ErrNotLeader the
// follower forward loop accumulates while waiting (which would otherwise leak
// as a 500). NOTE: this makes residual timeouts retryable; it does NOT shed
// load before doing the work — true admission-control backpressure is separate.
var ErrProposeTimeout = errors.New("cluster: propose deadline exceeded")

// ShardRPCTimeout exposes shardRPCTimeout so the streaming PUT pipeline (built
// in serveruntime, which cannot see the unexported const) can bound each remote
// shard write RPC. The spool path uses it as a TOTAL per-RPC wall-clock (the
// shard is materialized before the RPC); the streaming pipeline reinterprets the
// SAME value as an IDLE deadline (reset on each progress event), since there it
// would otherwise bound ingest+seal+RPC and abort a slow-but-progressing upload.
func ShardRPCTimeout() time.Duration { return shardRPCTimeout }

// ProposeForwardTimeout exposes proposeForwardTimeout so the boot wiring in
// serveruntime (which cannot see the unexported const) can set the forward
// SENDER's readiness deadline to the SAME generous bound the receiver commit
// uses. A follower→leader CompleteMultipartUpload forward carries no caller
// deadline, so ForwardSender.readinessRetry was the binding bound; a hardcoded
// 5s there guillotined forwards whose commit legitimately takes ~5.5s under
// load (proven by the local-leader path, which is uncapped and finishes at the
// same latency). Aligning the sender bound with the receiver's removes that
// mismatch.
func ProposeForwardTimeout() time.Duration { return proposeForwardTimeout }

const (
	ecShardWriteAttempts = 3
	ecShardWriteBackoff  = 250 * time.Millisecond

	bucketVersioningCacheTTL = 200 * time.Millisecond
)

type bucketVersioningCacheEntry struct {
	expiresAt time.Time
	state     string
}

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

type raftSnapshotRequest struct {
	ctx  context.Context
	resp chan raftSnapshotResponse
}

type raftSnapshotResponse struct {
	result raft.SnapshotResult
	err    error
}

// DistributedBackend implements storage.Backend over local object data,
// quorum-meta blobs, and meta-raft control-plane metadata.
type DistributedBackend struct {
	root string
	// store carries ALL metadata transactions. Ownership follows shared:
	// shared=false → Close() closes the store (backend owns it);
	// shared=true → the caller owns the store's lifecycle.
	store                            MetadataStore
	node                             RaftNode
	fsm                              *FSM
	keys                             *stateKeyspace
	groupID                          string // non-empty when constructed via NewDistributedBackendForGroup; used by Phase B1 append-segment peer-fetch
	shared                           bool
	logger                           zerolog.Logger
	lastApplied                      atomic.Uint64
	lastAppliedTerm                  atomic.Uint64
	snapRequests                     chan raftSnapshotRequest
	shardSvc                         *ShardService
	allNodes                         []string // all node addresses (including self) for shard placement
	selfAddr                         string   // this node's raft address (matches entries in allNodes)
	peerHealth                       *PeerHealth
	topologySnapshot                 atomic.Pointer[backendTopology]
	ecConfig                         ECConfig // Phase 18: erasure coding config (k+m shard parameters)
	ecConfigSnapshot                 atomic.Pointer[ECConfig]
	runtimeSnapshot                  atomic.Pointer[backendRuntimeSnapshot]
	shardLocks                       keyedRWMutex              // scrubbable.go: per-(bucket,key) RWMutex for ReadShard/WriteShard (refcounted, bounded)
	objectMetaRMWLocks               keyedRWMutex              // per-(bucket,key) serialization for append/tag/ACL/coalesce/relocation quorum-meta RMW (refcounted, bounded)
	objectWriteBucketLocks           keyedRWMutex              // per-bucket admission fence for object writes vs DeleteBucket emptiness/delete
	multipartLocks                   sync.Map                  // map[uploadID]*sync.RWMutex; serializes part writes against complete/abort cleanup
	incidentRecorder                 IncidentRecorder          // nil disables zero-ops incident recording
	quarantineRouter                 QuarantineRouter          // nil → leaf-local quarantine SET; set on group backends to route the SET to the owner
	bucketVersioningCache            sync.Map                  // bucket → bucketVersioningCacheEntry
	testBeforeChunkedMultipartCommit func() error              // test-only hook for chunked multipart commit preflight
	testBeforeAppendSegmentWrite     func()                    // test-only hook after append pre-check before segment write
	testOnListObjectVersionsCtx      func(ctx context.Context) // test-only hook: called with the ctx passed to ListObjectVersions

	// shardCache caches reconstructed/fetched EC shards. Sits in front of
	// getObjectEC's per-shard fan-out: a full hit (every needed shard
	// resident) skips disk and network entirely. Nil disables caching.
	// See internal/cache/shardcache for the rationale (sharded LRU,
	// lock-free counters, why we do not use an actor pattern here).
	shardCache *shardcache.Cache

	nodeStatsStore *gossip.NodeStatsStore // gossip-fed disk/RPS stats; wired by StartPlacementRuntime
	bl             *BoundedLoads          // hot-node detection; wired by StartPlacementRuntime
	clusterCfg     *ClusterConfig         // live policy view; wired by StartPlacementRuntime (defaults until then)

	// frozenSegSrc yields snapshot-frozen segment paths (bucket -> paths) for the
	// orphan-segment known-set. nil until SetFrozenSegmentPathSource wires the
	// snapshot Manager at boot. nil => AllFrozenSegmentPaths fails closed.
	frozenSegSrc func() (map[string][]string, error)

	// frozenObjVersionSrc yields snapshot-frozen full-object versions for the
	// orphan-SHARD known-set (full-object analogue of frozenSegSrc). nil until
	// SetFrozenObjectVersionSource wires the snapshot Manager at boot. nil =>
	// allFrozenObjectVersionDirs fails closed (WalkOrphanShards no-ops).
	frozenObjVersionSrc func() ([]storage.SnapshotObjectRef, error)

	// capProbe reports whether a RAW placement node ID advertises the
	// commit-combined capability (production: resolve node ID → raft addr, then
	// probe the CapabilityGate). nil until SetCapabilityProbe wires it at boot;
	// nil ⇒ the PUT commit tail treats every remote target as not-capable
	// (legacy two-round flow).
	capProbe func(nodeID string) bool

	// orphanShardSweepGate, when non-nil and returning true, permits the EC
	// full-object orphan-shard sweep. nil (default) => fail-closed: the sweep
	// never runs. Boot sets it to a feature-on predicate; per-candidate
	// multi-group safety lives in hostedGroupBackendsSrc (union live-set) +
	// owningGroupHostedFn (floated-shard gate) + the all-hosted caught-up gate.
	orphanShardSweepGate func() bool

	// hostedGroupBackendsSrc yields every locally-hosted data-group backend
	// (including this one). Boot wires it from dgMgr.All(); the orphan sweep
	// unions each backend's versioned live-set (each scans its own ks obj:
	// prefix on the shared store) so a sibling group's live versioned shards are
	// protected. nil (tests / un-wired) => []{b}: single-group, identical to the
	// pre-multi-group behavior.
	hostedGroupBackendsSrc func() []*DistributedBackend

	// owningGroupHostedFn reports whether the bucket's router-resolved owning
	// group is locally hosted. The shared ShardService dataDirs may hold shards
	// the balancer floated in from groups this node does not host (balancer.go is
	// group-blind by design; placement metadata tracks location). Such a shard
	// cannot be judged locally, so the sweep keeps it. nil (tests / un-wired) =>
	// true: this node judges every candidate (single-group).
	owningGroupHostedFn func(bucket string) bool

	// owningGroupBackendFn resolves a bucket to its owning data-group's backend
	// for the orphan-SEGMENT sweep (segments live per-group under that group's
	// b.root; each bucket has exactly one owning group). Returns nil when the
	// owner is not locally hosted (its segments aren't on this node). Boot wires
	// it from dgMgr.GroupForBucket. nil (tests / un-wired) => owningGroupBackend
	// returns this backend (single-group, identical to pre-multi-group behavior).
	owningGroupBackendFn func(bucket string) *DistributedBackend

	// gcFreshnessGate overrides the legacy data-Raft ReadIndex freshness barrier
	// for GC paths. nil preserves brownfield/tests' old CaughtUp behavior.
	gcFreshnessGate atomic.Pointer[gcFreshnessGateHolder]

	// gcSingletonOwnerFn gates GC work that rewrites global metadata. Local orphan
	// cleanup can run on every locally-authoritative node, but redundancy upgrade
	// relocation needs one deterministic runner per owning group.
	gcSingletonOwnerFn atomic.Pointer[gcSingletonOwnerHolder]

	metaBucketStore MetaBucketStore  // Task 7: cluster-wide bucket metadata seam; nil = not wired
	router          *Router          // PR-D: bucket→group routing; nil = no routing
	shardGroup      ShardGroupSource // v0.0.7.0: query active groups for hash assignment; nil = legacy single-group path

	// multiGeneration arms the cross-generation LWW read merge (S7-6). False (the
	// default) keeps readQuorumMeta/readQuorumMetaCmd on the local-first fast path
	// — byte-identical to legacy. The coordinator sets it true on every node once
	// the topology has >1 placement generation, so quorum-meta reads fan out and
	// pick the last-writer-wins copy across generations rather than returning a
	// stale same-generation local copy.
	multiGeneration atomic.Bool

	// qms is the extracted quorum-meta ORCHESTRATION module (quorum_meta_store.go):
	// the K-of-N fan-out write, the LWW read merge, the certainty-aware reclaim read
	// and the cluster-wide LIST/scan scatter-gather. The 21 facade methods on
	// DistributedBackend (quorum_meta.go) delegate to it. Built in SetShardService
	// AFTER shardSvc + the topology snapshot are set (see newQuorumMetaStore); nil
	// before then, but no caller invokes the delegated methods pre-wire (they would
	// have nil-panicked on b.shardSvc in the original too).
	qms *QuorumMetaStore

	// chunkedPutChunkSize is a test seam; zero keeps the production default.
	chunkedPutChunkSize int64

	// bypassBucketCheck skips the HeadBucket pre-check in PutObject. Set by
	// GroupBackend: bucket existence is guaranteed by the router (design doc
	// invariant 5), so the per-group DB need not duplicate the META-DB check.
	bypassBucketCheck bool

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

	// appendSideRecordsDisabled is a test seam for legacy coalesce unit tests
	// that intentionally exercise raw Segments in the quorum-meta manifest. The
	// production default is side-record mode enabled.
	appendSideRecordsDisabled bool

	// scrubOrphanAge is the age gate for WalkOrphanSegments. Set via SetScrubOrphanAge.
	scrubOrphanAge time.Duration

	// removeAll is the function used to physically remove a bucket directory.
	// Defaults to os.RemoveAll; tests inject a spy to verify ordering relative
	// to MetaBucketStore.DeleteBucket (consensus must precede physical remove).
	removeAll func(string) error
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

// NewDistributedBackend creates a new distributed storage backend over an
// injected MetadataStore (Phase 6.5 S3: the composition root opens the DB
// and wraps it; cluster no longer touches badger). Ownership: shared=false
// means the backend OWNS the injected store and Close() closes it (for
// badgermeta that closes the underlying BadgerDB); shared=true means the
// caller owns the store's lifecycle and Close() never touches it.
// The FSM apply loop must be started separately via RunApplyLoop.
// keys may be nil (uses an identity keyspace).
func NewDistributedBackend(root string, store MetadataStore, node RaftNode, keys *stateKeyspace, shared bool) (*DistributedBackend, error) {
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	if keys == nil {
		keys = newStateKeyspaceEmpty()
	}

	fsm := NewFSM(store, keys)

	b := &DistributedBackend{
		root:         root,
		store:        store,
		node:         node,
		fsm:          fsm,
		keys:         keys,
		shared:       shared,
		logger:       log.With().Str("component", "distributed-backend").Logger(),
		snapRequests: make(chan raftSnapshotRequest),
		clusterCfg:   NewClusterConfig(), // default config until StartPlacementRuntime wires the live pointer
		removeAll:    os.RemoveAll,
	}
	// Phase B2: wire the in-process coalesce worker + periodic backstop scan.
	// Lifecycle is bound to Close() via coalesceCancel.
	defCfg := DefaultCoalesceConfig()
	b.coalesceCfg.Store(&defCfg)
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
// keys carry groupID's keyspace prefix and which opens in shared-store mode
// (Close does NOT close the store — the caller owns the shared store's
// lifecycle). serveruntime uses this for the group-0 main backend over the
// per-node shared FSM-state store (C2 P3). groupID must be non-empty.
func NewDistributedBackendForGroup(root string, store MetadataStore, node RaftNode, groupID string) (*DistributedBackend, error) {
	keys, err := newStateKeyspace(groupID)
	if err != nil {
		return nil, fmt.Errorf("group %s: keyspace: %w", groupID, err)
	}
	b, err := NewDistributedBackend(root, store, node, keys, true)
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

func (b *DistributedBackend) bucketVersioningCacheGet(bucket string) (string, bool) {
	raw, ok := b.bucketVersioningCache.Load(bucket)
	if !ok {
		return "", false
	}

	entry, ok := raw.(bucketVersioningCacheEntry)
	if !ok {
		b.bucketVersioningCache.Delete(bucket)
		return "", false
	}
	if time.Now().After(entry.expiresAt) {
		b.bucketVersioningCache.Delete(bucket)
		return "", false
	}
	return entry.state, true
}

func (b *DistributedBackend) bucketVersioningCacheSet(bucket, state string) {
	b.bucketVersioningCache.Store(bucket, bucketVersioningCacheEntry{
		expiresAt: time.Now().Add(bucketVersioningCacheTTL),
		state:     state,
	})
}

func (b *DistributedBackend) bucketVersioningCacheDelete(bucket string) {
	b.bucketVersioningCache.Delete(bucket)
}

// SetCoalesceConfig updates the coalesce thresholds at runtime by storing
// them in the live b.coalesceCfg atomic, which the append/coalesce path
// reads on the next operation.
func (b *DistributedBackend) SetCoalesceConfig(cfg CoalesceConfig) {
	cfgCopy := cfg
	b.coalesceCfg.Store(&cfgCopy)
}

// SetECConfig configures erasure-coding shard parameters (k, m) for
// PutObject/GetObject. Call before serving traffic. The configured profile must
// fit the active write node set; invalid profiles make EC writes fail fast.
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
		b.fsm.SetDEKKeeper(svc.DEKKeeper(), svc.ClusterID())
	}
	topology := newBackendTopology(allNodes)
	b.selfAddr = topology.selfAddr
	b.allNodes = append([]string(nil), topology.allNodes...)
	b.peerHealth = topology.peerHealth
	b.topologySnapshot.Store(topology)
	b.publishRuntimeSnapshot(*topology, b.currentECConfig())
	// Build the quorum-meta orchestration store now that shardSvc is set. Built
	// ONCE here: its adapter fields are live accessors closing over b (local/peer/
	// groups read b.shardSvc / b.shardGroup every call), so a later shardSvc
	// reassignment or a deferred SetShardGroupSource is observed live with no
	// rebuild (see buildQuorumMetaStore). Facade methods call b.qms directly.
	b.qms = b.buildQuorumMetaStore()
}

// SetClusterNodes refreshes the configured placement node set without
// replacing the ShardService. Runtime join paths use this after meta-raft
// membership grows so new writes do not stay pinned to boot-time topology.
func (b *DistributedBackend) SetClusterNodes(allNodes []string) {
	topology := newBackendTopology(allNodes)
	b.topologySnapshot.Store(topology)
	b.publishRuntimeSnapshot(*topology, b.currentECConfig())
}

// StartPlacementRuntime wires the live ClusterConfig and gossip-fed
// gossip.NodeStatsStore into the backend, constructs BoundedLoads from them, and
// starts the periodic BoundedLoads refresh goroutine. Must be called after
// gossip infrastructure is up (store is being populated). Weighted placement
// and BoundedLoads skip are inactive until this is called.
//
// ctx governs the refresh goroutine lifetime — cancel it to stop.
func (b *DistributedBackend) StartPlacementRuntime(ctx context.Context, cfg *ClusterConfig, store *gossip.NodeStatsStore) {
	b.clusterCfg = cfg
	b.nodeStatsStore = store
	// Pass cfg directly so BoundedLoads reads C/CLow/MaxStale live on every
	// Refresh — runtime cluster_config patches take effect without restart.
	b.bl = NewBoundedLoads(store, cfg)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.bl.RefreshIfStale()
			}
		}
	}()
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

// CurrentECConfigForStartupRepair returns the resolved EC config for use during
// startup repair classification, where no caller-supplied default is available.
func (b *DistributedBackend) CurrentECConfigForStartupRepair() ECConfig {
	return b.currentECConfig()
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

// effectivePlacementNodes returns the node list used for EC stripe placement.
//
// Multi-node clusters: equals ecWriteNodes() — each cluster peer is one
// placement slot.
//
// Single-node multi-drive: returns the local node ID repeated by the local
// drive count. This lets PlaceShards / selectECPlacement keep their
// "len(nodes) >= NumShards" precondition without inventing a different shape
// for single-node deployments. The downstream local writer routes shardIdx
// to distinct drives via shardIdx % len(dataDirs), so the duplicated peer ID
// is correct: every shard is "sent to self" and self distributes the bytes
// across its own drives. EffectiveConfig consumers should pass
// len(effectivePlacementNodes()) instead of len(ecWriteNodes()) when
// computing the active EC stripe width.
func (b *DistributedBackend) effectivePlacementNodes() []string {
	nodes := b.ecWriteNodes()
	if len(nodes) != 1 || b.shardSvc == nil {
		return nodes
	}
	drives := len(b.shardSvc.DataDirs())
	if drives <= 1 {
		return nodes
	}
	replicated := make([]string, drives)
	for i := range replicated {
		replicated[i] = nodes[0]
	}
	return replicated
}

// peerPlacementWeights returns the per-peer disk-capacity weight snapshot
// aligned 1:1 with peers (weights[i] is peers[i]'s DiskAvailBytes, 0 when
// unknown/stale) and whether weighted placement is enabled. It mirrors the
// non-chunked fallback's weighting source (b.nodeStatsStore via
// objectWritePlacementNodeStatesFromRuntime + b.clusterCfg.WeightedHRWEnabled).
// selectShardPlacement applies the all-stale safeguard, so a nil/empty store
// degrades to unweighted HRW.
func (b *DistributedBackend) peerPlacementWeights(peers []string) ([]float64, bool) {
	enabled := b.clusterCfg.WeightedHRWEnabled()
	if !enabled {
		return nil, false
	}
	states := objectWritePlacementNodeStatesFromRuntime(peers, b.nodeStatsStore)
	if len(states) == 0 {
		return nil, true
	}
	weights := make([]float64, len(peers))
	for i, state := range states {
		weights[i] = float64(state.DiskAvailBytes)
	}
	return weights, true
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

// SetMultiGeneration arms (true) or disarms (false) the cross-generation LWW
// read merge for quorum-meta reads (S7-6). The coordinator calls it from
// rebuild() with generationCount() > 1 so that, once a topology generation has
// been added, reads pick the last-writer-wins copy across all generations
// instead of returning a stale local copy. The default (false) is the
// byte-identical single-generation fast path.
func (b *DistributedBackend) SetMultiGeneration(v bool) {
	b.multiGeneration.Store(v)
}

// RunApplyLoop consumes committed entries from the Raft node.
// This must run in a goroutine. It advances lastApplied for opaque command and
// non-command entries and restores snapshot entries for brownfield raft stores.
func (b *DistributedBackend) RunApplyLoop(stop <-chan struct{}) {
	a := &applyActor{fsm: b.fsm}
	a.run(b, stop)
}

// forwardReadIndex sends a StreamReadIndex RPC to leaderAddr and returns the leader's commitIndex.
// Wire: request=[empty], response=[8B commitIndex BE][4B errLen BE][errBytes...]
func (b *DistributedBackend) forwardReadIndex(ctx context.Context, leaderAddr string) (uint64, error) {
	if b.shardSvc == nil {
		return 0, fmt.Errorf("forwardReadIndex: no transport available")
	}
	reply, err := b.shardSvc.SendRequest(ctx, leaderAddr, transport.RouteForwardReadIndex, nil)
	if err != nil {
		return 0, fmt.Errorf("forwardReadIndex: %w", err)
	}
	if len(reply) < 12 {
		return 0, fmt.Errorf("forwardReadIndex: short response: %d bytes", len(reply))
	}
	idx := binary.BigEndian.Uint64(reply[0:8])
	errLen := binary.BigEndian.Uint32(reply[8:12])
	if errLen > 0 && len(reply) >= 12+int(errLen) {
		msg := string(reply[12 : 12+int(errLen)])
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
	h := func(_ []byte) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), proposeForwardTimeout)
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
		return resp, nil
	}
	// Native /forward/read-index buffered route. The handler ignores the
	// (empty) request payload; the leader outcome (commitIndex or error text)
	// is in-band in the reply payload.
	b.shardSvc.RegisterBufferedRoute(transport.RouteForwardReadIndex, h)
}

// ReadIndex returns a linearizable read fence index.
// On the leader it confirms leadership via heartbeat quorum.
// On a follower it forwards to the leader via StreamReadIndex cluster-transport RPC.
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
	return b.store.Close()
}

var _ storage.Backend = (*DistributedBackend)(nil)

// SetMetaBucketStore wires the cluster-wide bucket metadata seam (Task 7).
// When non-nil, callers can read/write bucket metadata through the meta-Raft
// FSM via MetaBucketStore() instead of reaching the FSM directly.
// Nil (default) means not yet wired; callers must check before use.
func (b *DistributedBackend) SetMetaBucketStore(s MetaBucketStore) { b.metaBucketStore = s }

// MetaBucketStore returns the wired MetaBucketStore, or nil if not yet wired.
func (b *DistributedBackend) MetaBucketStore() MetaBucketStore { return b.metaBucketStore }

// SetRouter wires a Router for bucket→group routing used by CreateBucket.
func (b *DistributedBackend) SetRouter(r *Router) { b.router = r }

// SetShardGroupSource wires a ShardGroupSource (typically *MetaFSM) so
// CreateBucket can query the active group list for hash-based assignment.
// Must be called before serving traffic. Nil falls back to Router.RouteKey
// only (legacy default-group behavior).
func (b *DistributedBackend) SetShardGroupSource(s ShardGroupSource) { b.shardGroup = s }

// SetCapabilityProbe wires the per-node commit-combined capability probe so the
// combined PUT commit tail can verify every remote placement node advertises
// commit-combined before choosing the 1-round flow (rolling-upgrade gate). The
// probe takes a RAW placement node ID and resolves it to the gate's evidence
// key (raft address) internally — see NewCommitCombinedProbe. Must be called
// before serving traffic (like SetShardGroupSource). nil/unwired ⇒ remote
// targets are treated as not-capable and the commit tail keeps the legacy two
// rounds.
func (b *DistributedBackend) SetCapabilityProbe(fn func(nodeID string) bool) {
	b.capProbe = fn
}

// --- Bucket operations ---

// FSMRef returns the underlying FSM so reshard / monitor code can iterate
// placements + object metas without reaching through the backend's private fields.
func (b *DistributedBackend) FSMRef() *FSM { return b.fsm }

func (b *DistributedBackend) SetIncidentRecorder(rec IncidentRecorder) {
	b.incidentRecorder = rec
}

// QuarantineRouter routes a quarantine SET to the object's owning group so the
// owner's objectMetaRMWLock serializes it against concurrent owner-side writes
// (tags/ACL/PUT). The scrubber's placement monitor can run on a non-owner node
// (the balancer floats shards across nodes), where a leaf-local RMW would only
// hold THIS node's lock — racing the owner's blob writer and risking a lost
// quarantine flag on the MetaSeq LWW tiebreak. Satisfied by *ClusterCoordinator
// (cluster) and by *DistributedBackend itself (single-node leaf fallback).
type QuarantineRouter interface {
	QuarantineObject(ctx context.Context, bucket, key, versionID, cause, reason string) error
}

// SetQuarantineRouter installs the owner-routing path for the quarantine SET
// portion of QuarantineCorruptShardLocal/...AtShardKey. nil (default) keeps the
// leaf-local SET (correct for single-node and for the owner's own backend).
func (b *DistributedBackend) SetQuarantineRouter(r QuarantineRouter) {
	b.quarantineRouter = r
}

// quarantineSet performs the quarantine write: through the owner-routing
// QuarantineRouter when wired (cluster placement monitor on a possibly-non-owner
// node), else the leaf-local blob RMW (single-node / owner-local).
func (b *DistributedBackend) quarantineSet(ctx context.Context, bucket, key, versionID, cause, reason string) error {
	if b.quarantineRouter != nil {
		return b.quarantineRouter.QuarantineObject(ctx, bucket, key, versionID, cause, reason)
	}
	return b.QuarantineObject(ctx, bucket, key, versionID, cause, reason)
}

// LiveNodes returns the list of cluster nodes currently considered reachable.
// This is the public counterpart of the internal liveNodes() method.
func (b *DistributedBackend) LiveNodes() []string { return b.liveNodes() }

// ECActive reports whether Phase 18 cluster EC will be applied to the next
// PutObject call (EC enabled + enough placement slots for k+m split).
// Placement slots count cluster nodes in multi-node deployments and local
// drive roots in single-node multi-drive deployments.
func (b *DistributedBackend) ECActive() bool {
	return b.currentECConfig().IsActive(len(b.effectivePlacementNodes()))
}

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

// objectMetaRMWLock serializes read-modify-write of an object's quorum-meta
// blob (tag/ACL mutation, relocation re-write) on the owning coordinator so a
// second RMW reads the first's result instead of clobbering it. It takes the
// per-(bucket,key) write lock and returns an unlock closure; the underlying
// lock is reclaimed once the last holder releases (bounded memory).
func (b *DistributedBackend) objectMetaRMWLock(bucket, key string) func() {
	return b.objectMetaRMWLocks.lockWrite(bucket + "\x00" + key)
}

// Node returns the RaftNode interface for leadership and raft control.
func (b *DistributedBackend) Node() RaftNode { return b.node }
