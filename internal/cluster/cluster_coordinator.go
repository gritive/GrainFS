package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
)

// DefaultMaxForwardBodyBytes is the body cap for AppendObject forward buffering.
// Raised to 64 MiB to match the HTTP-layer appendBodyMaxBytes cap so that
// large append bodies are not rejected at the coordinator before reaching the
// forward path. PutObject and UploadPart use streamed forwarding and are
// unaffected.
const DefaultMaxForwardBodyBytes = 64 * 1024 * 1024

// DefaultMaxForwardReplyBytes follows the transport frame guard. Forwarded
// GetObject still returns one framed response; 16 MiB EC smoke reads fit here
// without reintroducing the request-body buffering fixed for writes.
const DefaultMaxForwardReplyBytes = 64 * 1024 * 1024

// minForwardStreamBytes is the body size at or above which a forwarded
// PutObject OR UploadPart streams its body (body-less FlatBuffer args + a
// separate body stream) instead of buffering the whole body into the args
// FlatBuffer. Below it (and at/under the maxBody single-frame cap) the body
// rides inside the args FlatBuffer in a single frame. PutObject and UploadPart
// deliberately share ONE floor.
//
// Set to 4 MiB from BenchmarkForwardPutObjectWire — the END-TO-END forward bench
// (two real HTTPTransports + a real receiver, so it includes the Hertz HTTP
// processing AND the receiver body parsing, unlike the sender-only
// BenchmarkForwardBodyEncode). That bench shows the SINGLE-FRAME path is
// actually FASTER than streaming across the whole 0.5–5 MiB range and the gap
// widens with size (1 MiB: frame 9.1 ms vs stream 13.0 ms; 5 MiB: 26.9 vs
// 44.6 ms), because streaming chunks the body into many small allocations
// (~11x the allocs of the one-shot frame buffer) and per-chunk HTTP framing.
// Framing's only cost is ~1.4x peak memory (body buffered into the args
// FlatBuffer). So the floor is NOT a latency win — it is a MEMORY cap for large
// objects: above it, a forwarded PUT/UploadPart streams to avoid buffering the
// whole (potentially tens-of-MiB) body. 4 MiB caps a framed request at ~41 MB/op
// while keeping the faster frame path for the common 1–4 MiB range; the previous
// PutObject behaviour only streamed above the 64 MiB single-frame cap, buffering
// far larger bodies. PutObject and UploadPart share this floor.
const minForwardStreamBytes = 4 * 1024 * 1024 // 4 MiB (see BenchmarkForwardPutObjectWire)

const auditBucketName = "grainfs-audit"

// ErrCoordinatorNoRouter is returned when OpRouter is called on a
// coordinator that was constructed without a router (test/solo-node configs
// that should not be reaching the routing path).
var ErrCoordinatorNoRouter = errors.New("coordinator: router not configured")

var ErrObjectIndexRequired = errors.New("coordinator: object index entry required")

// dataGroupManagerLeaderProbe adapts *DataGroupManager to OpRouter's
// dataGroupLeaderProbe interface — keeps raft node access details in
// cluster_coordinator.go rather than leaking *DataGroupManager into the
// new op_routing.go module.
type dataGroupManagerLeaderProbe struct{ m *DataGroupManager }

func (p dataGroupManagerLeaderProbe) GroupLeaderIsSelf(groupID string) bool {
	if p.m == nil {
		return false
	}
	dg := p.m.Get(groupID)
	if dg == nil {
		return false
	}
	b := dg.Backend()
	if b == nil {
		return false
	}
	probe := b.leaderProbe()
	return probe != nil && probe.IsLeader()
}

func isSnapshotSystemBucket(bucket string) bool {
	return storage.IsInternalBucket(bucket) || bucket == auditBucketName
}

// dataGroupManagerLocalBackend adapts *DataGroupManager to LocalExecution's
// localBackendLookup interface.
type dataGroupManagerLocalBackend struct{ m *DataGroupManager }

func (a dataGroupManagerLocalBackend) Backend(groupID string) *GroupBackend {
	if a.m == nil {
		return nil
	}
	dg := a.m.Get(groupID)
	if dg == nil {
		return nil
	}
	return dg.Backend()
}

// ErrForwardBodySizeMismatch is returned when a forwarded data-plane reply
// reports success but the returned metadata size does not match the body bytes
// that crossed the wire. Treating this as success can commit an empty object
// during transient bootstrap races and make e2e retries impossible.
var ErrForwardBodySizeMismatch = errors.New("coordinator: forwarded body size mismatch")

// ClusterCoordinator implements storage.Backend by routing bucket-scoped ops
// to the per-group raft leader and delegating cluster-wide ops to the base
// (meta-FSM-backed) backend.
//
// Wiring (set in serve.go):
//   - base    : DistributedBackend (cluster-wide bucket ops via meta-FSM)
//   - groups  : DataGroupManager  (per-group GroupBackend lookup)
//   - router  : Router            (bucket → groupID, snapshot from meta-FSM)
//   - meta    : ShardGroupSource  (groupID → peer list, snapshot from meta-FSM)
//   - forward : ForwardSender     (0x08 wire dialer; nil disables forwarding)
//   - selfID  : this node's ID    (drives self-leader and self-voter checks)
type ClusterCoordinator struct {
	base        storage.Backend
	groups      *DataGroupManager
	router      *Router
	meta        ShardGroupSource
	forward     *ForwardSender
	selfID      string
	selfAliases []string
	addr        NodeAddressBook
	ecConfig    ECConfig
	runtime     atomic.Pointer[clusterCoordinatorRuntime]
	capGate     *CapabilityGate

	opRouter  *OpRouter
	localExec *LocalExecution

	maxBody             int64
	appendForwardBuffer *appendForwardBuffer

	// recordGenZero records the initial placement generation (gen-0) once, on the
	// first object write, so every node routes objects over the same raft-replicated
	// candidate set instead of its divergent boot-frozen snapshot. nil disables the
	// behavior (single-node / test wiring). See ensureGenZero.
	recordGenZero func(ctx context.Context, groupIDs []string) error
}

type clusterCoordinatorRuntime struct {
	opRouter  *OpRouter
	localExec *LocalExecution
	ecConfig  ECConfig
}

// NewClusterCoordinator constructs a coordinator with the legacy 5 MiB
// single-message body cap. Production wiring installs streamed body forwarding.
// groups/router/meta may be nil for tests that exercise only cluster-wide
// delegations; routeBucket returns ErrCoordinatorNoRouter when reached without
// a router.
func NewClusterCoordinator(
	base storage.Backend,
	groups *DataGroupManager,
	router *Router,
	meta ShardGroupSource,
	selfID string,
) *ClusterCoordinator {
	c := &ClusterCoordinator{
		base:                base,
		groups:              groups,
		router:              router,
		meta:                meta,
		selfID:              selfID,
		maxBody:             DefaultMaxForwardBodyBytes,
		appendForwardBuffer: newAppendForwardBuffer(DefaultAppendForwardBufferConfig().TotalBytes),
	}
	// Order matters: rebuild() first (single-threaded here) does the one-time
	// plain-field opRouter/localExec init and publishes a non-nil c.runtime; ONLY
	// then register the post-commit hook. A hook-fired rebuild (from the meta-raft
	// apply goroutine) then always sees a non-nil c.runtime and takes the
	// atomic-Store-only path — never racing the constructor's first rebuild on the
	// plain-field writes. Generations already in the FSM (snapshot restore) are
	// reflected by this rebuild; generations applied after registration are caught
	// by the hook (and the builder-method rebuilds that follow re-read regardless).
	c.rebuild()
	c.registerTopologyRebuildHook()
	return c
}

// registerTopologyRebuildHook wires a meta-FSM post-commit hook that re-runs
// rebuild() whenever an AddPlacementGeneration command is applied on this node
// (S7-6). Without it, an applied generation add would stay inert until an
// unrelated rebuild or a restart — rebuild() is otherwise only called from
// boot/builder wiring. The hook re-reads PlacementGenerations() into the
// OpRouter and re-propagates the multi-generation flag to backends. No-op when
// c.meta does not support post-commit registration (test stubs).
func (c *ClusterCoordinator) registerTopologyRebuildHook() {
	type postCommitRegistrar interface {
		RegisterPostCommit(PostCommitHook)
	}
	reg, ok := c.meta.(postCommitRegistrar)
	if !ok {
		return
	}
	reg.RegisterPostCommit(func(cmdType clusterpb.MetaCmdType, _ []byte) {
		if cmdType == clusterpb.MetaCmdTypeAddPlacementGeneration {
			c.rebuild()
		}
	})
}

// WithForwardSender attaches the transport dialer used to send 0x08 forward calls
// to peer nodes. Returns the receiver for builder-style chaining in serve.go.
func (c *ClusterCoordinator) WithForwardSender(s *ForwardSender) *ClusterCoordinator {
	c.forward = s
	c.rebuild()
	return c
}

// WithNodeAddressResolver attaches the cluster address book used to translate
// nodeID PeerIDs into dialable transport addresses for runtime forwarding.
func (c *ClusterCoordinator) WithNodeAddressResolver(book NodeAddressBook) *ClusterCoordinator {
	c.addr = book
	c.rebuild()
	return c
}

// WithSelfPeerAlias records an additional peer identifier for this process.
// Static seed groups historically use raft addresses while dynamic groups can
// use node IDs; both must be treated as local for self-voter/leader shortcuts.
func (c *ClusterCoordinator) WithSelfPeerAlias(id string) *ClusterCoordinator {
	if id == "" {
		return c
	}
	c.selfAliases = append(c.selfAliases, id)
	c.rebuild()
	return c
}

func (c *ClusterCoordinator) WithECConfig(cfg ECConfig) *ClusterCoordinator {
	c.ecConfig = cfg
	c.rebuild()
	return c
}

// SetAppendForwardBufferConfig replaces the appendForwardBuffer semaphore with
// a new one sized to cfg.TotalBytes. Intended for test wiring and CLI flag
// injection; not concurrent-safe with in-flight forwards.
func (c *ClusterCoordinator) SetAppendForwardBufferConfig(cfg AppendForwardBufferConfig) {
	c.appendForwardBuffer = newAppendForwardBuffer(cfg.TotalBytes)
}

func (c *ClusterCoordinator) WithCapabilityGate(gate *CapabilityGate) *ClusterCoordinator {
	c.capGate = gate
	return c
}

// WithGenZeroRecorder installs the closure that records the initial placement
// generation (gen-0) into the control-plane meta-FSM (leader-serialized, raft-
// replicated). Production wires it to MetaRaft.ProposeAddPlacementGenerationForwarding
// so a write on any node can establish gen-0. nil (single-node / test wiring)
// disables lazy gen-0 capture entirely. See ensureGenZero.
func (c *ClusterCoordinator) WithGenZeroRecorder(fn func(ctx context.Context, groupIDs []string) error) *ClusterCoordinator {
	c.recordGenZero = fn
	return c
}

// rebuild constructs the embedded OpRouter and LocalExecution from the
// current dependency state. Called from every builder method and from
// NewClusterCoordinator. Keeping the modules embedded rather than passed
// per-call avoids per-request allocations on the hot path.
func (c *ClusterCoordinator) rebuild() {
	opRouter := NewOpRouter(
		c.router,
		c.meta,
		c.addr,
		dataGroupManagerLeaderProbe{m: c.groups},
		c.ecConfig,
		c.selfID,
		c.selfAliases,
	)
	// S7-4: consume the FSM topology-generation registry when present. Empty
	// (the default) leaves the live-candidate-set placement untouched →
	// byte-identical. The MetaFSM implements placementGenerationSource; test
	// stubs typically do not, so they stay on the single-generation path.
	if src, ok := c.meta.(placementGenerationSource); ok {
		opRouter.applyGenerations(src.PlacementGenerations())
	}
	localExec := NewLocalExecution(dataGroupManagerLocalBackend{m: c.groups})
	if c.runtime.Load() == nil {
		c.opRouter = opRouter
		c.localExec = localExec
	}
	c.runtime.Store(&clusterCoordinatorRuntime{
		opRouter:  opRouter,
		localExec: localExec,
		ecConfig:  c.ecConfig,
	})
	// S7-6: arm the cross-generation LWW read merge on every backend once the
	// topology has more than one placement generation. At a single generation
	// (the default) this propagates false → byte-identical local-first reads.
	c.propagateMultiGeneration(opRouter.generationCount() > 1)
}

// propagateMultiGeneration sets the multi-generation read-merge flag on every
// data-group backend (and the meta backend) so cross-generation LWW reads arm
// consistently on this node. Derived from the meta-raft-replicated generation
// registry, so every node converges on the same value. Called from rebuild(),
// including the rebuild fired by the AddPlacementGeneration post-commit hook.
func (c *ClusterCoordinator) propagateMultiGeneration(multiGen bool) {
	if c.groups != nil {
		for _, g := range c.groups.All() {
			if b := g.Backend(); b != nil {
				b.SetMultiGeneration(multiGen)
			}
		}
	}
	if b, ok := c.base.(*DistributedBackend); ok && b != nil {
		b.SetMultiGeneration(multiGen)
	}
}

func (c *ClusterCoordinator) runtimeState() clusterCoordinatorRuntime {
	if state := c.runtime.Load(); state != nil {
		return *state
	}
	return clusterCoordinatorRuntime{
		opRouter:  c.opRouter,
		localExec: c.localExec,
		ecConfig:  c.ecConfig,
	}
}

func (c *ClusterCoordinator) forwardRuntime() forwardRuntime {
	return forwardRuntime{
		sender:      c.forward,
		meta:        c.meta,
		addr:        c.addr,
		selfID:      c.selfID,
		selfAliases: c.selfAliases,
		maxBody:     c.maxBody,
		appendBuf:   c.appendForwardBuffer,
	}
}

// PlacementExpansionPlan describes a proposed topology-generation growth (S7-7):
// Base is the placement group set objects are currently routed under (the
// OpRouter's boot-frozen / latest-generation set); Expanded is the candidate set
// derived from the live shard groups (which includes any groups formed by node
// joins since boot). Added is Expanded minus Base; Removed is Base minus
// Expanded. Removed is normally empty, but can be non-empty when a newly-joined
// group is WIDER than the Base groups: candidateGroupsFor keeps only the
// widest-peer-count groups, so narrower Base groups drop out of the active
// placement set. Their existing objects stay readable via the prior generation
// (gen-0 probe) but stop receiving new writes — the operator must be shown this,
// not just the additions. NoOp is true when Expanded equals Base.
type PlacementExpansionPlan struct {
	Base     []string
	Expanded []string
	Added    []string
	Removed  []string
	NoOp     bool
}

// PlanPlacementExpansion computes the topology-generation growth that would
// activate the currently-formed-but-unused shard groups for object placement
// (S7-7). It does NOT mutate anything — the caller (serveruntime) proposes the
// generation via MetaRaft.AddTopologyGeneration(plan.Base, plan.Expanded). Base
// comes from the OpRouter (boot-frozen original / latest generation), which is
// the only authoritative source of the set existing objects were placed under;
// the live shard groups alone cannot reconstruct it once new groups have joined.
func (c *ClusterCoordinator) PlanPlacementExpansion() (PlacementExpansionPlan, error) {
	base := append([]string(nil), c.runtimeState().opRouter.currentPlacementGroupIDs()...)
	if len(base) == 0 {
		return PlacementExpansionPlan{}, fmt.Errorf("placement expansion: no current placement groups (bootstrap or no EC-active groups)")
	}
	if c.meta == nil {
		return PlacementExpansionPlan{}, fmt.Errorf("placement expansion: no shard-group source")
	}
	candidates, err := candidateGroupsFor(c.meta.ShardGroups(), c.ecConfig)
	if err != nil {
		return PlacementExpansionPlan{}, fmt.Errorf("placement expansion: candidate groups: %w", err)
	}
	// Durability invariant: never let the operator record a non-redundant generation
	// in a multi-node cluster. ensureGenZero's self-heal assumes the latest generation
	// regresses to non-redundant only at gen-0/boot (all producers append redundant-or-
	// wider sets); an operator expansion to a 1+0 set would break that and could strand
	// the self-heal in a non-advancing re-propose. Refuse it here at the proposer.
	if err := redundantPlacementGate(candidates, metaNodeCount(c.meta)); err != nil {
		return PlacementExpansionPlan{}, fmt.Errorf("placement expansion: %w", err)
	}
	expanded := make([]string, len(candidates))
	for i, cand := range candidates {
		expanded[i] = cand.ID
	}
	if stringSlicesEqual(base, expanded) {
		return PlacementExpansionPlan{Base: base, Expanded: expanded, NoOp: true}, nil
	}
	baseSet := make(map[string]struct{}, len(base))
	for _, id := range base {
		baseSet[id] = struct{}{}
	}
	expandedSet := make(map[string]struct{}, len(expanded))
	for _, id := range expanded {
		expandedSet[id] = struct{}{}
	}
	var added []string
	for _, id := range expanded {
		if _, ok := baseSet[id]; !ok {
			added = append(added, id)
		}
	}
	var removed []string
	for _, id := range base {
		if _, ok := expandedSet[id]; !ok {
			removed = append(removed, id)
		}
	}
	return PlacementExpansionPlan{Base: base, Expanded: expanded, Added: added, Removed: removed}, nil
}

// stringSlicesEqual reports whether two already-sorted string slices are
// element-wise equal. Both base (currentGroupIDs) and expanded (candidateGroupsFor)
// are sorted candidate-ID lists, so this is a sound set-equality test.
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// routeReadOrBucket resolves an object read to its placement-group target via
// deterministic hash placement (S4-4c: index-free). When the frozen placement
// candidate list is empty (bootstrap) or the key resolves to no object, it
// falls back to bucket routing so internal/sole-voter reads still resolve. The
// authoritative metadata now lives in quorum meta; the GroupBackend read (local
// via ResolveRead, or forwarded to the leader) resolves it — there is no object
// index to consult for routing.
func (c *ClusterCoordinator) routeReadOrBucket(bucket, key, versionID string) (RouteTarget, error) {
	target, _, err := c.runtimeState().opRouter.RouteObjectRead(bucket, key, versionID)
	if errors.Is(err, storage.ErrObjectNotFound) || errors.Is(err, ErrObjectIndexRequired) {
		if fallback, _, fallbackErr := c.routeWriteOrBucket(bucket, key); fallbackErr == nil {
			return fallback, nil
		}
	}
	return target, err
}

// placementGenerationSource is the optional MetaFSM capability that exposes the
// topology-generation registry. OpRouter consumes it via the coordinator
// (rebuild) rather than widening ShardGroupSource.
type placementGenerationSource interface {
	PlacementGenerations() []placementGeneration
}

// routeReadGenerations resolves an object read to one target per topology
// generation, newest-first (S7-4 probe order). At a single generation it returns
// the same single target routeReadOrBucket would, preserving the bootstrap
// bucket-route fallback. The returned slice is non-empty on success.
func (c *ClusterCoordinator) routeReadGenerations(bucket, key, versionID string) ([]RouteTarget, error) {
	targets, err := c.runtimeState().opRouter.RouteObjectReadGenerations(bucket, key, versionID)
	if errors.Is(err, storage.ErrObjectNotFound) || errors.Is(err, ErrObjectIndexRequired) {
		if fallback, _, fallbackErr := c.routeWriteOrBucket(bucket, key); fallbackErr == nil {
			return []RouteTarget{fallback}, nil
		}
	}
	return targets, err
}

// probeRead runs do against each topology-generation target newest-first,
// advancing to the next (older) generation ONLY on a definitive
// ErrObjectNotFound. Any other error (group unavailable, transport failure) is
// returned immediately (fail-closed) so a transiently-down older-generation
// group never masquerades as a 404 for an object that exists. At a single
// generation this is exactly one attempt — byte-identical to legacy routing.
func (c *ClusterCoordinator) probeRead(bucket, key, versionID string, do func(target RouteTarget) error) error {
	targets, err := c.routeReadGenerations(bucket, key, versionID)
	if err != nil {
		return err
	}
	var lastErr error
	for _, target := range targets {
		err := do(target)
		if err == nil {
			return nil
		}
		if !errors.Is(err, storage.ErrObjectNotFound) {
			return err
		}
		lastErr = err
	}
	return lastErr
}

// routeWriteOrBucket falls back to RouteBucket for single-node wiring (no EC).
func (c *ClusterCoordinator) routeWriteOrBucket(bucket, key string) (RouteTarget, ShardGroupEntry, error) {
	state := c.runtimeState()
	if state.ecConfig.NumShards() == 0 {
		target, err := state.opRouter.RouteBucket(bucket)
		return target, ShardGroupEntry{ID: target.GroupID}, err
	}
	return state.opRouter.RouteObjectWrite(bucket, key)
}

func (c *ClusterCoordinator) routeAppendOrBucket(bucket, key string, expectedOffset int64) (RouteTarget, ShardGroupEntry, error) {
	return c.routeWriteOrBucket(bucket, key)
}

// liveCandidateGroupIDs returns the candidate placement group-ID set derived
// from the LIVE shard-group registry (sorted, as candidateGroupsFor returns it).
// Unlike the OpRouter's boot-frozen placementGroupIDs, this reflects every group
// that has joined since boot — the correct gen-0 ground truth at formation, when
// zero objects have been written.
func (c *ClusterCoordinator) liveCandidateGroupIDs() ([]string, error) {
	if c.meta == nil {
		return nil, fmt.Errorf("gen-0: no shard-group source")
	}
	candidates, err := candidateGroupsFor(c.meta.ShardGroups(), c.ecConfig)
	if err != nil {
		return nil, err
	}
	// Durability gate: defer gen-0 capture while the candidate set is non-redundant
	// (1+0 single-peer groups) in a multi-node cluster. Capturing gen-0 here would
	// pin every object to a group a single node loss destroys. ensureGenZero treats
	// the error as "not ready yet" and retries on the next write. See redundantPlacementGate.
	if err := redundantPlacementGate(candidates, metaNodeCount(c.meta)); err != nil {
		return nil, err
	}
	ids := make([]string, len(candidates))
	for i, cand := range candidates {
		ids[i] = cand.ID
	}
	return ids, nil
}

// ensureGenZero lazily establishes a consistent, REDUNDANT placement generation on
// object writes. In a per-join cluster each node boot-freezes a partial, divergent
// shard-group snapshot, so the same key hash-routes to different groups on different
// nodes (append fragments → InvalidWriteOffset). Recording a generation once into the
// meta-FSM (replicated) makes every node's rebuild() apply the SAME candidate set, so
// routing converges.
//
// Durability self-heal: liveCandidateGroupIDs is gated to redundant-only groups, so
// while the multi-node cluster is still forming (only 1+0 single-peer groups exist)
// gen-0 capture is DEFERRED — no object is pinned to a group a single node loss would
// destroy. If a generation was nonetheless recorded over a non-redundant set (the
// narrow formation-race window where only one node is registered), this advances it:
// once redundant groups form, it appends a new generation over them so subsequent
// writes route redundantly, while objects under the old generation stay readable via
// the newest-first read probe. When the latest generation is already redundant,
// further growth is the operator's job (expand-placement), so this no-ops.
//
// Concurrent writers all propose the same set; the FSM apply dedups against the whole
// registry, so they converge without a new lock. Best-effort: a propose failure is
// logged and retried by the next write rather than failing the user's request.
func (c *ClusterCoordinator) ensureGenZero(ctx context.Context) {
	if c.recordGenZero == nil {
		return
	}
	src, ok := c.meta.(placementGenerationSource)
	if !ok {
		return
	}
	gens := src.PlacementGenerations()
	if len(gens) > 0 && c.placementGenerationRedundant(gens[len(gens)-1]) {
		return // latest generation already redundant — further growth is the operator's
	}
	// Invariant that bounds the self-heal: the only producers of placement
	// generations are this self-heal (appends redundant sets only) and the operator
	// expand-placement (appends WIDER, so still redundant, sets). So once any
	// redundant generation is appended the latest stays redundant — the latest is
	// non-redundant only at gen-0/boot before the first redundant append. Therefore
	// the redundant set computed below is always NEW (not a registry duplicate), so
	// the FSM dedup never silently no-ops it into a non-advancing loop.
	ids, err := c.liveCandidateGroupIDs()
	if err != nil || len(ids) == 0 {
		return // no redundant candidate groups yet — defer until the cluster forms them
	}
	if err := c.recordGenZero(ctx, ids); err != nil {
		log.Warn().Err(err).Strs("groups", ids).
			Msg("redundant placement generation record failed; retrying on next write")
		return
	}
	log.Debug().Strs("groups", ids).Int("prior_generations", len(gens)).
		Msg("recorded redundant placement generation")
}

// placementGenerationRedundant reports whether a placement generation's groups can
// survive a single-node loss. candidateGroupsFor produces a uniform-width set, so the
// first live-resolvable group represents the generation. An unresolvable generation
// (groups gone) is treated as non-redundant so the self-heal re-derives from live.
func (c *ClusterCoordinator) placementGenerationRedundant(gen placementGeneration) bool {
	if c.meta == nil {
		return false
	}
	for _, id := range gen.groupIDs {
		if g, ok := c.meta.ShardGroup(id); ok {
			return DesiredECConfigForGroup(g).Redundant()
		}
	}
	return false
}

func (c *ClusterCoordinator) requireObjectBucket(ctx context.Context, bucket string) error {
	if storage.IsInternalBucket(bucket) || c.base == nil {
		return nil
	}
	if c.bucketAssigned(bucket) {
		return nil
	}
	err := c.base.HeadBucket(ctx, bucket)
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrBucketNotFound) && c.bucketAssigned(bucket) {
		return nil
	}
	return err
}

func (c *ClusterCoordinator) matchSelfPeer(id string) bool {
	_, ok := NewShardGroupPeerSet(ShardGroupEntry{PeerIDs: []string{id}}).MatchLocal(c.selfID, c.selfAliases...)
	return ok
}

// --- Cluster-wide delegations (4 ops) ---
//
// These bypass routing entirely. CreateBucket and friends are always served by
// the meta-Raft (via base = DistributedBackend), keeping bucket-creation
// linearizable across the cluster regardless of which group later owns it.

func (c *ClusterCoordinator) CreateBucket(ctx context.Context, bucket string) error {
	return c.base.CreateBucket(ctx, bucket)
}

// CreateBucketBypassReserved seeds a reserved bucket via the meta-Raft, bypassing
// the reserved-name guard. Called only during bootstrap to create "default" and "_grainfs".
func (c *ClusterCoordinator) CreateBucketBypassReserved(ctx context.Context, bucket string) error {
	type bypassSeeder interface {
		CreateBucketBypassReserved(ctx context.Context, bucket string) error
	}
	if s, ok := c.base.(bypassSeeder); ok {
		return s.CreateBucketBypassReserved(ctx, bucket)
	}
	return c.base.CreateBucket(ctx, bucket)
}

func (c *ClusterCoordinator) HeadBucket(ctx context.Context, bucket string) error {
	err := c.base.HeadBucket(ctx, bucket)
	if err == nil {
		return nil
	}
	if c.bucketAssigned(bucket) {
		return nil
	}
	return err
}
func (c *ClusterCoordinator) DeleteBucket(ctx context.Context, bucket string) error {
	if c.router != nil && c.meta != nil {
		objects, err := c.ListObjects(ctx, bucket, "", 1)
		if err != nil {
			return err
		}
		if len(objects) > 0 {
			return storage.ErrBucketNotEmpty
		}
	}
	return c.base.DeleteBucket(ctx, bucket)
}

// ForceDeleteBucket deletes all objects in the bucket and then removes it.
// Unlike DeleteBucket, it does not fail when the bucket is non-empty.
func (c *ClusterCoordinator) ForceDeleteBucket(ctx context.Context, bucket string) error {
	return c.base.ForceDeleteBucket(ctx, bucket)
}

func (c *ClusterCoordinator) ListBuckets(ctx context.Context) ([]string, error) {
	buckets, err := c.base.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(buckets))
	for _, bucket := range buckets {
		seen[bucket] = struct{}{}
	}
	if src, ok := c.meta.(interface{ BucketAssignments() map[string]string }); ok {
		for bucket := range src.BucketAssignments() {
			seen[bucket] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for bucket := range seen {
		out = append(out, bucket)
	}
	sort.Strings(out)
	return out, nil
}

func (c *ClusterCoordinator) bucketAssigned(bucket string) bool {
	if c.router != nil {
		if _, ok := c.router.ExplicitGroup(bucket); ok {
			return true
		}
	}
	if src, ok := c.meta.(interface{ BucketAssignments() map[string]string }); ok {
		_, ok := src.BucketAssignments()[bucket]
		return ok
	}
	return false
}

func (c *ClusterCoordinator) SetBucketVersioning(bucket, state string) error {
	type proposer interface {
		SetBucketVersioningPropose(bucket, state string) error
	}
	type bucketVersioner interface {
		SetBucketVersioning(bucket, state string) error
	}
	// Cluster-aware pre-check: on a freshly bootstrapped cluster the follower
	// may have the bucket assignment from meta-Raft but not yet have applied
	// the CmdCreateBucket data-Raft entry locally. The base layer's
	// local-only pre-check would reject the request with NoSuchBucket and
	// warp's `versioned` workload would fail at PutBucketVersioning.
	if err := c.HeadBucket(context.Background(), bucket); err != nil {
		return err
	}
	// Prefer the propose-only entrypoint when the base exposes it; that
	// skips the duplicate local HeadBucket pre-check inside
	// DistributedBackend.SetBucketVersioning, which would otherwise reject
	// the follower path we just allowed through the cluster-aware check.
	if p, ok := c.base.(proposer); ok {
		return p.SetBucketVersioningPropose(bucket, state)
	}
	v, ok := c.base.(bucketVersioner)
	if !ok {
		return ErrCoordinatorNoRouter
	}
	return v.SetBucketVersioning(bucket, state)
}

func (c *ClusterCoordinator) GetBucketVersioning(bucket string) (string, error) {
	type bucketVersioner interface {
		GetBucketVersioning(bucket string) (string, error)
	}
	v, ok := c.base.(bucketVersioner)
	if !ok {
		return "", ErrCoordinatorNoRouter
	}
	return v.GetBucketVersioning(bucket)
}

func (c *ClusterCoordinator) GetBucketSoleAuthEpoch(bucket string) (uint32, error) {
	type soleAuthEpochReader interface {
		GetBucketSoleAuthEpoch(bucket string) (uint32, error)
	}
	v, ok := c.base.(soleAuthEpochReader)
	if !ok {
		return 0, ErrCoordinatorNoRouter
	}
	return v.GetBucketSoleAuthEpoch(bucket)
}

// GetBucketSoleAuthority returns the live soleauth state for the bucket by
// delegating to the base backend (DistributedBackend). Returns soleAuthOff
// ("off") when the key is absent. Used by the cluster-wide snapshot guard to
// detect soleauth-on buckets before attempting a cluster-wide capture.
func (c *ClusterCoordinator) GetBucketSoleAuthority(bucket string) (string, error) {
	type soleAuthorityReader interface {
		GetBucketSoleAuthority(bucket string) (string, error)
	}
	v, ok := c.base.(soleAuthorityReader)
	if !ok {
		return "", ErrCoordinatorNoRouter
	}
	return v.GetBucketSoleAuthority(bucket)
}

func (c *ClusterCoordinator) SetBucketPolicy(bucket string, policyJSON []byte) error {
	type proposer interface {
		SetBucketPolicyPropose(bucket string, policyJSON []byte) error
	}
	type policyBackend interface {
		SetBucketPolicy(bucket string, policyJSON []byte) error
	}
	if err := c.HeadBucket(context.Background(), bucket); err != nil {
		return err
	}
	if p, ok := c.base.(proposer); ok {
		return p.SetBucketPolicyPropose(bucket, policyJSON)
	}
	p, ok := c.base.(policyBackend)
	if !ok {
		return ErrCoordinatorNoRouter
	}
	return p.SetBucketPolicy(bucket, policyJSON)
}

func (c *ClusterCoordinator) GetBucketPolicy(bucket string) ([]byte, error) {
	type policyBackend interface {
		GetBucketPolicy(bucket string) ([]byte, error)
	}
	p, ok := c.base.(policyBackend)
	if !ok {
		return nil, ErrCoordinatorNoRouter
	}
	return p.GetBucketPolicy(bucket)
}

func (c *ClusterCoordinator) DeleteBucketPolicy(bucket string) error {
	type proposer interface {
		DeleteBucketPolicyPropose(bucket string) error
	}
	type policyBackend interface {
		DeleteBucketPolicy(bucket string) error
	}
	if err := c.HeadBucket(context.Background(), bucket); err != nil {
		return err
	}
	if p, ok := c.base.(proposer); ok {
		return p.DeleteBucketPolicyPropose(bucket)
	}
	p, ok := c.base.(policyBackend)
	if !ok {
		return ErrCoordinatorNoRouter
	}
	return p.DeleteBucketPolicy(bucket)
}

// ListAllObjects implements storage.Snapshotable by enumerating bucket-routed
// object versions across every cluster-wide bucket.
func (c *ClusterCoordinator) ListAllObjects() ([]storage.SnapshotObject, error) {
	if c.router == nil || c.groups == nil {
		snap, ok := c.base.(storage.Snapshotable)
		if !ok {
			return nil, storage.ErrSnapshotNotSupported
		}
		return snap.ListAllObjects()
	}
	buckets, err := c.ListBuckets(context.Background())
	if err != nil {
		return nil, err
	}
	var out []storage.SnapshotObject
	for _, bucket := range buckets {
		if isSnapshotSystemBucket(bucket) {
			log.Debug().
				Str("event", "snapshot_list_skip_system_bucket").
				Str("bucket", bucket).
				Msg("snapshot metadata listing skipped system bucket")
			continue
		}
		// Fail-closed guard: cluster-wide snapshot capture cannot correctly
		// enumerate per-node per-version quorum-meta blobs for soleauth-on
		// buckets. Per-node capture is required (S4c-d). Never silently produce
		// a fidelity-less snapshot.
		if saState, saErr := c.GetBucketSoleAuthority(bucket); saErr != nil {
			return nil, fmt.Errorf("cluster-wide snapshot capture: soleauth state lookup failed for bucket %q: %w", bucket, saErr)
		} else if saState == soleAuthOn {
			return nil, fmt.Errorf("cluster-wide snapshot capture does not support soleauth-on bucket %q: per-node capture required (S4c-d)", bucket)
		}
		versions, err := c.ListObjectVersions(context.Background(), bucket, "", 0)
		if err != nil {
			return nil, err
		}
		for _, version := range versions {
			snap := storage.SnapshotObject{
				Bucket:         bucket,
				Key:            version.Key,
				ETag:           version.ETag,
				Size:           version.Size,
				Modified:       version.LastModified,
				VersionID:      version.VersionID,
				IsDeleteMarker: version.IsDeleteMarker,
				IsLatest:       version.IsLatest,
				// Tags copied (not aliased) so snapshot survives even if the
				// enrichment block below is skipped (delete marker or
				// GetObjectVersion error). Mirror of snapshotable.go fix in
				// e7c7114d — otherwise the previously-fixed RestoreObjects
				// Tags-forward path is dead code on the coordinator route.
				Tags: append([]storage.Tag(nil), version.Tags...),
			}
			if !version.IsDeleteMarker {
				// Enrich metadata from the data file when readable. A metadata
				// snapshot must not fail just because one blob is currently
				// unreadable (e.g. mid-write, EC-stored without a plain-file
				// fallback) — fall back to the version-listing fields.
				if rc, obj, err := c.GetObjectVersion(context.Background(), bucket, version.Key, version.VersionID); err == nil {
					_ = rc.Close()
					snap.ETag = obj.ETag
					snap.Size = obj.Size
					snap.ContentType = obj.ContentType
					snap.Modified = obj.LastModified
					snap.ACL = obj.ACL
					// Parity with ACL enrichment: prefer the authoritative
					// obj.Tags over the version-listing fallback when readable.
					snap.Tags = append([]storage.Tag(nil), obj.Tags...)
				} else {
					log.Warn().Str("bucket", bucket).Str("key", version.Key).
						Str("version", version.VersionID).Err(err).
						Msg("ListAllObjects: object data unreadable, snapshotting metadata only")
				}
			}
			out = append(out, snap)
		}
	}
	return out, nil
}

// RestoreObjects implements storage.Snapshotable by routing object metadata
// restore to the data group that owns each object's bucket.
func (c *ClusterCoordinator) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	if c.router == nil || c.groups == nil {
		snap, ok := c.base.(storage.Snapshotable)
		if !ok {
			return 0, nil, storage.ErrSnapshotNotSupported
		}
		return snap.RestoreObjects(objects)
	}

	// Fail-closed guard: if any snapshot object's bucket is currently
	// soleauth-on, refuse the whole restore before mutating anything.
	// Per-node restore is required for soleauth-on buckets (S4c-d).
	seen := make(map[string]struct{}, len(objects))
	for _, obj := range objects {
		if _, already := seen[obj.Bucket]; already {
			continue
		}
		seen[obj.Bucket] = struct{}{}
		saState, saErr := c.GetBucketSoleAuthority(obj.Bucket)
		if saErr != nil {
			return 0, nil, fmt.Errorf("cluster-wide snapshot restore: soleauth state lookup failed for bucket %q: %w", obj.Bucket, saErr)
		}
		if saState == soleAuthOn {
			return 0, nil, fmt.Errorf("cluster-wide snapshot restore does not support soleauth-on bucket %q: per-node restore required (S4c-d)", obj.Bucket)
		}
	}

	want := make(map[string]struct{}, len(objects))
	for _, obj := range objects {
		want[obj.Bucket+"\x00"+obj.Key] = struct{}{}
	}
	current, err := c.ListAllObjects()
	if err != nil {
		return 0, nil, err
	}
	for _, obj := range current {
		if _, ok := want[obj.Bucket+"\x00"+obj.Key]; ok {
			continue
		}
		if err := c.DeleteObject(context.Background(), obj.Bucket, obj.Key); err != nil {
			return 0, nil, err
		}
	}

	byGroup := make(map[string][]storage.SnapshotObject)
	for _, obj := range objects {
		target, _, err := c.routeWriteOrBucket(obj.Bucket, obj.Key)
		if err != nil {
			return 0, nil, err
		}
		byGroup[target.GroupID] = append(byGroup[target.GroupID], obj)
	}

	var restored int
	var stale []storage.StaleBlob
	for groupID, groupObjects := range byGroup {
		gb := c.localBackend(groupID)
		if gb == nil {
			return restored, stale, ErrCoordinatorNoRouter
		}
		count, groupStale, err := gb.RestoreObjects(groupObjects)
		restored += count
		stale = append(stale, groupStale...)
		if err != nil {
			log.Warn().
				Str("event", "snapshot_restore_group_failed").
				Str("group_id", groupID).
				Int("objects", len(groupObjects)).
				Int("restored", restored).
				Int("stale_blobs", len(stale)).
				Err(err).
				Msg("snapshot restore failed while restoring placement group")
			return restored, stale, err
		}
	}
	return restored, stale, nil
}

// ListAllBuckets implements storage.BucketSnapshotable by delegating to the
// base backend.
func (c *ClusterCoordinator) ListAllBuckets() ([]storage.SnapshotBucket, error) {
	snap, ok := c.base.(storage.BucketSnapshotable)
	if !ok {
		return nil, storage.ErrSnapshotNotSupported
	}
	return snap.ListAllBuckets()
}

// RestoreBuckets implements storage.BucketSnapshotable by delegating to the
// base backend.
func (c *ClusterCoordinator) RestoreBuckets(buckets []storage.SnapshotBucket) error {
	// Fail-closed guard (BEFORE any delegation): snapshot.Restore replays
	// RestoreBuckets BEFORE RestoreObjects (snapshot/manager.go). A routed
	// cluster cannot correctly restore soleauth-on buckets cluster-wide
	// (per-node capture/restore is required, S4c-d), and RestoreObjects already
	// rejects them. Reject the whole bucket replay up front so a soleauth-on
	// snapshot never flips a bucket to terminal `on` and then fails the object
	// restore — that would leave a partial restore (bucket metadata mutated,
	// objects absent).
	for _, b := range buckets {
		if b.SoleAuthState == soleAuthOn {
			return fmt.Errorf("cluster-wide snapshot restore does not support soleauth-on bucket %q: per-node restore required (S4c-d)", b.Name)
		}
	}
	snap, ok := c.base.(storage.BucketSnapshotable)
	if !ok {
		return storage.ErrSnapshotNotSupported
	}
	return snap.RestoreBuckets(buckets)
}

func contextWithObjectWritePlacement(ctx context.Context, group ShardGroupEntry) context.Context {
	if len(group.PeerIDs) == 0 {
		return ContextWithPlacementGroup(ctx, group.ID)
	}
	return ContextWithPlacementGroupEntry(ctx, group)
}

type bucketVersionedCtxKey struct{}

// ContextWithBucketVersioning stamps an authoritative versioning decision for
// this PUT. The S3 handler resolves the bucket versioning state at the edge
// (the same place AppendObject reads it) and ALWAYS stamps it — both enabled
// and disabled — so the per-group commit backend never has to read bucket
// versioning itself. That read would be a control/data-plane boundary
// violation (the commit backend has no replicated bucketver state). The
// decision also rides the forward wire (PutObjectArgs.versioning_state) so a
// forwarded PUT received by another node carries the same authoritative value.
func ContextWithBucketVersioning(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, bucketVersionedCtxKey{}, enabled)
}

// bucketVersioningFromContext returns the stamped versioning decision and
// whether it was resolved. resolved is false only when no edge/forward layer
// stamped the context (e.g. an in-process DistributedBackend test that bypasses
// the coordinator), in which case the caller falls back to a local read.
func bucketVersioningFromContext(ctx context.Context) (enabled bool, resolved bool) {
	v, ok := ctx.Value(bucketVersionedCtxKey{}).(bool)
	return v, ok
}

// BucketVersioningFromContext is the exported accessor for the stamped
// versioning decision. Used by the server edge (and tests) to inspect whether
// the authoritative versioning flag was threaded into ctx.
func BucketVersioningFromContext(ctx context.Context) (enabled bool, resolved bool) {
	return bucketVersioningFromContext(ctx)
}

type bucketSoleAuthEpochCtxKey struct{}

// ContextWithBucketSoleAuthEpoch stamps the originating node's authoritative
// sole-authority epoch for this PUT. The S3 handler resolves the bucket's
// soleauthepoch:{b} value at the edge (the same place it can read replicated
// bucket state) and stamps it so the per-group commit backend never has to read
// it itself — that read would be a control/data-plane boundary violation (the
// commit backend has no replicated soleauthepoch state). The decision also
// rides the forward wire (PutObjectArgs.soleauth_epoch, +1 encoded) so a
// forwarded PUT received by another node carries the same authoritative epoch.
func ContextWithBucketSoleAuthEpoch(ctx context.Context, epoch uint32) context.Context {
	return context.WithValue(ctx, bucketSoleAuthEpochCtxKey{}, epoch)
}

// bucketSoleAuthEpochFromContext returns the stamped sole-authority epoch and
// whether it was resolved. resolved is false only when no edge/forward layer
// stamped the context, in which case the caller falls back to a local read.
func bucketSoleAuthEpochFromContext(ctx context.Context) (epoch uint32, resolved bool) {
	v, ok := ctx.Value(bucketSoleAuthEpochCtxKey{}).(uint32)
	return v, ok
}

// BucketSoleAuthEpochFromContext is the exported accessor for the stamped
// sole-authority epoch. Used by the server edge (and tests) to inspect whether
// the authoritative epoch was threaded into ctx.
func BucketSoleAuthEpochFromContext(ctx context.Context) (epoch uint32, resolved bool) {
	return bucketSoleAuthEpochFromContext(ctx)
}

func topologyForwardWriteError(group ShardGroupEntry, err error) error {
	if err == nil || !errors.Is(err, ErrNoReachablePeer) || len(group.PeerIDs) == 0 {
		return err
	}
	cfg := DesiredECConfigForGroup(group)
	if cfg.NumShards() == 0 || len(group.PeerIDs) < cfg.NumShards() {
		return err
	}
	return &ErrInsufficientPlacementTargets{
		Operation:     "put_object",
		GroupID:       group.ID,
		Desired:       cfg,
		Configured:    cloneStringSlice(group.PeerIDs),
		Unavailable:   cloneStringSlice(group.PeerIDs),
		FailureReason: fmt.Sprintf("forward target unavailable: %v", err),
	}
}

func logForwardReplyDecodeError(err error, bucket, key, groupID string, op raftpb.ForwardOp, reply []byte) {
	log.Warn().
		Err(err).
		Str("bucket", bucket).
		Str("key", key).
		Str("group_id", groupID).
		Str("op", op.String()).
		Str("forward_status", forwardReplyStatusString(op, reply)).
		Bool("has_object", forwardReplyHasObject(reply)).
		Int("reply_bytes", len(reply)).
		Msg("forward: decode reply failed")
}

// localBackend returns the GroupBackend embedded in the named group. Caller
// guarantees groups != nil and the group exists. Returns nil if any link is
// missing.
func (c *ClusterCoordinator) localBackend(groupID string) *GroupBackend {
	if c.groups == nil {
		return nil
	}
	dg := c.groups.Get(groupID)
	if dg == nil {
		return nil
	}
	return dg.Backend()
}

// --- Bucket-scoped routings (8 of 10 — PutObject + UploadPart in T7) ---
//
// All eight share the same shape:
//  1. routeBucket → groupID, peer order, self-leader hint
//  2. self-leader: call local GroupBackend (skip wire)
//  3. else: forward.Send → reply parse
//
// The wire opcode is one of raftpb.ForwardOp* values; the reply layout is
// dictated by ForwardReply (see forward_codec.go).

// GetObject reads the object body and metadata. Production forwarding streams
// response body bytes after the metadata reply; legacy tests can still use the
// single-frame read_body fallback.
func (c *ClusterCoordinator) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	// S4-4c: index-free. ResolveRead serves locally only when this node can
	// answer authoritatively (sole voter, internal bucket, or a leader doing a
	// linearizable ReadIndex); otherwise it returns nil and we forward to the
	// placement-group leader. The GroupBackend resolves object metadata from
	// quorum meta (local file → peer fan-out) and folds a delete-marker latest
	// version to ErrObjectNotFound, so the unversioned-GET 404 semantics that the
	// object index used to short-circuit are now served by the backend read.
	// S7-4: probeRead tries each topology generation newest-first; at a single
	// generation (the default) it is one attempt against the same target.
	var (
		rc  io.ReadCloser
		obj *storage.Object
	)
	err := c.probeRead(bucket, key, "", func(target RouteTarget) error {
		if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
			return err
		} else if gb != nil {
			r, o, e := gb.GetObject(ctx, bucket, key)
			if e != nil {
				return e
			}
			rc, obj = r, o
			return nil
		}
		args := buildGetObjectArgs(bucket, key, versioningStateFromContext(ctx))
		r, o, e := c.forwardRuntime().readObject(ctx, target, raftpb.ForwardOpGetObject, args)
		if e != nil {
			return e
		}
		rc, obj = r, o
		return nil
	})
	return rc, obj, err
}

func (c *ClusterCoordinator) GetObjectVersion(
	ctx context.Context, bucket, key, versionID string,
) (io.ReadCloser, *storage.Object, error) {
	// S4-4c: index-free (see GetObject). Local serve only when authoritative;
	// otherwise forward to the placement-group leader.
	// S7-4c: a specific versionID lives in exactly one generation; probeRead
	// walks generations newest-first until the version is found (advancing only
	// on not-found, fail-closed otherwise). Single generation → one attempt.
	var (
		rc  io.ReadCloser
		obj *storage.Object
	)
	err := c.probeRead(bucket, key, versionID, func(target RouteTarget) error {
		if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
			return err
		} else if gb != nil {
			r, o, e := gb.getObjectVersionCtx(ctx, bucket, key, versionID)
			if e != nil {
				return e
			}
			rc, obj = r, o
			return nil
		}
		args := buildGetObjectVersionArgs(bucket, key, versionID, versioningStateFromContext(ctx))
		r, o, e := c.forwardRuntime().readObject(ctx, target, raftpb.ForwardOpGetObjectVersion, args)
		if e != nil {
			return e
		}
		rc, obj = r, o
		return nil
	})
	return rc, obj, err
}

func (c *ClusterCoordinator) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	// S4-4c: index-free (see GetObject). The GroupBackend HeadObject folds a
	// delete-marker latest version to ErrObjectNotFound, preserving the
	// unversioned-HEAD 404 semantics the object index used to short-circuit.
	// S7-4: probeRead tries each topology generation newest-first; one attempt
	// at a single generation (the default).
	var obj *storage.Object
	err := c.probeRead(bucket, key, "", func(target RouteTarget) error {
		if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
			return err
		} else if gb != nil {
			o, e := gb.HeadObject(ctx, bucket, key)
			if e != nil {
				return e
			}
			obj = o
			return nil
		}
		args := buildHeadObjectArgs(bucket, key, versioningStateFromContext(ctx))
		o, e := c.forwardRuntime().headObject(ctx, target, raftpb.ForwardOpHeadObject, args, bucket, key)
		if e != nil {
			return e
		}
		obj = o
		return nil
	})
	return obj, err
}

func (c *ClusterCoordinator) HeadObjectVersion(ctx context.Context, bucket, key, versionID string) (*storage.Object, error) {
	// S7-4c: probe generations newest-first for the specific version (one
	// attempt at a single generation).
	var obj *storage.Object
	err := c.probeRead(bucket, key, versionID, func(target RouteTarget) error {
		if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
			return err
		} else if gb != nil {
			o, e := gb.headObjectVersionCtx(ctx, bucket, key, versionID)
			if e != nil {
				return e
			}
			obj = o
			return nil
		}
		args := buildHeadObjectVersionArgs(bucket, key, versionID, versioningStateFromContext(ctx))
		o, e := c.forwardRuntime().headObject(ctx, target, raftpb.ForwardOpHeadObjectVersion, args, bucket, key)
		if e != nil {
			return e
		}
		obj = o
		return nil
	})
	return obj, err
}

func (c *ClusterCoordinator) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := c.DeleteObjectReturningMarker(bucket, key)
	return err
}

func (c *ClusterCoordinator) DeleteObjectReturningMarker(bucket, key string) (string, error) {
	ctx := context.Background()
	if err := c.requireObjectBucket(ctx, bucket); err != nil {
		return "", err
	}
	target, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return "", err
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return "", err
	} else if gb != nil {
		return gb.DeleteObjectReturningMarker(bucket, key)
	}
	args := buildDeleteObjectArgs(bucket, key)
	return c.forwardRuntime().deleteObject(ctx, target, args)
}

func (c *ClusterCoordinator) DeleteObjectVersion(bucket, key, versionID string) error {
	ctx := context.Background()
	targets, err := c.routeReadGenerations(bucket, key, versionID)
	if err != nil {
		return err
	}
	// A version record lives in exactly one generation group — whichever the key
	// hashed to when that version was written — but routing cannot tell which
	// without reading. applyDeleteObjectVersion is idempotent (no-op when the
	// version is absent), so we cannot use probeRead's stop-on-first-success loop
	// (the newest-gen group would "succeed" by no-op and we'd never reach the
	// resident older-gen group). Instead fan the delete out to every generation
	// group: the resident group deletes the record, the rest no-op idempotently
	// (a group that holds neither this version nor a stale lat: pointer to it does
	// nothing; the apply.go:394-427 latest-recompute is local and harmless).
	// Dedup repeated group IDs (a key may hash to the same group across
	// generations) and fail-closed on the first real error so the client retries
	// the whole (idempotent) fan-out rather than leaving the record behind in a
	// transiently-unreachable group.
	seen := make(map[string]struct{}, len(targets))
	var firstErr error
	for _, target := range targets {
		if _, dup := seen[target.GroupID]; dup {
			continue
		}
		seen[target.GroupID] = struct{}{}
		if derr := c.deleteObjectVersionOnTarget(ctx, target, bucket, key, versionID); derr != nil && firstErr == nil {
			firstErr = derr
		}
	}
	return firstErr
}

func (c *ClusterCoordinator) deleteObjectVersionOnTarget(ctx context.Context, target RouteTarget, bucket, key, versionID string) error {
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return err
	} else if gb != nil {
		return gb.DeleteObjectVersion(bucket, key, versionID)
	}
	args := buildDeleteObjectVersionArgs(bucket, key, versionID)
	return c.forwardRuntime().mutateFrame(ctx, target, raftpb.ForwardOpDeleteObjectVersion, args)
}

func (c *ClusterCoordinator) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	objects, _, err := c.ListObjectsPage(ctx, bucket, prefix, "", maxKeys)
	return objects, err
}

// ListObjectsPage returns one S3 ListObjects page. Entries with key > marker
// are returned, up to maxKeys. truncated reports whether more entries match
// beyond the returned slice — the S3 handler maps this to IsTruncated and
// NextMarker.
func (c *ClusterCoordinator) ListObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) (objects []*storage.Object, truncated bool, err error) {
	target, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return nil, false, err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return nil, false, err
	} else if gb != nil {
		return gb.ListObjectsPage(ctx, bucket, prefix, marker, maxKeys)
	}
	return c.forwardRuntime().listObjects(ctx, target, bucket, prefix, marker, maxKeys)
}

// ListObjectVersions enumerates every version of every object in the bucket.
// Objects are key-hash-placed across shard groups (RouteObjectWrite →
// groupIDForObject), so a single-group read (RouteBucket) would only see the
// versions that happen to live in the bucket's assigned group. This fans out
// across ALL shard groups, unions each group's local FSM enumeration, and
// reconciles a single authoritative IsLatest per key.
func (c *ClusterCoordinator) ListObjectVersions(
	ctx context.Context, bucket, prefix string, maxKeys int,
) ([]*storage.ObjectVersion, error) {
	state := c.runtimeState()
	groups := c.shardGroupsForVersionedList()
	// Internal buckets are single-group and unversioned; with ≤1 group the
	// fan-out is identical to the single-group read, so keep the simpler path
	// (also covers legacy / minimally-wired tests with no meta source).
	if storage.IsInternalBucket(bucket) || len(groups) <= 1 {
		return c.listObjectVersionsSingleGroup(ctx, state, bucket, prefix, maxKeys)
	}
	// Validate the bucket once (the single-group path returned ErrNoSuchBucket
	// via RouteBucket; preserve that — group leaves bypass the bucket check).
	if _, err := state.opRouter.RouteBucket(bucket); err != nil {
		return nil, err
	}
	merged, err := c.fanOutListObjectVersions(ctx, state, groups, bucket, prefix, maxKeys)
	if err != nil {
		return nil, err
	}
	reconcileVersionIsLatest(merged)
	sortObjectVersions(merged)
	if maxKeys > 0 && len(merged) > maxKeys {
		merged = merged[:maxKeys]
	}
	return merged, nil
}

// shardGroupsForVersionedList returns the cluster-wide shard group list (NOT
// c.groups.All(), which is local-only). Empty when no meta source is wired.
func (c *ClusterCoordinator) shardGroupsForVersionedList() []ShardGroupEntry {
	if c.meta == nil {
		return nil
	}
	return c.meta.ShardGroups()
}

func (c *ClusterCoordinator) listObjectVersionsSingleGroup(
	ctx context.Context, state clusterCoordinatorRuntime, bucket, prefix string, maxKeys int,
) ([]*storage.ObjectVersion, error) {
	target, err := state.opRouter.RouteBucket(bucket)
	if err != nil {
		return nil, err
	}
	if gb, err := state.localExec.ResolveRead(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.ListObjectVersions(ctx, bucket, prefix, maxKeys)
	}
	return c.forwardRuntime().listObjectVersions(ctx, target, bucket, prefix, maxKeys)
}

// fanOutListObjectVersions queries every shard group's per-version FSM
// concurrently (local backend or forwarded) and unions the results. Fail-closed:
// a single group error fails the whole LIST rather than silently returning a
// partial version set, which would look like data loss (snapshots also depend
// on this via ListAllObjects).
func (c *ClusterCoordinator) fanOutListObjectVersions(
	ctx context.Context, state clusterCoordinatorRuntime, groups []ShardGroupEntry, bucket, prefix string, maxKeys int,
) ([]*storage.ObjectVersion, error) {
	type groupResult struct {
		versions []*storage.ObjectVersion
		err      error
	}
	ch := make(chan groupResult, len(groups))
	for _, g := range groups {
		g := g
		go func() {
			target, err := state.opRouter.routeGroup(g.ID)
			if err != nil {
				ch <- groupResult{err: fmt.Errorf("ListObjectVersions route group %s: %w", g.ID, err)}
				return
			}
			if gb, err := state.localExec.ResolveRead(ctx, target); err != nil {
				ch <- groupResult{err: err}
			} else if gb != nil {
				vs, lerr := gb.ListObjectVersions(ctx, bucket, prefix, maxKeys)
				ch <- groupResult{versions: vs, err: lerr}
			} else {
				vs, ferr := c.forwardRuntime().listObjectVersions(ctx, target, bucket, prefix, maxKeys)
				ch <- groupResult{versions: vs, err: ferr}
			}
		}()
	}
	var (
		merged   []*storage.ObjectVersion
		firstErr error
	)
	for range groups {
		r := <-ch
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		merged = append(merged, r.versions...)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return merged, nil
}

// reconcileVersionIsLatest enforces exactly one IsLatest per key after a
// cross-group merge. Candidates are ONLY versions a group already flagged
// IsLatest (its lat: pointer) — a non-flagged version (e.g. a PreserveLatest
// write) is never promoted. When a key split across groups left >1 flagged
// version (divergent lat:), the newest (max UUIDv7 VersionID) wins and the
// others are demoted. No-op in the common single-group-per-key case.
func reconcileVersionIsLatest(versions []*storage.ObjectVersion) {
	latestByKey := make(map[string]*storage.ObjectVersion)
	for _, v := range versions {
		if !v.IsLatest {
			continue
		}
		cur, ok := latestByKey[v.Key]
		if !ok {
			latestByKey[v.Key] = v
			continue
		}
		if v.VersionID > cur.VersionID {
			cur.IsLatest = false
			latestByKey[v.Key] = v
		} else {
			v.IsLatest = false
		}
	}
}

// sortObjectVersions orders by (Key asc, VersionID desc) — newest version first
// within each key (UUIDv7 is lex-ASC-by-time). Mirrors the per-group leaf sort.
func sortObjectVersions(versions []*storage.ObjectVersion) {
	sort.Slice(versions, func(i, j int) bool {
		if versions[i].Key != versions[j].Key {
			return versions[i].Key < versions[j].Key
		}
		return versions[i].VersionID > versions[j].VersionID
	})
}

// WalkObjects buffers ALL matching objects on the server and returns them in
// one reply. Callers expecting large keysets should use ListObjects with
// maxKeys pagination instead.
func (c *ClusterCoordinator) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	target, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return err
	} else if gb != nil {
		return gb.WalkObjects(ctx, bucket, prefix, fn)
	}
	return c.forwardRuntime().walkObjects(ctx, target, bucket, prefix, fn)
}

func (c *ClusterCoordinator) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*storage.MultipartUpload, error) {
	return c.multipartRuntime().createMultipartUpload(ctx, bucket, key, contentType)
}

// CreateMultipartUploadWithTags routes to the resolved data group, mirroring
// CreateMultipartUpload but carrying tags. Tags materialise onto the finalised
// object via the existing Raft-replicated CmdPutObjectMeta path on Complete
// (clusterMultipartMeta.Tags → objectMeta.Tags). When the resolved target is
// remote the tags ride along in CreateMultipartUploadArgs.tags so the receiver
// dispatches to GroupBackend.CreateMultipartUploadWithTags.
func (c *ClusterCoordinator) CreateMultipartUploadWithTags(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (string, error) {
	return c.multipartRuntime().createMultipartUploadWithTags(ctx, bucket, key, contentType, tags)
}

func (c *ClusterCoordinator) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	return c.multipartRuntime().completeMultipartUpload(ctx, bucket, key, uploadID, parts)
}

func (c *ClusterCoordinator) PutObject(
	ctx context.Context, bucket, key string, r io.Reader, contentType string,
) (*storage.Object, error) {
	return c.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, nil)
}

func (c *ClusterCoordinator) PutObjectWithUserMetadata(
	ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string,
) (*storage.Object, error) {
	return c.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:       bucket,
		Key:          key,
		Body:         r,
		ContentType:  contentType,
		UserMetadata: userMetadata,
	})
}

func (c *ClusterCoordinator) PutObjectWithRequest(ctx context.Context, req storage.PutObjectRequest) (*storage.Object, error) {
	bucket, key, r := req.Bucket, req.Key, req.Body
	if err := c.requireObjectBucket(ctx, bucket); err != nil {
		return nil, err
	}
	c.ensureGenZero(ctx)
	routeStart := time.Now()
	target, group, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return nil, err
	}
	sizeClass := PutTraceSizeUnknown
	if s, ok := r.(interface{ Len() int }); ok {
		sizeClass = putTraceSizeClass(int64(s.Len()), c.maxBody)
	}
	ctx = contextWithObjectWritePlacement(ctx, group)
	if gb, err := c.runtimeState().localExec.ResolveObjectWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		ctx = ContextWithPutTrace(ctx, PutTraceRequest{
			Bucket:      bucket,
			Key:         key,
			GroupID:     group.ID,
			Ingress:     PutTraceIngressLocalLeader,
			SizeClass:   sizeClass,
			ForwardMode: PutTraceForwardNone,
		})
		ObservePutTraceStage(ctx, PutTraceStageRouteWrite, routeStart, PutTraceStageFields{})
		// Single path: every local PUT goes through PutObjectWithRequest (the
		// other PutObject* methods are thin wrappers around it), and the forward
		// path below now carries the same user metadata, so a PUT has the same
		// effect on a voter or a non-voter node.
		obj, err := gb.PutObjectWithRequest(ctx, req)
		if err != nil {
			return nil, err
		}
		return obj, nil
	}
	return c.forwardRuntime().putObject(ctx, target, group, req, routeStart)
}

func (c *ClusterCoordinator) PutObjectWithUserMetadataResult(
	ctx context.Context,
	bucket, key string,
	r io.Reader,
	contentType string,
	userMetadata map[string]string,
) (*storage.PutObjectResult, error) {
	return c.PutObjectWithRequestResult(ctx, storage.PutObjectRequest{
		Bucket:       bucket,
		Key:          key,
		Body:         r,
		ContentType:  contentType,
		UserMetadata: userMetadata,
	})
}

func (c *ClusterCoordinator) PutObjectWithRequestResult(ctx context.Context, req storage.PutObjectRequest) (*storage.PutObjectResult, error) {
	previous, err := c.previousObjectForMutation(ctx, req.Bucket, req.Key)
	if err != nil {
		log.Warn().
			Err(err).
			Str("bucket", req.Bucket).
			Str("key", req.Key).
			Bool("has_sse", req.SystemMetadata.SSEAlgorithm != "").
			Msg("coordinator put: previous object lookup failed")
		return nil, err
	}
	obj, err := c.PutObjectWithRequest(ctx, req)
	if err != nil {
		log.Warn().
			Err(err).
			Str("bucket", req.Bucket).
			Str("key", req.Key).
			Bool("has_sse", req.SystemMetadata.SSEAlgorithm != "").
			Msg("coordinator put: write failed")
		return nil, err
	}
	facts, err := objectFactsForMutation("PutObject", obj)
	if err != nil {
		return nil, err
	}
	return &storage.PutObjectResult{Object: facts, Previous: previous}, nil
}

func (c *ClusterCoordinator) previousObjectForMutation(ctx context.Context, bucket, key string) (storage.PreviousObject, error) {
	obj, err := c.HeadObject(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return storage.PreviousObject{}, nil
		}
		return storage.PreviousObject{}, err
	}
	return previousObjectFacts(obj)
}

func previousObjectFacts(obj *storage.Object) (storage.PreviousObject, error) {
	if obj == nil {
		return storage.PreviousObject{}, storage.InvalidMutationResultError{Op: "HeadObject", Field: "object", Reason: "nil object"}
	}
	if obj.Size < 0 {
		return storage.PreviousObject{}, storage.InvalidMutationResultError{Op: "HeadObject", Field: "size", Reason: "negative size"}
	}
	return storage.PreviousObject{
		Exists:    true,
		Size:      obj.Size,
		ETag:      obj.ETag,
		VersionID: obj.VersionID,
	}, nil
}

func objectFactsForMutation(op string, obj *storage.Object) (storage.ObjectFacts, error) {
	if obj == nil {
		return storage.ObjectFacts{}, storage.InvalidMutationResultError{Op: op, Field: "object", Reason: "nil object"}
	}
	if obj.Size < 0 {
		return storage.ObjectFacts{}, storage.InvalidMutationResultError{Op: op, Field: "size", Reason: "negative size"}
	}
	if obj.ETag == "" {
		return storage.ObjectFacts{}, storage.InvalidMutationResultError{Op: op, Field: "etag", Reason: "empty etag"}
	}
	return storage.ObjectFacts{
		Size:         obj.Size,
		ETag:         obj.ETag,
		VersionID:    obj.VersionID,
		LastModified: obj.LastModified,
		SSEAlgorithm: obj.SSEAlgorithm,
	}, nil
}

func (c *ClusterCoordinator) PutObjectWithACL(
	bucket, key string, r io.Reader, contentType string, acl uint8,
) (*storage.Object, error) {
	ctx := context.Background()
	obj, err := c.PutObject(ctx, bucket, key, r, contentType)
	if err != nil {
		return nil, err
	}
	if err := c.SetObjectACL(bucket, key, acl); err != nil {
		if obj == nil || obj.VersionID == "" {
			return nil, fmt.Errorf("%w: acl error: %v; rollback error: missing version id",
				storage.UnsupportedOperationError{Op: "PutObjectWithACL", Reason: storage.UnsupportedReasonRollbackFailed},
				err,
			)
		}
		if rollbackErr := c.DeleteObjectVersion(bucket, key, obj.VersionID); rollbackErr != nil {
			return nil, fmt.Errorf("%w: acl error: %v; rollback error: %v",
				storage.UnsupportedOperationError{Op: "PutObjectWithACL", Reason: storage.UnsupportedReasonRollbackFailed},
				err,
				rollbackErr,
			)
		}
		return nil, err
	}
	obj.ACL = acl
	return obj, nil
}

func (c *ClusterCoordinator) SetObjectACL(bucket, key string, acl uint8) error {
	ctx := context.Background()
	target, err := c.routeReadOrBucket(bucket, key, "")
	if err != nil {
		return err
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return err
	} else if gb != nil {
		// S4-4c: index-free. The ACL change is a read-modify-write against the
		// quorum-meta file (Phase 3); the old index-gated propose path is gone.
		return gb.SetObjectACL(bucket, key, acl)
	}
	args := buildSetObjectACLArgs(bucket, key, acl)
	return c.forwardRuntime().mutateFrame(ctx, target, raftpb.ForwardOpSetObjectACL, args)
}

// SetObjectTags satisfies storage.ObjectTagsSetter. Routes the tag write
// either to the locally-resolvable group backend (if self is leader) or
// forwards to the owning peer via ForwardOpSetObjectTags. Mirrors SetObjectACL.
func (c *ClusterCoordinator) SetObjectTags(bucket, key, versionID string, tags []storage.Tag) error {
	ctx := context.Background()
	target, err := c.routeReadOrBucket(bucket, key, "")
	if err != nil {
		return err
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return err
	} else if gb != nil {
		// S4-4c: index-free. Tag write is a read-modify-write against the
		// quorum-meta file (Phase 3); the old index-gated propose path is gone.
		return gb.SetObjectTags(bucket, key, versionID, tags)
	}
	args := buildSetObjectTagsArgs(bucket, key, versionID, tags)
	return c.forwardRuntime().mutateFrame(ctx, target, raftpb.ForwardOpSetObjectTags, args)
}

// GetObjectTags satisfies storage.ObjectTagsGetter. Routes the tag read to
// the locally-resolvable group backend when available, otherwise forwards to
// the owning peer via ForwardOpGetObjectTags. Mirrors SetObjectTags's forward
// path so multi-group cluster deployments serve S3 GetObjectTagging instead
// of erroring with "not implemented".
func (c *ClusterCoordinator) GetObjectTags(bucket, key, versionID string) ([]storage.Tag, error) {
	ctx := context.Background()
	target, err := c.routeReadOrBucket(bucket, key, "")
	if err != nil {
		return nil, err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.GetObjectTags(bucket, key, versionID)
	}
	return c.forwardRuntime().getObjectTags(ctx, target, bucket, key, versionID)
}

// WriteAt implements the pwrite fast path for routed internal buckets such as
// NFSv4. Uses the generic RMW path (GetObject -> modify -> PutObject) which
// works correctly for both encrypted and unencrypted backends.
func (c *ClusterCoordinator) WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*storage.Object, error) {
	_, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return nil, err
	}

	var existing []byte
	rc, _, err := c.GetObject(ctx, bucket, key)
	if err == nil {
		existing, err = io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, err
		}
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		return nil, err
	}

	end := offset + uint64(len(data))
	if end < offset {
		return nil, storage.ErrEntityTooLarge
	}
	if uint64(len(existing)) < end {
		existing = append(existing, make([]byte, end-uint64(len(existing)))...)
	}
	copy(existing[offset:], data)
	return c.PutObject(ctx, bucket, key, bytes.NewReader(existing), "application/octet-stream")
}

// Truncate implements the SETATTR-size path for internal buckets via a generic
// RMW (GetObject -> resize -> PutObject), which works correctly for both
// encrypted and unencrypted backends.
func (c *ClusterCoordinator) Truncate(ctx context.Context, bucket, key string, size int64) error {
	if size < 0 {
		return storage.ErrEntityTooLarge
	}
	_, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return err
	}

	var existing []byte
	rc, _, err := c.GetObject(ctx, bucket, key)
	if err == nil {
		existing, err = io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return err
		}
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		return err
	}
	cur := int64(len(existing))
	switch {
	case cur > size:
		existing = existing[:size]
	case cur < size:
		existing = append(existing, make([]byte, size-cur)...)
	}
	_, err = c.PutObject(ctx, bucket, key, bytes.NewReader(existing), "application/octet-stream")
	return err
}

// ReadAt implements the pread fast path for routed internal buckets. Local
// leaders use the group backend's zero-copy path. Follower voters may serve
// immutable object-index reads locally only after their local metadata matches
// the cluster object-index entry; stale followers still forward to the leader.
func (c *ClusterCoordinator) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	if offset < 0 {
		return 0, errors.New("coordinator: negative ReadAt offset")
	}
	// S7-4c: probe generations newest-first (one attempt at a single
	// generation). The no-readDialer fallback delegates to GetObject, which
	// probes on its own; the local and forward-readAt paths route per target.
	var n int
	err := c.probeRead(bucket, key, "", func(target RouteTarget) error {
		if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
			return err
		} else if gb != nil {
			var e error
			n, e = gb.ReadAt(ctx, bucket, key, offset, buf)
			return e
		}
		if c.forward == nil {
			return ErrCoordinatorNoRouter
		}
		if c.forward.readDialer == nil {
			rc, _, e := c.GetObject(ctx, bucket, key)
			if e != nil {
				return e
			}
			defer rc.Close()
			if _, e := io.CopyN(io.Discard, rc, offset); e != nil {
				return e
			}
			n, e = io.ReadFull(rc, buf)
			return e
		}
		args := buildReadAtArgs(bucket, key, offset, int64(len(buf)))
		var e error
		n, e = c.forwardRuntime().readAt(ctx, target, args, buf)
		return e
	})
	return n, err
}

func (c *ClusterCoordinator) ReadAtObject(ctx context.Context, bucket, key string, obj *storage.Object, offset int64, buf []byte) (int, error) {
	if obj == nil {
		return c.ReadAt(ctx, bucket, key, offset, buf)
	}
	if obj.Key != "" && obj.Key != key {
		return 0, fmt.Errorf("coordinator: ReadAt object key mismatch: got %q, want %q", obj.Key, key)
	}
	if offset < 0 {
		return 0, errors.New("coordinator: negative ReadAt offset")
	}
	// S7-4c: probe generations newest-first; non-authoritative falls back to
	// ReadAt (which probes too). One attempt at a single generation.
	var n int
	err := c.probeRead(bucket, key, "", func(target RouteTarget) error {
		gb, rerr := c.runtimeState().localExec.ResolveRead(ctx, target)
		if rerr != nil {
			return rerr
		}
		if gb == nil {
			var e error
			n, e = c.ReadAt(ctx, bucket, key, offset, buf)
			return e
		}
		var e error
		n, e = gb.ReadAtObject(ctx, bucket, key, obj, offset, buf)
		return e
	})
	return n, err
}

func (c *ClusterCoordinator) PreferReadAt(bucket string) bool {
	return true
}

// PreferWriteAt always returns false. The plain-file pwrite fast-path has been
// removed; all internal-bucket writes now use the encrypted RMW path via PutObject.
func (c *ClusterCoordinator) PreferWriteAt(bucket string) bool {
	return false
}

func (c *ClusterCoordinator) UploadPart(
	ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader, contentMD5Hex string,
) (*storage.Part, error) {
	return c.multipartRuntime().uploadPart(ctx, bucket, key, uploadID, partNumber, r, contentMD5Hex)
}

type forwardBodyBytesProvider interface {
	ForwardBodyBytes() []byte
}

func forwardBodyBytes(r io.Reader, maxBody int64) ([]byte, error) {
	if provider, ok := r.(forwardBodyBytesProvider); ok {
		body := provider.ForwardBodyBytes()
		if int64(len(body)) > maxBody {
			return nil, storage.ErrEntityTooLarge
		}
		return body, nil
	}
	return readBoundedBody(r, maxBody)
}

func readBoundedBody(r io.Reader, maxBody int64) ([]byte, error) {
	// Forward-frame and retry boundaries need a replayable body. Keep this
	// allocation explicit, capped, and shared so ReadAll cannot creep into
	// unbounded hot paths.
	body, err := io.ReadAll(io.LimitReader(r, maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBody {
		return nil, storage.ErrEntityTooLarge
	}
	return body, nil
}

// AppendObject implements storage.AppendObjecter at the cluster-coordinator
// level. It routes the append to the owner shard group:
//   - local path: dispatches into GroupBackend.AppendObject (DistributedBackend
//     handles the data-Raft propose + apply-error propagation per Phase A)
//     and then commits ObjectIndex on the meta-Raft so the cluster view is
//     consistent with the data plane.
//   - forward path: streams the body to the owner via ForwardSender.SendStream
//     using AppendObjectForwardArgs; the receiver proposes the data append and
//     commits ObjectIndex. The ingress node then re-proposes the same index
//     entry so its local meta-FSM observes read-your-writes promptly.
//
// Stale placement retry: the FSM-level ErrStalePlacement signal (see apply.go)
// is observable on the local-exec branch only because forward replies carry
// ForwardStatus enums rather than the raw sentinel. Forwarded requests are
// single-attempt for now; if rebalance races become observable we'll thread a
// new ForwardStatus value in a follow-up. The local-branch retry is bounded
// at maxAppendStaleRetries.
func (c *ClusterCoordinator) AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*storage.Object, error) {
	if err := c.requireObjectBucket(ctx, bucket); err != nil {
		return nil, err
	}
	c.ensureGenZero(ctx)
	target, group, err := c.routeAppendOrBucket(bucket, key, expectedOffset)
	if err != nil {
		return nil, err
	}
	ctx = contextWithObjectWritePlacement(ctx, group)

	// Local-exec branch — DistributedBackend.AppendObject already performs the
	// cluster-aware pre-check (offset/cap/non-appendable). We add a bounded
	// retry on ErrStalePlacement so a placement rebalance window doesn't
	// surface as a 503 to the caller.
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return c.appendObjectLocalWithRetry(ctx, gb, bucket, key, expectedOffset, r)
	}

	obj, err := c.forwardRuntime().appendObject(ctx, target, group, bucket, key, expectedOffset, r)
	if err != nil {
		return nil, err
	}
	c.waitLocalAppendVisible(ctx, target, bucket, key, obj)
	return obj, nil
}

func (c *ClusterCoordinator) waitLocalAppendVisible(ctx context.Context, target RouteTarget, bucket, key string, want *storage.Object) {
	if want == nil || !target.SelfIsVoter || c.groups == nil {
		return
	}
	gb := c.localBackend(target.GroupID)
	if gb == nil {
		return
	}
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		obj, err := gb.HeadObject(waitCtx, bucket, key)
		if err == nil && obj.Size >= want.Size {
			return
		}
		select {
		case <-waitCtx.Done():
			log.Warn().
				Str("event", "append_local_visibility_wait_timeout").
				Str("bucket", bucket).
				Str("key", key).
				Str("group_id", target.GroupID).
				Int64("want_size", want.Size).
				Err(waitCtx.Err()).
				Msg("append completed before ingress local replica observed appended object")
			return
		case <-ticker.C:
		}
	}
}

// maxAppendStaleRetries bounds the transparent retry on FSM-level
// ErrStalePlacement before the coordinator surfaces it to the caller.
const maxAppendStaleRetries = 2

// appendObjectLocalWithRetry calls gb.AppendObject and retries on
// ErrStalePlacement up to maxAppendStaleRetries times. The body must be a
// Seeker so we can rewind between attempts; non-seekable readers are
// buffered once into memory under the existing c.maxBody cap.
func (c *ClusterCoordinator) appendObjectLocalWithRetry(
	ctx context.Context, gb *GroupBackend, bucket, key string, expectedOffset int64, r io.Reader,
) (*storage.Object, error) {
	seeker, ok := r.(io.Seeker)
	var buffered []byte
	if !ok {
		body, err := readBoundedBody(r, c.maxBody)
		if err != nil {
			return nil, err
		}
		buffered = body
	}

	var lastErr error
	for attempt := 0; attempt <= maxAppendStaleRetries; attempt++ {
		var body io.Reader
		if buffered != nil {
			body = bytes.NewReader(buffered)
		} else {
			if attempt > 0 {
				if _, err := seeker.Seek(0, io.SeekStart); err != nil {
					return nil, fmt.Errorf("rewind for retry: %w", err)
				}
			}
			body = r
		}
		obj, err := gb.AppendObject(ctx, bucket, key, expectedOffset, body)
		if err == nil {
			return obj, nil
		}
		if !errors.Is(err, ErrStalePlacement) {
			return nil, err
		}
		lastErr = err
	}
	return nil, fmt.Errorf("append: stale placement after %d retries: %w", maxAppendStaleRetries, lastErr)
}

func forwardBodyExceedsSingleFrameCap(r io.Reader, maxBody int64) bool {
	seeker, ok := r.(io.Seeker)
	if !ok {
		return true
	}
	cur, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return true
	}
	end, err := seeker.Seek(0, io.SeekEnd)
	if _, seekErr := seeker.Seek(cur, io.SeekStart); err == nil && seekErr != nil {
		err = seekErr
	}
	if err != nil {
		return true
	}
	return end-cur > maxBody
}

// shouldStreamForwardBody decides whether a forwarded PutObject/UploadPart body
// streams (true) or rides in a single args FlatBuffer frame (false). It streams
// when the body exceeds the maxBody single-frame cap OR is at least
// minForwardStreamBytes. Non-seekable bodies always stream (size unknown). The
// io.Seeker size probe rewinds to the original offset, so it does not consume
// the body.
func shouldStreamForwardBody(r io.Reader, maxBody int64) bool {
	if forwardBodyExceedsSingleFrameCap(r, maxBody) {
		return true
	}
	seeker, ok := r.(io.Seeker)
	if !ok {
		return true
	}
	cur, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return true
	}
	end, err := seeker.Seek(0, io.SeekEnd)
	if _, seekErr := seeker.Seek(cur, io.SeekStart); err == nil && seekErr != nil {
		err = seekErr
	}
	if err != nil {
		return true
	}
	return end-cur >= minForwardStreamBytes
}

func (c *ClusterCoordinator) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	return c.multipartRuntime().abortMultipartUpload(ctx, bucket, key, uploadID)
}

// ListMultipartUploads scans local data-group backends and forwards to owners
// for placeholder groups so bucket-wide results are complete from any node.
func (c *ClusterCoordinator) ListMultipartUploads(ctx context.Context, bucket, prefix string, maxUploads int) ([]*storage.MultipartUpload, error) {
	if c.groups == nil {
		return c.base.ListMultipartUploads(ctx, bucket, prefix, maxUploads)
	}
	var uploads []*storage.MultipartUpload
	groups := c.groups.All()
	for _, dg := range groups {
		gb := dg.Backend()
		if gb == nil {
			groupUploads, err := c.forwardListMultipartUploads(ctx, dg.ID(), bucket, prefix)
			if err != nil {
				return nil, err
			}
			// Group-encode listed IDs with the group they came from so clients
			// get the same uploadID Create returned (session ops route by it).
			uploads = append(uploads, c.wrapMultipartUploads(groupUploads, dg.ID())...)
			continue
		}
		groupUploads, err := gb.ListMultipartUploads(ctx, bucket, prefix, 0)
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, c.wrapMultipartUploads(groupUploads, dg.ID())...)
	}
	if len(groups) == 0 && c.base != nil {
		return c.base.ListMultipartUploads(ctx, bucket, prefix, maxUploads)
	}
	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].CreatedAt != uploads[j].CreatedAt {
			return uploads[i].CreatedAt < uploads[j].CreatedAt
		}
		if uploads[i].Key != uploads[j].Key {
			return uploads[i].Key < uploads[j].Key
		}
		return uploads[i].UploadID < uploads[j].UploadID
	})
	if maxUploads > 0 && len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
	}
	return uploads, nil
}

func (c *ClusterCoordinator) forwardListMultipartUploads(ctx context.Context, groupID, bucket, prefix string) ([]*storage.MultipartUpload, error) {
	if c.forward == nil {
		return nil, rejectIncompleteMultipartListing(compat.OperationListMultipartUploads)
	}
	target, err := c.runtimeState().opRouter.routeGroup(groupID)
	if err != nil {
		return nil, err
	}
	if len(target.Peers) == 0 {
		return nil, rejectIncompleteMultipartListing(compat.OperationListMultipartUploads)
	}
	if err := c.requireMultipartListingPeerCapability(compat.OperationListMultipartUploads, target.Peers); err != nil {
		return nil, err
	}
	return c.forwardRuntime().listMultipartUploads(ctx, target, bucket, prefix, 0)
}

// ListParts routes by the group encoded in the uploadID (falling back to the
// legacy (bucket, key) hash for un-prefixed IDs): local group backend first;
// otherwise the peer-transport capability gate must pass before forwarding to
// the remote data-group leader.
func (c *ClusterCoordinator) ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]storage.Part, error) {
	target, _, rawID, err := c.routeMultipartSession(bucket, key, uploadID)
	if err != nil {
		return nil, err
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.ListParts(ctx, bucket, key, rawID, maxParts)
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	if err := c.requireMultipartListingPeerCapability(compat.OperationListParts, target.Peers); err != nil {
		return nil, err
	}
	return c.forwardRuntime().listParts(ctx, target, bucket, key, rawID, maxParts)
}

func (c *ClusterCoordinator) requireMultipartListingPeerCapability(op compat.Operation, peers []string) error {
	if c.capGate == nil {
		return nil
	}
	resolved := peers
	if book, ok := c.meta.(NodeAddressBook); ok && book != nil {
		if addrs, err := ResolveNodeAddresses(book, peers); err == nil {
			resolved = addrs
		}
	}
	_, err := c.capGate.RequirePeerTransportCapability(compat.CapabilityMultipartListingV1, op, resolved, time.Now())
	return err
}

func (c *ClusterCoordinator) multipartListingCapabilityPeers(target RouteTarget, group ShardGroupEntry) []string {
	if len(target.Peers) > 0 {
		return target.Peers
	}
	if len(group.PeerIDs) > 0 {
		return append([]string(nil), group.PeerIDs...)
	}
	if c.meta != nil {
		if entry, ok := c.meta.ShardGroup(target.GroupID); ok {
			return append([]string(nil), entry.PeerIDs...)
		}
	}
	return nil
}

func rejectIncompleteMultipartListing(op compat.Operation) error {
	return compat.Reject(compat.GatePlan{
		Capability: compat.CapabilityMultipartListingV1,
		Scope:      compat.ScopeDataGroup,
		Severity:   compat.SeverityHard,
		Operation:  op,
		Unknown:    []compat.NodeID{"data_group"},
	})
}

var (
	_ storage.Backend     = (*ClusterCoordinator)(nil)
	_ storage.PartialIO   = (*ClusterCoordinator)(nil)
	_ storage.Truncatable = (*ClusterCoordinator)(nil)
)

// ScanObjectsGrouped fans out to all locally-owned shard groups so that
// objects written to any shard group (not just group-0 / base) are visible
// to the lifecycle expiration scan. In single-node mode every seeded group is
// locally owned; in cluster mode the leader scans its own shard groups.
// Falls back to c.base when no data groups are registered (tests / legacy).
func (c *ClusterCoordinator) ScanObjectsGrouped(bucket string) (<-chan storage.ObjectKeyGroup, error) {
	type scanner interface {
		ScanObjectsGrouped(bucket string) (<-chan storage.ObjectKeyGroup, error)
	}

	// No DataGroupManager — fall back to base (covers tests and legacy single-group mode).
	if c.groups == nil {
		sc, ok := c.base.(scanner)
		if !ok {
			return nil, storage.UnsupportedOperationError{Op: "ScanObjectsGrouped", Reason: storage.UnsupportedReasonNoAdapter}
		}
		return sc.ScanObjectsGrouped(bucket)
	}

	groups := c.groups.All()
	if len(groups) == 0 {
		sc, ok := c.base.(scanner)
		if !ok {
			return nil, storage.UnsupportedOperationError{Op: "ScanObjectsGrouped", Reason: storage.UnsupportedReasonNoAdapter}
		}
		return sc.ScanObjectsGrouped(bucket)
	}

	// Collect per-group channels; only locally-owned groups (Backend() != nil) participate.
	var srcs []<-chan storage.ObjectKeyGroup
	for _, dg := range groups {
		gb := dg.Backend()
		if gb == nil {
			continue // not locally owned — skip (no RPC fan-out for lifecycle)
		}
		ch, err := gb.ScanObjectsGrouped(bucket)
		if err != nil {
			return nil, fmt.Errorf("ScanObjectsGrouped group %s: %w", dg.ID(), err)
		}
		srcs = append(srcs, ch)
	}
	if len(srcs) == 0 {
		// No local backends (all placeholder groups) — nothing to scan.
		out := make(chan storage.ObjectKeyGroup)
		close(out)
		return out, nil
	}

	out := make(chan storage.ObjectKeyGroup, 16)
	go func() {
		defer close(out)
		for _, src := range srcs {
			for g := range src {
				out <- g
			}
		}
	}()
	return out, nil
}

// ScanLocalMultipartUploads fans out to all locally-owned shard groups so that
// MPU metadata stored in any shard group's keyspace is visible to the lifecycle
// worker. MPU metadata is stored in the shard group that owns the write path for
// the bucket (not necessarily group-0 / base). Falls back to c.base when no data
// groups are registered (tests / legacy single-group mode).
func (c *ClusterCoordinator) ScanLocalMultipartUploads(bucket string) (<-chan storage.MultipartUploadRecord, error) {
	type scanner interface {
		ScanLocalMultipartUploads(bucket string) (<-chan storage.MultipartUploadRecord, error)
	}

	// No DataGroupManager — fall back to base (covers tests and legacy single-group mode).
	if c.groups == nil {
		sc, ok := c.base.(scanner)
		if !ok {
			return nil, storage.UnsupportedOperationError{Op: "ScanLocalMultipartUploads", Reason: storage.UnsupportedReasonNoAdapter}
		}
		return sc.ScanLocalMultipartUploads(bucket)
	}

	groups := c.groups.All()
	if len(groups) == 0 {
		sc, ok := c.base.(scanner)
		if !ok {
			return nil, storage.UnsupportedOperationError{Op: "ScanLocalMultipartUploads", Reason: storage.UnsupportedReasonNoAdapter}
		}
		return sc.ScanLocalMultipartUploads(bucket)
	}

	// Collect per-group channels; only locally-owned groups (Backend() != nil) participate.
	// Keep the source group ID alongside each channel: lifecycle abort feeds these
	// records back into AbortMultipartUpload, which routes by the group encoded in
	// the upload ID. Without re-encoding here, expired MPUs created under group-ID
	// routing would fall back to legacy hash routing and leak on divergent nodes.
	type groupScan struct {
		groupID string
		ch      <-chan storage.MultipartUploadRecord
	}
	var srcs []groupScan
	for _, dg := range groups {
		gb := dg.Backend()
		if gb == nil {
			continue // not locally owned — skip (no RPC fan-out for lifecycle)
		}
		ch, err := gb.ScanLocalMultipartUploads(bucket)
		if err != nil {
			return nil, fmt.Errorf("ScanLocalMultipartUploads group %s: %w", dg.ID(), err)
		}
		srcs = append(srcs, groupScan{groupID: dg.ID(), ch: ch})
	}
	if len(srcs) == 0 {
		out := make(chan storage.MultipartUploadRecord)
		close(out)
		return out, nil
	}

	wrap := c.multipartGroupIDRouting()
	out := make(chan storage.MultipartUploadRecord, 16)
	go func() {
		defer close(out)
		for _, src := range srcs {
			for rec := range src.ch {
				if wrap {
					rec.UploadID = encodeMultipartUploadID(src.groupID, rec.UploadID)
				}
				out <- rec
			}
		}
	}()
	return out, nil
}

// ScrubPeerStat is the cluster-package-local snapshot of one peer's scrub
// session state. Returned by ScrubSessionStat fan-out; serve.go's adapter
// converts to admin.ScrubJobInfo so cluster does not import admin.
type ScrubPeerStat struct {
	Bucket       string
	KeyPrefix    string
	DryRun       bool
	Status       string
	StartedAt    int64
	DoneAt       int64
	Checked      int64
	Healthy      int64
	Detected     int64
	Repaired     int64
	Unrepairable int64
	Skipped      int64
	OwnedHere    bool
}

// ScrubSessionStat fans out a ScrubSessionStat RPC to every peer in the
// cluster (excluding self), aggregating per-peer scrub stats for cluster-wide
// admin GET /v1/scrub/jobs/<id>. Each peer call has a 5s timeout; failures
// are returned as the second slice so admin can flag partial=true.
func (c *ClusterCoordinator) ScrubSessionStat(ctx context.Context, sessionID string) ([]ScrubPeerStat, []string, error) {
	if c.forward == nil || c.addr == nil {
		return nil, nil, nil
	}
	nodes := c.addr.Nodes()
	if len(nodes) <= 1 {
		return nil, nil, nil
	}
	args := buildScrubSessionStatArgs(sessionID)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		infos    []ScrubPeerStat
		failures []string
	)
	for _, n := range nodes {
		if c.matchSelfPeer(n.ID) {
			continue
		}
		wg.Add(1)
		go func(node MetaNodeEntry) {
			defer wg.Done()
			peerCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			reply, err := c.forward.Send(peerCtx, []string{node.Address}, "", raftpb.ForwardOpScrubSessionStat, args)
			if err != nil {
				mu.Lock()
				failures = append(failures, node.ID)
				mu.Unlock()
				return
			}
			info, ok := decodeScrubSessionStatReply(reply)
			if !ok {
				mu.Lock()
				failures = append(failures, node.ID)
				mu.Unlock()
				return
			}
			if !info.found {
				return
			}
			mu.Lock()
			infos = append(infos, info.toPeerStat())
			mu.Unlock()
		}(n)
	}
	wg.Wait()
	return infos, failures, nil
}

func buildScrubSessionStatArgs(sessionID string) []byte {
	b := flatbuffers.NewBuilder(64)
	sidOff := b.CreateString(sessionID)
	raftpb.ScrubSessionStatArgsStart(b)
	raftpb.ScrubSessionStatArgsAddSessionId(b, sidOff)
	b.Finish(raftpb.ScrubSessionStatArgsEnd(b))
	return b.FinishedBytes()
}

type scrubSessionStatDecoded struct {
	found        bool
	bucket       string
	keyPrefix    string
	dryRun       bool
	status       string
	startedAt    int64
	doneAt       int64
	checked      int64
	healthy      int64
	detected     int64
	repaired     int64
	unrepairable int64
	skipped      int64
	ownedHere    bool
}

func (d scrubSessionStatDecoded) toPeerStat() ScrubPeerStat {
	return ScrubPeerStat{
		Bucket:       d.bucket,
		KeyPrefix:    d.keyPrefix,
		DryRun:       d.dryRun,
		Status:       d.status,
		StartedAt:    d.startedAt,
		DoneAt:       d.doneAt,
		Checked:      d.checked,
		Healthy:      d.healthy,
		Detected:     d.detected,
		Repaired:     d.repaired,
		Unrepairable: d.unrepairable,
		Skipped:      d.skipped,
		OwnedHere:    d.ownedHere,
	}
}

func decodeScrubSessionStatReply(reply []byte) (scrubSessionStatDecoded, bool) {
	if len(reply) == 0 {
		return scrubSessionStatDecoded{}, false
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	if fr == nil || fr.Status() != raftpb.ForwardStatusOK {
		return scrubSessionStatDecoded{}, false
	}
	ss := fr.ScrubSession(nil)
	if ss == nil {
		return scrubSessionStatDecoded{}, false
	}
	return scrubSessionStatDecoded{
		found:        ss.Found(),
		bucket:       string(ss.Bucket()),
		keyPrefix:    string(ss.KeyPrefix()),
		dryRun:       ss.DryRun(),
		status:       string(ss.Status()),
		startedAt:    ss.StartedAt(),
		doneAt:       ss.DoneAt(),
		checked:      ss.Checked(),
		healthy:      ss.Healthy(),
		detected:     ss.Detected(),
		repaired:     ss.Repaired(),
		unrepairable: ss.Unrepairable(),
		skipped:      ss.Skipped(),
		ownedHere:    ss.OwnedHere(),
	}, true
}
