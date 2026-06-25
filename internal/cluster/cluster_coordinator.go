package cluster

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
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

func (c *ClusterCoordinator) requireObjectBucket(ctx context.Context, bucket string) error {
	if c.base == nil {
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

var (
	_ storage.Backend   = (*ClusterCoordinator)(nil)
	_ storage.PartialIO = (*ClusterCoordinator)(nil)
)
