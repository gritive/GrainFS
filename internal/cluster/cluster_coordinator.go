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

	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/metrics"
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
	if b == nil || b.Node() == nil {
		return false
	}
	return b.Node().IsLeader()
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

// metaObjectIndexAdapter is a helper that extracts the narrow
// objectIndexLookup interface from a ShardGroupSource via type assertion.
// Returns nil if meta does not implement the index methods (test wiring;
// production meta-FSM always does).
func metaObjectIndexAdapter(meta ShardGroupSource) objectIndexLookup {
	if src, ok := meta.(objectIndexLookup); ok {
		return src
	}
	return nil
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
	indexWriter objectIndexProposer
	capGate     *CapabilityGate

	opRouter  *OpRouter
	localExec *LocalExecution

	maxBody             int64
	appendForwardBuffer *appendForwardBuffer
}

type clusterCoordinatorRuntime struct {
	opRouter  *OpRouter
	localExec *LocalExecution
	ecConfig  ECConfig
}

type objectIndexSource interface {
	ObjectIndexLatest(bucket, key string) (ObjectIndexEntry, bool)
	ObjectIndexVersion(bucket, key, versionID string) (ObjectIndexEntry, bool)
}

type objectIndexListSource interface {
	ObjectIndexLatestEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry
	ObjectIndexLatestEntriesPage(bucket, prefix, marker string, maxKeys int) (entries []ObjectIndexEntry, truncated bool)
	ObjectIndexVersionEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry
}

type objectIndexProposer interface {
	ProposeObjectIndex(ctx context.Context, entry ObjectIndexEntry, preserveLatest bool) error
	ProposeDeleteObjectIndex(ctx context.Context, bucket, key, versionID string) error
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
	c.rebuild()
	return c
}

// WithForwardSender attaches the QUIC dialer used to send 0x08 forward calls
// to peer nodes. Returns the receiver for builder-style chaining in serve.go.
func (c *ClusterCoordinator) WithForwardSender(s *ForwardSender) *ClusterCoordinator {
	c.forward = s
	c.rebuild()
	return c
}

// WithNodeAddressResolver attaches the cluster address book used to translate
// nodeID PeerIDs into dialable QUIC addresses for runtime forwarding.
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

func (c *ClusterCoordinator) WithObjectIndexProposer(p objectIndexProposer) *ClusterCoordinator {
	c.indexWriter = p
	c.rebuild()
	return c
}

func (c *ClusterCoordinator) WithCapabilityGate(gate *CapabilityGate) *ClusterCoordinator {
	c.capGate = gate
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
		metaObjectIndexAdapter(c.meta),
		c.addr,
		dataGroupManagerLeaderProbe{m: c.groups},
		c.ecConfig,
		c.selfID,
		c.selfAliases,
	)
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

// routeReadOrBucket picks RouteObjectRead when an object index is configured
// (production wiring), and falls back to RouteBucket when not (test wiring
// without an objectIndexProposer / objectIndexSource). Preserves the legacy
// routeObjectLatest/Version dispatch behavior that callers depended on.
func (c *ClusterCoordinator) routeReadOrBucket(bucket, key, versionID string) (RouteTarget, error) {
	state := c.runtimeState()
	if c.indexWriter == nil {
		return state.opRouter.RouteBucket(bucket)
	}
	target, _, err := state.opRouter.RouteObjectRead(bucket, key, versionID)
	if errors.Is(err, storage.ErrObjectNotFound) {
		fallback, _, fallbackErr := c.routeWriteOrBucket(bucket, key)
		if fallbackErr == nil {
			return fallback, nil
		}
	}
	return target, err
}

// routeWriteOrBucket picks RouteObjectWrite (EC-aware placement) when an
// object index proposer or EC config is configured (production wiring), and
// falls back to a bucket-only route when neither is set. Preserves the legacy
// routeObjectWrite short-circuit:
//
//	indexWriter == nil && ecConfig.NumShards() == 0 → routeBucket
func (c *ClusterCoordinator) routeWriteOrBucket(bucket, key string) (RouteTarget, ShardGroupEntry, error) {
	state := c.runtimeState()
	if c.indexWriter == nil && state.ecConfig.NumShards() == 0 {
		target, err := state.opRouter.RouteBucket(bucket)
		return target, ShardGroupEntry{ID: target.GroupID}, err
	}
	return state.opRouter.RouteObjectWrite(bucket, key)
}

func (c *ClusterCoordinator) routeAppendOrBucket(bucket, key string, expectedOffset int64) (RouteTarget, ShardGroupEntry, error) {
	state := c.runtimeState()
	if c.indexWriter != nil && metaObjectIndexAdapter(c.meta) != nil && !storage.IsInternalBucket(bucket) {
		target, entry, err := state.opRouter.RouteObjectRead(bucket, key, "")
		if err == nil {
			group, ok := c.meta.ShardGroup(entry.PlacementGroupID)
			if !ok {
				return RouteTarget{}, ShardGroupEntry{}, ErrNoGroup
			}
			return target, group, nil
		}
		if !errors.Is(err, storage.ErrObjectNotFound) {
			return RouteTarget{}, ShardGroupEntry{}, err
		}
	}
	return c.routeWriteOrBucket(bucket, key)
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
		versions, err := c.ListObjectVersions(bucket, "", 0)
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
				if rc, obj, err := c.GetObjectVersion(bucket, version.Key, version.VersionID); err == nil {
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
	snap, ok := c.base.(storage.BucketSnapshotable)
	if !ok {
		return storage.ErrSnapshotNotSupported
	}
	return snap.RestoreBuckets(buckets)
}

func (c *ClusterCoordinator) commitObjectIndex(ctx context.Context, bucket, key string, obj *storage.Object, group ShardGroupEntry, isDeleteMarker bool) error {
	if c.indexWriter == nil {
		return nil
	}
	if storage.IsInternalBucket(bucket) {
		return nil
	}
	if _, ok := PutTraceRequestFromContext(ctx); !ok {
		ctx = ContextWithPutTrace(ctx, PutTraceRequest{
			Bucket:      bucket,
			Key:         key,
			GroupID:     group.ID,
			Ingress:     PutTraceIngressLocalLeader,
			SizeClass:   PutTraceSizeUnknown,
			ForwardMode: PutTraceForwardNone,
		})
	}
	entry := buildObjectIndexEntry(group, bucket, key, obj, isDeleteMarker)
	stageStart := time.Now()
	err := c.indexWriter.ProposeObjectIndex(ctx, entry, false)
	fields := PutTraceStageFields{MetaProposeSite: "coordinator", MetaProposeCount: 1}
	if err != nil {
		fields.Error = err.Error()
	}
	ObservePutTraceStage(ctx, PutTraceStageMetaIndexPropose, stageStart, fields)
	return err
}

func objectIndexECConfigForGroup(group ShardGroupEntry) ECConfig {
	return AutoECConfigForClusterSize(len(group.PeerIDs))
}

func objectIndexNodeIDsForGroup(group ShardGroupEntry, cfg ECConfig) []string {
	n := cfg.NumShards()
	if n > 0 && len(group.PeerIDs) >= n {
		return cloneStringSlice(group.PeerIDs[:n])
	}
	return cloneStringSlice(group.PeerIDs)
}

func contextWithObjectWritePlacement(ctx context.Context, group ShardGroupEntry) context.Context {
	if len(group.PeerIDs) == 0 {
		return ContextWithPlacementGroup(ctx, group.ID)
	}
	return ContextWithPlacementGroupEntry(ctx, group)
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

func objectIndexEntryToObject(entry ObjectIndexEntry) *storage.Object {
	obj := &storage.Object{
		Key:          entry.Key,
		Size:         entry.Size,
		ContentType:  entry.ContentType,
		ETag:         entry.ETag,
		LastModified: entry.ModTime,
		VersionID:    entry.VersionID,
	}
	if len(entry.Parts) > 0 {
		parts := make([]storage.MultipartPartEntry, len(entry.Parts))
		copy(parts, entry.Parts)
		obj.Parts = parts
	}
	return obj
}

// TODO(post-launch): widen ObjectIndexEntry with Tags if List-via-coordinator
// must surface tags. Today clients call GetObjectTagging separately.
func objectIndexEntryToVersion(entry ObjectIndexEntry, isLatest bool) *storage.ObjectVersion {
	return &storage.ObjectVersion{
		Key:            entry.Key,
		VersionID:      entry.VersionID,
		IsLatest:       isLatest,
		IsDeleteMarker: entry.IsDeleteMarker,
		LastModified:   entry.ModTime,
		ETag:           entry.ETag,
		Size:           entry.Size,
	}
}

func (c *ClusterCoordinator) objectIndexListSource() (objectIndexListSource, bool) {
	if c.indexWriter == nil {
		return nil, false
	}
	src, ok := c.meta.(objectIndexListSource)
	return src, ok
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
	target, err := c.routeReadOrBucket(bucket, key, "")
	if err != nil {
		return nil, nil, err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return nil, nil, err
	} else if gb != nil {
		return gb.GetObject(ctx, bucket, key)
	}
	if c.forward == nil {
		return nil, nil, ErrCoordinatorNoRouter
	}
	args := buildGetObjectArgs(bucket, key)
	if c.forward.readDialer != nil {
		return c.forwardReadObject(ctx, target.Peers, target.GroupID, raftpb.ForwardOpGetObject, args)
	}
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpGetObject, args)
	if err != nil {
		return nil, nil, err
	}
	obj, err := objectFromReply(reply)
	if err != nil {
		return nil, nil, err
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	body := fr.ReadBodyBytes()
	// Reply buffer is reused by ForwardSender — copy the body bytes into a
	// caller-owned slice before wrapping. obj already deep-copies via accessors.
	bodyCopy := make([]byte, len(body))
	copy(bodyCopy, body)
	if obj.Size != int64(len(bodyCopy)) {
		return nil, nil, ErrForwardBodySizeMismatch
	}
	return io.NopCloser(bytes.NewReader(bodyCopy)), obj, nil
}

func (c *ClusterCoordinator) GetObjectVersion(
	bucket, key, versionID string,
) (io.ReadCloser, *storage.Object, error) {
	ctx := context.Background()
	target, err := c.routeReadOrBucket(bucket, key, versionID)
	if err != nil {
		return nil, nil, err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return nil, nil, err
	} else if gb != nil {
		return gb.GetObjectVersion(bucket, key, versionID)
	}
	if c.forward == nil {
		return nil, nil, ErrCoordinatorNoRouter
	}
	args := buildGetObjectVersionArgs(bucket, key, versionID)
	if c.forward.readDialer != nil {
		return c.forwardReadObject(ctx, target.Peers, target.GroupID, raftpb.ForwardOpGetObjectVersion, args)
	}
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpGetObjectVersion, args)
	if err != nil {
		return nil, nil, err
	}
	obj, err := objectFromReply(reply)
	if err != nil {
		return nil, nil, err
	}
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	body := fr.ReadBodyBytes()
	bodyCopy := make([]byte, len(body))
	copy(bodyCopy, body)
	if obj.Size != int64(len(bodyCopy)) {
		return nil, nil, ErrForwardBodySizeMismatch
	}
	return io.NopCloser(bytes.NewReader(bodyCopy)), obj, nil
}

func (c *ClusterCoordinator) forwardReadObject(
	ctx context.Context,
	peers []string,
	groupID string,
	op raftpb.ForwardOp,
	args []byte,
) (io.ReadCloser, *storage.Object, error) {
	reply, body, err := c.forward.SendReadStream(ctx, peers, groupID, op, args)
	if err != nil {
		return nil, nil, err
	}
	obj, err := objectFromReply(reply)
	if err != nil {
		if body != nil {
			_ = body.Close()
		}
		return nil, nil, err
	}
	return &forwardReadValidator{rc: body, want: obj.Size}, obj, nil
}

type forwardReadValidator struct {
	rc   io.ReadCloser
	want int64
	got  int64
}

func (r *forwardReadValidator) Read(p []byte) (int, error) {
	if r.got >= r.want {
		return 0, io.EOF
	}
	if remaining := r.want - r.got; int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.rc.Read(p)
	r.got += int64(n)
	if r.got == r.want {
		return n, nil
	}
	if err == io.EOF {
		return n, ErrForwardBodySizeMismatch
	}
	return n, err
}

func (r *forwardReadValidator) Close() error {
	return r.rc.Close()
}

func (c *ClusterCoordinator) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	target, err := c.routeReadOrBucket(bucket, key, "")
	if err != nil {
		return nil, err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.HeadObject(ctx, bucket, key)
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildHeadObjectArgs(bucket, key)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpHeadObject, args)
	if err != nil {
		return nil, err
	}
	return objectFromReply(reply)
}

func (c *ClusterCoordinator) HeadObjectVersion(bucket, key, versionID string) (*storage.Object, error) {
	ctx := context.Background()
	target, err := c.routeReadOrBucket(bucket, key, versionID)
	if err != nil {
		return nil, err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.HeadObjectVersion(bucket, key, versionID)
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildHeadObjectVersionArgs(bucket, key, versionID)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpHeadObjectVersion, args)
	if err != nil {
		return nil, err
	}
	return objectFromReply(reply)
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
	var (
		target RouteTarget
		group  ShardGroupEntry
		err    error
	)
	if c.indexWriter == nil {
		target, err = c.runtimeState().opRouter.RouteBucket(bucket)
		group = ShardGroupEntry{ID: target.GroupID}
	} else {
		var entry ObjectIndexEntry
		target, entry, err = c.runtimeState().opRouter.RouteObjectRead(bucket, key, "")
		if errors.Is(err, storage.ErrObjectNotFound) {
			target, group, err = c.routeWriteOrBucket(bucket, key)
		} else {
			group = ShardGroupEntry{ID: entry.PlacementGroupID, PeerIDs: entry.NodeIDs}
		}
	}
	if err != nil {
		return "", err
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return "", err
	} else if gb != nil {
		markerID, err := gb.DeleteObjectReturningMarker(bucket, key)
		if err != nil {
			return "", err
		}
		marker := &storage.Object{Key: key, VersionID: markerID, LastModified: time.Now().Unix()}
		if err := c.commitObjectIndex(ctx, bucket, key, marker, group, true); err != nil {
			return "", err
		}
		return markerID, nil
	}
	if c.forward == nil {
		return "", ErrCoordinatorNoRouter
	}
	args := buildDeleteObjectArgs(bucket, key)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpDeleteObject, args)
	if err != nil {
		return "", err
	}
	obj, err := objectFromReply(reply)
	if err == nil {
		return obj.VersionID, nil
	}
	if errors.Is(err, errInternalReply) {
		return "", parseReplyStatus(reply)
	}
	return "", err
}

func (c *ClusterCoordinator) DeleteObjectVersion(bucket, key, versionID string) error {
	ctx := context.Background()
	target, err := c.routeReadOrBucket(bucket, key, versionID)
	if err != nil {
		return err
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return err
	} else if gb != nil {
		if err := gb.DeleteObjectVersion(bucket, key, versionID); err != nil {
			return err
		}
		if c.indexWriter != nil {
			return c.indexWriter.ProposeDeleteObjectIndex(ctx, bucket, key, versionID)
		}
		return nil
	}
	if c.forward == nil {
		return ErrCoordinatorNoRouter
	}
	args := buildDeleteObjectVersionArgs(bucket, key, versionID)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpDeleteObjectVersion, args)
	if err != nil {
		return err
	}
	return parseReplyStatus(reply)
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
	if !storage.IsInternalBucket(bucket) {
		if src, ok := c.objectIndexListSource(); ok {
			if err := c.HeadBucket(ctx, bucket); err != nil {
				return nil, false, err
			}
			entries, more := src.ObjectIndexLatestEntriesPage(bucket, prefix, marker, maxKeys)
			objects = make([]*storage.Object, 0, len(entries))
			for _, entry := range entries {
				objects = append(objects, objectIndexEntryToObject(entry))
			}
			return objects, more, nil
		}
	}
	target, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return nil, false, err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return nil, false, err
	} else if gb != nil {
		return gb.ListObjectsPage(ctx, bucket, prefix, marker, maxKeys)
	}
	if c.forward == nil {
		return nil, false, ErrCoordinatorNoRouter
	}
	args := buildListObjectsArgs(bucket, prefix, marker, int32(maxKeys))
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpListObjects, args)
	if err != nil {
		return nil, false, err
	}
	objs, err := objectsFromReply(reply)
	if err != nil {
		return nil, false, err
	}
	more := maxKeys > 0 && len(objs) > maxKeys
	if more {
		objs = objs[:maxKeys]
	}
	return objs, more, nil
}

func (c *ClusterCoordinator) ListObjectVersions(
	bucket, prefix string, maxKeys int,
) ([]*storage.ObjectVersion, error) {
	ctx := context.Background()
	if src, ok := c.objectIndexListSource(); ok {
		if err := c.HeadBucket(ctx, bucket); err != nil {
			return nil, err
		}
		latestSrc, _ := c.meta.(objectIndexSource)
		entries := src.ObjectIndexVersionEntries(bucket, prefix, maxKeys)
		versions := make([]*storage.ObjectVersion, 0, len(entries))
		for _, entry := range entries {
			isLatest := false
			if latestSrc != nil {
				latest, ok := latestSrc.ObjectIndexLatest(entry.Bucket, entry.Key)
				isLatest = ok && latest.VersionID == entry.VersionID
			}
			versions = append(versions, objectIndexEntryToVersion(entry, isLatest))
		}
		return versions, nil
	}
	target, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return nil, err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.ListObjectVersions(bucket, prefix, maxKeys)
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildListObjectVersionsArgs(bucket, prefix, int32(maxKeys))
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpListObjectVersions, args)
	if err != nil {
		return nil, err
	}
	return objectVersionsFromReply(reply)
}

// WalkObjects buffers ALL matching objects on the server and returns them in
// one reply. Callers expecting large keysets should use ListObjects with
// maxKeys pagination instead.
func (c *ClusterCoordinator) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	if src, ok := c.objectIndexListSource(); ok {
		if err := c.HeadBucket(ctx, bucket); err != nil {
			return err
		}
		entries := src.ObjectIndexLatestEntries(bucket, prefix, 0)
		for _, entry := range entries {
			if err := fn(objectIndexEntryToObject(entry)); err != nil {
				return err
			}
		}
		return nil
	}
	target, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return err
	} else if gb != nil {
		return gb.WalkObjects(ctx, bucket, prefix, fn)
	}
	if c.forward == nil {
		return ErrCoordinatorNoRouter
	}
	args := buildWalkObjectsArgs(bucket, prefix)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpWalkObjects, args)
	if err != nil {
		return err
	}
	objs, err := objectsFromReply(reply)
	if err != nil {
		return err
	}
	for _, o := range objs {
		if err := fn(o); err != nil {
			return err
		}
	}
	return nil
}

func (c *ClusterCoordinator) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*storage.MultipartUpload, error) {
	if err := c.requireObjectBucket(ctx, bucket); err != nil {
		return nil, err
	}
	target, group, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return nil, err
	}
	ctx = contextWithObjectWritePlacement(ctx, group)
	if err := c.requireMultipartListingPeerCapability(compat.OperationCreateMultipartUpload, c.multipartListingCapabilityPeers(target, group)); err != nil {
		return nil, err
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.CreateMultipartUpload(ctx, bucket, key, contentType)
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildCreateMultipartUploadArgs(bucket, key, contentType)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpCreateMultipartUpload, args)
	if err != nil {
		return nil, err
	}
	return uploadFromReply(reply)
}

func (c *ClusterCoordinator) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	if err := c.requireObjectBucket(ctx, bucket); err != nil {
		return nil, err
	}
	target, group, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return nil, err
	}
	if c.indexWriter != nil {
		ctx = contextWithObjectWritePlacement(ctx, group)
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		obj, err := gb.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
		if err != nil {
			return nil, err
		}
		return obj, c.commitObjectIndex(ctx, bucket, key, obj, group, false)
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildCompleteMultipartUploadArgs(bucket, key, uploadID, parts)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpCompleteMultipartUpload, args)
	if err != nil {
		return nil, err
	}
	obj, err := objectFromReply(reply)
	if err != nil {
		return nil, err
	}
	return obj, c.commitObjectIndex(ctx, bucket, key, obj, group, false)
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
	bucket, key, r, contentType := req.Bucket, req.Key, req.Body, req.ContentType
	userMetadata := req.UserMetadata
	if err := c.requireObjectBucket(ctx, bucket); err != nil {
		return nil, err
	}
	routeStart := time.Now()
	target, group, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return nil, err
	}
	sizeClass := PutTraceSizeUnknown
	if s, ok := r.(interface{ Len() int }); ok {
		sizeClass = putTraceSizeClass(int64(s.Len()), c.maxBody)
	}
	if c.indexWriter != nil {
		ctx = contextWithObjectWritePlacement(ctx, group)
	}
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
		var obj *storage.Object
		if req.SystemMetadata.SSEAlgorithm != "" || req.ACL != nil {
			obj, err = gb.PutObjectWithRequest(ctx, req)
		} else {
			obj, err = gb.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, userMetadata)
		}
		if err != nil {
			return nil, err
		}
		return obj, c.commitObjectIndex(ctx, bucket, key, obj, group, false)
	}
	if len(userMetadata) > 0 || req.SystemMetadata.SSEAlgorithm != "" || req.ACL != nil {
		return nil, storage.UnsupportedOperationError{Op: "PutObjectWithRequest", Reason: storage.UnsupportedReasonNoAdapter}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}

	if c.forward.streamDialer != nil && forwardBodyExceedsSingleFrameCap(r, c.maxBody) {
		args := buildPutObjectArgs(bucket, key, contentType, nil)
		ctx = ContextWithPutTrace(ctx, PutTraceRequest{
			Bucket:      bucket,
			Key:         key,
			GroupID:     target.GroupID,
			Ingress:     PutTraceIngressForwardedNonLeader,
			SizeClass:   PutTraceSizeLarge,
			ForwardMode: PutTraceForwardStream,
		})
		ObservePutTraceStage(ctx, PutTraceStageRouteWrite, routeStart, PutTraceStageFields{})
		resolveStart := time.Now()
		peers := c.forward.ResolveLeaderPeers(ctx, target.Peers, target.GroupID, bucket, key)
		ObservePutTraceStage(ctx, PutTraceStageForwardResolveLeader, resolveStart, PutTraceStageFields{})
		reply, err := c.forward.SendStream(ctx, peers, target.GroupID, raftpb.ForwardOpPutObject, args, r)
		if err != nil {
			return nil, topologyForwardWriteError(group, err)
		}
		obj, err := objectFromReply(reply)
		if err != nil {
			return nil, err
		}
		return obj, nil
	}

	body, err := io.ReadAll(io.LimitReader(r, c.maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > c.maxBody {
		return nil, storage.ErrEntityTooLarge
	}
	args := buildPutObjectArgs(bucket, key, contentType, body)
	ctx = ContextWithPutTrace(ctx, PutTraceRequest{
		Bucket:      bucket,
		Key:         key,
		GroupID:     target.GroupID,
		Ingress:     PutTraceIngressForwardedNonLeader,
		SizeClass:   putTraceSizeClass(int64(len(body)), c.maxBody),
		ForwardMode: PutTraceForwardFrame,
	})
	ObservePutTraceStage(ctx, PutTraceStageRouteWrite, routeStart, PutTraceStageFields{})
	resolveStart := time.Now()
	peers := c.forward.ResolveLeaderPeers(ctx, target.Peers, target.GroupID, bucket, key)
	ObservePutTraceStage(ctx, PutTraceStageForwardResolveLeader, resolveStart, PutTraceStageFields{})
	reply, err := c.forward.Send(ctx, peers, target.GroupID, raftpb.ForwardOpPutObject, args)
	if err != nil {
		return nil, topologyForwardWriteError(group, err)
	}
	obj, err := objectFromReply(reply)
	if err != nil {
		return nil, err
	}
	if obj.Size != int64(len(body)) {
		return nil, ErrForwardBodySizeMismatch
	}
	return obj, nil
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
		return nil, err
	}
	obj, err := c.PutObjectWithRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	facts, err := objectFactsForMutation("PutObject", obj)
	if err != nil {
		return nil, err
	}
	return &storage.PutObjectResult{Object: facts, Previous: previous}, nil
}

func (c *ClusterCoordinator) previousObjectForMutation(ctx context.Context, bucket, key string) (storage.PreviousObject, error) {
	if !storage.IsInternalBucket(bucket) && c.indexWriter != nil {
		if src := metaObjectIndexAdapter(c.meta); src != nil {
			entry, ok := src.ObjectIndexLatest(bucket, key)
			if !ok || entry.IsDeleteMarker {
				return storage.PreviousObject{}, nil
			}
			return storage.PreviousObject{
				Exists:    true,
				Size:      entry.Size,
				ETag:      entry.ETag,
				VersionID: entry.VersionID,
			}, nil
		}
	}
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
		return gb.SetObjectACL(bucket, key, acl)
	}
	if c.forward == nil {
		return ErrCoordinatorNoRouter
	}
	args := buildSetObjectACLArgs(bucket, key, acl)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpSetObjectACL, args)
	if err != nil {
		return err
	}
	return parseReplyStatus(reply)
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
		return gb.SetObjectTags(bucket, key, versionID, tags)
	}
	if c.forward == nil {
		return ErrCoordinatorNoRouter
	}
	args := buildSetObjectTagsArgs(bucket, key, versionID, tags)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpSetObjectTags, args)
	if err != nil {
		return err
	}
	return parseReplyStatus(reply)
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
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildGetObjectTagsArgs(bucket, key, versionID)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpGetObjectTags, args)
	if err != nil {
		return nil, err
	}
	return tagsFromReply(reply)
}

// WriteAt implements the pwrite fast path for routed internal buckets such as
// NFSv4. WAL exposes WriteAt to NFS, so the coordinator must either pass it to
// the local group leader or provide a correct routed fallback.
func (c *ClusterCoordinator) WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*storage.Object, error) {
	target, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.SelfIsOnlyVoter {
		if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
			return nil, err
		} else if gb != nil {
			return gb.WriteAt(ctx, bucket, key, offset, data)
		}
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

// Truncate implements the SETATTR-size fast path for routed internal buckets.
func (c *ClusterCoordinator) Truncate(ctx context.Context, bucket, key string, size int64) error {
	if size < 0 {
		return storage.ErrEntityTooLarge
	}
	target, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return err
	}
	if target.SelfIsOnlyVoter {
		if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
			return err
		} else if gb != nil {
			return gb.Truncate(ctx, bucket, key, size)
		}
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
// leaders use the group backend's zero-copy path; non-leaders fall back through
// regular routed GetObject for correctness.
func (c *ClusterCoordinator) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	if offset < 0 {
		return 0, errors.New("coordinator: negative ReadAt offset")
	}
	target, err := c.routeReadOrBucket(bucket, key, "")
	if err != nil {
		return 0, err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return 0, err
	} else if gb != nil {
		return gb.ReadAt(ctx, bucket, key, offset, buf)
	}

	if c.forward == nil {
		return 0, ErrCoordinatorNoRouter
	}
	if c.forward.readDialer == nil {
		rc, _, err := c.GetObject(ctx, bucket, key)
		if err != nil {
			return 0, err
		}
		defer rc.Close()
		if _, err := io.CopyN(io.Discard, rc, offset); err != nil {
			return 0, err
		}
		return io.ReadFull(rc, buf)
	}

	args := buildReadAtArgs(bucket, key, offset, int64(len(buf)))
	if int64(len(buf)) <= c.maxBody {
		reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpReadAt, args)
		if err != nil {
			return 0, err
		}
		return readAtReplyInto(reply, buf)
	}

	reply, body, err := c.forward.SendReadStream(ctx, target.Peers, target.GroupID, raftpb.ForwardOpReadAt, args)
	if err != nil {
		return 0, err
	}
	defer body.Close()
	if err := parseReplyStatus(reply); err != nil {
		return 0, err
	}
	return io.ReadFull(body, buf)
}

func (c *ClusterCoordinator) PreferReadAt(bucket string) bool {
	return true
}

func (c *ClusterCoordinator) PreferWriteAt(bucket string) bool {
	if !storage.IsInternalBucket(bucket) {
		return false
	}
	target, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return false
	}
	if !target.SelfIsOnlyVoter {
		return false
	}
	gb, err := c.runtimeState().localExec.ResolveWrite(context.Background(), target)
	if err != nil || gb == nil {
		return false
	}
	return gb.PreferWriteAt(bucket)
}

func (c *ClusterCoordinator) UploadPart(
	ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader,
) (*storage.Part, error) {
	if err := c.requireObjectBucket(ctx, bucket); err != nil {
		return nil, err
	}
	target, _, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return nil, err
	}
	if c.indexWriter != nil {
		ctx = ContextWithPlacementGroup(ctx, target.GroupID)
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.UploadPart(ctx, bucket, key, uploadID, partNumber, r)
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}

	if c.forward.streamDialer != nil {
		args := buildUploadPartArgs(bucket, key, uploadID, int32(partNumber), nil)
		streamCtx := ctx
		peers := c.forward.ResolveLeaderPeers(streamCtx, target.Peers, target.GroupID, bucket, key)
		reply, err := c.forward.SendStream(streamCtx, peers, target.GroupID, raftpb.ForwardOpUploadPart, args, r)
		if err != nil {
			return nil, err
		}
		return partFromReply(reply)
	}

	body, err := io.ReadAll(io.LimitReader(r, c.maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > c.maxBody {
		return nil, storage.ErrEntityTooLarge
	}
	args := buildUploadPartArgs(bucket, key, uploadID, int32(partNumber), body)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpUploadPart, args)
	if err != nil {
		return nil, err
	}
	part, err := partFromReply(reply)
	if err != nil {
		return nil, err
	}
	if part.Size != int64(len(body)) {
		return nil, ErrForwardBodySizeMismatch
	}
	return part, nil
}

// AppendObject implements storage.AppendObjecter at the cluster-coordinator
// level. It routes the append to the owner shard group:
//   - local path: dispatches into GroupBackend.AppendObject (DistributedBackend
//     handles the data-Raft propose + apply-error propagation per Phase A)
//     and then commits ObjectIndex on the meta-Raft so the cluster view is
//     consistent with the data plane.
//   - forward path: streams the body to the owner via ForwardSender.SendStream
//     using AppendObjectForwardArgs; the receiver does the propose + commits
//     ObjectIndex itself (handleAppendObjectStream) so we don't double-commit.
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
	target, group, err := c.routeAppendOrBucket(bucket, key, expectedOffset)
	if err != nil {
		return nil, err
	}
	if c.indexWriter != nil {
		ctx = contextWithObjectWritePlacement(ctx, group)
	}

	// Local-exec branch — DistributedBackend.AppendObject already performs the
	// cluster-aware pre-check (offset/cap/non-appendable). We add a bounded
	// retry on ErrStalePlacement so a placement rebalance window doesn't
	// surface as a 503 to the caller.
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		obj, err := c.appendObjectLocalWithRetry(ctx, gb, bucket, key, expectedOffset, r)
		if err != nil {
			return nil, err
		}
		if err := c.commitObjectIndex(ctx, bucket, key, obj, group, false); err != nil {
			return nil, err
		}
		return obj, nil
	}

	// Forward branch — buffer the body under c.maxBody before acquiring the
	// semaphore so the reservation size is exact. Buffering here mirrors what
	// appendObjectLocalWithRetry does on the local-exec branch.
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	if c.forward.streamDialer == nil {
		return nil, ErrCoordinatorNoRouter
	}
	forwardBody, err := io.ReadAll(io.LimitReader(r, c.maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(forwardBody)) > c.maxBody {
		return nil, storage.ErrEntityTooLarge
	}
	bodyLen := int64(len(forwardBody))
	if c.appendForwardBuffer != nil {
		if err := c.appendForwardBuffer.Acquire(ctx, bodyLen); err != nil {
			metrics.AppendForwardBufferRejectedTotal.Inc()
			return nil, err
		}
		defer c.appendForwardBuffer.Release(bodyLen)
	}
	args := buildAppendObjectForwardArgs(bucket, key, expectedOffset)
	peers := c.forward.ResolveLeaderPeers(ctx, target.Peers, target.GroupID, bucket, key)
	reply, err := c.forward.SendStream(ctx, peers, target.GroupID, raftpb.ForwardOpAppendObject, args, bytes.NewReader(forwardBody))
	if err != nil {
		return nil, topologyForwardWriteError(group, err)
	}
	// Receiver already committed ObjectIndex — no second commit here.
	return objectFromReply(reply)
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
		body, err := io.ReadAll(io.LimitReader(r, c.maxBody+1))
		if err != nil {
			return nil, err
		}
		if int64(len(body)) > c.maxBody {
			return nil, storage.ErrEntityTooLarge
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

func (c *ClusterCoordinator) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	if err := c.requireObjectBucket(ctx, bucket); err != nil {
		return err
	}
	target, _, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return err
	}
	if c.indexWriter != nil {
		ctx = ContextWithPlacementGroup(ctx, target.GroupID)
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return err
	} else if gb != nil {
		return gb.AbortMultipartUpload(ctx, bucket, key, uploadID)
	}
	if c.forward == nil {
		return ErrCoordinatorNoRouter
	}
	args := buildAbortMultipartUploadArgs(bucket, key, uploadID)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpAbortMultipartUpload, args)
	if err != nil {
		return err
	}
	return parseReplyStatus(reply)
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
			uploads = append(uploads, groupUploads...)
			continue
		}
		groupUploads, err := gb.ListMultipartUploads(ctx, bucket, prefix, 0)
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, groupUploads...)
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
	args := buildListMultipartUploadsArgs(bucket, prefix, 0)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpListMultipartUploads, args)
	if err != nil {
		return nil, err
	}
	return multipartUploadsFromReply(reply)
}

// ListParts routes by (bucket, key): local group backend first; otherwise the
// peer-transport capability gate must pass before forwarding to the remote
// data-group leader.
func (c *ClusterCoordinator) ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]storage.Part, error) {
	target, _, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return nil, err
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.ListParts(ctx, bucket, key, uploadID, maxParts)
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	if err := c.requireMultipartListingPeerCapability(compat.OperationListParts, target.Peers); err != nil {
		return nil, err
	}
	args := buildListPartsArgs(bucket, key, uploadID, int32(maxParts))
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpListParts, args)
	if err != nil {
		return nil, err
	}
	return partsFromReply(reply)
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

// ScrubPeerStat is the cluster-package-local snapshot of one peer's scrub
// session state. Returned by ScrubSessionStat fan-out; serve.go's adapter
// converts to admin.ScrubJobInfo so cluster does not import admin.
type ScrubPeerStat struct {
	Bucket       string
	KeyPrefix    string
	Scope        int32 // 0=full, 1=live (matches scrubber.ScrubScope)
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
	scope        int32
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
		Scope:        d.scope,
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
		scope:        ss.Scope(),
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
