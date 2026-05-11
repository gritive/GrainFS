package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
)

// DefaultMaxForwardBodyBytes is the compatibility cap for legacy single-message
// forwarding. Production wiring uses streamed body forwarding for PutObject and
// UploadPart so larger objects do not allocate into the forward frame.
const DefaultMaxForwardBodyBytes = 5 * 1024 * 1024

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
	if b == nil || b.RaftNode() == nil {
		return false
	}
	return b.RaftNode().IsLeader()
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
	indexWriter objectIndexProposer

	opRouter  *OpRouter
	localExec *LocalExecution

	maxBody int64
}

type objectIndexSource interface {
	ObjectIndexLatest(bucket, key string) (ObjectIndexEntry, bool)
	ObjectIndexVersion(bucket, key, versionID string) (ObjectIndexEntry, bool)
}

type objectIndexListSource interface {
	ObjectIndexLatestEntries(bucket, prefix string, maxKeys int) []ObjectIndexEntry
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
		base:    base,
		groups:  groups,
		router:  router,
		meta:    meta,
		selfID:  selfID,
		maxBody: DefaultMaxForwardBodyBytes,
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

func (c *ClusterCoordinator) WithObjectIndexProposer(p objectIndexProposer) *ClusterCoordinator {
	c.indexWriter = p
	c.rebuild()
	return c
}

// rebuild constructs the embedded OpRouter and LocalExecution from the
// current dependency state. Called from every builder method and from
// NewClusterCoordinator. Keeping the modules embedded rather than passed
// per-call avoids per-request allocations on the hot path.
func (c *ClusterCoordinator) rebuild() {
	c.opRouter = NewOpRouter(
		c.router,
		c.meta,
		metaObjectIndexAdapter(c.meta),
		c.addr,
		dataGroupManagerLeaderProbe{m: c.groups},
		c.ecConfig,
		c.selfID,
		c.selfAliases,
	)
	c.localExec = NewLocalExecution(dataGroupManagerLocalBackend{m: c.groups})
}

// routeReadOrBucket picks RouteObjectRead when an object index is configured
// (production wiring), and falls back to RouteBucket when not (test wiring
// without an objectIndexProposer / objectIndexSource). Preserves the legacy
// routeObjectLatest/Version dispatch behavior that callers depended on.
func (c *ClusterCoordinator) routeReadOrBucket(bucket, key, versionID string) (RouteTarget, error) {
	if c.indexWriter == nil {
		return c.opRouter.RouteBucket(bucket)
	}
	target, _, err := c.opRouter.RouteObjectRead(bucket, key, versionID)
	return target, err
}

// routeWriteOrBucket picks RouteObjectWrite (EC-aware placement) when an
// object index proposer or EC config is configured (production wiring), and
// falls back to a bucket-only route when neither is set. Preserves the legacy
// routeObjectWrite short-circuit:
//
//	indexWriter == nil && ecConfig.NumShards() == 0 → routeBucket
func (c *ClusterCoordinator) routeWriteOrBucket(bucket, key string) (RouteTarget, ShardGroupEntry, error) {
	if c.indexWriter == nil && c.ecConfig.NumShards() == 0 {
		target, err := c.opRouter.RouteBucket(bucket)
		return target, ShardGroupEntry{ID: target.GroupID}, err
	}
	return c.opRouter.RouteObjectWrite(bucket, key)
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
	type bucketVersioner interface {
		SetBucketVersioning(bucket, state string) error
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
		target, err := c.opRouter.RouteBucket(obj.Bucket)
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
	ecConfig := objectIndexECConfigForGroup(group)
	entry := ObjectIndexEntry{
		Bucket:           bucket,
		Key:              key,
		VersionID:        obj.VersionID,
		PlacementGroupID: group.ID,
		Size:             obj.Size,
		ContentType:      obj.ContentType,
		ETag:             obj.ETag,
		ModTime:          obj.LastModified,
		ECData:           uint8(ecConfig.DataShards),
		ECParity:         uint8(ecConfig.ParityShards),
		NodeIDs:          objectIndexNodeIDsForGroup(group, ecConfig),
		IsDeleteMarker:   isDeleteMarker,
	}
	return c.indexWriter.ProposeObjectIndex(ctx, entry, false)
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

func objectIndexEntryToObject(entry ObjectIndexEntry) *storage.Object {
	return &storage.Object{
		Key:          entry.Key,
		Size:         entry.Size,
		ContentType:  entry.ContentType,
		ETag:         entry.ETag,
		LastModified: entry.ModTime,
		VersionID:    entry.VersionID,
	}
}

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
	if gb, err := c.localExec.ResolveRead(ctx, target); err != nil {
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
	if gb, err := c.localExec.ResolveRead(ctx, target); err != nil {
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
	n, err := r.rc.Read(p)
	r.got += int64(n)
	if err == io.EOF && r.got != r.want {
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
	if gb, err := c.localExec.ResolveRead(ctx, target); err != nil {
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

func (c *ClusterCoordinator) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := c.DeleteObjectReturningMarker(bucket, key)
	return err
}

func (c *ClusterCoordinator) DeleteObjectReturningMarker(bucket, key string) (string, error) {
	ctx := context.Background()
	var (
		target RouteTarget
		entry  ObjectIndexEntry
		err    error
	)
	if c.indexWriter == nil {
		target, err = c.opRouter.RouteBucket(bucket)
		entry = ObjectIndexEntry{Bucket: bucket, Key: key, PlacementGroupID: target.GroupID}
	} else {
		target, entry, err = c.opRouter.RouteObjectRead(bucket, key, "")
	}
	if err != nil {
		return "", err
	}
	if gb, err := c.localExec.ResolveWrite(ctx, target); err != nil {
		return "", err
	} else if gb != nil {
		markerID, err := gb.DeleteObjectReturningMarker(bucket, key)
		if err != nil {
			return "", err
		}
		marker := &storage.Object{Key: key, VersionID: markerID, LastModified: time.Now().Unix()}
		if err := c.commitObjectIndex(ctx, bucket, key, marker, ShardGroupEntry{ID: target.GroupID, PeerIDs: entry.NodeIDs}, true); err != nil {
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
		if err := c.commitObjectIndex(ctx, bucket, key, obj, ShardGroupEntry{ID: target.GroupID, PeerIDs: entry.NodeIDs}, true); err != nil {
			return "", err
		}
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
	if gb, err := c.localExec.ResolveWrite(ctx, target); err != nil {
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
	if err := parseReplyStatus(reply); err != nil {
		return err
	}
	if c.indexWriter != nil {
		return c.indexWriter.ProposeDeleteObjectIndex(ctx, bucket, key, versionID)
	}
	return nil
}

func (c *ClusterCoordinator) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	if src, ok := c.objectIndexListSource(); ok {
		if err := c.HeadBucket(ctx, bucket); err != nil {
			return nil, err
		}
		entries := src.ObjectIndexLatestEntries(bucket, prefix, maxKeys)
		objects := make([]*storage.Object, 0, len(entries))
		for _, entry := range entries {
			objects = append(objects, objectIndexEntryToObject(entry))
		}
		return objects, nil
	}
	target, err := c.opRouter.RouteBucket(bucket)
	if err != nil {
		return nil, err
	}
	if gb, err := c.localExec.ResolveRead(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.ListObjects(ctx, bucket, prefix, maxKeys)
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildListObjectsArgs(bucket, prefix, int32(maxKeys))
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpListObjects, args)
	if err != nil {
		return nil, err
	}
	return objectsFromReply(reply)
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
	target, err := c.opRouter.RouteBucket(bucket)
	if err != nil {
		return nil, err
	}
	if gb, err := c.localExec.ResolveRead(ctx, target); err != nil {
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
	target, err := c.opRouter.RouteBucket(bucket)
	if err != nil {
		return err
	}
	if gb, err := c.localExec.ResolveRead(ctx, target); err != nil {
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
	target, group, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return nil, err
	}
	if c.indexWriter != nil {
		ctx = contextWithObjectWritePlacement(ctx, group)
	}
	if gb, err := c.localExec.ResolveWrite(ctx, target); err != nil {
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
	target, group, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return nil, err
	}
	if c.indexWriter != nil {
		ctx = contextWithObjectWritePlacement(ctx, group)
	}
	if gb, err := c.localExec.ResolveWrite(ctx, target); err != nil {
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
	target, group, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return nil, err
	}
	if c.indexWriter != nil {
		ctx = contextWithObjectWritePlacement(ctx, group)
	}
	if gb, err := c.localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		obj, err := gb.PutObject(ctx, bucket, key, r, contentType)
		if err != nil {
			return nil, err
		}
		return obj, c.commitObjectIndex(ctx, bucket, key, obj, group, false)
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}

	if c.forward.streamDialer != nil && forwardBodyExceedsSingleFrameCap(r, c.maxBody) {
		args := buildPutObjectArgs(bucket, key, contentType, nil)
		streamCtx := ctx
		peers := c.forward.ResolveLeaderPeers(streamCtx, target.Peers, target.GroupID, bucket, key)
		reply, err := c.forward.SendStream(streamCtx, peers, target.GroupID, raftpb.ForwardOpPutObject, args, r)
		if err != nil {
			return nil, err
		}
		obj, err := objectFromReply(reply)
		if err != nil {
			return nil, err
		}
		return obj, c.commitObjectIndex(ctx, bucket, key, obj, group, false)
	}

	body, err := io.ReadAll(io.LimitReader(r, c.maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > c.maxBody {
		return nil, storage.ErrEntityTooLarge
	}
	args := buildPutObjectArgs(bucket, key, contentType, body)
	reply, err := c.forward.Send(ctx, target.Peers, target.GroupID, raftpb.ForwardOpPutObject, args)
	if err != nil {
		return nil, err
	}
	obj, err := objectFromReply(reply)
	if err != nil {
		return nil, err
	}
	if obj.Size != int64(len(body)) {
		return nil, ErrForwardBodySizeMismatch
	}
	return obj, c.commitObjectIndex(ctx, bucket, key, obj, group, false)
}

// WriteAt implements the pwrite fast path for routed internal buckets such as
// NFSv4. WAL exposes WriteAt to NFS, so the coordinator must either pass it to
// the local group leader or provide a correct routed fallback.
func (c *ClusterCoordinator) WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*storage.Object, error) {
	target, err := c.opRouter.RouteBucket(bucket)
	if err != nil {
		return nil, err
	}
	if gb, err := c.localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.WriteAt(ctx, bucket, key, offset, data)
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
	target, err := c.opRouter.RouteBucket(bucket)
	if err != nil {
		return err
	}
	if gb, err := c.localExec.ResolveWrite(ctx, target); err != nil {
		return err
	} else if gb != nil {
		return gb.Truncate(ctx, bucket, key, size)
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
	if gb, err := c.localExec.ResolveRead(ctx, target); err != nil {
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
		if err := parseReplyStatus(reply); err != nil {
			return 0, err
		}
		fr := raftpb.GetRootAsForwardReply(reply, 0)
		body := fr.ReadBodyBytes()
		n := copy(buf, body)
		if n != len(buf) || n != len(body) {
			return n, ErrForwardBodySizeMismatch
		}
		return n, nil
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
	target, err := c.opRouter.RouteBucket(bucket)
	if err != nil {
		return false
	}
	gb, err := c.localExec.ResolveWrite(context.Background(), target)
	if err != nil || gb == nil {
		return false
	}
	return gb.PreferWriteAt(bucket)
}

func (c *ClusterCoordinator) UploadPart(
	ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader,
) (*storage.Part, error) {
	target, _, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return nil, err
	}
	if c.indexWriter != nil {
		ctx = ContextWithPlacementGroup(ctx, target.GroupID)
	}
	if gb, err := c.localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.UploadPart(ctx, bucket, key, uploadID, partNumber, r)
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}

	if c.forward.streamDialer != nil && forwardBodyExceedsSingleFrameCap(r, c.maxBody) {
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
	target, _, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return err
	}
	if c.indexWriter != nil {
		ctx = ContextWithPlacementGroup(ctx, target.GroupID)
	}
	if gb, err := c.localExec.ResolveWrite(ctx, target); err != nil {
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

// ListMultipartUploads delegates to the local DistributedBackend, which scans
// the FSM-replicated multipartKey range. First-slice limitation: cluster
// multipart metadata does not yet record bucket+key, so this returns an empty
// list in cluster mode. Single-node mode (LocalBackend) returns the real
// list.
func (c *ClusterCoordinator) ListMultipartUploads(ctx context.Context, bucket, prefix string, maxUploads int) ([]*storage.MultipartUpload, error) {
	return c.base.ListMultipartUploads(ctx, bucket, prefix, maxUploads)
}

// ListParts routes by (bucket, key) to the data Raft group that owns the
// upload's part files. If the local node is on the routed group, parts are
// read directly; otherwise we surface the parts visible at the local
// DistributedBackend (best-effort, see DistributedBackend.ListParts).
// Forwarding ListParts to the remote group's leader is a follow-up.
func (c *ClusterCoordinator) ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]storage.Part, error) {
	target, _, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return nil, err
	}
	if gb, err := c.localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.ListParts(ctx, bucket, key, uploadID, maxParts)
	}
	return c.base.ListParts(ctx, bucket, key, uploadID, maxParts)
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
