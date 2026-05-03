package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"

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

// ErrCoordinatorNoRouter is returned when routeBucket is called on a
// coordinator that was constructed without a router (test/solo-node configs
// that should not be reaching the routing path).
var ErrCoordinatorNoRouter = errors.New("coordinator: router not configured")

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
	base    storage.Backend
	groups  *DataGroupManager
	router  *Router
	meta    ShardGroupSource
	forward *ForwardSender
	selfID  string
	addr    NodeAddressBook

	maxBody int64
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
	return &ClusterCoordinator{
		base:    base,
		groups:  groups,
		router:  router,
		meta:    meta,
		selfID:  selfID,
		maxBody: DefaultMaxForwardBodyBytes,
	}
}

// WithForwardSender attaches the QUIC dialer used to send 0x08 forward calls
// to peer nodes. Returns the receiver for builder-style chaining in serve.go.
func (c *ClusterCoordinator) WithForwardSender(s *ForwardSender) *ClusterCoordinator {
	c.forward = s
	return c
}

// WithNodeAddressResolver attaches the cluster address book used to translate
// nodeID PeerIDs into dialable QUIC addresses for runtime forwarding.
func (c *ClusterCoordinator) WithNodeAddressResolver(book NodeAddressBook) *ClusterCoordinator {
	c.addr = book
	return c
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
				rc, obj, err := c.GetObjectVersion(bucket, version.Key, version.VersionID)
				if err != nil {
					return nil, err
				}
				_ = rc.Close()
				snap.ETag = obj.ETag
				snap.Size = obj.Size
				snap.ContentType = obj.ContentType
				snap.Modified = obj.LastModified
				snap.ACL = obj.ACL
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
		target, err := c.routeBucket(obj.Bucket)
		if err != nil {
			return 0, nil, err
		}
		byGroup[target.groupID] = append(byGroup[target.groupID], obj)
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

// --- Routing helper ---

// routeTarget captures everything an op needs to dispatch a bucket-scoped
// call: which group owns the bucket, peers in attempt order, and whether
// self can short-circuit the wire.
type routeTarget struct {
	groupID      string
	peers        []string
	selfIsLeader bool
	selfIsVoter  bool
}

// routeBucket resolves bucket → group → peer list for the bucket-scoped ops in
// T6/T7. Returns:
//   - ErrCoordinatorNoRouter if router is nil (config error)
//   - storage.ErrNoSuchBucket if no shard-group is assigned to the bucket
//   - ErrUnknownGroup if the assigned group is missing from meta-FSM
//
// selfIsLeader is true only when self is a voter AND the local GroupBackend's
// raft.Node currently holds leadership — used by op handlers to skip the wire
// and call the local backend directly (perf hint, not a correctness gate).
func (c *ClusterCoordinator) routeBucket(bucket string) (routeTarget, error) {
	if c.router == nil {
		return routeTarget{}, ErrCoordinatorNoRouter
	}
	dg, err := c.router.RouteKey(bucket, "")
	if err != nil || dg == nil {
		return routeTarget{}, storage.ErrNoSuchBucket
	}
	if c.meta == nil {
		return routeTarget{}, ErrUnknownGroup
	}
	var entry ShardGroupEntry
	var ok bool
	if src, fast := c.meta.(shardGroupNoCopySource); fast {
		entry, ok = src.shardGroupNoCopy(dg.ID())
	} else {
		entry, ok = c.meta.ShardGroup(dg.ID())
	}
	if !ok || len(entry.PeerIDs) == 0 {
		return routeTarget{}, ErrUnknownGroup
	}
	t := routeTarget{
		groupID: entry.ID,
	}
	for _, p := range entry.PeerIDs {
		if p == c.selfID {
			t.selfIsVoter = true
			break
		}
	}
	if t.selfIsVoter && c.groups != nil {
		if dg2 := c.groups.Get(entry.ID); dg2 != nil && dg2.Backend() != nil &&
			dg2.Backend().RaftNode() != nil && dg2.Backend().RaftNode().IsLeader() {
			t.selfIsLeader = true
		}
	}
	if t.selfIsLeader {
		return t, nil
	}

	peerIDs := PeersForForward(entry, c.selfID)
	peers := peerIDs
	if c.addr != nil {
		resolved, err := ResolveNodeAddresses(c.addr, peerIDs)
		if err != nil {
			return routeTarget{}, err
		}
		peers = resolved
	}
	t.peers = peers
	return t, nil
}

// localBackend returns the GroupBackend embedded in the named group. Caller
// guarantees groups != nil and the group exists (typically via routeBucket
// having returned selfIsLeader = true). Returns nil if any link is missing.
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
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.GetObject(ctx, bucket, key)
		}
	}
	if c.forward == nil {
		return nil, nil, ErrCoordinatorNoRouter
	}
	args := buildGetObjectArgs(bucket, key)
	if c.forward.readDialer != nil {
		return c.forwardReadObject(ctx, target.peers, target.groupID, raftpb.ForwardOpGetObject, args)
	}
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpGetObject, args)
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
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.GetObjectVersion(bucket, key, versionID)
		}
	}
	if c.forward == nil {
		return nil, nil, ErrCoordinatorNoRouter
	}
	args := buildGetObjectVersionArgs(bucket, key, versionID)
	if c.forward.readDialer != nil {
		return c.forwardReadObject(ctx, target.peers, target.groupID, raftpb.ForwardOpGetObjectVersion, args)
	}
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpGetObjectVersion, args)
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
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.HeadObject(ctx, bucket, key)
		}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildHeadObjectArgs(bucket, key)
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpHeadObject, args)
	if err != nil {
		return nil, err
	}
	return objectFromReply(reply)
}

func (c *ClusterCoordinator) DeleteObject(ctx context.Context, bucket, key string) error {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.DeleteObject(ctx, bucket, key)
		}
	}
	if c.forward == nil {
		return ErrCoordinatorNoRouter
	}
	args := buildDeleteObjectArgs(bucket, key)
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpDeleteObject, args)
	if err != nil {
		return err
	}
	return parseReplyStatus(reply)
}

func (c *ClusterCoordinator) DeleteObjectReturningMarker(bucket, key string) (string, error) {
	ctx := context.Background()
	target, err := c.routeBucket(bucket)
	if err != nil {
		return "", err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.DeleteObjectReturningMarker(bucket, key)
		}
	}
	if c.forward == nil {
		return "", ErrCoordinatorNoRouter
	}
	args := buildDeleteObjectArgs(bucket, key)
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpDeleteObject, args)
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
	target, err := c.routeBucket(bucket)
	if err != nil {
		return err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.DeleteObjectVersion(bucket, key, versionID)
		}
	}
	if c.forward == nil {
		return ErrCoordinatorNoRouter
	}
	args := buildDeleteObjectVersionArgs(bucket, key, versionID)
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpDeleteObjectVersion, args)
	if err != nil {
		return err
	}
	return parseReplyStatus(reply)
}

func (c *ClusterCoordinator) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.ListObjects(ctx, bucket, prefix, maxKeys)
		}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildListObjectsArgs(bucket, prefix, int32(maxKeys))
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpListObjects, args)
	if err != nil {
		return nil, err
	}
	return objectsFromReply(reply)
}

func (c *ClusterCoordinator) ListObjectVersions(
	bucket, prefix string, maxKeys int,
) ([]*storage.ObjectVersion, error) {
	ctx := context.Background()
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.ListObjectVersions(bucket, prefix, maxKeys)
		}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildListObjectVersionsArgs(bucket, prefix, int32(maxKeys))
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpListObjectVersions, args)
	if err != nil {
		return nil, err
	}
	return objectVersionsFromReply(reply)
}

// WalkObjects buffers ALL matching objects on the server and returns them in
// one reply. Callers expecting large keysets should use ListObjects with
// maxKeys pagination instead.
func (c *ClusterCoordinator) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.WalkObjects(ctx, bucket, prefix, fn)
		}
	}
	if c.forward == nil {
		return ErrCoordinatorNoRouter
	}
	args := buildWalkObjectsArgs(bucket, prefix)
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpWalkObjects, args)
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
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.CreateMultipartUpload(ctx, bucket, key, contentType)
		}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildCreateMultipartUploadArgs(bucket, key, contentType)
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpCreateMultipartUpload, args)
	if err != nil {
		return nil, err
	}
	return uploadFromReply(reply)
}

func (c *ClusterCoordinator) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
		}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildCompleteMultipartUploadArgs(bucket, key, uploadID, parts)
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpCompleteMultipartUpload, args)
	if err != nil {
		return nil, err
	}
	return objectFromReply(reply)
}

func (c *ClusterCoordinator) PutObject(
	ctx context.Context, bucket, key string, r io.Reader, contentType string,
) (*storage.Object, error) {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.PutObject(ctx, bucket, key, r, contentType)
		}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}

	if c.forward.streamDialer != nil && forwardBodyExceedsSingleFrameCap(r, c.maxBody) {
		args := buildPutObjectArgs(bucket, key, contentType, nil)
		streamCtx := ctx
		peers := c.forward.ResolveLeaderPeers(streamCtx, target.peers, target.groupID, bucket, key)
		reply, err := c.forward.SendStream(streamCtx, peers, target.groupID, raftpb.ForwardOpPutObject, args, r)
		if err != nil {
			return nil, err
		}
		return objectFromReply(reply)
	}

	body, err := io.ReadAll(io.LimitReader(r, c.maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > c.maxBody {
		return nil, storage.ErrEntityTooLarge
	}
	args := buildPutObjectArgs(bucket, key, contentType, body)
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpPutObject, args)
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
	return obj, nil
}

// WriteAt implements the pwrite fast path for routed internal buckets such as
// NFSv4. WAL exposes WriteAt to NFS, so the coordinator must either pass it to
// the local group leader or provide a correct routed fallback.
func (c *ClusterCoordinator) WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*storage.Object, error) {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
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
	target, err := c.routeBucket(bucket)
	if err != nil {
		return err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
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
	target, err := c.routeBucket(bucket)
	if err != nil {
		return 0, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.ReadAt(ctx, bucket, key, offset, buf)
		}
	}

	rc, _, err := c.GetObject(ctx, bucket, key)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return 0, err
	}
	if offset >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(buf, data[offset:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

func (c *ClusterCoordinator) UploadPart(
	ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader,
) (*storage.Part, error) {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.UploadPart(ctx, bucket, key, uploadID, partNumber, r)
		}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}

	if c.forward.streamDialer != nil && forwardBodyExceedsSingleFrameCap(r, c.maxBody) {
		args := buildUploadPartArgs(bucket, key, uploadID, int32(partNumber), nil)
		streamCtx := ctx
		peers := c.forward.ResolveLeaderPeers(streamCtx, target.peers, target.groupID, bucket, key)
		reply, err := c.forward.SendStream(streamCtx, peers, target.groupID, raftpb.ForwardOpUploadPart, args, r)
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
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpUploadPart, args)
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
	target, err := c.routeBucket(bucket)
	if err != nil {
		return err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.AbortMultipartUpload(ctx, bucket, key, uploadID)
		}
	}
	if c.forward == nil {
		return ErrCoordinatorNoRouter
	}
	args := buildAbortMultipartUploadArgs(bucket, key, uploadID)
	reply, err := c.forward.Send(ctx, target.peers, target.groupID, raftpb.ForwardOpAbortMultipartUpload, args)
	if err != nil {
		return err
	}
	return parseReplyStatus(reply)
}

var (
	_ storage.Backend     = (*ClusterCoordinator)(nil)
	_ storage.PartialIO   = (*ClusterCoordinator)(nil)
	_ storage.Truncatable = (*ClusterCoordinator)(nil)
)
