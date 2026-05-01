package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
)

// DefaultMaxForwardBodyBytes is the 5 MB hard cap on PutObject and UploadPart
// body size enforced by ClusterCoordinator before encoding the FBS args. The
// cap is the trade-off for the single-message wire model — chunked streaming
// would have removed it but at the cost of a transport refactor.
const DefaultMaxForwardBodyBytes = 5 * 1024 * 1024

// ErrCoordinatorNoRouter is returned when routeBucket is called on a
// coordinator that was constructed without a router (test/solo-node configs
// that should not be reaching the routing path).
var ErrCoordinatorNoRouter = errors.New("coordinator: router not configured")

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

	maxBody int64
}

// NewClusterCoordinator constructs a coordinator with the 5 MB default body
// cap. groups/router/meta may be nil for tests that exercise only cluster-wide
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

// --- Cluster-wide delegations (4 ops) ---
//
// These bypass routing entirely. CreateBucket and friends are always served by
// the meta-Raft (via base = DistributedBackend), keeping bucket-creation
// linearizable across the cluster regardless of which group later owns it.

func (c *ClusterCoordinator) CreateBucket(bucket string) error { return c.base.CreateBucket(bucket) }
func (c *ClusterCoordinator) HeadBucket(bucket string) error   { return c.base.HeadBucket(bucket) }
func (c *ClusterCoordinator) DeleteBucket(bucket string) error { return c.base.DeleteBucket(bucket) }
func (c *ClusterCoordinator) ListBuckets() ([]string, error)   { return c.base.ListBuckets() }

// ListAllObjects implements storage.Snapshotable by enumerating bucket-routed
// objects across every cluster-wide bucket.
func (c *ClusterCoordinator) ListAllObjects() ([]storage.SnapshotObject, error) {
	if c.router == nil || c.groups == nil {
		snap, ok := c.base.(storage.Snapshotable)
		if !ok {
			return nil, storage.ErrSnapshotNotSupported
		}
		return snap.ListAllObjects()
	}
	buckets, err := c.ListBuckets()
	if err != nil {
		return nil, err
	}
	var out []storage.SnapshotObject
	for _, bucket := range buckets {
		if err := c.WalkObjects(bucket, "", func(obj *storage.Object) error {
			out = append(out, storage.SnapshotObject{
				Bucket:         bucket,
				Key:            obj.Key,
				ETag:           obj.ETag,
				Size:           obj.Size,
				ContentType:    obj.ContentType,
				Modified:       obj.LastModified,
				VersionID:      obj.VersionID,
				IsDeleteMarker: obj.IsDeleteMarker,
				IsLatest:       true,
				ACL:            obj.ACL,
			})
			return nil
		}); err != nil {
			return nil, err
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
		if err := c.DeleteObject(obj.Bucket, obj.Key); err != nil {
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
func (c *ClusterCoordinator) routeBucket(bucket string) (*routeTarget, error) {
	if c.router == nil {
		return nil, ErrCoordinatorNoRouter
	}
	dg, err := c.router.RouteKey(bucket, "")
	if err != nil || dg == nil {
		return nil, storage.ErrNoSuchBucket
	}
	if c.meta == nil {
		return nil, ErrUnknownGroup
	}
	entry, ok := c.meta.ShardGroup(dg.ID())
	if !ok || len(entry.PeerIDs) == 0 {
		return nil, ErrUnknownGroup
	}
	t := &routeTarget{
		groupID: entry.ID,
		peers:   PeersForForward(entry, c.selfID),
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

// GetObject reads the object body and metadata. Body is embedded inside the
// reply (≤5 MB enforced server-side). Returns ErrNoSuchBucket / ErrObjectNotFound
// for the obvious cases.
func (c *ClusterCoordinator) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.GetObject(bucket, key)
		}
	}
	if c.forward == nil {
		return nil, nil, ErrCoordinatorNoRouter
	}
	args := buildGetObjectArgs(bucket, key)
	reply, err := c.forward.Send(context.TODO(), target.peers, target.groupID, raftpb.ForwardOpGetObject, args)
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
	return io.NopCloser(bytes.NewReader(bodyCopy)), obj, nil
}

func (c *ClusterCoordinator) HeadObject(bucket, key string) (*storage.Object, error) {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.HeadObject(bucket, key)
		}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildHeadObjectArgs(bucket, key)
	reply, err := c.forward.Send(context.TODO(), target.peers, target.groupID, raftpb.ForwardOpHeadObject, args)
	if err != nil {
		return nil, err
	}
	return objectFromReply(reply)
}

func (c *ClusterCoordinator) DeleteObject(bucket, key string) error {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.DeleteObject(bucket, key)
		}
	}
	if c.forward == nil {
		return ErrCoordinatorNoRouter
	}
	args := buildDeleteObjectArgs(bucket, key)
	reply, err := c.forward.Send(context.TODO(), target.peers, target.groupID, raftpb.ForwardOpDeleteObject, args)
	if err != nil {
		return err
	}
	return parseReplyStatus(reply)
}

func (c *ClusterCoordinator) ListObjects(bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.ListObjects(bucket, prefix, maxKeys)
		}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildListObjectsArgs(bucket, prefix, int32(maxKeys))
	reply, err := c.forward.Send(context.TODO(), target.peers, target.groupID, raftpb.ForwardOpListObjects, args)
	if err != nil {
		return nil, err
	}
	return objectsFromReply(reply)
}

// WalkObjects buffers ALL matching objects on the server and returns them in
// one reply (≤5 MB cap on the encoded size). Callers expecting >5 MB worth of
// keys should use ListObjects with maxKeys pagination instead.
func (c *ClusterCoordinator) WalkObjects(bucket, prefix string, fn func(*storage.Object) error) error {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.WalkObjects(bucket, prefix, fn)
		}
	}
	if c.forward == nil {
		return ErrCoordinatorNoRouter
	}
	args := buildWalkObjectsArgs(bucket, prefix)
	reply, err := c.forward.Send(context.TODO(), target.peers, target.groupID, raftpb.ForwardOpWalkObjects, args)
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

func (c *ClusterCoordinator) CreateMultipartUpload(bucket, key, contentType string) (*storage.MultipartUpload, error) {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.CreateMultipartUpload(bucket, key, contentType)
		}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildCreateMultipartUploadArgs(bucket, key, contentType)
	reply, err := c.forward.Send(context.TODO(), target.peers, target.groupID, raftpb.ForwardOpCreateMultipartUpload, args)
	if err != nil {
		return nil, err
	}
	return uploadFromReply(reply)
}

func (c *ClusterCoordinator) CompleteMultipartUpload(bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.CompleteMultipartUpload(bucket, key, uploadID, parts)
		}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildCompleteMultipartUploadArgs(bucket, key, uploadID, parts)
	reply, err := c.forward.Send(context.TODO(), target.peers, target.groupID, raftpb.ForwardOpCompleteMultipartUpload, args)
	if err != nil {
		return nil, err
	}
	return objectFromReply(reply)
}

// PutObject buffers the body up to maxBody+1 bytes (5 MB +1 to detect the
// over-limit case in one read). The full body is embedded inside the FBS args
// and sent in a single message — chunked streaming would have removed this
// cap but at the cost of a transport refactor. See design doc §"핵심 단순화".
func (c *ClusterCoordinator) PutObject(
	bucket, key string, r io.Reader, contentType string,
) (*storage.Object, error) {
	body, err := io.ReadAll(io.LimitReader(r, c.maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > c.maxBody {
		return nil, storage.ErrEntityTooLarge
	}

	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.PutObject(bucket, key, bytes.NewReader(body), contentType)
		}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildPutObjectArgs(bucket, key, contentType, body)
	reply, err := c.forward.Send(context.TODO(), target.peers, target.groupID, raftpb.ForwardOpPutObject, args)
	if err != nil {
		return nil, err
	}
	return objectFromReply(reply)
}

// UploadPart applies the same 5 MB body cap as PutObject. S3 spec allows up to
// 5 GB per part — clients targeting GrainFS must split larger payloads.
func (c *ClusterCoordinator) UploadPart(
	bucket, key, uploadID string, partNumber int, r io.Reader,
) (*storage.Part, error) {
	body, err := io.ReadAll(io.LimitReader(r, c.maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > c.maxBody {
		return nil, storage.ErrEntityTooLarge
	}

	target, err := c.routeBucket(bucket)
	if err != nil {
		return nil, err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.UploadPart(bucket, key, uploadID, partNumber, bytes.NewReader(body))
		}
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	args := buildUploadPartArgs(bucket, key, uploadID, int32(partNumber), body)
	reply, err := c.forward.Send(context.TODO(), target.peers, target.groupID, raftpb.ForwardOpUploadPart, args)
	if err != nil {
		return nil, err
	}
	return partFromReply(reply)
}

func (c *ClusterCoordinator) AbortMultipartUpload(bucket, key, uploadID string) error {
	target, err := c.routeBucket(bucket)
	if err != nil {
		return err
	}
	if target.selfIsLeader {
		if gb := c.localBackend(target.groupID); gb != nil {
			return gb.AbortMultipartUpload(bucket, key, uploadID)
		}
	}
	if c.forward == nil {
		return ErrCoordinatorNoRouter
	}
	args := buildAbortMultipartUploadArgs(bucket, key, uploadID)
	reply, err := c.forward.Send(context.TODO(), target.peers, target.groupID, raftpb.ForwardOpAbortMultipartUpload, args)
	if err != nil {
		return err
	}
	return parseReplyStatus(reply)
}

// Compile-time assertion: ClusterCoordinator implements storage.Backend.
var _ storage.Backend = (*ClusterCoordinator)(nil)
