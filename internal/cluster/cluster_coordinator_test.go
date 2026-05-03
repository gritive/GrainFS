package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/wal"
	"github.com/stretchr/testify/require"
)

// fakeBackend records every storage.Backend call so tests can assert that the
// coordinator's cluster-wide ops delegate without touching the routing path.
// Bucket-scoped methods are unimplemented — the routing tests in T6 use a
// different fake (with FBS reply injection).
type fakeBackend struct {
	calls      []string
	listResult []string
	createErr  error
	headErr    error
	deleteErr  error
	listErr    error
}

func (f *fakeBackend) record(call string) { f.calls = append(f.calls, call) }

func (f *fakeBackend) CreateBucket(bucket string) error {
	f.record(fmt.Sprintf("CreateBucket:%s", bucket))
	return f.createErr
}
func (f *fakeBackend) HeadBucket(bucket string) error {
	f.record(fmt.Sprintf("HeadBucket:%s", bucket))
	return f.headErr
}
func (f *fakeBackend) DeleteBucket(bucket string) error {
	f.record(fmt.Sprintf("DeleteBucket:%s", bucket))
	return f.deleteErr
}
func (f *fakeBackend) ListBuckets() ([]string, error) {
	f.record("ListBuckets")
	return f.listResult, f.listErr
}

// Bucket-scoped methods — unused in T5; T6 routing tests use a different fake.
func (f *fakeBackend) PutObject(bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	return nil, fmt.Errorf("fakeBackend.PutObject not implemented")
}
func (f *fakeBackend) GetObject(bucket, key string) (io.ReadCloser, *storage.Object, error) {
	return nil, nil, fmt.Errorf("fakeBackend.GetObject not implemented")
}
func (f *fakeBackend) HeadObject(bucket, key string) (*storage.Object, error) {
	return nil, fmt.Errorf("fakeBackend.HeadObject not implemented")
}
func (f *fakeBackend) DeleteObject(bucket, key string) error {
	return fmt.Errorf("fakeBackend.DeleteObject not implemented")
}
func (f *fakeBackend) ListObjects(bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	return nil, fmt.Errorf("fakeBackend.ListObjects not implemented")
}
func (f *fakeBackend) WalkObjects(bucket, prefix string, fn func(*storage.Object) error) error {
	return fmt.Errorf("fakeBackend.WalkObjects not implemented")
}
func (f *fakeBackend) CreateMultipartUpload(bucket, key, contentType string) (*storage.MultipartUpload, error) {
	return nil, fmt.Errorf("fakeBackend.CreateMultipartUpload not implemented")
}
func (f *fakeBackend) UploadPart(bucket, key, uploadID string, partNumber int, r io.Reader) (*storage.Part, error) {
	return nil, fmt.Errorf("fakeBackend.UploadPart not implemented")
}
func (f *fakeBackend) CompleteMultipartUpload(bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	return nil, fmt.Errorf("fakeBackend.CompleteMultipartUpload not implemented")
}
func (f *fakeBackend) AbortMultipartUpload(bucket, key, uploadID string) error {
	return fmt.Errorf("fakeBackend.AbortMultipartUpload not implemented")
}

// TestClusterCoordinator_CreateBucket_DelegatesToBase verifies that the 4
// cluster-wide ops bypass routing entirely and delegate straight to the base
// storage backend (these touch the meta-FSM via base, not data groups).
func TestClusterCoordinator_CreateBucket_DelegatesToBase(t *testing.T) {
	base := &fakeBackend{}
	c := NewClusterCoordinator(base, nil, nil, nil, "self")
	require.NoError(t, c.CreateBucket("bk1"))
	require.Equal(t, []string{"CreateBucket:bk1"}, base.calls)
}

func TestClusterCoordinator_HeadBucket_DelegatesToBase(t *testing.T) {
	base := &fakeBackend{}
	c := NewClusterCoordinator(base, nil, nil, nil, "self")
	require.NoError(t, c.HeadBucket("bk1"))
	require.Equal(t, []string{"HeadBucket:bk1"}, base.calls)
}

func TestClusterCoordinator_DeleteBucket_DelegatesToBase(t *testing.T) {
	base := &fakeBackend{}
	c := NewClusterCoordinator(base, nil, nil, nil, "self")
	require.NoError(t, c.DeleteBucket("bk1"))
	require.Equal(t, []string{"DeleteBucket:bk1"}, base.calls)
}

func TestClusterCoordinator_DeleteBucket_ChecksRoutedDataGroupBeforeBaseDelete(t *testing.T) {
	base := &fakeBackend{}
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("g1", []string{"peer-a"}))
	router := NewRouter(mgr)
	router.AssignBucket("bk1", "g1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g1": {ID: "g1", PeerIDs: []string{"peer-a"}},
	}}
	d := &recordingDialer{replyByOp: map[raftpb.ForwardOp][]byte{
		raftpb.ForwardOpListObjects: buildObjectsReply("bk1", []*storage.Object{
			{Key: "file.txt", Size: 4},
		}),
	}}
	c := NewClusterCoordinator(base, mgr, router, meta, "self").WithForwardSender(NewForwardSender(d.dial))

	err := c.DeleteBucket("bk1")
	require.ErrorIs(t, err, storage.ErrBucketNotEmpty)
	require.Empty(t, base.calls)
	require.Len(t, d.calls, 1)
	require.Equal(t, raftpb.ForwardOpListObjects, d.calls[0].op)
}

func TestClusterCoordinator_ListBuckets_DelegatesToBase(t *testing.T) {
	base := &fakeBackend{listResult: []string{"a", "b"}}
	c := NewClusterCoordinator(base, nil, nil, nil, "self")
	got, err := c.ListBuckets()
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, got)
	require.Equal(t, []string{"ListBuckets"}, base.calls)
}

type fakeBucketAssignmentSource struct {
	fakeShardGroupSource
	assignments map[string]string
}

func (f *fakeBucketAssignmentSource) BucketAssignments() map[string]string {
	out := make(map[string]string, len(f.assignments))
	for k, v := range f.assignments {
		out[k] = v
	}
	return out
}

func TestClusterCoordinator_ListBuckets_MergesMetaAssignments(t *testing.T) {
	base := &fakeBackend{listResult: []string{"local"}}
	meta := &fakeBucketAssignmentSource{
		assignments: map[string]string{
			"default": "group-0",
			"local":   "group-0",
		},
	}
	c := NewClusterCoordinator(base, nil, nil, meta, "self")

	got, err := c.ListBuckets()
	require.NoError(t, err)
	require.Equal(t, []string{"default", "local"}, got)
	require.Equal(t, []string{"ListBuckets"}, base.calls)
}

func TestClusterCoordinator_HeadBucket_UsesMetaAssignmentWhenBaseIsEmpty(t *testing.T) {
	base := &fakeBackend{headErr: storage.ErrBucketNotFound}
	meta := &fakeBucketAssignmentSource{
		assignments: map[string]string{"default": "group-0"},
	}
	c := NewClusterCoordinator(base, nil, nil, meta, "self")

	require.NoError(t, c.HeadBucket("default"))
	require.Equal(t, []string{"HeadBucket:default"}, base.calls)
}

func TestClusterCoordinator_ListAllObjects_RoutesThroughDataGroup(t *testing.T) {
	base := &fakeBackend{listResult: []string{"photos"}}
	gb := newTestGroupBackend(t, "group-1")
	_, err := gb.PutObject("photos", "a.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"test-node"}},
	}}
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node")

	objs, err := c.ListAllObjects()
	require.NoError(t, err)
	require.Len(t, objs, 1)
	require.Equal(t, storage.SnapshotObject{
		Bucket:      "photos",
		Key:         "a.txt",
		ETag:        objs[0].ETag,
		Size:        5,
		ContentType: "text/plain",
		Modified:    objs[0].Modified,
		VersionID:   objs[0].VersionID,
		IsLatest:    true,
	}, objs[0])
	require.NotEmpty(t, objs[0].ETag)
	require.NotEmpty(t, objs[0].VersionID)
}

func TestClusterCoordinator_WALWriteAtReadAt_RoutesToLocalGroup(t *testing.T) {
	base := &fakeBackend{listResult: []string{storage.NFS4BucketName}}
	gb := newTestGroupBackend(t, "group-1")

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket(storage.NFS4BucketName, "group-1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"test-node"}},
	}}
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node")

	w, err := wal.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	wrapped := wal.NewBackend(c, w)

	obj, err := wrapped.WriteAt(storage.NFS4BucketName, "fio/file.bin", 4, []byte("data"))
	require.NoError(t, err)
	require.Equal(t, int64(8), obj.Size)

	require.NoError(t, wrapped.Truncate(storage.NFS4BucketName, "fio/file.bin", 6))

	buf := make([]byte, 8)
	n, err := wrapped.ReadAt(storage.NFS4BucketName, "fio/file.bin", 0, buf)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 6, n)
	require.Equal(t, []byte{0, 0, 0, 0, 'd', 'a', 0, 0}, buf)
}

func TestClusterCoordinator_RestoreObjects_RemovesDataGroupExtras(t *testing.T) {
	base := &fakeBackend{listResult: []string{"photos"}}
	gb := newTestGroupBackend(t, "group-1")
	_, err := gb.PutObject("photos", "keep.txt", strings.NewReader("keep"), "text/plain")
	require.NoError(t, err)
	_, err = gb.PutObject("photos", "extra.txt", strings.NewReader("extra"), "text/plain")
	require.NoError(t, err)

	current, err := gb.ListObjects("photos", "", 100)
	require.NoError(t, err)
	var keep storage.SnapshotObject
	for _, obj := range current {
		if obj.Key == "keep.txt" {
			keep = storage.SnapshotObject{
				Bucket:      "photos",
				Key:         obj.Key,
				ETag:        obj.ETag,
				Size:        obj.Size,
				ContentType: obj.ContentType,
				Modified:    obj.LastModified,
				VersionID:   obj.VersionID,
				IsLatest:    true,
				ACL:         obj.ACL,
			}
			break
		}
	}
	require.Equal(t, "keep.txt", keep.Key)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"test-node"}},
	}}
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node")

	restored, stale, err := c.RestoreObjects([]storage.SnapshotObject{keep})
	require.NoError(t, err)
	require.Equal(t, 1, restored)
	require.Empty(t, stale)

	objs, err := gb.ListObjects("photos", "", 100)
	require.NoError(t, err)
	require.Len(t, objs, 1)
	require.Equal(t, "keep.txt", objs[0].Key)
}

// TestClusterCoordinator_RouteBucket_NoRouter verifies that routeBucket fails
// fast when the coordinator was constructed without a router (test/legacy
// solo-node configurations). Caller sees a clear error, not a nil panic.
func TestClusterCoordinator_RouteBucket_NoRouter(t *testing.T) {
	base := &fakeBackend{}
	c := NewClusterCoordinator(base, nil, nil, nil, "self")
	_, err := c.routeBucket("bk")
	require.Error(t, err)
}

// TestClusterCoordinator_RouteBucket_UnknownBucket verifies that a bucket
// without a shard-group assignment surfaces as ErrNoSuchBucket — distinct
// from base.ErrBucketNotFound (which fires when the bucket dir is missing).
func TestClusterCoordinator_RouteBucket_UnknownBucket(t *testing.T) {
	base := &fakeBackend{}
	mgr := NewDataGroupManager()
	router := NewRouter(mgr)
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{}}
	c := NewClusterCoordinator(base, mgr, router, meta, "self")
	_, err := c.routeBucket("missing")
	require.ErrorIs(t, err, storage.ErrNoSuchBucket)
}

// TestClusterCoordinator_RouteBucket_SelfIsVoter_NotLeader verifies that when
// self is in the peer list but not the leader (no GroupBackend wired to make
// it a leader), routeBucket returns selfIsVoter=true, selfIsLeader=false.
// This is the path that forwards to a peer rather than calling local backend.
func TestClusterCoordinator_RouteBucket_SelfIsVoter_NotLeader(t *testing.T) {
	base := &fakeBackend{}
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-1", []string{"a", "self", "b"})) // no backend
	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"a", "self", "b"}},
	}}
	c := NewClusterCoordinator(base, mgr, router, meta, "self")
	target, err := c.routeBucket("photos")
	require.NoError(t, err)
	require.Equal(t, "group-1", target.groupID)
	require.True(t, target.selfIsVoter)
	require.False(t, target.selfIsLeader)
	// PeersForForward order: non-self first, self last.
	require.Equal(t, []string{"a", "b", "self"}, target.peers)
}

func TestClusterCoordinator_RouteBucket_ResolvesNodeIDPeersToAddresses(t *testing.T) {
	base := &fakeBackend{}
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-1", []string{"node-a", "self", "node-b"}))
	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")
	meta := NewMetaFSM()
	require.NoError(t, meta.applyCmd(makeAddNodeCmd(t, "node-a", "10.0.0.1:7001", 0)))
	require.NoError(t, meta.applyCmd(makeAddNodeCmd(t, "node-b", "10.0.0.2:7001", 0)))
	require.NoError(t, meta.applyCmd(makeAddNodeCmd(t, "self", "10.0.0.3:7001", 0)))
	require.NoError(t, meta.applyCmd(makePutShardGroupCmd(t, "group-1", []string{"node-a", "self", "node-b"})))

	c := NewClusterCoordinator(base, mgr, router, meta, "self").WithNodeAddressResolver(meta)
	target, err := c.routeBucket("photos")
	require.NoError(t, err)
	require.Equal(t, []string{"10.0.0.1:7001", "10.0.0.2:7001", "10.0.0.3:7001"}, target.peers)
	require.True(t, target.selfIsVoter)
}

type countingAddressBook struct {
	calls int
}

func (b *countingAddressBook) ResolveNodeAddress(idOrAddr string) (string, bool) {
	b.calls++
	return idOrAddr, true
}

func (b *countingAddressBook) Nodes() []MetaNodeEntry {
	b.calls++
	return nil
}

func TestClusterCoordinator_RouteBucket_LocalLeaderSkipsPeerResolution(t *testing.T) {
	base := &fakeBackend{}
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"node-a", "self", "node-b"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"node-a", "self", "node-b"}},
	}}
	addr := &countingAddressBook{}
	c := NewClusterCoordinator(base, mgr, router, meta, "self").WithNodeAddressResolver(addr)

	target, err := c.routeBucket("photos")
	require.NoError(t, err)
	require.True(t, target.selfIsVoter)
	require.True(t, target.selfIsLeader)
	require.Zero(t, addr.calls, "local leader fast path should not allocate/resolve forward peers")
	require.Empty(t, target.peers)

	allocs := testing.AllocsPerRun(100, func() {
		target, err := c.routeBucket("photos")
		if err != nil {
			t.Fatalf("routeBucket failed: %v", err)
		}
		if !target.selfIsLeader {
			t.Fatalf("routeBucket lost local leader fast path")
		}
	})
	require.Zero(t, allocs, "local leader route should not allocate")
}

func TestClusterCoordinator_RouteBucket_MetaFSMLocalLeaderAvoidsShardGroupCopy(t *testing.T) {
	base := &fakeBackend{}
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"node-a", "self", "node-b"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")
	meta := NewMetaFSM()
	require.NoError(t, meta.applyCmd(makePutShardGroupCmd(t, "group-1", []string{"node-a", "self", "node-b"})))
	c := NewClusterCoordinator(base, mgr, router, meta, "self")

	target, err := c.routeBucket("photos")
	require.NoError(t, err)
	require.True(t, target.selfIsLeader)

	allocs := testing.AllocsPerRun(100, func() {
		target, err := c.routeBucket("photos")
		if err != nil {
			t.Fatalf("routeBucket failed: %v", err)
		}
		if !target.selfIsLeader {
			t.Fatalf("routeBucket lost local leader fast path")
		}
	})
	require.Zero(t, allocs, "MetaFSM local leader route should not copy peer slices")
}

// --- T6 forward-path test scaffolding ---

// recordingDialer captures every (peer, payload) pair the ForwardSender hands
// it and returns a canned reply. The op-specific reply bytes are provided by
// the caller via replyByOp; missing op → buildErrorReply(Internal).
type recordingDialer struct {
	calls         []dialerCall
	streamCalls   []dialerCall
	replyByOp     map[raftpb.ForwardOp][]byte
	streamReplyBy map[raftpb.ForwardOp][]byte
	defaultErr    error
}

type dialerCall struct {
	peer  string
	op    raftpb.ForwardOp
	gid   string
	args  []byte
	rawly []byte // raw payload (decoded inside the test if asserting)
}

func (d *recordingDialer) dial(ctx context.Context, peer string, payload []byte) ([]byte, error) {
	if d.defaultErr != nil {
		return nil, d.defaultErr
	}
	gid, op, args, err := decodeForwardPayload(payload)
	if err != nil {
		return nil, err
	}
	// copy args because they alias payload, which the sender may reuse.
	argsCopy := make([]byte, len(args))
	copy(argsCopy, args)
	d.calls = append(d.calls, dialerCall{peer: peer, op: op, gid: gid, args: argsCopy, rawly: payload})
	if reply, ok := d.replyByOp[op]; ok {
		return reply, nil
	}
	return buildSimpleReply(raftpb.ForwardStatusInternal, ""), nil
}

func (d *recordingDialer) stream(ctx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
	if d.defaultErr != nil {
		return nil, d.defaultErr
	}
	gid, op, args, err := decodeForwardPayload(payload)
	if err != nil {
		return nil, err
	}
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	argsCopy := make([]byte, len(args))
	copy(argsCopy, args)
	d.streamCalls = append(d.streamCalls, dialerCall{peer: peer, op: op, gid: gid, args: argsCopy, rawly: bodyBytes})
	if reply, ok := d.streamReplyBy[op]; ok {
		return reply, nil
	}
	if reply, ok := d.replyByOp[op]; ok {
		return reply, nil
	}
	return buildSimpleReply(raftpb.ForwardStatusInternal, ""), nil
}

// setupCoordWithForward builds a coordinator wired to a recording dialer for
// a single bucket → group mapping. self is NOT a voter so all calls forward.
func setupCoordWithForward(t *testing.T, bucket, groupID string, peers []string) (*ClusterCoordinator, *recordingDialer) {
	t.Helper()
	base := &fakeBackend{}
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup(groupID, peers))
	router := NewRouter(mgr)
	router.AssignBucket(bucket, groupID)
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		groupID: {ID: groupID, PeerIDs: peers},
	}}
	d := &recordingDialer{replyByOp: map[raftpb.ForwardOp][]byte{}, streamReplyBy: map[raftpb.ForwardOp][]byte{}}
	sender := NewForwardSender(d.dial)
	c := NewClusterCoordinator(base, mgr, router, meta, "self").WithForwardSender(sender)
	return c, d
}

// TestClusterCoordinator_GetObject_Forward verifies the GetObject routing
// path: routeBucket → ForwardSender.Send → objectFromReply. body is embedded
// inside ForwardReply.read_body.
func TestClusterCoordinator_GetObject_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a", "b"})
	d.replyByOp[raftpb.ForwardOpGetObject] = buildGetObjectReply(
		&storage.Object{Key: "k", Size: 5, ETag: "etag", ContentType: "text/plain"},
		"bk", []byte("hello"),
	)

	rc, obj, err := c.GetObject("bk", "k")
	require.NoError(t, err)
	require.Equal(t, int64(5), obj.Size)
	require.Equal(t, "etag", obj.ETag)
	body, _ := io.ReadAll(rc)
	rc.Close()
	require.Equal(t, []byte("hello"), body)
	require.Len(t, d.calls, 1)
	require.Equal(t, raftpb.ForwardOpGetObject, d.calls[0].op)
	require.Equal(t, "g1", d.calls[0].gid)
}

func TestClusterCoordinator_GetObject_ForwardRejectsSizeMismatch(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpGetObject] = buildGetObjectReply(
		&storage.Object{Key: "k", Size: 5, ETag: "etag", ContentType: "text/plain"},
		"bk", []byte{},
	)

	_, _, err := c.GetObject("bk", "k")
	require.ErrorIs(t, err, ErrForwardBodySizeMismatch)
}

func TestClusterCoordinator_GetObject_Forward_AboveLegacyCap(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	body := bytes.Repeat([]byte("g"), DefaultMaxForwardBodyBytes+1024)
	d.replyByOp[raftpb.ForwardOpGetObject] = buildGetObjectReply(
		&storage.Object{Key: "large", Size: int64(len(body)), ETag: "etag-large", ContentType: "application/octet-stream"},
		"bk", body,
	)

	rc, obj, err := c.GetObject("bk", "large")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Equal(t, body, got)
}

func TestClusterCoordinator_VersionedOps_LocalLeader(t *testing.T) {
	base := &fakeBackend{}
	gb := newTestGroupBackend(t, "group-1")

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("bk", "group-1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"test-node"}},
	}}
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node")

	v1, err := c.PutObject("bk", "k", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	v2, err := c.PutObject("bk", "k", strings.NewReader("v2"), "text/plain")
	require.NoError(t, err)
	require.NotEqual(t, v1.VersionID, v2.VersionID)

	rc, gotV1, err := c.GetObjectVersion("bk", "k", v1.VersionID)
	require.NoError(t, err)
	body, err := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, err)
	require.Equal(t, v1.VersionID, gotV1.VersionID)
	require.Equal(t, "v1", string(body))

	versions, err := c.ListObjectVersions("bk", "", 100)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	require.Equal(t, v2.VersionID, versions[0].VersionID)

	markerID, err := c.DeleteObjectReturningMarker("bk", "k")
	require.NoError(t, err)
	require.NotEmpty(t, markerID)

	versions, err = c.ListObjectVersions("bk", "", 100)
	require.NoError(t, err)
	require.Len(t, versions, 3)
	require.True(t, versions[0].IsDeleteMarker)

	require.NoError(t, c.DeleteObjectVersion("bk", "k", v1.VersionID))
	versions, err = c.ListObjectVersions("bk", "", 100)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	for _, v := range versions {
		require.NotEqual(t, v1.VersionID, v.VersionID)
	}
}

func TestClusterCoordinator_GetObjectVersion_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpGetObjectVersion] = buildGetObjectReply(
		&storage.Object{Key: "k", Size: 2, ETag: "etag-v1", ContentType: "text/plain", VersionID: "vid-1"},
		"bk", []byte("v1"),
	)

	rc, obj, err := c.GetObjectVersion("bk", "k", "vid-1")
	require.NoError(t, err)
	body, readErr := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, readErr)
	require.Equal(t, "vid-1", obj.VersionID)
	require.Equal(t, []byte("v1"), body)
	require.Len(t, d.calls, 1)
	require.Equal(t, raftpb.ForwardOpGetObjectVersion, d.calls[0].op)
	args := raftpb.GetRootAsGetObjectVersionArgs(d.calls[0].args, 0)
	require.Equal(t, "bk", string(args.Bucket()))
	require.Equal(t, "k", string(args.Key()))
	require.Equal(t, "vid-1", string(args.VersionId()))
}

func TestClusterCoordinator_DeleteObjectVersion_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpDeleteObjectVersion] = buildOKReply()

	require.NoError(t, c.DeleteObjectVersion("bk", "k", "vid-1"))
	require.Len(t, d.calls, 1)
	require.Equal(t, raftpb.ForwardOpDeleteObjectVersion, d.calls[0].op)
	args := raftpb.GetRootAsDeleteObjectVersionArgs(d.calls[0].args, 0)
	require.Equal(t, "bk", string(args.Bucket()))
	require.Equal(t, "k", string(args.Key()))
	require.Equal(t, "vid-1", string(args.VersionId()))
}

func TestClusterCoordinator_ListObjectVersions_ForwardPreservesVersionFlags(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpListObjectVersions] = buildObjectVersionsReply([]*storage.ObjectVersion{
		{
			Key:            "k",
			VersionID:      "vid-delete",
			IsLatest:       true,
			IsDeleteMarker: true,
			ETag:           "delete-marker",
			Size:           0,
			LastModified:   1234,
		},
		{
			Key:          "k",
			VersionID:    "vid-live",
			ETag:         "etag-live",
			Size:         2,
			LastModified: 1200,
		},
	})

	versions, err := c.ListObjectVersions("bk", "k", 100)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	require.True(t, versions[0].IsLatest)
	require.True(t, versions[0].IsDeleteMarker)
	require.False(t, versions[1].IsLatest)
	require.False(t, versions[1].IsDeleteMarker)
	require.Equal(t, raftpb.ForwardOpListObjectVersions, d.calls[0].op)
	args := raftpb.GetRootAsListObjectVersionsArgs(d.calls[0].args, 0)
	require.Equal(t, "bk", string(args.Bucket()))
	require.Equal(t, "k", string(args.Prefix()))
	require.Equal(t, int32(100), args.MaxKeys())
}

func TestClusterCoordinator_HeadObject_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpHeadObject] = buildObjectReply(
		&storage.Object{Key: "k", Size: 99, ETag: "etag-x"}, "bk",
	)
	obj, err := c.HeadObject("bk", "k")
	require.NoError(t, err)
	require.Equal(t, int64(99), obj.Size)
	require.Equal(t, raftpb.ForwardOpHeadObject, d.calls[0].op)
}

func TestClusterCoordinator_DeleteObject_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpDeleteObject] = buildOKReply()
	require.NoError(t, c.DeleteObject("bk", "k"))
	require.Equal(t, raftpb.ForwardOpDeleteObject, d.calls[0].op)
}

func TestClusterCoordinator_DeleteObjectReturningMarker_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpDeleteObject] = buildObjectReply(
		&storage.Object{Key: "k", VersionID: "delete-marker-1"}, "bk",
	)

	markerID, err := c.DeleteObjectReturningMarker("bk", "k")
	require.NoError(t, err)
	require.Equal(t, "delete-marker-1", markerID)
	require.Equal(t, raftpb.ForwardOpDeleteObject, d.calls[0].op)
}

func TestClusterCoordinator_ListObjects_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpListObjects] = buildObjectsReply("bk", []*storage.Object{
		{Key: "k1", Size: 1},
		{Key: "k2", Size: 2},
	})
	out, err := c.ListObjects("bk", "p/", 100)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, "k1", out[0].Key)
	require.Equal(t, raftpb.ForwardOpListObjects, d.calls[0].op)
}

func TestClusterCoordinator_WalkObjects_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpWalkObjects] = buildObjectsReply("bk", []*storage.Object{
		{Key: "a1"}, {Key: "a2"}, {Key: "a3"},
	})
	var seen []string
	require.NoError(t, c.WalkObjects("bk", "a", func(o *storage.Object) error {
		seen = append(seen, o.Key)
		return nil
	}))
	require.Equal(t, []string{"a1", "a2", "a3"}, seen)
	require.Equal(t, raftpb.ForwardOpWalkObjects, d.calls[0].op)
}

func TestClusterCoordinator_WalkObjects_FnError_Stops(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpWalkObjects] = buildObjectsReply("bk", []*storage.Object{
		{Key: "a1"}, {Key: "a2"},
	})
	stopErr := errors.New("stop")
	err := c.WalkObjects("bk", "a", func(o *storage.Object) error {
		return stopErr
	})
	require.ErrorIs(t, err, stopErr)
}

func TestClusterCoordinator_CreateMultipartUpload_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpCreateMultipartUpload] = buildUploadReply("bk", "k", "upload-1")
	up, err := c.CreateMultipartUpload("bk", "k", "text/plain")
	require.NoError(t, err)
	require.Equal(t, "upload-1", up.UploadID)
	require.Equal(t, raftpb.ForwardOpCreateMultipartUpload, d.calls[0].op)
}

func TestClusterCoordinator_CompleteMultipartUpload_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpCompleteMultipartUpload] = buildObjectReply(
		&storage.Object{Key: "k", Size: 1024, ETag: "merged-etag"}, "bk",
	)
	obj, err := c.CompleteMultipartUpload("bk", "k", "upload-1", []storage.Part{
		{PartNumber: 1, ETag: "p1"}, {PartNumber: 2, ETag: "p2"},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1024), obj.Size)
	require.Equal(t, "merged-etag", obj.ETag)
	require.Equal(t, raftpb.ForwardOpCompleteMultipartUpload, d.calls[0].op)
}

func TestClusterCoordinator_AbortMultipartUpload_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpAbortMultipartUpload] = buildOKReply()
	require.NoError(t, c.AbortMultipartUpload("bk", "k", "upload-1"))
	require.Equal(t, raftpb.ForwardOpAbortMultipartUpload, d.calls[0].op)
}

// TestClusterCoordinator_GetObject_NoSuchBucketStatus verifies that a server-
// side NoSuchBucket reply gets surfaced as storage.ErrNoSuchBucket — the
// status code conversion is the contract S3 handlers depend on for 404 vs 500.
func TestClusterCoordinator_GetObject_NoSuchBucketStatus(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpGetObject] = buildSimpleReply(raftpb.ForwardStatusNoSuchBucket, "")
	_, _, err := c.GetObject("bk", "k")
	require.ErrorIs(t, err, storage.ErrNoSuchBucket)
}

// --- T7 PutObject + UploadPart ---

// TestClusterCoordinator_PutObject_Forward verifies the full PutObject path
// with a sub-cap body — body bytes ride inside FBS args, single-message wire.
func TestClusterCoordinator_PutObject_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a", "self"})
	body := bytes.Repeat([]byte("x"), 4*1024*1024) // 4 MB, under cap
	d.replyByOp[raftpb.ForwardOpPutObject] = buildObjectReply(
		&storage.Object{Key: "k", Size: int64(len(body)), ETag: "etag-put"}, "bk",
	)
	obj, err := c.PutObject("bk", "k", bytes.NewReader(body), "application/octet-stream")
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Equal(t, raftpb.ForwardOpPutObject, d.calls[0].op)
}

func TestClusterCoordinator_PutObject_ForwardRejectsSizeMismatch(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	body := []byte("non-empty-body")
	d.replyByOp[raftpb.ForwardOpPutObject] = buildObjectReply(
		&storage.Object{Key: "k", Size: 0, ETag: "etag-empty"}, "bk",
	)

	_, err := c.PutObject("bk", "k", bytes.NewReader(body), "application/octet-stream")
	require.ErrorIs(t, err, ErrForwardBodySizeMismatch)
}

// TestClusterCoordinator_PutObject_TooLarge_413 verifies the 5 MB hard cap is
// enforced BEFORE encoding/forwarding — caller sees ErrEntityTooLarge without
// any wire activity (recordingDialer recorded no calls).
func TestClusterCoordinator_PutObject_TooLarge_413(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	body := bytes.Repeat([]byte("x"), 6*1024*1024) // 6 MB, over cap
	_, err := c.PutObject("bk", "k", bytes.NewReader(body), "")
	require.ErrorIs(t, err, storage.ErrEntityTooLarge)
	require.Empty(t, d.calls)
}

// TestClusterCoordinator_UploadPart_Forward — happy path under cap.
func TestClusterCoordinator_UploadPart_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	body := bytes.Repeat([]byte("y"), 1024)
	d.replyByOp[raftpb.ForwardOpUploadPart] = buildPartReply(
		&storage.Part{PartNumber: 7, ETag: "etag-part", Size: int64(len(body))},
	)
	p, err := c.UploadPart("bk", "k", "uid", 7, bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, 7, p.PartNumber)
	require.Equal(t, "etag-part", p.ETag)
	require.Equal(t, raftpb.ForwardOpUploadPart, d.calls[0].op)
}

func TestClusterCoordinator_UploadPart_ForwardRejectsSizeMismatch(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	body := []byte("part-body")
	d.replyByOp[raftpb.ForwardOpUploadPart] = buildPartReply(
		&storage.Part{PartNumber: 7, ETag: "etag-part", Size: 0},
	)

	_, err := c.UploadPart("bk", "k", "uid", 7, bytes.NewReader(body))
	require.ErrorIs(t, err, ErrForwardBodySizeMismatch)
}

// TestClusterCoordinator_UploadPart_5MB_Cap — same enforcement on multipart.
func TestClusterCoordinator_UploadPart_5MB_Cap(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	body := bytes.Repeat([]byte("x"), 6*1024*1024)
	_, err := c.UploadPart("bk", "k", "uid", 1, bytes.NewReader(body))
	require.ErrorIs(t, err, storage.ErrEntityTooLarge)
	require.Empty(t, d.calls)
}

func TestClusterCoordinator_PutObject_StreamForward_AboveLegacyCap(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	c.forward.WithStreamDialer(d.stream)
	body := bytes.Repeat([]byte("z"), DefaultMaxForwardBodyBytes+1024)
	d.streamReplyBy[raftpb.ForwardOpPutObject] = buildObjectReply(
		&storage.Object{Key: "k", Size: int64(len(body)), ETag: "etag-stream"}, "bk",
	)

	obj, err := c.PutObject("bk", "k", bytes.NewReader(body), "application/octet-stream")
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Len(t, d.calls, 1, "streamed PutObject should only use single-message preflight")
	require.Equal(t, raftpb.ForwardOpHeadObject, d.calls[0].op)
	require.Len(t, d.streamCalls, 1)
	require.Equal(t, body, d.streamCalls[0].rawly)
	require.Equal(t, raftpb.ForwardOpPutObject, d.streamCalls[0].op)

	args := raftpb.GetRootAsPutObjectArgs(d.streamCalls[0].args, 0)
	require.Zero(t, args.BodyLength(), "stream metadata must not embed the object body")
}

func TestClusterCoordinator_PutObject_StreamDialerSmallBodyUsesSingleMessage(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	c.forward.WithStreamDialer(d.stream)
	body := []byte("small-forward-body")
	d.replyByOp[raftpb.ForwardOpPutObject] = buildObjectReply(
		&storage.Object{Key: "k", Size: int64(len(body)), ETag: "etag-put"}, "bk",
	)

	obj, err := c.PutObject("bk", "k", bytes.NewReader(body), "application/octet-stream")
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Empty(t, d.streamCalls)
	require.Len(t, d.calls, 1)
	require.Equal(t, raftpb.ForwardOpPutObject, d.calls[0].op)

	args := raftpb.GetRootAsPutObjectArgs(d.calls[0].args, 0)
	require.Equal(t, body, args.BodyBytes())
}

func TestClusterCoordinator_UploadPart_StreamForward_AboveLegacyCap(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	c.forward.WithStreamDialer(d.stream)
	body := bytes.Repeat([]byte("p"), DefaultMaxForwardBodyBytes+1024)
	d.streamReplyBy[raftpb.ForwardOpUploadPart] = buildPartReply(
		&storage.Part{PartNumber: 1, ETag: "etag-part", Size: int64(len(body))},
	)

	part, err := c.UploadPart("bk", "k", "uid", 1, bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), part.Size)
	require.Len(t, d.calls, 1, "streamed UploadPart should only use single-message preflight")
	require.Equal(t, raftpb.ForwardOpHeadObject, d.calls[0].op)
	require.Len(t, d.streamCalls, 1)
	require.Equal(t, body, d.streamCalls[0].rawly)

	args := raftpb.GetRootAsUploadPartArgs(d.streamCalls[0].args, 0)
	require.Zero(t, args.BodyLength(), "stream metadata must not embed the part body")
}

func TestClusterCoordinator_UploadPart_StreamDialerSmallBodyUsesSingleMessage(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	c.forward.WithStreamDialer(d.stream)
	body := []byte("small-part-body")
	d.replyByOp[raftpb.ForwardOpUploadPart] = buildPartReply(
		&storage.Part{PartNumber: 1, ETag: "etag-part", Size: int64(len(body))},
	)

	part, err := c.UploadPart("bk", "k", "uid", 1, bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), part.Size)
	require.Empty(t, d.streamCalls)
	require.Len(t, d.calls, 1)
	require.Equal(t, raftpb.ForwardOpUploadPart, d.calls[0].op)

	args := raftpb.GetRootAsUploadPartArgs(d.calls[0].args, 0)
	require.Equal(t, body, args.BodyBytes())
}
