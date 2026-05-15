package cluster

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/raft"
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

func TestClusterCoordinatorSelfPeerAlias(t *testing.T) {
	c := NewClusterCoordinator(nil, nil, nil, nil, "node-a").WithSelfPeerAlias("127.0.0.1:9001")

	require.True(t, c.matchSelfPeer("node-a"))
	require.True(t, c.matchSelfPeer("127.0.0.1:9001"))
	require.False(t, c.matchSelfPeer("node-b"))

	peers := NewShardGroupPeerSet(ShardGroupEntry{
		ID:      "group-1",
		PeerIDs: []string{"127.0.0.1:9001", "127.0.0.1:9002", "node-a"},
	}).ForwardOrder(c.selfID, c.selfAliases...)
	require.Equal(t, []string{"127.0.0.1:9002", "127.0.0.1:9001", "node-a"}, peers)
}

func TestClusterCoordinator_WithECConfigConcurrentRouting(t *testing.T) {
	c := NewClusterCoordinator(&fakeBackend{}, nil, nil, nil, "self")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			c.WithECConfig(AutoECConfigForClusterSize(3 + i%3))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_, _, _ = c.routeWriteOrBucket("bucket", "key")
			_, _ = c.routeReadOrBucket("bucket", "key", "")
		}
	}()
	wg.Wait()
}

func (f *fakeBackend) record(call string) { f.calls = append(f.calls, call) }

func (f *fakeBackend) CreateBucket(ctx context.Context, bucket string) error {
	_ = ctx
	f.record(fmt.Sprintf("CreateBucket:%s", bucket))
	return f.createErr
}
func (f *fakeBackend) HeadBucket(ctx context.Context, bucket string) error {
	_ = ctx
	f.record(fmt.Sprintf("HeadBucket:%s", bucket))
	return f.headErr
}
func (f *fakeBackend) DeleteBucket(ctx context.Context, bucket string) error {
	_ = ctx
	f.record(fmt.Sprintf("DeleteBucket:%s", bucket))
	return f.deleteErr
}
func (f *fakeBackend) ForceDeleteBucket(ctx context.Context, bucket string) error {
	_ = ctx
	f.record(fmt.Sprintf("ForceDeleteBucket:%s", bucket))
	return f.deleteErr
}
func (f *fakeBackend) ListBuckets(ctx context.Context) ([]string, error) {
	_ = ctx
	f.record("ListBuckets")
	return f.listResult, f.listErr
}

// Bucket-scoped methods — unused in T5; T6 routing tests use a different fake.
func (f *fakeBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	_ = ctx
	return nil, fmt.Errorf("fakeBackend.PutObject not implemented")
}
func (f *fakeBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	_ = ctx
	return nil, nil, fmt.Errorf("fakeBackend.GetObject not implemented")
}
func (f *fakeBackend) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	_ = ctx
	return nil, fmt.Errorf("fakeBackend.HeadObject not implemented")
}
func (f *fakeBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	_ = ctx
	return fmt.Errorf("fakeBackend.DeleteObject not implemented")
}
func (f *fakeBackend) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	_ = ctx
	return nil, fmt.Errorf("fakeBackend.ListObjects not implemented")
}
func (f *fakeBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	_ = ctx
	return fmt.Errorf("fakeBackend.WalkObjects not implemented")
}
func (f *fakeBackend) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*storage.MultipartUpload, error) {
	_ = ctx
	return nil, fmt.Errorf("fakeBackend.CreateMultipartUpload not implemented")
}
func (f *fakeBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader) (*storage.Part, error) {
	_ = ctx
	return nil, fmt.Errorf("fakeBackend.UploadPart not implemented")
}
func (f *fakeBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	_ = ctx
	return nil, fmt.Errorf("fakeBackend.CompleteMultipartUpload not implemented")
}
func (f *fakeBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	_ = ctx
	return fmt.Errorf("fakeBackend.AbortMultipartUpload not implemented")
}
func (f *fakeBackend) ListMultipartUploads(ctx context.Context, bucket, prefix string, maxUploads int) ([]*storage.MultipartUpload, error) {
	_ = ctx
	return nil, fmt.Errorf("fakeBackend.ListMultipartUploads not implemented")
}
func (f *fakeBackend) ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]storage.Part, error) {
	_ = ctx
	return nil, fmt.Errorf("fakeBackend.ListParts not implemented")
}

// TestClusterCoordinator_CreateBucket_DelegatesToBase verifies that the 4
// cluster-wide ops bypass routing entirely and delegate straight to the base
// storage backend (these touch the meta-FSM via base, not data groups).
func TestClusterCoordinator_CreateBucket_DelegatesToBase(t *testing.T) {
	base := &fakeBackend{}
	c := NewClusterCoordinator(base, nil, nil, nil, "self")
	require.NoError(t, c.CreateBucket(context.Background(), "bk1"))
	require.Equal(t, []string{"CreateBucket:bk1"}, base.calls)
}

func TestClusterCoordinator_HeadBucket_DelegatesToBase(t *testing.T) {
	base := &fakeBackend{}
	c := NewClusterCoordinator(base, nil, nil, nil, "self")
	require.NoError(t, c.HeadBucket(context.Background(), "bk1"))
	require.Equal(t, []string{"HeadBucket:bk1"}, base.calls)
}

func TestClusterCoordinator_DeleteBucket_DelegatesToBase(t *testing.T) {
	base := &fakeBackend{}
	c := NewClusterCoordinator(base, nil, nil, nil, "self")
	require.NoError(t, c.DeleteBucket(context.Background(), "bk1"))
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

	err := c.DeleteBucket(context.Background(), "bk1")
	require.ErrorIs(t, err, storage.ErrBucketNotEmpty)
	require.Empty(t, base.calls)
	require.Len(t, d.calls, 1)
	require.Equal(t, raftpb.ForwardOpListObjects, d.calls[0].op)
}

func TestClusterCoordinator_DeleteBucket_UsesLocalSingletonGroupBeforeForward(t *testing.T) {
	base := &fakeBackend{}
	gb := newTestFollowerGroupBackend(t, "g1", "self")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("g1", []string{"self"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("empty-bucket", "g1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g1": {ID: "g1", PeerIDs: []string{"self"}},
	}}
	d := &recordingDialer{defaultErr: ErrNoReachablePeer}
	c := NewClusterCoordinator(base, mgr, router, meta, "self").
		WithForwardSender(NewForwardSender(d.dial))

	require.NoError(t, c.DeleteBucket(context.Background(), "empty-bucket"))
	require.Equal(t, []string{"DeleteBucket:empty-bucket"}, base.calls)
	require.Empty(t, d.calls)
}

func TestClusterCoordinator_PutObject_WaitsForLocalSingletonLeaderBeforeForward(t *testing.T) {
	base := &fakeBackend{}
	gb := newTestFollowerGroupBackend(t, "g1", "self")
	stopApply := make(chan struct{})
	go gb.RunApplyLoop(stopApply)
	t.Cleanup(func() { close(stopApply) })
	go func() {
		time.Sleep(50 * time.Millisecond)
		gb.Node().Start()
	}()
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("g1", []string{"self"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("write-bucket", "g1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g1": {ID: "g1", PeerIDs: []string{"self"}},
	}}
	d := &recordingDialer{defaultErr: ErrNoReachablePeer}
	c := NewClusterCoordinator(base, mgr, router, meta, "self").
		WithForwardSender(NewForwardSender(d.dial))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	obj, err := c.PutObject(ctx, "write-bucket", "key", strings.NewReader("body"), "text/plain")
	require.NoError(t, err)
	require.Equal(t, int64(4), obj.Size)
	require.Empty(t, d.calls)
}

func TestClusterCoordinator_PutObject_RejectsMissingBucketBeforeGroupWrite(t *testing.T) {
	base := &fakeBackend{headErr: storage.ErrBucketNotFound}
	gb := newTestFollowerGroupBackend(t, "g1", "self")
	gb.Node().Start()
	stopApply := make(chan struct{})
	go gb.RunApplyLoop(stopApply)
	t.Cleanup(func() { close(stopApply) })

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("g1", []string{"self"}, gb))
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g1": {ID: "g1", PeerIDs: []string{"self"}},
	}}
	c := NewClusterCoordinator(base, mgr, NewRouter(mgr), meta, "self").
		WithECConfig(ECConfig{DataShards: 1, ParityShards: 0}).
		WithObjectIndexProposer(noopObjectIndexProposer{})

	_, err := c.PutObject(context.Background(), "missing-bucket", "key", strings.NewReader("body"), "text/plain")

	require.ErrorIs(t, err, storage.ErrBucketNotFound)
	require.Equal(t, []string{"HeadBucket:missing-bucket"}, base.calls)
	_, _, getErr := gb.GetObject(context.Background(), "missing-bucket", "key")
	require.ErrorIs(t, getErr, storage.ErrObjectNotFound)
}

func TestClusterCoordinator_PutObject_AllowsMetaAssignedBucketBeforeLocalBucketRow(t *testing.T) {
	base := &fakeBackend{headErr: storage.ErrBucketNotFound}
	gb := newTestFollowerGroupBackend(t, "g1", "self")
	gb.Node().Start()
	stopApply := make(chan struct{})
	go gb.RunApplyLoop(stopApply)
	t.Cleanup(func() { close(stopApply) })

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("g1", []string{"self"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("assigned-bucket", "g1")
	meta := &fakeBucketAssignmentSource{
		fakeShardGroupSource: fakeShardGroupSource{groups: map[string]ShardGroupEntry{
			"g1": {ID: "g1", PeerIDs: []string{"self"}},
		}},
		assignments: map[string]string{"assigned-bucket": "g1"},
	}
	c := NewClusterCoordinator(base, mgr, router, meta, "self").
		WithECConfig(ECConfig{DataShards: 1, ParityShards: 0}).
		WithObjectIndexProposer(noopObjectIndexProposer{})

	obj, err := c.PutObject(context.Background(), "assigned-bucket", "key", strings.NewReader("body"), "text/plain")

	require.NoError(t, err)
	require.Equal(t, int64(4), obj.Size)
	require.Equal(t, []string{"HeadBucket:assigned-bucket"}, base.calls)
}

func TestClusterCoordinator_PutObjectWithACLThroughWALRoutesToLocalGroup(t *testing.T) {
	base := &fakeBackend{}
	gb := newTestFollowerGroupBackend(t, "g1", "self")
	stopApply := make(chan struct{})
	go gb.RunApplyLoop(stopApply)
	t.Cleanup(func() { close(stopApply) })
	gb.Node().Start()

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("g1", []string{"self"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("write-bucket", "g1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g1": {ID: "g1", PeerIDs: []string{"self"}},
	}}
	d := &recordingDialer{defaultErr: ErrNoReachablePeer}
	c := NewClusterCoordinator(base, mgr, router, meta, "self").
		WithForwardSender(NewForwardSender(d.dial))
	w, err := wal.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	ops := storage.NewOperations(wal.NewBackend(c, w))

	obj, err := ops.PutObjectWithACL(context.Background(), "write-bucket", "key", strings.NewReader("body"), "text/plain", 7)

	require.NoError(t, err)
	require.Equal(t, uint8(7), obj.ACL)
	require.Empty(t, d.calls)
	require.Eventually(t, func() bool {
		head, err := gb.HeadObject(context.Background(), "write-bucket", "key")
		return err == nil && head.ACL == 7
	}, time.Second, 10*time.Millisecond)
}

type emptyObjectIndexMeta struct {
	fakeShardGroupSource
}

func (m *emptyObjectIndexMeta) ObjectIndexLatest(bucket, key string) (ObjectIndexEntry, bool) {
	return ObjectIndexEntry{}, false
}

func (m *emptyObjectIndexMeta) ObjectIndexVersion(bucket, key, versionID string) (ObjectIndexEntry, bool) {
	return ObjectIndexEntry{}, false
}

func TestClusterCoordinator_DeleteObject_MissingObjectIsIdempotentWhenBucketExists(t *testing.T) {
	base := &fakeBackend{}
	gb := newTestFollowerGroupBackend(t, "g1", "self")
	gb.Node().Start()
	stopApply := make(chan struct{})
	go gb.RunApplyLoop(stopApply)
	t.Cleanup(func() { close(stopApply) })

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("g1", []string{"self"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("existing-bucket", "g1")
	meta := &emptyObjectIndexMeta{fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g1": {ID: "g1", PeerIDs: []string{"self"}},
	}}}
	proposer := &recordingObjectIndexProposer{}
	c := NewClusterCoordinator(base, mgr, router, meta, "self").
		WithObjectIndexProposer(proposer)

	markerID, err := c.DeleteObjectReturningMarker("existing-bucket", "missing-key")

	require.NoError(t, err)
	require.NotEmpty(t, markerID)
	require.Equal(t, []string{"HeadBucket:existing-bucket"}, base.calls)
	require.Len(t, proposer.entries, 1)
	require.True(t, proposer.entries[0].IsDeleteMarker)
	require.Equal(t, "missing-key", proposer.entries[0].Key)
}

func TestClusterCoordinator_HeadObject_UsesLocalSingletonVoterReadBeforeForward(t *testing.T) {
	base := &fakeBackend{}
	gb := newTestFollowerGroupBackend(t, "g1", "self")
	metaBytes, err := marshalObjectMeta(objectMeta{
		Key:          "key",
		Size:         4,
		ContentType:  "text/plain",
		ETag:         "etag",
		LastModified: time.Now().Unix(),
	})
	require.NoError(t, err)
	require.NoError(t, gb.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(bucketKey("read-bucket"), []byte{1}); err != nil {
			return err
		}
		return txn.Set(objectMetaKey("read-bucket", "key"), metaBytes)
	}))
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("g1", []string{"self"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("read-bucket", "g1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"g1": {ID: "g1", PeerIDs: []string{"self"}},
	}}
	d := &recordingDialer{defaultErr: ErrNoReachablePeer}
	c := NewClusterCoordinator(base, mgr, router, meta, "self").
		WithForwardSender(NewForwardSender(d.dial))

	obj, err := c.HeadObject(context.Background(), "read-bucket", "key")
	require.NoError(t, err)
	require.Equal(t, int64(4), obj.Size)
	require.Empty(t, d.calls)
}

func newTestFollowerGroupBackend(t testing.TB, groupID, nodeID string) *GroupBackend {
	t.Helper()
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir + "/meta").WithLogger(nil))
	require.NoError(t, err)
	node, closeRaft, err := newRaftNode(raft.DefaultConfig(nodeID, nil), dir)
	require.NoError(t, err)
	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers")
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers")
		},
	)
	svc := NewShardService(dir+"/shards", nil)
	gb, err := NewGroupBackend(GroupBackendConfig{
		ID:       groupID,
		Root:     dir,
		DB:       db,
		Node:     node,
		PeerIDs:  []string{nodeID},
		ShardSvc: svc,
		EC:       ECConfig{DataShards: 1, ParityShards: 0},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, gb.Close())
		if closeRaft != nil {
			require.NoError(t, closeRaft())
		}
	})
	return gb
}

func TestClusterCoordinator_ListBuckets_DelegatesToBase(t *testing.T) {
	base := &fakeBackend{listResult: []string{"a", "b"}}
	c := NewClusterCoordinator(base, nil, nil, nil, "self")
	got, err := c.ListBuckets(context.Background())
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

type noopObjectIndexProposer struct{}

func (noopObjectIndexProposer) ProposeObjectIndex(context.Context, ObjectIndexEntry, bool) error {
	return nil
}

func (noopObjectIndexProposer) ProposeDeleteObjectIndex(context.Context, string, string, string) error {
	return nil
}

type recordingObjectIndexProposer struct {
	entries []ObjectIndexEntry
	deleted []string
}

func (r *recordingObjectIndexProposer) ProposeObjectIndex(_ context.Context, entry ObjectIndexEntry, _ bool) error {
	r.entries = append(r.entries, entry)
	return nil
}

func (r *recordingObjectIndexProposer) ProposeDeleteObjectIndex(_ context.Context, bucket, key, versionID string) error {
	r.deleted = append(r.deleted, bucket+"/"+key+"/"+versionID)
	return nil
}

func TestClusterCoordinator_CommitObjectIndexUsesPlacementGroupECProfile(t *testing.T) {
	proposer := &recordingObjectIndexProposer{}
	c := NewClusterCoordinator(nil, nil, nil, nil, "self").
		WithECConfig(ECConfig{DataShards: 4, ParityShards: 2}).
		WithObjectIndexProposer(proposer)
	obj := &storage.Object{
		Key:          "photo.jpg",
		Size:         12,
		ContentType:  "image/jpeg",
		ETag:         "etag",
		LastModified: 100,
		VersionID:    "v1",
	}
	group := ShardGroupEntry{
		ID:      "group-5",
		PeerIDs: []string{"n1", "n2", "n3", "n4", "n5"},
	}

	require.NoError(t, c.commitObjectIndex(context.Background(), "photos", "photo.jpg", obj, group, false))

	require.Len(t, proposer.entries, 1)
	require.Equal(t, uint8(3), proposer.entries[0].ECData)
	require.Equal(t, uint8(2), proposer.entries[0].ECParity)
	require.Equal(t, group.PeerIDs, proposer.entries[0].NodeIDs)
}

func TestClusterCoordinator_PutObject_MapsTopologyForwardUnreachableToPlacementUnavailable(t *testing.T) {
	base := &fakeBackend{}
	peers := []string{
		"127.0.0.1:7001",
		"127.0.0.1:7002",
		"127.0.0.1:7003",
		"127.0.0.1:7004",
		"127.0.0.1:7005",
		"127.0.0.1:7006",
	}
	groupID := "group-wide"
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup(groupID, peers))
	router := NewRouter(mgr)
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		groupID: {ID: groupID, PeerIDs: peers},
	}}
	d := &recordingDialer{defaultErr: ErrNoReachablePeer}
	c := NewClusterCoordinator(base, mgr, router, meta, "self").
		WithForwardSender(NewForwardSender(d.dial)).
		WithECConfig(AutoECConfigForClusterSize(len(peers))).
		WithObjectIndexProposer(noopObjectIndexProposer{})

	_, err := c.PutObject(context.Background(), "bk", "k", strings.NewReader("body"), "text/plain")

	require.ErrorIs(t, err, ErrPlacementTargetsUnavailable)
}

func TestClusterCoordinator_CommitObjectIndexRecordsActualShardTargets(t *testing.T) {
	proposer := &recordingObjectIndexProposer{}
	c := NewClusterCoordinator(nil, nil, nil, nil, "self").
		WithObjectIndexProposer(proposer)
	obj := &storage.Object{
		Key:          "large-group.bin",
		Size:         12,
		ContentType:  "application/octet-stream",
		ETag:         "etag",
		LastModified: 100,
		VersionID:    "v1",
	}
	group := ShardGroupEntry{
		ID:      "group-9",
		PeerIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9"},
	}

	require.NoError(t, c.commitObjectIndex(context.Background(), "photos", "large-group.bin", obj, group, false))

	require.Len(t, proposer.entries, 1)
	require.Equal(t, uint8(6), proposer.entries[0].ECData)
	require.Equal(t, uint8(2), proposer.entries[0].ECParity)
	require.Equal(t, group.PeerIDs[:8], proposer.entries[0].NodeIDs)
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

	got, err := c.ListBuckets(context.Background())
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

	require.NoError(t, c.HeadBucket(context.Background(), "default"))
	require.Equal(t, []string{"HeadBucket:default"}, base.calls)
}

func TestClusterCoordinator_ListObjects_UsesObjectIndexAcrossPlacementGroups(t *testing.T) {
	base := &fakeBackend{listResult: []string{"photos"}}
	gb1 := newTestGroupBackend(t, "group-1")
	gb2 := newTestGroupBackend(t, "group-2")
	a, err := gb1.PutObject(context.Background(), "photos", "a.txt", strings.NewReader("a"), "text/plain")
	require.NoError(t, err)
	b, err := gb2.PutObject(context.Background(), "photos", "b.txt", strings.NewReader("bb"), "text/plain")
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb1))
	mgr.Add(NewDataGroupWithBackend("group-2", []string{"test-node"}, gb2))
	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")
	meta := NewMetaFSM()
	require.NoError(t, meta.applyCmd(makePutShardGroupCmd(t, "group-1", []string{"test-node"})))
	require.NoError(t, meta.applyCmd(makePutShardGroupCmd(t, "group-2", []string{"test-node"})))
	require.NoError(t, meta.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-1")))
	require.NoError(t, meta.applyCmd(makePutObjectIndexCmd(t, ObjectIndexEntry{
		Bucket: "photos", Key: "a.txt", VersionID: a.VersionID,
		PlacementGroupID: "group-1", Size: a.Size, ContentType: a.ContentType,
		ETag: a.ETag, ModTime: a.LastModified,
	}, false)))
	require.NoError(t, meta.applyCmd(makePutObjectIndexCmd(t, ObjectIndexEntry{
		Bucket: "photos", Key: "b.txt", VersionID: b.VersionID,
		PlacementGroupID: "group-2", Size: b.Size, ContentType: b.ContentType,
		ETag: b.ETag, ModTime: b.LastModified,
	}, false)))
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node").
		WithObjectIndexProposer(noopObjectIndexProposer{})

	objs, err := c.ListObjects(context.Background(), "photos", "", 100)
	require.NoError(t, err)
	require.Equal(t, []string{"a.txt", "b.txt"}, []string{objs[0].Key, objs[1].Key})

	var walked []string
	require.NoError(t, c.WalkObjects(context.Background(), "photos", "", func(obj *storage.Object) error {
		walked = append(walked, obj.Key)
		return nil
	}))
	require.Equal(t, []string{"a.txt", "b.txt"}, walked)

	versions, err := c.ListObjectVersions("photos", "", 100)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	require.Equal(t, []string{"a.txt", "b.txt"}, []string{versions[0].Key, versions[1].Key})
}

func TestClusterCoordinator_GetObjectFallsBackToPlacementWhenIndexIsLagging(t *testing.T) {
	base := &fakeBackend{}
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "photos"))
	_, err := gb.PutObject(context.Background(), "photos", "img.jpg", strings.NewReader("image"), "image/jpeg")
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")
	meta := NewMetaFSM()
	require.NoError(t, meta.applyCmd(makePutShardGroupCmd(t, "group-1", []string{"test-node"})))
	require.NoError(t, meta.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-1")))
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node").
		WithECConfig(ECConfig{DataShards: 1, ParityShards: 0}).
		WithObjectIndexProposer(noopObjectIndexProposer{})

	rc, obj, err := c.GetObject(context.Background(), "photos", "img.jpg")
	require.NoError(t, err)
	defer rc.Close()
	body, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "img.jpg", obj.Key)
	require.Equal(t, "image", string(body))
}

func TestClusterCoordinator_DeleteObjectVersion_RemovesObjectIndex(t *testing.T) {
	base := &fakeBackend{listResult: []string{"photos"}}
	gb := newTestGroupBackend(t, "group-1")
	obj, err := gb.PutObject(context.Background(), "photos", "a.txt", strings.NewReader("a"), "text/plain")
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")
	meta := NewMetaFSM()
	require.NoError(t, meta.applyCmd(makePutShardGroupCmd(t, "group-1", []string{"test-node"})))
	require.NoError(t, meta.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-1")))
	require.NoError(t, meta.applyCmd(makePutObjectIndexCmd(t, ObjectIndexEntry{
		Bucket: "photos", Key: "a.txt", VersionID: obj.VersionID,
		PlacementGroupID: "group-1", Size: obj.Size, ContentType: obj.ContentType,
		ETag: obj.ETag, ModTime: obj.LastModified,
	}, false)))
	proposer := &recordingObjectIndexProposer{}
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node").
		WithObjectIndexProposer(proposer)

	require.NoError(t, c.DeleteObjectVersion("photos", "a.txt", obj.VersionID))
	require.Equal(t, []string{"photos/a.txt/" + obj.VersionID}, proposer.deleted)
}

func TestClusterCoordinator_FindObjectIndexOrphans_ScansGroupLocalObjects(t *testing.T) {
	base := &fakeBackend{listResult: []string{"photos"}}
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "photos"))
	obj, err := gb.PutObject(context.Background(), "photos", "orphan.txt", strings.NewReader("body"), "text/plain")
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")
	meta := NewMetaFSM()
	require.NoError(t, meta.applyCmd(makePutShardGroupCmd(t, "group-1", []string{"test-node"})))
	require.NoError(t, meta.applyCmd(makePutBucketAssignmentCmd(t, "photos", "group-1")))
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node").
		WithObjectIndexProposer(noopObjectIndexProposer{})

	issues, err := c.FindObjectIndexOrphans(context.Background())
	require.NoError(t, err)
	require.Len(t, issues, 1)
	require.Equal(t, ObjectIndexIssueOrphan, issues[0].Kind)
	require.Equal(t, "photos", issues[0].Bucket)
	require.Equal(t, "orphan.txt", issues[0].Key)
	require.Equal(t, obj.VersionID, issues[0].VersionID)
}

func TestClusterCoordinator_ListAllObjects_RoutesThroughDataGroup(t *testing.T) {
	base := &fakeBackend{listResult: []string{"photos"}}
	gb := newTestGroupBackend(t, "group-1")
	_, err := gb.PutObject(context.Background(), "photos", "a.txt", strings.NewReader("hello"), "text/plain")
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

func TestClusterCoordinator_ListAllObjects_TolerantOfUnreadableBlob(t *testing.T) {
	base := &fakeBackend{listResult: []string{"photos"}}
	gb := newTestGroupBackend(t, "group-1")
	v, err := gb.PutObject(context.Background(), "photos", "a.txt", strings.NewReader("hello"), "text/plain")
	require.NoError(t, err)

	// Delete the on-disk shards so GetObjectVersion fails, while the meta/version
	// record stays. ListAllObjects must still return the object using the
	// version-listing metadata rather than aborting the whole snapshot.
	require.NoError(t, gb.shardSvc.DeleteLocalShards("photos", "a.txt/"+v.VersionID))

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"test-node"}},
	}}
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node")

	objs, err := c.ListAllObjects()
	require.NoError(t, err, "listing must not fail just because one blob is unreadable")
	require.Len(t, objs, 1)
	require.Equal(t, "photos", objs[0].Bucket)
	require.Equal(t, "a.txt", objs[0].Key)
	require.Equal(t, v.VersionID, objs[0].VersionID)
	require.True(t, objs[0].IsLatest)
}

func TestClusterCoordinator_ListAllObjects_PreservesVersionsAndDeleteMarkers(t *testing.T) {
	base := &fakeBackend{listResult: []string{"photos"}}
	gb := newTestGroupBackend(t, "group-1")
	v1, err := gb.PutObject(context.Background(), "photos", "a.txt", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	v2, err := gb.PutObject(context.Background(), "photos", "a.txt", strings.NewReader("v2"), "text/plain")
	require.NoError(t, err)
	markerID, err := gb.DeleteObjectReturningMarker("photos", "a.txt")
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
	byVersion := map[string]storage.SnapshotObject{}
	for _, obj := range objs {
		byVersion[obj.VersionID] = obj
	}
	require.Len(t, byVersion, 3)
	require.Contains(t, byVersion, v1.VersionID)
	require.Contains(t, byVersion, v2.VersionID)
	require.Contains(t, byVersion, markerID)
	require.False(t, byVersion[v1.VersionID].IsLatest)
	require.False(t, byVersion[v2.VersionID].IsLatest)
	require.True(t, byVersion[markerID].IsLatest)
	require.True(t, byVersion[markerID].IsDeleteMarker)
	require.Equal(t, "text/plain", byVersion[v1.VersionID].ContentType)
	require.Equal(t, "text/plain", byVersion[v2.VersionID].ContentType)
}

func TestClusterCoordinator_WALWriteAtReadAt_RoutesToLocalGroup(t *testing.T) {
	base := &fakeBackend{listResult: []string{"__grainfs_vfs_default"}}
	gb := newTestGroupBackend(t, "group-1")

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("__grainfs_vfs_default", "group-1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"test-node"}},
	}}
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node")

	w, err := wal.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	wrapped := wal.NewBackend(c, w)
	require.True(t, wrapped.PreferWriteAt("__grainfs_vfs_default"))
	require.False(t, wrapped.PreferWriteAt("photos"))

	require.NoError(t, wrapped.Truncate(context.Background(), "__grainfs_vfs_default", "fio/sparse.bin", 12))
	sparse := make([]byte, 12)
	n, err := wrapped.ReadAt(context.Background(), "__grainfs_vfs_default", "fio/sparse.bin", 0, sparse)
	require.NoError(t, err)
	require.Equal(t, 12, n)
	require.Equal(t, make([]byte, 12), sparse)

	obj, err := wrapped.WriteAt(context.Background(), "__grainfs_vfs_default", "fio/file.bin", 4, []byte("data"))
	require.NoError(t, err)
	require.Equal(t, int64(8), obj.Size)
	require.Empty(t, obj.ETag)

	require.NoError(t, wrapped.Truncate(context.Background(), "__grainfs_vfs_default", "fio/file.bin", 6))

	buf := make([]byte, 8)
	n, err = wrapped.ReadAt(context.Background(), "__grainfs_vfs_default", "fio/file.bin", 0, buf)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 6, n)
	require.Equal(t, []byte{0, 0, 0, 0, 'd', 'a', 0, 0}, buf)
}

func TestClusterCoordinator_PreferWriteAtFalseForMultiVoterInternalBucket(t *testing.T) {
	base := &fakeBackend{listResult: []string{"__grainfs_vfs_default"}}
	gb := newTestGroupBackend(t, "group-1")

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node", "node-2", "node-3"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("__grainfs_vfs_default", "group-1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"test-node", "node-2", "node-3"}},
	}}
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node")

	require.False(t, c.PreferWriteAt("__grainfs_vfs_default"),
		"multi-voter internal buckets must use PutObject replication, not local-only pwrite")
}

func TestClusterCoordinator_InternalReadAtFallsBackWhenObjectIndexMissing(t *testing.T) {
	base := &fakeBackend{listResult: []string{"__grainfs_vfs_default"}}
	gb := newTestGroupBackend(t, "group-1")

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("__grainfs_vfs_default", "group-1")
	meta := NewMetaFSM()
	require.NoError(t, meta.applyCmd(makePutShardGroupCmd(t, "group-1", []string{"test-node"})))
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node").
		WithObjectIndexProposer(noopObjectIndexProposer{})

	require.NoError(t, c.Truncate(context.Background(), "__grainfs_vfs_default", "fio/file.bin", 5))
	_, err := c.WriteAt(context.Background(), "__grainfs_vfs_default", "fio/file.bin", 1, []byte("abc"))
	require.NoError(t, err)

	obj, err := c.HeadObject(context.Background(), "__grainfs_vfs_default", "fio/file.bin")
	require.NoError(t, err)
	require.Equal(t, int64(5), obj.Size)

	buf := make([]byte, 5)
	n, err := c.ReadAt(context.Background(), "__grainfs_vfs_default", "fio/file.bin", 0, buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte{0, 'a', 'b', 'c', 0}, buf)
}

func TestClusterCoordinator_RestoreObjects_RemovesDataGroupExtras(t *testing.T) {
	base := &fakeBackend{listResult: []string{"photos"}}
	gb := newTestGroupBackend(t, "group-1")
	_, err := gb.PutObject(context.Background(), "photos", "keep.txt", strings.NewReader("keep"), "text/plain")
	require.NoError(t, err)
	_, err = gb.PutObject(context.Background(), "photos", "extra.txt", strings.NewReader("extra"), "text/plain")
	require.NoError(t, err)

	current, err := gb.ListObjects(context.Background(), "photos", "", 100)
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

	objs, err := gb.ListObjects(context.Background(), "photos", "", 100)
	require.NoError(t, err)
	require.Len(t, objs, 1)
	require.Equal(t, "keep.txt", objs[0].Key)
}

func TestClusterCoordinator_RestoreObjects_RoutesByObjectPlacement(t *testing.T) {
	base := &fakeBackend{listResult: []string{"photos"}}
	gb1 := newTestGroupBackend(t, "group-1")
	gb2 := newTestGroupBackend(t, "group-2")
	require.NoError(t, gb1.CreateBucket(context.Background(), "photos"))
	require.NoError(t, gb2.CreateBucket(context.Background(), "photos"))

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb1))
	mgr.Add(NewDataGroupWithBackend("group-2", []string{"test-node"}, gb2))
	router := NewRouter(mgr)
	router.AssignBucket("photos", "group-1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"test-node"}},
		"group-2": {ID: "group-2", PeerIDs: []string{"test-node"}},
	}}
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node").
		WithECConfig(ECConfig{DataShards: 1, ParityShards: 0})

	keyForGroup := func(groupID string) string {
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key-%s-%d.txt", groupID, i)
			group, err := SelectObjectPlacementGroup("photos", key, meta.ShardGroups(), ECConfig{DataShards: 1, ParityShards: 0})
			require.NoError(t, err)
			if group.ID == groupID {
				return key
			}
		}
		require.FailNow(t, "could not find key for group", groupID)
		return ""
	}
	key1 := keyForGroup("group-1")
	key2 := keyForGroup("group-2")

	obj1, err := c.PutObject(context.Background(), "photos", key1, strings.NewReader("one"), "text/plain")
	require.NoError(t, err)
	obj2, err := c.PutObject(context.Background(), "photos", key2, strings.NewReader("two"), "text/plain")
	require.NoError(t, err)

	snap := []storage.SnapshotObject{
		{Bucket: "photos", Key: key1, ETag: obj1.ETag, Size: obj1.Size, ContentType: obj1.ContentType, Modified: obj1.LastModified, VersionID: obj1.VersionID, IsLatest: true},
		{Bucket: "photos", Key: key2, ETag: obj2.ETag, Size: obj2.Size, ContentType: obj2.ContentType, Modified: obj2.LastModified, VersionID: obj2.VersionID, IsLatest: true},
	}
	restored, stale, err := c.RestoreObjects(snap)
	require.NoError(t, err)
	require.Equal(t, 2, restored)
	require.Empty(t, stale)

	_, err = gb1.HeadObject(context.Background(), "photos", key1)
	require.NoError(t, err)
	_, err = gb2.HeadObject(context.Background(), "photos", key2)
	require.NoError(t, err)
}

// --- T6 forward-path test scaffolding ---

// recordingDialer captures every (peer, payload) pair the ForwardSender hands
// it and returns a canned reply. The op-specific reply bytes are provided by
// the caller via replyByOp; missing op → buildErrorReply(Internal).
type recordingDialer struct {
	calls         []dialerCall
	streamCalls   []dialerCall
	readCalls     []dialerCall
	replyByOp     map[raftpb.ForwardOp][]byte
	streamReplyBy map[raftpb.ForwardOp][]byte
	readReplyBy   map[raftpb.ForwardOp][]byte
	readBodyBy    map[raftpb.ForwardOp][]byte
	replyFunc     func(peer string, op raftpb.ForwardOp, args []byte) ([]byte, error, bool)
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
	if d.replyFunc != nil {
		if reply, err, ok := d.replyFunc(peer, op, argsCopy); ok {
			return reply, err
		}
	}
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

func (d *recordingDialer) readStream(ctx context.Context, peer string, payload []byte) ([]byte, io.ReadCloser, error) {
	if d.defaultErr != nil {
		return nil, nil, d.defaultErr
	}
	gid, op, args, err := decodeForwardPayload(payload)
	if err != nil {
		return nil, nil, err
	}
	argsCopy := make([]byte, len(args))
	copy(argsCopy, args)
	d.readCalls = append(d.readCalls, dialerCall{peer: peer, op: op, gid: gid, args: argsCopy, rawly: payload})
	if reply, ok := d.readReplyBy[op]; ok {
		return reply, io.NopCloser(bytes.NewReader(d.readBodyBy[op])), nil
	}
	return buildSimpleReply(raftpb.ForwardStatusInternal, ""), io.NopCloser(bytes.NewReader(nil)), nil
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
	d := &recordingDialer{
		replyByOp:     map[raftpb.ForwardOp][]byte{},
		streamReplyBy: map[raftpb.ForwardOp][]byte{},
		readReplyBy:   map[raftpb.ForwardOp][]byte{},
		readBodyBy:    map[raftpb.ForwardOp][]byte{},
	}
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

	rc, obj, err := c.GetObject(context.Background(), "bk", "k")
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

	_, _, err := c.GetObject(context.Background(), "bk", "k")
	require.ErrorIs(t, err, ErrForwardBodySizeMismatch)
}

func TestClusterCoordinator_GetObject_Forward_AboveLegacyCap(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	body := bytes.Repeat([]byte("g"), DefaultMaxForwardBodyBytes+1024)
	d.replyByOp[raftpb.ForwardOpGetObject] = buildGetObjectReply(
		&storage.Object{Key: "large", Size: int64(len(body)), ETag: "etag-large", ContentType: "application/octet-stream"},
		"bk", body,
	)

	rc, obj, err := c.GetObject(context.Background(), "bk", "large")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Equal(t, body, got)
}

func TestClusterCoordinator_GetObject_ForwardUsesReadStream(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	body := bytes.Repeat([]byte("g"), int(DefaultMaxForwardReplyBytes)+1024)
	d.readReplyBy[raftpb.ForwardOpGetObject] = buildGetObjectReply(
		&storage.Object{Key: "large", Size: int64(len(body)), ETag: "etag-large", ContentType: "application/octet-stream"},
		"bk", nil,
	)
	d.readBodyBy[raftpb.ForwardOpGetObject] = body
	c.forward.WithReadStreamDialer(d.readStream)

	rc, obj, err := c.GetObject(context.Background(), "bk", "large")
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Equal(t, body, got)
	require.Empty(t, d.calls)
	require.Len(t, d.readCalls, 1)
	require.Equal(t, raftpb.ForwardOpGetObject, d.readCalls[0].op)
}

func TestClusterCoordinator_ReadAt_FallbackRejectsNegativeOffset(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpGetObject] = buildGetObjectReply(
		&storage.Object{Key: "k", Size: 5, ETag: "etag", ContentType: "application/octet-stream"},
		"bk", []byte("hello"),
	)

	require.NotPanics(t, func() {
		n, err := c.ReadAt(context.Background(), "bk", "k", -1, make([]byte, 2))
		require.Zero(t, n)
		require.Error(t, err)
	})
}

func TestClusterCoordinator_ReadAt_ForwardSmallRangeUsesSingleFrame(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	body := []byte("abcdef")
	d.replyByOp[raftpb.ForwardOpReadAt] = buildReadAtReply(body)
	c.forward.WithReadStreamDialer(d.readStream)

	buf := make([]byte, len(body))
	n, err := c.ReadAt(context.Background(), "bk", "k", 10, buf)

	require.NoError(t, err)
	require.Equal(t, len(body), n)
	require.Equal(t, body, buf)
	require.Len(t, d.calls, 1)
	require.Equal(t, raftpb.ForwardOpReadAt, d.calls[0].op)
	require.Empty(t, d.readCalls)
	args := raftpb.GetRootAsReadAtArgs(d.calls[0].args, 0)
	require.Equal(t, int64(10), args.Offset())
	require.Equal(t, int64(len(body)), args.Length())
}

func TestClusterCoordinator_ReadAt_ForwardShortBodyReturnsEOF(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpReadAt] = buildReadAtReply([]byte("tail"))
	c.forward.WithReadStreamDialer(d.readStream)

	buf := make([]byte, 128)
	n, err := c.ReadAt(context.Background(), "bk", "k", 0, buf)

	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 4, n)
	require.Equal(t, []byte("tail"), buf[:n])
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

	v1, err := c.PutObject(context.Background(), "bk", "k", strings.NewReader("v1"), "text/plain")
	require.NoError(t, err)
	v2, err := c.PutObject(context.Background(), "bk", "k", strings.NewReader("v2"), "text/plain")
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

func TestClusterCoordinator_GetObjectVersion_ForwardUsesReadStream(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	body := bytes.Repeat([]byte("v"), int(DefaultMaxForwardReplyBytes)+1024)
	d.readReplyBy[raftpb.ForwardOpGetObjectVersion] = buildGetObjectReply(
		&storage.Object{Key: "k", Size: int64(len(body)), ETag: "etag-v1", ContentType: "application/octet-stream", VersionID: "vid-1"},
		"bk", nil,
	)
	d.readBodyBy[raftpb.ForwardOpGetObjectVersion] = body
	c.forward.WithReadStreamDialer(d.readStream)

	rc, obj, err := c.GetObjectVersion("bk", "k", "vid-1")
	require.NoError(t, err)
	got, readErr := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, readErr)
	require.Equal(t, "vid-1", obj.VersionID)
	require.Equal(t, body, got)
	require.Empty(t, d.calls)
	require.Len(t, d.readCalls, 1)
	require.Equal(t, raftpb.ForwardOpGetObjectVersion, d.readCalls[0].op)
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
	obj, err := c.HeadObject(context.Background(), "bk", "k")
	require.NoError(t, err)
	require.Equal(t, int64(99), obj.Size)
	require.Equal(t, raftpb.ForwardOpHeadObject, d.calls[0].op)
}

func TestClusterCoordinator_DeleteObject_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpDeleteObject] = buildOKReply()
	require.NoError(t, c.DeleteObject(context.Background(), "bk", "k"))
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
	out, err := c.ListObjects(context.Background(), "bk", "p/", 100)
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
	require.NoError(t, c.WalkObjects(context.Background(), "bk", "a", func(o *storage.Object) error {
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
	err := c.WalkObjects(context.Background(), "bk", "a", func(o *storage.Object) error {
		return stopErr
	})
	require.ErrorIs(t, err, stopErr)
}

func TestClusterCoordinator_CreateMultipartUpload_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpCreateMultipartUpload] = buildUploadReply("bk", "k", "upload-1")
	up, err := c.CreateMultipartUpload(context.Background(), "bk", "k", "text/plain")
	require.NoError(t, err)
	require.Equal(t, "upload-1", up.UploadID)
	require.Equal(t, raftpb.ForwardOpCreateMultipartUpload, d.calls[0].op)
}

func TestClusterCoordinator_CompleteMultipartUpload_Forward(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpCompleteMultipartUpload] = buildObjectReply(
		&storage.Object{Key: "k", Size: 1024, ETag: "merged-etag"}, "bk",
	)
	obj, err := c.CompleteMultipartUpload(context.Background(), "bk", "k", "upload-1", []storage.Part{
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
	require.NoError(t, c.AbortMultipartUpload(context.Background(), "bk", "k", "upload-1"))
	require.Equal(t, raftpb.ForwardOpAbortMultipartUpload, d.calls[0].op)
}

// TestClusterCoordinator_GetObject_NoSuchBucketStatus verifies that a server-
// side NoSuchBucket reply gets surfaced as storage.ErrNoSuchBucket — the
// status code conversion is the contract S3 handlers depend on for 404 vs 500.
func TestClusterCoordinator_GetObject_NoSuchBucketStatus(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpGetObject] = buildSimpleReply(raftpb.ForwardStatusNoSuchBucket, "")
	_, _, err := c.GetObject(context.Background(), "bk", "k")
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
	obj, err := c.PutObject(context.Background(), "bk", "k", bytes.NewReader(body), "application/octet-stream")
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Len(t, d.calls, 1)
	require.Equal(t, raftpb.ForwardOpPutObject, d.calls[0].op)
}

func TestClusterCoordinator_PutObject_ForwardRetriesHintForSmallFrame(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"peer-A", "peer-C"})
	body := []byte("small-forward-body")
	d.replyFunc = func(peer string, op raftpb.ForwardOp, args []byte) ([]byte, error, bool) {
		switch op {
		case raftpb.ForwardOpPutObject:
			if peer == "peer-A" {
				return notLeaderReplyBytes(t, "peer-B"), nil, true
			}
			if peer == "peer-B" {
				return buildObjectReply(
					&storage.Object{Key: "k", Size: int64(len(body)), ETag: "etag-put"}, "bk",
				), nil, true
			}
		}
		return buildSimpleReply(raftpb.ForwardStatusInternal, ""), nil, true
	}

	obj, err := c.PutObject(context.Background(), "bk", "k", bytes.NewReader(body), "application/octet-stream")

	require.NoError(t, err)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Len(t, d.calls, 2)
	require.Equal(t, raftpb.ForwardOpPutObject, d.calls[0].op)
	require.Equal(t, "peer-A", d.calls[0].peer)
	require.Equal(t, raftpb.ForwardOpPutObject, d.calls[1].op)
	require.Equal(t, "peer-B", d.calls[1].peer)
}

func TestClusterCoordinator_PutObject_ForwardLeavesObjectIndexToReceiver(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a", "self"})
	proposer := &recordingObjectIndexProposer{}
	c.WithObjectIndexProposer(proposer)
	body := []byte("manifest")
	d.replyByOp[raftpb.ForwardOpPutObject] = buildObjectReply(
		&storage.Object{Key: "k", Size: int64(len(body)), ETag: "etag-put", ContentType: "application/octet-stream", VersionID: "v1"},
		"bk",
	)

	obj, err := c.PutObject(context.Background(), "bk", "k", bytes.NewReader(body), "application/octet-stream")

	require.NoError(t, err)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Empty(t, proposer.entries, "forward receiver owns object-index commit for forwarded PUTs")
}

func TestClusterCoordinator_PutObject_StreamForwardLeavesObjectIndexToReceiver(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a", "self"})
	c.forward.WithStreamDialer(d.stream)
	proposer := &recordingObjectIndexProposer{}
	c.WithObjectIndexProposer(proposer)
	body := bytes.Repeat([]byte("z"), DefaultMaxForwardBodyBytes+1024)
	d.streamReplyBy[raftpb.ForwardOpPutObject] = buildObjectReply(
		&storage.Object{Key: "k", Size: int64(len(body)), ETag: "etag-stream", ContentType: "application/octet-stream", VersionID: "v1"},
		"bk",
	)

	obj, err := c.PutObject(context.Background(), "bk", "k", bytes.NewReader(body), "application/octet-stream")

	require.NoError(t, err)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Len(t, d.streamCalls, 1)
	require.Empty(t, proposer.entries, "forward receiver owns object-index commit for streamed forwarded PUTs")
}

func TestClusterCoordinator_PutObject_ForwardRejectsSizeMismatch(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	body := []byte("non-empty-body")
	d.replyByOp[raftpb.ForwardOpPutObject] = buildObjectReply(
		&storage.Object{Key: "k", Size: 0, ETag: "etag-empty"}, "bk",
	)

	_, err := c.PutObject(context.Background(), "bk", "k", bytes.NewReader(body), "application/octet-stream")
	require.ErrorIs(t, err, ErrForwardBodySizeMismatch)
}

// TestClusterCoordinator_PutObject_TooLarge_413 verifies the 5 MB hard cap is
// enforced BEFORE encoding/forwarding — caller sees ErrEntityTooLarge without
// any wire activity (recordingDialer recorded no calls).
func TestClusterCoordinator_PutObject_TooLarge_413(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	body := bytes.Repeat([]byte("x"), 6*1024*1024) // 6 MB, over cap
	_, err := c.PutObject(context.Background(), "bk", "k", bytes.NewReader(body), "")
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
	p, err := c.UploadPart(context.Background(), "bk", "k", "uid", 7, bytes.NewReader(body))
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

	_, err := c.UploadPart(context.Background(), "bk", "k", "uid", 7, bytes.NewReader(body))
	require.ErrorIs(t, err, ErrForwardBodySizeMismatch)
}

// TestClusterCoordinator_UploadPart_5MB_Cap — same enforcement on multipart.
func TestClusterCoordinator_UploadPart_5MB_Cap(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	body := bytes.Repeat([]byte("x"), 6*1024*1024)
	_, err := c.UploadPart(context.Background(), "bk", "k", "uid", 1, bytes.NewReader(body))
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

	obj, err := c.PutObject(context.Background(), "bk", "k", bytes.NewReader(body), "application/octet-stream")
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

	obj, err := c.PutObject(context.Background(), "bk", "k", bytes.NewReader(body), "application/octet-stream")
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

	part, err := c.UploadPart(context.Background(), "bk", "k", "uid", 1, bytes.NewReader(body))
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

	part, err := c.UploadPart(context.Background(), "bk", "k", "uid", 1, bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), part.Size)
	require.Empty(t, d.streamCalls)
	require.Len(t, d.calls, 1)
	require.Equal(t, raftpb.ForwardOpUploadPart, d.calls[0].op)

	args := raftpb.GetRootAsUploadPartArgs(d.calls[0].args, 0)
	require.Equal(t, body, args.BodyBytes())
}

func TestClusterCoordinator_CompleteMultipartUpload_ForwardCommitsObjectIndex(t *testing.T) {
	c, d := setupCoordWithForward(t, "b", "g1", []string{"a"})
	proposer := &recordingObjectIndexProposer{}
	c.WithObjectIndexProposer(proposer)

	d.replyByOp[raftpb.ForwardOpCompleteMultipartUpload] = buildObjectReply(
		&storage.Object{Key: "k", VersionID: "v1", Size: 5, ETag: "etag"}, "b",
	)

	_, err := c.CompleteMultipartUpload(context.Background(), "b", "k", "upload-1", []storage.Part{
		{PartNumber: 1, ETag: "part-1", Size: 5},
	})
	require.NoError(t, err)
	require.Len(t, proposer.entries, 1)
	require.Equal(t, "b", proposer.entries[0].Bucket)
	require.Equal(t, "k", proposer.entries[0].Key)
	require.Equal(t, "v1", proposer.entries[0].VersionID)
	require.Equal(t, "g1", proposer.entries[0].PlacementGroupID)
}

func TestClusterCoordinator_PutObjectForwardFrameRecordsTrace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", path)
	reloadPutTraceSinkForTest()

	c, d := setupCoordWithForward(t, "bench", "group-1", []string{"peer-a", "self"})
	d.replyByOp[raftpb.ForwardOpPutObject] = buildObjectReply(
		&storage.Object{Key: "trace-key", Size: 4, VersionID: "v1", LastModified: time.Now().Unix()},
		"bench",
	)

	_, err := c.PutObject(context.Background(), "bench", "trace-key", strings.NewReader("body"), "text/plain")
	require.NoError(t, err)

	events := readPutTraceEvents(t, path)
	requirePutTraceStage(t, events, PutTraceStageRouteWrite)
	requirePutTraceStage(t, events, PutTraceStageForwardSendFrame)
	require.Equal(t, PutTraceIngressForwardedNonLeader, events[0].Ingress)
	require.Equal(t, PutTraceForwardFrame, events[0].ForwardMode)
}

func TestClusterCoordinator_PutObjectLocalRecordsLocalTrace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", path)
	reloadPutTraceSinkForTest()

	base := &fakeBackend{}
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	router := NewRouter(mgr)
	router.AssignBucket("bench", "group-1")
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"test-node"}},
	}}
	c := NewClusterCoordinator(base, mgr, router, meta, "test-node").WithObjectIndexProposer(noopObjectIndexProposer{})

	_, err := c.PutObject(context.Background(), "bench", "local-trace-key", strings.NewReader("body"), "text/plain")
	require.NoError(t, err)

	events := readPutTraceEvents(t, path)
	requirePutTraceStage(t, events, PutTraceStageRouteWrite)
	for _, ev := range events {
		require.Equal(t, PutTraceIngressLocalLeader, ev.Ingress)
	}
}

func readPutTraceEvents(t *testing.T, path string) []PutTraceEvent {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	var out []PutTraceEvent
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var ev PutTraceEvent
		require.NoError(t, json.Unmarshal(sc.Bytes(), &ev))
		out = append(out, ev)
	}
	require.NoError(t, sc.Err())
	require.NotEmpty(t, out)
	return out
}

func requirePutTraceStage(t *testing.T, events []PutTraceEvent, stage PutTraceStage) {
	t.Helper()
	for _, ev := range events {
		if ev.Stage == stage {
			return
		}
	}
	require.Failf(t, "missing stage", "stage %s not found in %#v", stage, events)
}
