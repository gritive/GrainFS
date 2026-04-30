package cluster

import (
	"fmt"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
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

func TestClusterCoordinator_ListBuckets_DelegatesToBase(t *testing.T) {
	base := &fakeBackend{listResult: []string{"a", "b"}}
	c := NewClusterCoordinator(base, nil, nil, nil, "self")
	got, err := c.ListBuckets()
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, got)
	require.Equal(t, []string{"ListBuckets"}, base.calls)
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
