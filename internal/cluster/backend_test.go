package cluster

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

type blockingSnapshotter struct {
	entered chan struct{}
	release chan struct{}
	closed  atomic.Bool
}

func newBlockingSnapshotter() *blockingSnapshotter {
	return &blockingSnapshotter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingSnapshotter) Snapshot() ([]byte, error) {
	if s.closed.CompareAndSwap(false, true) {
		close(s.entered)
	}
	<-s.release
	return []byte("blocked-snapshot"), nil
}

func (s *blockingSnapshotter) Restore(raft.SnapshotMeta, []byte) error {
	return nil
}

type clusterTestTB interface {
	Helper()
	Cleanup(func())
	TempDir() string
	Errorf(format string, args ...interface{})
	FailNow()
	Fatalf(format string, args ...interface{})
}

// newTestDistributedBackend creates a DistributedBackend backed by a local Raft node.
func newTestDistributedBackend(t clusterTestTB) *DistributedBackend {
	t.Helper()
	dir := t.TempDir()

	metaDir := dir + "/meta"
	dbOpts := badgerutil.SmallOptions(metaDir)
	db, err := badger.Open(dbOpts)
	require.NoError(t, err)

	cfg := raft.DefaultConfig("test-node", nil)
	node, closeRaft, err := newRaftNode(cfg, dir)
	require.NoError(t, err)
	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers")
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers")
		},
	)
	node.Start()
	require.NoError(t, node.Bootstrap())

	for range 2000 {
		if node.IsLeader() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.True(t, node.IsLeader(), "no-peers node must become leader")

	backend, err := NewDistributedBackend(dir, db, node, nil, false)
	require.NoError(t, err)

	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 0})
	svc := NewShardService(backend.root, nil)
	backend.SetShardService(svc, []string{backend.selfAddr})

	stopApply := make(chan struct{})
	go backend.RunApplyLoop(stopApply)

	t.Cleanup(func() {
		// Stop coalesce worker / backstop scan before tearing down DB.
		if backend.coalesceCancel != nil {
			backend.coalesceCancel()
		}
		if backend.coalesce != nil {
			backend.coalesce.Stop()
		}
		close(stopApply)
		node.Close()
		db.Close()
		if closeRaft != nil {
			_ = closeRaft()
		}
	})

	return backend
}

func TestProposalForwardPeersFallsBackToShardServicePeers(t *testing.T) {
	got := proposalForwardPeers(nil, []string{"127.0.0.1:7001", "127.0.0.1:7002"}, "127.0.0.1:7002")
	require.Equal(t, []string{"127.0.0.1:7001"}, got)
}

func TestDistributedBackend_Close(t *testing.T) {
	dir := t.TempDir()

	metaDir := dir + "/meta"
	dbOpts := badgerutil.SmallOptions(metaDir)
	db, err := badger.Open(dbOpts)
	require.NoError(t, err)

	cfg := raft.DefaultConfig("test-node", nil)
	node, closeRaft, err := newRaftNode(cfg, dir)
	require.NoError(t, err)
	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers")
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers")
		},
	)
	node.Start()
	defer node.Close()
	defer func() {
		if closeRaft != nil {
			_ = closeRaft()
		}
	}()

	backend, err := NewDistributedBackend(dir, db, node, nil, false)
	require.NoError(t, err)

	err = backend.Close()
	require.NoError(t, err)
}

func TestSelectPeerByLoad_ReturnsLightestWhenOverloaded(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", RequestsPerSec: 300.0}) // overloaded
	store.Set(NodeStats{NodeID: "node-b", RequestsPerSec: 50.0})
	store.Set(NodeStats{NodeID: "node-c", RequestsPerSec: 80.0})

	peer, ok := selectPeerByLoad(store, "node-a", 1.3)
	require.True(t, ok)
	require.Equal(t, "node-b", peer) // lowest load
}

func TestSelectPeerByLoad_NoRedirectWhenBalanced(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", RequestsPerSec: 100.0})
	store.Set(NodeStats{NodeID: "node-b", RequestsPerSec: 90.0})
	store.Set(NodeStats{NodeID: "node-c", RequestsPerSec: 110.0})

	// median ~100, node-a = 100, threshold 1.3 → 100 <= 100*1.3 → no redirect
	_, ok := selectPeerByLoad(store, "node-a", 1.3)
	require.False(t, ok)
}

func TestSelectPeerByLoad_SingleNode(t *testing.T) {
	store := NewNodeStatsStore(1 * time.Minute)
	store.Set(NodeStats{NodeID: "node-a", RequestsPerSec: 1000.0})

	_, ok := selectPeerByLoad(store, "node-a", 1.3)
	require.False(t, ok, "single node: no peers to redirect to")
}

type mockBucketAssigner struct {
	fn func(ctx context.Context, bucket, groupID string) error
}

func (m *mockBucketAssigner) ProposeBucketAssignment(ctx context.Context, bucket, groupID string) error {
	return m.fn(ctx, bucket, groupID)
}

func TestDistributedBackend_SetBucketAssigner_NilNoPanic(t *testing.T) {
	b := newTestDistributedBackend(t)
	b.SetBucketAssigner(nil)
	require.NoError(t, b.CreateBucket(context.Background(), "photos"))
}

func TestDistributedBackend_CreateBucket_AssignerWithoutRouter_Errors(t *testing.T) {
	b := newTestDistributedBackend(t)
	b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
		return nil
	}})
	// router not set → must return an error, not panic
	err := b.CreateBucket(context.Background(), "photos")
	require.Error(t, err)
	require.Contains(t, err.Error(), "router not configured")
}

func TestDistributedBackend_CreateBucket_CallsAssigner(t *testing.T) {
	b := newTestDistributedBackend(t)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-0", []string{"node-0"}))
	r := NewRouter(mgr)
	r.SetDefault("group-0")
	b.SetRouter(r)

	var calledBucket, calledGroup string
	b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
		calledBucket = bucket
		calledGroup = groupID
		return nil
	}})

	require.NoError(t, b.CreateBucket(context.Background(), "photos"))
	require.Equal(t, "photos", calledBucket)
	require.Equal(t, "group-0", calledGroup)
}

func TestDistributedBackend_CreateBucket_AssignsBeforeStrictRoute(t *testing.T) {
	b := newTestDistributedBackend(t)
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-0", []string{"node-0"}))
	r := NewRouter(mgr)
	r.SetDefault("group-0")
	r.SetRequireExplicitAssignments(true)

	b.SetRouter(r)
	b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-0": {ID: "group-0", PeerIDs: []string{"node-0"}},
	}})
	var assignedBucket, assignedGroup string
	b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
		assignedBucket = bucket
		assignedGroup = groupID
		return nil
	}})

	require.NoError(t, b.CreateBucket(context.Background(), "photos"))
	require.Equal(t, "photos", assignedBucket)
	require.Equal(t, "group-0", assignedGroup)
	r.AssignBucket(assignedBucket, assignedGroup)

	g, err := r.RouteKey("photos", "image.jpg")
	require.NoError(t, err)
	require.Equal(t, "group-0", g.ID())
}

func TestDistributedBackend_CreateBucket_AssignsToWidestECGroup(t *testing.T) {
	b := newTestDistributedBackend(t)
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup("group-1", []string{"node-0"}))
	mgr.Add(NewDataGroup("group-8", []string{"node-0", "node-1", "node-2"}))
	r := NewRouter(mgr)
	r.SetDefault("group-1")
	r.SetRequireExplicitAssignments(true)

	b.SetRouter(r)
	b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-1": {ID: "group-1", PeerIDs: []string{"node-0"}},
		"group-8": {ID: "group-8", PeerIDs: []string{"node-0", "node-1", "node-2"}},
	}})
	var assignedGroup string
	b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
		assignedGroup = groupID
		return nil
	}})

	require.NoError(t, b.CreateBucket(context.Background(), "__grainfs_volumes"))
	require.Equal(t, "group-8", assignedGroup)
}

func TestDistributedBackend_CreateBucket_RouterError_Propagates(t *testing.T) {
	b := newTestDistributedBackend(t)

	// Router with no default and no assignment → RouteKey returns ErrNoGroup
	mgr := NewDataGroupManager()
	r := NewRouter(mgr)
	b.SetRouter(r)
	b.SetBucketAssigner(&mockBucketAssigner{fn: func(ctx context.Context, bucket, groupID string) error {
		return nil
	}})

	err := b.CreateBucket(context.Background(), "photos")
	require.Error(t, err)
}

func TestDistributedBackend_ForceDeleteBucket_DeletesObjectsAndBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	require.NoError(t, b.CreateBucket(ctx, "todelete"))
	_, err := b.PutObject(ctx, "todelete", "a.txt", strings.NewReader("aaa"), "text/plain")
	require.NoError(t, err)
	_, err = b.PutObject(ctx, "todelete", "b.txt", strings.NewReader("bbb"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, b.ForceDeleteBucket(ctx, "todelete"))
	require.ErrorIs(t, b.HeadBucket(ctx, "todelete"), storage.ErrBucketNotFound)
}

func TestDistributedBackend_ForceDeleteBucket_NotFound(t *testing.T) {
	b := newTestDistributedBackend(t)
	err := b.ForceDeleteBucket(context.Background(), "nope")
	require.ErrorIs(t, err, storage.ErrBucketNotFound)
}

func TestDistributedBackend_ForceDeleteBucket_CtxCancelledPropagates(t *testing.T) {
	// 취소된 ctx로 호출하면 HeadBucket 단계에서 곧바로 ctx 에러를 반환해야 한다.
	// (propose 내부에서 context.Background()를 쓰지 않음을 확인)
	b := newTestDistributedBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel
	err := b.ForceDeleteBucket(ctx, "any")
	require.Error(t, err)
}

func TestDistributedBackend_ForceDeleteBucket_MultiVersion(t *testing.T) {
	// 같은 키를 여러 번 PutObject하면 versioned obj: 키가 여러 개 생긴다.
	// WalkObjects는 최신 버전만 반환하므로 이전 버전 키가 남아 DeleteBucket이
	// ErrBucketNotEmpty를 반환하는 회귀를 방지한다.
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	require.NoError(t, b.CreateBucket(ctx, "mv-bucket"))
	for i := range 3 {
		_, err := b.PutObject(ctx, "mv-bucket", "doc.txt", strings.NewReader(fmt.Sprintf("v%d", i)), "text/plain")
		require.NoError(t, err)
	}

	require.NoError(t, b.ForceDeleteBucket(ctx, "mv-bucket"))
	require.ErrorIs(t, b.HeadBucket(ctx, "mv-bucket"), storage.ErrBucketNotFound)
}

func TestDistributedBackend_ForceDeleteBucket_SlashKeyAndVersionedPrefix(t *testing.T) {
	// 버킷에 슬래시 포함 키 "dir/file"(비버전)과 그 접두사인 키 "dir"(버전)이
	// 함께 존재할 때, latMap 충돌로 "dir/file"이 key="dir" versionID="file"로
	// 잘못 분류되지 않고, 두 키 모두 정상 삭제되어야 한다.
	b := newTestDistributedBackend(t)
	ctx := context.Background()

	require.NoError(t, b.CreateBucket(ctx, "slash-bucket"))
	// versioned key "dir"
	_, err := b.PutObject(ctx, "slash-bucket", "dir", strings.NewReader("versioned"), "text/plain")
	require.NoError(t, err)
	// legacy unversioned key "dir/file" (put before PutObject started versioning)
	_, err = b.PutObject(ctx, "slash-bucket", "dir/file", strings.NewReader("nested"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, b.ForceDeleteBucket(ctx, "slash-bucket"))
	require.ErrorIs(t, b.HeadBucket(ctx, "slash-bucket"), storage.ErrBucketNotFound)
}
