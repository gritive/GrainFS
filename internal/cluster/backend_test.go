package cluster

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/raft"
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
	svc := NewShardService(backend.root, nil, withTestWAL(t))
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
		if backend.shardSvc != nil {
			_ = backend.shardSvc.Close()
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
