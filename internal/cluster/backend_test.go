package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgermeta"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/gossip"
	"github.com/gritive/GrainFS/internal/raft"
)

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
	backend, _ := newTestDistributedBackendWithDB(t)
	return backend
}

// newTestDistributedBackendWithDB is newTestDistributedBackend plus the raw
// BadgerDB handle the test opened, for tests that need raw verification or
// corruption injection (Phase 6.5 S6.5-3: DistributedBackend no longer
// exposes its store as *badger.DB).
func newTestDistributedBackendWithDB(t clusterTestTB) (*DistributedBackend, *badger.DB) {
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

	backend, err := NewDistributedBackend(dir, badgermeta.Wrap(db), node, nil, false)
	require.NoError(t, err)

	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 0})
	keeper, clusterID := testDEKKeeper(t)
	svc := NewShardService(backend.root, nil, WithShardDEKKeeper(keeper, clusterID))
	backend.SetShardService(svc, []string{backend.selfAddr})
	// Wire the direct-FSM MetaBucketStore so bucket-write paths work without a
	// real meta-Raft cluster. Bucket mutations are applied directly to the local
	// FSM, keeping the local read paths (HeadBucket, etc.) consistent.
	backend.SetMetaBucketStore(newDirectFSMMetaBucketStore(backend.fsm))

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

	return backend, db
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

	backend, err := NewDistributedBackend(dir, badgermeta.Wrap(db), node, nil, false)
	require.NoError(t, err)

	err = backend.Close()
	require.NoError(t, err)
}

func TestSelectPeerByLoad_ReturnsLightestWhenOverloaded(t *testing.T) {
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	store.Set(gossip.NodeStats{NodeID: "node-a", RequestsPerSec: 300.0}) // overloaded
	store.Set(gossip.NodeStats{NodeID: "node-b", RequestsPerSec: 50.0})
	store.Set(gossip.NodeStats{NodeID: "node-c", RequestsPerSec: 80.0})

	peer, ok := selectPeerByLoad(store, "node-a", 1.3)
	require.True(t, ok)
	require.Equal(t, "node-b", peer) // lowest load
}

func TestSelectPeerByLoad_NoRedirectWhenBalanced(t *testing.T) {
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	store.Set(gossip.NodeStats{NodeID: "node-a", RequestsPerSec: 100.0})
	store.Set(gossip.NodeStats{NodeID: "node-b", RequestsPerSec: 90.0})
	store.Set(gossip.NodeStats{NodeID: "node-c", RequestsPerSec: 110.0})

	// median ~100, node-a = 100, threshold 1.3 → 100 <= 100*1.3 → no redirect
	_, ok := selectPeerByLoad(store, "node-a", 1.3)
	require.False(t, ok)
}

func TestSelectPeerByLoad_SingleNode(t *testing.T) {
	store := gossip.NewNodeStatsStore(1 * time.Minute)
	store.Set(gossip.NodeStats{NodeID: "node-a", RequestsPerSec: 1000.0})

	_, ok := selectPeerByLoad(store, "node-a", 1.3)
	require.False(t, ok, "single node: no peers to redirect to")
}
