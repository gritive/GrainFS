package cluster

import (
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/raft"
)

type singletonBackendTestTB interface {
	Helper()
	Cleanup(func())
	TempDir() string
	Fatalf(format string, args ...interface{})
}

// NewSingletonBackendForTest spins up a DistributedBackend as a one-node
// Raft cluster with no peers, suitable for package tests in other modules
// that previously used ECBackend. The returned backend runs its apply loop
// in a goroutine cancelled on t.Cleanup.
//
// Exported (despite its "ForTest" feel) because Go cannot import _test.go
// helpers from another package. Callers outside of test context should
// construct DistributedBackend directly.
//
// As of M5 PR 29 the GRAINFS_RAFT_V2 flag is gone; this helper always
// instantiates a v2 raft node via newRaftNode.
func NewSingletonBackendForTest(t singletonBackendTestTB) *DistributedBackend {
	t.Helper()
	dir := t.TempDir()

	metaDir := dir + "/meta"
	db, err := badger.Open(badgerutil.SmallOptions(metaDir))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}

	cfg := raft.DefaultConfig("test-node", nil)
	node, closeFn, err := newRaftNode(cfg, dir)
	if err != nil {
		db.Close()
		t.Fatalf("newRaftNode: %v", err)
	}
	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers")
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers")
		},
	)
	node.Start()
	if err := node.Bootstrap(); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	for range 200 {
		if node.IsLeader() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !node.IsLeader() {
		t.Fatalf("no-peers node must become leader")
	}

	backend, err := NewDistributedBackend(dir, db, node, nil, false)
	if err != nil {
		t.Fatalf("NewDistributedBackend: %v", err)
	}

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
		if closeFn != nil {
			_ = closeFn()
		}
		db.Close()
	})

	return backend
}
