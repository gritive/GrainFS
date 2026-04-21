package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/raft"
)

// NewSingletonBackendForTest spins up a DistributedBackend as a one-node
// Raft cluster with no peers, suitable for package tests in other modules
// that previously used ECBackend. The returned backend runs its apply loop
// in a goroutine cancelled on t.Cleanup.
//
// Exported (despite its "ForTest" feel) because Go cannot import _test.go
// helpers from another package. Callers outside of test context should
// construct DistributedBackend directly.
func NewSingletonBackendForTest(t *testing.T) *DistributedBackend {
	t.Helper()
	dir := t.TempDir()

	metaDir := dir + "/meta"
	db, err := badger.Open(badger.DefaultOptions(metaDir).WithLogger(nil))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}

	raftDir := dir + "/raft"
	logStore, err := raft.NewBadgerLogStore(raftDir)
	if err != nil {
		db.Close()
		t.Fatalf("open raft store: %v", err)
	}

	cfg := raft.DefaultConfig("test-node", nil)
	node := raft.NewNode(cfg, logStore)
	node.SetTransport(
		func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, fmt.Errorf("no peers")
		},
		func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, fmt.Errorf("no peers")
		},
	)
	node.Start()

	for range 200 {
		if node.State() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if node.State() != raft.Leader {
		t.Fatalf("no-peers node must become leader")
	}

	backend, err := NewDistributedBackend(dir, db, node)
	if err != nil {
		t.Fatalf("NewDistributedBackend: %v", err)
	}

	stopApply := make(chan struct{})
	go backend.RunApplyLoop(stopApply)

	t.Cleanup(func() {
		close(stopApply)
		node.Stop()
		db.Close()
		logStore.Close()
	})

	return backend
}
