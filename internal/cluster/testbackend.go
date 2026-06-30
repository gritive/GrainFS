package cluster

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/metastore"
	"github.com/gritive/GrainFS/internal/raft"
)

type singletonBackendTestTB interface {
	Helper()
	Cleanup(func())
	TempDir() string
	Fatalf(format string, args ...interface{})
}

// directFSMMetaBucketStore is a MetaBucketStore implementation for tests and
// single-node tooling. It applies bucket-mutation commands directly to a local
// MetaFSM (the meta-raft layer). Record / RecordLinearized / AllRecords read
// from the MetaFSM so HeadBucket, GetBucketVersioning, GetBucketPolicy, and
// ListBuckets all see consistent state.
//
// Task 12: the group-0 dual-write path is removed. Bucket control-plane is now
// entirely in meta-raft; all group-0 bucket applies are no-ops.
type directFSMMetaBucketStore struct {
	meta *MetaFSM
}

// newDirectFSMMetaBucketStore returns a MetaBucketStore that applies mutations
// directly to a fresh MetaFSM. Safe for tests and single-node server starts
// where no meta-Raft quorum is available.
func newDirectFSMMetaBucketStore(_ *FSM) MetaBucketStore {
	return &directFSMMetaBucketStore{meta: NewMetaFSM()}
}

func (s *directFSMMetaBucketStore) CreateBucket(_ context.Context, bucket, groupID string, bypassReserved bool) error {
	// MetaFSM.applyCreateBucket requires non-empty groupID; use "local" when
	// unset (tests / single-node paths that don't assign a data group).
	if groupID == "" {
		groupID = "local"
	}
	metaRaw, err := encodeMetaCreateBucketCmd(bucket, groupID, bypassReserved)
	if err != nil {
		return err
	}
	cmd, err := encodeMetaCmd(MetaCmdTypeCreateBucket, metaRaw)
	if err != nil {
		return err
	}
	return s.meta.applyCmd(cmd)
}

func (s *directFSMMetaBucketStore) DeleteBucket(_ context.Context, bucket string) error {
	metaRaw, err := encodeMetaDeleteBucketCmd(bucket)
	if err != nil {
		return err
	}
	cmd, err := encodeMetaCmd(MetaCmdTypeDeleteBucket, metaRaw)
	if err != nil {
		return err
	}
	return s.meta.applyCmd(cmd)
}

func (s *directFSMMetaBucketStore) SetVersioning(_ context.Context, bucket, state string) error {
	metaRaw, err := encodeMetaSetBucketVersioningCmd(bucket, state)
	if err != nil {
		return err
	}
	cmd, err := encodeMetaCmd(MetaCmdTypeSetBucketVersioning, metaRaw)
	if err != nil {
		return err
	}
	return s.meta.applyCmd(cmd)
}

func (s *directFSMMetaBucketStore) SetPolicy(_ context.Context, bucket string, policy []byte) error {
	metaRaw, err := encodeMetaSetBucketPolicyCmd(bucket, append([]byte(nil), policy...))
	if err != nil {
		return err
	}
	cmd, err := encodeMetaCmd(MetaCmdTypeSetBucketPolicy, metaRaw)
	if err != nil {
		return err
	}
	return s.meta.applyCmd(cmd)
}

func (s *directFSMMetaBucketStore) DeletePolicy(_ context.Context, bucket string) error {
	metaRaw, err := encodeMetaDeleteBucketPolicyCmd(bucket)
	if err != nil {
		return err
	}
	cmd, err := encodeMetaCmd(MetaCmdTypeDeleteBucketPolicy, metaRaw)
	if err != nil {
		return err
	}
	return s.meta.applyCmd(cmd)
}

func (s *directFSMMetaBucketStore) Record(bucket string) (BucketRecord, bool) {
	return s.meta.BucketRecord(bucket)
}

func (s *directFSMMetaBucketStore) RecordLinearized(_ context.Context, bucket string) (BucketRecord, bool, error) {
	rec, ok := s.meta.BucketRecord(bucket)
	return rec, ok, nil
}

func (s *directFSMMetaBucketStore) AllRecords() map[string]BucketRecord {
	return s.meta.AllBucketRecords()
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
// singletonTestShardGroupSource is a fixed single-group ShardGroupSource used by
// NewSingletonBackendForTest so the streaming chunked PUT path has a shard group
// without a real cluster boot. The group's peers are all the local node.
type singletonTestShardGroupSource struct {
	group ShardGroupEntry
}

func newSingletonTestShardGroupSource(selfAddr string, numShards int) singletonTestShardGroupSource {
	if numShards < 1 {
		numShards = 1
	}
	peers := make([]string, numShards)
	for i := range peers {
		peers[i] = selfAddr
	}
	return singletonTestShardGroupSource{group: ShardGroupEntry{ID: "group-0", PeerIDs: peers}}
}

func (s singletonTestShardGroupSource) ShardGroup(id string) (ShardGroupEntry, bool) {
	if id == s.group.ID {
		return s.group, true
	}
	return ShardGroupEntry{}, false
}

func (s singletonTestShardGroupSource) ShardGroups() []ShardGroupEntry {
	return []ShardGroupEntry{s.group}
}

func NewSingletonBackendForTest(t singletonBackendTestTB) *DistributedBackend {
	t.Helper()
	dir := t.TempDir()

	// Phase 6.5: metadata goes to an in-memory MetadataStore — the contract's
	// conformance suite pins it to badger semantics, so package tests get the
	// same behavior without cluster touching badger.
	store := metastore.NewMemStore()

	cfg := raft.DefaultConfig("test-node", nil)
	node, closeFn, err := newRaftNode(cfg, dir)
	if err != nil {
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

	backend, err := NewDistributedBackend(dir, store, node, nil, false)
	if err != nil {
		t.Fatalf("NewDistributedBackend: %v", err)
	}

	backend.SetECConfig(ECConfig{DataShards: 1, ParityShards: 0})
	clusterID := bytes.Repeat([]byte{0x42}, 16)
	keeper, kErr := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x91}, encrypt.KEKSize), clusterID)
	if kErr != nil {
		t.Fatalf("test DEK keeper: %v", kErr)
	}
	svc := NewShardService(backend.root, nil, WithShardDEKKeeper(keeper, clusterID))
	backend.SetShardService(svc, []string{backend.selfAddr})
	// Wire a single-node ShardGroupSource so the streaming chunked PUT path (which
	// requires a non-nil shard group — production always wires one) works without a
	// real cluster boot. Sized to the current EC stripe (1+0 here); callers that
	// widen the EC config must re-wire a group of matching width.
	backend.SetShardGroupSource(newSingletonTestShardGroupSource(backend.selfAddr, backend.currentECConfig().NumShards()))
	// Wire the direct-FSM MetaBucketStore so all bucket-write paths work without
	// a real meta-Raft cluster. Bucket mutations are applied directly to the
	// local group-0 FSM, keeping read paths (HeadBucket, etc.) consistent.
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
		if closeFn != nil {
			_ = closeFn()
		}
	})

	return backend
}
