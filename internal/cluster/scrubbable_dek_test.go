package cluster

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgermeta"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
)

// newTestDistributedBackendDEK mirrors newTestDistributedBackend but wires the
// ShardService with WithShardDEKKeeper — the production shape after PR #631.
// This reproduces the at-rest hole the scrubber-repair fix closes: the pre-fix
// path would have returned plaintext under this shape.
func newTestDistributedBackendDEK(t *testing.T, extraSvcOpts ...ShardServiceOption) *DistributedBackend {
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
	kek := bytes.Repeat([]byte{0x77}, encrypt.KEKSize)
	clusterID := bytes.Repeat([]byte{0x78}, 16)
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err)
	svc := NewShardService(backend.root, nil, append([]ShardServiceOption{WithShardDEKKeeper(keeper, clusterID)}, extraSvcOpts...)...)
	require.NotNil(t, svc.DEKKeeper(), "production shape: DEK keeper must be wired")
	backend.SetShardService(svc, []string{backend.selfAddr})
	wireTestShardGroup(backend)

	stopApply := make(chan struct{})
	go backend.RunApplyLoop(stopApply)

	t.Cleanup(func() {
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

// TestWriteShardSealsRepairedShardWithDEK asserts the scrubber-repair write path
// seals shards as GFSENC3 ciphertext via the DEK seam, not plaintext — closing
// the P1 at-rest hole where the pre-fix path returned plaintext (encryptor nil).
func TestWriteShardSealsRepairedShardWithDEK(t *testing.T) {
	b := newTestDistributedBackendDEK(t) // production-shaped: WithShardDEKKeeper, encryptor nil
	bucket, key, versionID := "bkt", "obj", "v0000000000000001"
	total := b.currentECConfig().NumShards()
	// ShardPaths returns the real data-dir paths the scrubber/repair uses; the
	// embedded shardKey is key+"/"+versionID (what shardServiceKeyFromPath reverses).
	path := b.ShardPaths(bucket, key, versionID, total)[0]

	plain := bytes.Repeat([]byte("repaired-shard-data"), 500)
	require.NoError(t, b.WriteShard(bucket, key, versionID, 0, path, plain))

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, eccodec.IsEncryptedShard(raw), "repaired shard must be GFSENC3 ciphertext")
	require.False(t, bytes.Contains(raw, plain[:64]), "plaintext run must not be on disk")

	// Reads back through the integrity path (ReadLocalShard, GFSENC3-aware).
	got, err := b.ReadShard(bucket, key, versionID, 0, path)
	require.NoError(t, err)
	require.Equal(t, plain, got)
}
