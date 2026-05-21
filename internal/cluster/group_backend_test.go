package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/raft"
)

// newTestGroupBackend wires a single-voter GroupBackend backed by fresh
// BadgerDB + RaftNode. Mirrors newTestDistributedBackend (single-node leader).
func newTestGroupBackend(t clusterTestTB, groupID string) *GroupBackend {
	t.Helper()
	dir := t.TempDir()

	metaDir := dir + "/meta"
	db, err := badger.Open(badgerutil.SmallOptions(metaDir))
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

	// Wait for leader election
	for range 2000 {
		if node.IsLeader() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.True(t, node.IsLeader(), "no-peers node must become leader")

	svc := NewShardService(dir+"/shards", nil)
	gb, err := NewGroupBackend(GroupBackendConfig{
		ID:       groupID,
		Root:     dir,
		DB:       db,
		Node:     node,
		PeerIDs:  []string{"test-node"},
		ShardSvc: svc,
		EC:       ECConfig{DataShards: 1, ParityShards: 0},
	})
	require.NoError(t, err)

	stopApply := make(chan struct{})
	go gb.RunApplyLoop(stopApply)

	t.Cleanup(func() {
		close(stopApply)
		_ = gb.Close()
		if closeRaft != nil {
			_ = closeRaft()
		}
	})

	return gb
}

func TestGroupBackend_ID(t *testing.T) {
	gb := newTestGroupBackend(t, "group-7")
	require.Equal(t, "group-7", gb.ID())
}

func TestGroupBackend_PutGetRoundTrip(t *testing.T) {
	gb := newTestGroupBackend(t, "group-rt")

	require.NoError(t, gb.CreateBucket(context.Background(), "b1"))

	body := []byte("hello-world")
	_, err := gb.PutObject(context.Background(), "b1", "k1", bytes.NewReader(body), "text/plain")
	require.NoError(t, err)

	r, _, err := gb.GetObject(context.Background(), "b1", "k1")
	require.NoError(t, err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.True(t, bytes.Equal(body, got), "round-trip body mismatch: got %q want %q", got, body)
}

func TestGroupBackend_InternalPutObjectReadableViaReadAt(t *testing.T) {
	gb := newTestGroupBackend(t, "group-internal-readat")

	const bucket = "__grainfs_volumes"
	require.NoError(t, gb.CreateBucket(context.Background(), bucket))

	body := []byte("nbd-block-payload")
	_, err := gb.PutObject(context.Background(), bucket, "__vol/default/blk_000000000000", bytes.NewReader(body), "application/octet-stream")
	require.NoError(t, err)

	buf := make([]byte, len(body))
	n, err := gb.ReadAt(context.Background(), bucket, "__vol/default/blk_000000000000", 0, buf)
	require.NoError(t, err)
	require.Equal(t, len(body), n)
	require.Equal(t, body, buf)
}

func TestGroupBackend_ListBuckets(t *testing.T) {
	gb := newTestGroupBackend(t, "group-l")
	for _, b := range []string{"a", "b", "c"} {
		require.NoError(t, gb.CreateBucket(context.Background(), b))
	}
	got, err := gb.ListBuckets(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b", "c"}, got)
	require.NotContains(t, strings.Join(got, ","), "other-group-bucket")
}

func TestGroupBackend_CloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badgerutil.SmallOptions(dir + "/meta"))
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
	t.Cleanup(func() {
		if closeRaft != nil {
			_ = closeRaft()
		}
	})

	gb, err := NewGroupBackend(GroupBackendConfig{
		ID:   "group-close",
		Root: dir,
		DB:   db,
		Node: node,
	})
	require.NoError(t, err)

	require.NoError(t, gb.Close())
	require.True(t, gb.IsClosed())
	require.NoError(t, gb.Close(), "Close must be idempotent")
}
