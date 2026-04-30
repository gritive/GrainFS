package cluster

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// newTestGroupBackend wires a single-voter GroupBackend backed by fresh
// BadgerDB + raft.Node. Mirrors newTestDistributedBackend (single-node leader).
func newTestGroupBackend(t testing.TB, groupID string) *GroupBackend {
	t.Helper()
	dir := t.TempDir()

	metaDir := dir + "/meta"
	db, err := badger.Open(badger.DefaultOptions(metaDir).WithLogger(nil))
	require.NoError(t, err)

	raftDir := dir + "/raft"
	logStore, err := raft.NewBadgerLogStore(raftDir)
	require.NoError(t, err)

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

	// Wait for leader election
	for range 200 {
		if node.State() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, raft.Leader, node.State(), "no-peers node must become leader")

	gb, err := NewGroupBackend(GroupBackendConfig{
		ID:      groupID,
		Root:    dir,
		DB:      db,
		Node:    node,
		PeerIDs: []string{"test-node"},
	})
	require.NoError(t, err)

	stopApply := make(chan struct{})
	go gb.RunApplyLoop(stopApply)

	t.Cleanup(func() {
		close(stopApply)
		_ = gb.Close()
		logStore.Close()
	})

	return gb
}

func TestGroupBackend_ID(t *testing.T) {
	gb := newTestGroupBackend(t, "group-7")
	require.Equal(t, "group-7", gb.ID())
}

func TestGroupBackend_PutGetRoundTrip(t *testing.T) {
	gb := newTestGroupBackend(t, "group-rt")

	require.NoError(t, gb.CreateBucket("b1"))

	body := []byte("hello-world")
	_, err := gb.PutObject("b1", "k1", bytes.NewReader(body), "text/plain")
	require.NoError(t, err)

	r, _, err := gb.GetObject("b1", "k1")
	require.NoError(t, err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.True(t, bytes.Equal(body, got), "round-trip body mismatch: got %q want %q", got, body)
}

func TestGroupBackend_ListBuckets(t *testing.T) {
	gb := newTestGroupBackend(t, "group-l")
	for _, b := range []string{"a", "b", "c"} {
		require.NoError(t, gb.CreateBucket(b))
	}
	got, err := gb.ListBuckets()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b", "c"}, got)
	require.NotContains(t, strings.Join(got, ","), "other-group-bucket")
}

func TestGroupBackend_CloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir + "/meta").WithLogger(nil))
	require.NoError(t, err)
	logStore, err := raft.NewBadgerLogStore(dir + "/raft")
	require.NoError(t, err)
	t.Cleanup(func() { logStore.Close() })

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
