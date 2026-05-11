package serveruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/raft"
)

// TestNewRaftV2NodeForServeruntime_DurableStoresAtSubdir verifies the
// serveruntime entry point used by run.go when GRAINFS_RAFT_V2=serveruntime
// is set. The node is constructed against a durable Badger DB at
// <raftDir>/raft-v2/ and returns a closeFn the caller must register on the
// bootState cleanup stack.
func TestNewRaftV2NodeForServeruntime_DurableStoresAtSubdir(t *testing.T) {
	tmp := t.TempDir()
	rcfg := raft.DefaultConfig("srv-node", nil)

	node, closeFn, err := cluster.NewRaftV2NodeForServeruntime(rcfg, tmp)
	require.NoError(t, err)
	require.NotNil(t, node, "v2 node must be constructed")
	require.NotNil(t, closeFn, "closeFn must be non-nil so caller can release the Badger DB")

	// Smoke transport stubs — bootstrap a single-node cluster, propose,
	// drain. The PR 26 dispatch in run.go skips QUIC RPC wiring for v2
	// (deferred to PR 27); this test mirrors that behaviour by using
	// noop transports.
	node.SetTransport(
		func(string, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
			return nil, errNoTransport
		},
		func(string, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
			return nil, errNoTransport
		},
	)
	node.Start()
	t.Cleanup(func() {
		node.Close()
		_ = closeFn()
	})

	require.NoError(t, node.Bootstrap())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for !node.IsLeader() {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for v2 node to become leader")
		case <-time.After(10 * time.Millisecond):
		}
	}

	pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pcancel()
	idx, err := node.ProposeWait(pctx, []byte("serveruntime-smoke"))
	require.NoError(t, err)
	require.Greater(t, idx, uint64(0))

	// The on-disk sub-directory must exist.
	subDir := filepath.Join(tmp, "raft-v2")
	info, err := os.Stat(subDir)
	require.NoError(t, err, "<raftDir>/raft-v2/ must exist after construction")
	require.True(t, info.IsDir())

	// The constructed node must be the v2 adapter, not a v1 *raft.Node.
	// (Type assertion via the cluster.RaftNode interface.)
	_, isV1 := node.(*raft.Node)
	require.False(t, isV1, "serveruntime v2 entry point must not return *raft.Node")
}

// errNoTransport is the sentinel returned by smoke-test transports so the v2
// node observes "no peers reachable" — equivalent to a single-node deployment.
var errNoTransport = stubErr("no transport")

type stubErr string

func (e stubErr) Error() string { return string(e) }
