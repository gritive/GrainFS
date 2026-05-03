package main

import (
	"context"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func newClusterJoinTestCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "join <peer>", Args: cobra.ExactArgs(1), RunE: runClusterJoin}
	cmd.Flags().String("data-dir", "", "")
	cmd.Flags().String("node-id", "", "")
	cmd.Flags().String("raft-addr", "", "")
	cmd.Flags().String("cluster-key", "", "")
	return cmd
}

func TestRunClusterJoinCallsSharedRealJoinPath(t *testing.T) {
	var got struct {
		called     bool
		peer       string
		dataDir    string
		nodeID     string
		raftAddr   string
		clusterKey string
	}
	prev := runClusterJoinNode
	runClusterJoinNode = func(ctx context.Context, peer, dataDir, nodeID, raftAddr, clusterKey string) error {
		got.called = true
		got.peer = peer
		got.dataDir = dataDir
		got.nodeID = nodeID
		got.raftAddr = raftAddr
		got.clusterKey = clusterKey
		return ctx.Err()
	}
	t.Cleanup(func() { runClusterJoinNode = prev })

	cmd := newClusterJoinTestCommand()
	require.NoError(t, cmd.Flags().Set("data-dir", t.TempDir()))
	require.NoError(t, cmd.Flags().Set("node-id", "node-b"))
	require.NoError(t, cmd.Flags().Set("raft-addr", "127.0.0.1:9002"))
	require.NoError(t, cmd.Flags().Set("cluster-key", "join-key"))

	require.NoError(t, runClusterJoin(cmd, []string{"127.0.0.1:9001"}))
	require.True(t, got.called)
	require.Equal(t, "127.0.0.1:9001", got.peer)
	require.Equal(t, "node-b", got.nodeID)
	require.Equal(t, "127.0.0.1:9002", got.raftAddr)
	require.Equal(t, "join-key", got.clusterKey)
	require.NotEmpty(t, got.dataDir)
}
