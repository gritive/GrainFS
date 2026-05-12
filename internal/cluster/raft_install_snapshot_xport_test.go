package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestRaftNode_SetInstallSnapshotTransport_V2 asserts the setter is reachable
// through the cluster.RaftNode interface and persists no error on the v2
// adapter. Behavioral coverage is in raftv2_meta_quic_test.go (M6.2.2).
func TestRaftNode_SetInstallSnapshotTransport_V2(t *testing.T) {
	rcfg := raft.DefaultConfig("n1", nil)
	node, closeFn, err := newRaftNode(rcfg, nil, t.TempDir())
	require.NoError(t, err)
	node.SetTransport(
		func(string, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) { return nil, nil },
		func(string, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) { return nil, nil },
	)
	node.Start()
	t.Cleanup(func() {
		node.Close()
		if closeFn != nil {
			_ = closeFn()
		}
	})

	called := false
	node.SetInstallSnapshotTransport(func(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
		called = true
		return &raft.InstallSnapshotReply{Term: args.Term}, nil
	})
	_ = called
}
