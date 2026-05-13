package cluster

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestRaftNode_SetInstallSnapshotTransport_V2 asserts the setter is reachable
// through the cluster.RaftNode interface and persists no error on the v2
// adapter. Behavioral coverage is in raftv2_meta_quic_test.go (M6.2.2).
func TestRaftNode_SetInstallSnapshotTransport_V2(t *testing.T) {
	rcfg := raft.DefaultConfig("n1", nil)
	node, closeFn, err := newRaftNode(rcfg, t.TempDir())
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

func TestV2TransportBridge_SendInstallSnapshotPreservesLearnerSuffrage(t *testing.T) {
	bridge := &raftTransportBridge{}
	fn := installSnapshotFn(func(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
		require.Equal(t, "n2", peer)
		require.ElementsMatch(t, []raft.Server{
			{ID: "n1", Suffrage: raft.Voter},
			{ID: "n3", Suffrage: raft.NonVoter},
		}, args.Servers)
		return &raft.InstallSnapshotReply{Term: args.Term}, nil
	})
	bridge.sendIS.Store(&fn)

	reply, err := bridge.SendInstallSnapshot("n2", &raft.InstallSnapshotArgs{
		Term:          7,
		LeaderID:      "n1",
		Configuration: []string{"n1"},
		Learners:      map[string]string{"n3": "addr3"},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(7), reply.Term)
}

func TestRaftV2Node_HandleInstallSnapshotPreservesLearnerSuffrage(t *testing.T) {
	inner, err := raft.NewNode(raft.Config{ID: "n1", Peers: []string{"n2"}})
	require.NoError(t, err)
	node := newRaftNodeAdapter(inner)
	node.Start()
	t.Cleanup(node.Close)
	go func() {
		for range node.ApplyCh() {
		}
	}()

	reply := node.HandleInstallSnapshot(&raft.InstallSnapshotArgs{
		Term:              1,
		LeaderID:          "leader",
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Servers: []raft.Server{
			{ID: "n1", Suffrage: raft.Voter},
			{ID: "n3", Suffrage: raft.NonVoter},
		},
		Data: []byte("snapshot"),
	})
	require.Equal(t, uint64(1), reply.Term)

	require.Eventually(t, func() bool {
		cfg := node.Configuration()
		if len(cfg.Servers) != 2 {
			return false
		}
		byID := make(map[string]raft.ServerSuffrage, len(cfg.Servers))
		for _, srv := range cfg.Servers {
			byID[srv.ID] = srv.Suffrage
		}
		return byID["n1"] == raft.Voter && byID["n3"] == raft.NonVoter
	}, time.Second, 10*time.Millisecond)
}
