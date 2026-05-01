package scenarios

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/raft/chaos"
)

// TestJoint_Chaos_PartitionDuringJoint_ForceAbortClearsManagedLearners covers
// the ForceAbort path from the joint consensus plan: managed learners added by
// ChangeMembership are partitioned during JointEntering, JointLeave cannot form
// C_new quorum, and an operator abort reverts to C_old while removing those
// learners from learnerIDs.
func TestJoint_Chaos_PartitionDuringJoint_ForceAbortClearsManagedLearners(t *testing.T) {
	cl := chaos.NewCluster(t, 3,
		chaos.WithHeartbeatTimeout(200*time.Millisecond),
		chaos.WithElectionTimeout(800*time.Millisecond),
	)
	cl.StartAll()

	leader := cl.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "initial leader")

	node3 := cl.AddNode("node-3")
	node4 := cl.AddNode("node-4")
	require.NotNil(t, node3)
	require.NotNil(t, node4)

	var removes []string
	for _, id := range cl.NodeIDs()[:3] {
		if id != leader.ID() {
			removes = append(removes, id)
		}
	}
	require.Len(t, removes, 2, "test needs two old followers to remove")

	errCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		errCh <- leader.ChangeMembership(ctx,
			[]raft.ServerEntry{
				{ID: "node-3", Address: "node-3"},
				{ID: "node-4", Address: "node-4"},
			},
			removes,
		)
	}()

	require.Eventually(t, func() bool {
		phase, _, _, _ := leader.JointPhase()
		return phase == raft.JointEntering
	}, 5*time.Second, 20*time.Millisecond, "joint phase must enter")

	cl.PartitionPeer("node-3")
	cl.PartitionPeer("node-4")
	defer cl.HealPartition("node-3")
	defer cl.HealPartition("node-4")

	abortCtx, abortCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer abortCancel()
	require.NoError(t, leader.ForceAbortJoint(abortCtx),
		"ForceAbortJoint must commit using C_old quorum while C_new is unavailable")

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, raft.ErrJointAborted,
			"ChangeMembership must report abort after C_new quorum is unavailable")
	case <-time.After(10 * time.Second):
		t.Fatal("ChangeMembership did not return after joint abort timeout")
	}

	require.Eventually(t, func() bool {
		phase, _, _, _ := leader.JointPhase()
		return phase == raft.JointNone
	}, 5*time.Second, 20*time.Millisecond, "joint phase must revert to none")

	learners := leader.LearnerIDs()
	require.NotContains(t, learners, "node-3",
		"managed learner node-3 must be removed from learnerIDs after abort")
	require.NotContains(t, learners, "node-4",
		"managed learner node-4 must be removed from learnerIDs after abort")

	cfg := leader.Configuration()
	servers := make(map[string]raft.ServerSuffrage, len(cfg.Servers))
	for _, s := range cfg.Servers {
		servers[s.ID] = s.Suffrage
	}
	for _, id := range []string{"node-0", "node-1", "node-2"} {
		suffrage, ok := servers[id]
		require.True(t, ok, "C_old voter %s must remain after abort", id)
		require.Equal(t, raft.Voter, suffrage)
	}
	require.NotContains(t, servers, "node-3")
	require.NotContains(t, servers, "node-4")
}
