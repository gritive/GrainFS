package cluster_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/raft/chaos"
)

// TestVoterMigration_SelfRemovalWithRetry exercises the full self-removal
// path with leadership transfer and verifies the NEW leader retries the
// migration successfully.
//
// Scenario (PR-D S2):
//  1. 3-node chaos cluster, baseline entries written
//  2. Add 4th node (node-3)
//  3. Call MoveReplica(group-0, leader → node-3) with localNodeID == leaderID
//  4. Verify: TransferLeadership called, ErrLeadershipTransferred returned
//  5. Wait for new leader election from remaining voters
//  6. Execute MoveReplica(group-0, node-1 → node-3) on NEW leader
//  7. Verify: Migration completes successfully, node-3 added, old leader removed
//
// This replaces TestVoterMigration_SelfRemoval_E2E removed in PR-K2, which
// tested single-node self-removal via ChangeMembership. The new test validates
// the multi-node retry flow after ErrLeadershipTransferred.
func TestVoterMigration_SelfRemovalWithRetry_E2E(t *testing.T) {
	cl := chaos.NewCluster(t, 3)
	cl.StartAll()

	ctx := context.Background()

	leader := cl.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "leader election timeout")
	oldLeaderID := leader.ID()

	// Write baseline data to confirm cluster is functional
	for i := 0; i < 5; i++ {
		_, err := leader.ProposeWait(ctx, []byte("baseline"))
		require.NoError(t, err)
	}

	// Add 4th node
	node3 := cl.AddNode("node-3")

	// Build address book and ShardGroupUpdater
	allIDs := cl.NodeIDs()
	addrBook := addrBookFromChaos(allIDs)
	sgUpdater := &fakeSGUpdater{}
	dgMgr := cluster.NewDataGroupManager()

	// DataGroup uses original 3 nodes as voters
	dgMgr.Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0", "node-1", "node-2"}, nil))

	// Create executor with localNodeID == oldLeaderID (self-removal scenario)
	exec := cluster.NewDataGroupPlanExecutorForTest(
		oldLeaderID, dgMgr, addrBook, sgUpdater,
		func(dg *cluster.DataGroup) cluster.DataRaftNode {
			return &raftNodeAdapter{n: leader}
		},
	)

	// Step 1: Attempt MoveReplica(leader → node-3) with localNodeID == leaderID
	// This should trigger TransferLeadership and return ErrLeadershipTransferred
	err := exec.MoveReplica(ctx, "group-0", oldLeaderID, "node-3")
	require.ErrorIs(t, err, cluster.ErrLeadershipTransferred,
		"self-removal must return ErrLeadershipTransferred")

	t.Logf("self-removal guard fired: TransferLeadership called, ErrLeadershipTransferred returned")

	// Step 2: Wait for NEW leader to emerge from DATA-RAFT voters
	// (node-3 is meta-Raft only, not part of data-Raft group)
	var newLeader *raft.Node
	require.Eventually(t, func() bool {
		for _, id := range []string{"node-0", "node-1", "node-2"} {
			if n := cl.NodeByID(id); n != nil && n.ID() != oldLeaderID {
				currentLeader := cl.CurrentLeader()
				if currentLeader != nil && currentLeader.ID() == n.ID() {
					newLeader = currentLeader
					return true
				}
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "new leader must be elected from data-raft voters")

	require.NotNil(t, newLeader)
	newLeaderID := newLeader.ID()
	require.NotEqual(t, oldLeaderID, newLeaderID)
	require.Contains(t, []string{"node-0", "node-1", "node-2"}, newLeaderID,
		"new leader must be a data-raft voter, not meta-raft-only node-3")
	t.Logf("new leader elected from data-raft voters: %s", newLeaderID)

	// Step 3: Create executor on NEW leader with localNodeID == newLeaderID
	// Re-use the same fromNode (oldLeaderID) - the plan is still valid
	newExec := cluster.NewDataGroupPlanExecutorForTest(
		newLeaderID, dgMgr, addrBook, sgUpdater,
		func(dg *cluster.DataGroup) cluster.DataRaftNode {
			return &raftNodeAdapter{n: newLeader}
		},
	)

	// Step 4: Retry MoveReplica on NEW leader
	// fromNode = oldLeaderID (no longer leader), toNode = node-3
	err = newExec.MoveReplica(ctx, "group-0", oldLeaderID, "node-3")
	require.NoError(t, err, "retry on new leader must succeed")

	// Step 5: Verify MetaFSM updated with new membership
	require.Len(t, sgUpdater.proposed, 1)
	assert.Equal(t, "group-0", sgUpdater.proposed[0].ID)
	assert.Contains(t, sgUpdater.proposed[0].PeerIDs, "node-3")
	assert.NotContains(t, sgUpdater.proposed[0].PeerIDs, oldLeaderID,
		"old leader must be removed from group membership")

	// Step 6: Verify node-3 caught up with new leader
	assert.Eventually(t, func() bool {
		return node3.CommittedIndex() == newLeader.CommittedIndex()
	}, 5*time.Second, 50*time.Millisecond, "node-3 must catch up to new leader commit index")

	// Step 7: Verify local DataGroupManager updated
	dg := dgMgr.Get("group-0")
	require.NotNil(t, dg)
	assert.Contains(t, dg.PeerIDs(), "node-3")
	assert.NotContains(t, dg.PeerIDs(), oldLeaderID)

	t.Logf("self-removal with retry ok: old leader %s handed off leadership, new leader %s completed migration",
		oldLeaderID, newLeaderID)
}
