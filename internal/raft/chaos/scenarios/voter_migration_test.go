package scenarios_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft/chaos"
)

// TestVoterMigration_AddLearnerPromoteRemove verifies the full membership
// migration path on a real 3-node cluster:
//  1. Start a 3-node cluster (node-0, node-1, node-2)
//  2. Write baseline entries to confirm replication
//  3. AddLearner("node-3", "") on the leader — peerKey = nodeID in chaos transport
//  4. Wait for node-3's matchIndex >= committed
//  5. PromoteToVoter("node-3")
//  6. RemoveVoter("node-1") — node-1 evicted from quorum
//  7. Write more entries — cluster must still commit (quorum = 2 of {node-0, node-2, node-3})
func TestVoterMigration_AddLearnerPromoteRemove(t *testing.T) {
	cl := chaos.NewCluster(t, 3)
	cl.StartAll()

	ctx := context.Background()

	leader := cl.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "leader election timeout")

	// Write baseline entries
	for i := 0; i < 5; i++ {
		_, err := leader.ProposeWait(ctx, []byte("entry"))
		require.NoError(t, err)
	}

	// Add 4th node as learner.
	// In chaos transport, nodeID is the transport address, so addr = "" means peerKey = id.
	node3 := cl.AddNode("node-3")
	err := leader.AddLearner(node3.ID(), "")
	require.NoError(t, err)

	// Wait for learner to be tracked and catch up
	committed := leader.CommittedIndex()
	require.Eventually(t, func() bool {
		match, ok := leader.PeerMatchIndex(node3.ID())
		return ok && match >= committed
	}, 10*time.Second, 50*time.Millisecond, "learner did not catch up")

	// Promote to full voter
	require.NoError(t, leader.PromoteToVoter(node3.ID()))

	// Remove node-1 (chaos transport: addr = nodeID)
	node1 := cl.NodeByID("node-1")
	require.NotNil(t, node1)
	require.NoError(t, leader.RemoveVoter(node1.ID()))

	// Cluster must still commit entries with new quorum {node-0, node-2, node-3}
	for i := 0; i < 5; i++ {
		_, err := leader.ProposeWait(ctx, []byte("post-migration"))
		require.NoError(t, err)
	}

	// node-3 must have replicated all entries
	assert.Eventually(t, func() bool {
		match, ok := leader.PeerMatchIndex(node3.ID())
		return ok && match == leader.CommittedIndex()
	}, 5*time.Second, 50*time.Millisecond, "node-3 did not replicate post-migration entries")
}
