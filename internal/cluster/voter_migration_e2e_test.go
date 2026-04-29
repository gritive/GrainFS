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

// chaosNodeAddrBook wraps a chaos.Cluster as a NodeAddressBook.
// In chaos transport, nodeID is the "address"; no separate QUIC port.
type chaosNodeAddrBook struct {
	nodes []cluster.MetaNodeEntry
}

func (b *chaosNodeAddrBook) Nodes() []cluster.MetaNodeEntry { return b.nodes }

// addrBookFromChaos builds MetaNodeEntries from the cluster's current node IDs.
// addr == id because the chaos transport uses nodeID as the routing key.
func addrBookFromChaos(ids []string) *chaosNodeAddrBook {
	entries := make([]cluster.MetaNodeEntry, len(ids))
	for i, id := range ids {
		entries[i] = cluster.MetaNodeEntry{ID: id, Address: id}
	}
	return &chaosNodeAddrBook{nodes: entries}
}

// raftNodeAdapter wraps *raft.Node to satisfy cluster.DataRaftNode while using
// nodeID as both the learner ID and peerKey (addr="" → peerKey=id rule).
type raftNodeAdapter struct{ n *raft.Node }

func (a *raftNodeAdapter) IsLeader() bool         { return a.n.IsLeader() }
func (a *raftNodeAdapter) CommittedIndex() uint64 { return a.n.CommittedIndex() }
func (a *raftNodeAdapter) PeerMatchIndex(pk string) (uint64, bool) {
	return a.n.PeerMatchIndex(pk)
}
func (a *raftNodeAdapter) AddLearner(id, _ string) error  { return a.n.AddLearner(id, "") }
func (a *raftNodeAdapter) PromoteToVoter(id string) error { return a.n.PromoteToVoter(id) }
func (a *raftNodeAdapter) RemoveVoter(id string) error    { return a.n.RemoveVoter(id) }

// TestVoterMigration_ViaDataGroupPlanExecutor is an end-to-end test that
// exercises the full DataGroupPlanExecutor.MoveReplica path with real raft.Node
// objects (in-process chaos transport, no fakes):
//
//  1. 3-node chaos cluster, write baseline entries
//  2. Register a 4th node, then call MoveReplica(group-0, node-1→node-3)
//  3. Verify node-3 is a voter, node-1 is evicted, cluster still commits
func TestVoterMigration_ViaDataGroupPlanExecutor(t *testing.T) {
	cl := chaos.NewCluster(t, 3)
	cl.StartAll()

	ctx := context.Background()

	leader := cl.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "leader election timeout")

	// Write baseline data
	for i := 0; i < 5; i++ {
		_, err := leader.ProposeWait(ctx, []byte("baseline"))
		require.NoError(t, err)
	}

	// Register 4th node
	node3 := cl.AddNode("node-3")

	// Build executor wiring — NodeIDs() already includes node-3 after AddNode.
	allIDs := cl.NodeIDs()
	addrBook := addrBookFromChaos(allIDs)
	sgUpdater := &fakeSGUpdater{}
	dgMgr := cluster.NewDataGroupManager()

	// DataGroup "group-0" uses node-0, node-1, node-2 as voters
	dgMgr.Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0", "node-1", "node-2"}, nil))

	// Inject real raft.Node via adapter so executor uses the real Raft state machine
	exec := cluster.NewDataGroupPlanExecutorForTest(
		dgMgr, addrBook, sgUpdater,
		func(dg *cluster.DataGroup) cluster.DataRaftNode {
			return &raftNodeAdapter{n: leader}
		},
	)

	// Execute voter migration: node-1 → node-3
	err := exec.MoveReplica(ctx, "group-0", "node-1", "node-3")
	require.NoError(t, err)

	// MetaFSM notified with new membership
	require.Len(t, sgUpdater.proposed, 1)
	assert.Equal(t, "group-0", sgUpdater.proposed[0].ID)
	assert.Contains(t, sgUpdater.proposed[0].PeerIDs, "node-3")
	assert.NotContains(t, sgUpdater.proposed[0].PeerIDs, "node-1")

	// node-3 must be fully caught up
	require.Eventually(t, func() bool {
		match, ok := leader.PeerMatchIndex("node-3")
		return ok && match == leader.CommittedIndex()
	}, 10*time.Second, 50*time.Millisecond, "node-3 not caught up")

	// node-3 and node-0 / node-2 form the new quorum; cluster must still commit
	for i := 0; i < 5; i++ {
		_, err := leader.ProposeWait(ctx, []byte("post-migration"))
		require.NoError(t, err)
	}

	// DataGroupManager updated locally
	dg := dgMgr.Get("group-0")
	require.NotNil(t, dg)
	assert.Contains(t, dg.PeerIDs(), "node-3")
	assert.NotContains(t, dg.PeerIDs(), "node-1")

	// node-3 node is live and has the latest entries
	assert.Eventually(t, func() bool {
		return node3.CommittedIndex() == leader.CommittedIndex()
	}, 5*time.Second, 50*time.Millisecond, "node-3 committed index diverged")
}
