package chaos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCluster_ThreeNodeElectsLeader(t *testing.T) {
	cluster := NewCluster(t, 3)
	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader, "no leader elected")

	// Exactly one leader.
	leaderCount := 0
	for _, n := range cluster.Nodes() {
		if n.ID() == leader.ID() {
			leaderCount++
		}
	}
	assert.Equal(t, 1, leaderCount, "exactly one leader")
}

func TestCluster_RestartLeaderElectsNew(t *testing.T) {
	cluster := NewCluster(t, 3)
	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)
	oldLeaderID := leader.ID()

	cluster.RestartNode(oldLeaderID)

	require.Eventually(t, func() bool {
		return cluster.CurrentLeader() != nil
	}, 5*time.Second, 50*time.Millisecond, "no leader after restart")
}

// TestCluster_WaitForLeader_ReturnsNilOnTimeout exercises the timeout path of
// WaitForLeader: a brand-new cluster with no nodes started must return nil.
func TestCluster_WaitForLeader_ReturnsNilOnTimeout(t *testing.T) {
	cluster := NewCluster(t, 3)
	// Deliberately do not call StartAll — no election can happen.
	got := cluster.WaitForLeader(50 * time.Millisecond)
	assert.Nil(t, got, "WaitForLeader must return nil when timeout elapses")
}

// TestCluster_DriverWrappers exercises the one-line Cluster delegate methods
// (PartitionPeer, HealPartition, DropMessage) via the underlying transport.
func TestCluster_DriverWrappers(t *testing.T) {
	cluster := NewCluster(t, 2)
	cluster.StartAll()

	require.Eventually(t, func() bool {
		return cluster.CurrentLeader() != nil
	}, 5*time.Second, 50*time.Millisecond)

	ids := cluster.NodeIDs()
	require.Len(t, ids, 2)
	a, b := ids[0], ids[1]

	// PartitionPeer: messages to/from a must be blocked.
	cluster.PartitionPeer(a)
	assert.False(t, cluster.transport.shouldDeliver(a, b), "partition: a→b blocked")
	assert.False(t, cluster.transport.shouldDeliver(b, a), "partition: b→a blocked")

	// HealPartition: messages must be unblocked.
	cluster.HealPartition(a)
	assert.True(t, cluster.transport.shouldDeliver(a, b), "heal: a→b restored")
	assert.True(t, cluster.transport.shouldDeliver(b, a), "heal: b→a restored")

	// DropMessage: next message a→b dropped, b→a unaffected.
	cluster.DropMessage(a, b, 1)
	assert.False(t, cluster.transport.shouldDeliver(a, b), "drop counter: a→b blocked")
	assert.True(t, cluster.transport.shouldDeliver(b, a), "drop counter: b→a unaffected")
}
