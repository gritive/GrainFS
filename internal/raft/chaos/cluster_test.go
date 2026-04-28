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
