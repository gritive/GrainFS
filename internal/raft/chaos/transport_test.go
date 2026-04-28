package chaos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

func TestChaosTransport_HappyPathRequestVote(t *testing.T) {
	tr := NewChaosTransport()

	cfgA := raft.DefaultConfig("A", []string{"B"})
	cfgB := raft.DefaultConfig("B", []string{"A"})
	a := raft.NewNode(cfgA)
	b := raft.NewNode(cfgB)
	t.Cleanup(func() { a.Close(); b.Close() })

	tr.Register(a)
	tr.Register(b)
	tr.Wire(a)
	tr.Wire(b)

	a.Start()
	b.Start()

	// Wait for a leader to be elected via real RequestVote/AppendEntries flow.
	require.Eventually(t, func() bool {
		return a.IsLeader() || b.IsLeader()
	}, 5*time.Second, 50*time.Millisecond, "no leader elected through ChaosTransport")

	// Both nodes must agree on which is leader.
	leaderID := a.LeaderID()
	if leaderID == "" {
		leaderID = b.LeaderID()
	}
	assert.NotEmpty(t, leaderID)
	assert.Equal(t, leaderID, a.LeaderID(), "A must know the leader")
	assert.Equal(t, leaderID, b.LeaderID(), "B must know the leader")
}
