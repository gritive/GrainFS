package scenarios

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft/chaos"
)

// TestLeaderIsolation_CheckQuorumStepsDown verifies that an isolated leader
// (cannot reach majority of followers) voluntarily steps down within a
// bounded time, so that minority-side clients see ErrNotLeader instead of
// stale-but-accepted writes that will never commit.
//
// Without CheckQuorum (current state, PR 1a closes this): the isolated
// leader stays in the Leader state indefinitely, accepting Propose calls
// that block forever waiting for replication.
//
// With CheckQuorum (PR 1a): the leader counts heartbeat responses. If it
// fails to hear from a majority within N rounds (default N=3), it steps
// down to Follower.
//
// Acceptance for PR 1a: remove the t.Skip line; this test must pass.
func TestLeaderIsolation_CheckQuorumStepsDown(t *testing.T) {

	cluster := chaos.NewCluster(t, 3)
	cluster.StartAll()

	leader := cluster.WaitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Isolate the leader — no follower can hear it, it can hear no follower.
	cluster.PartitionPeer(leader.ID())

	// CheckQuorum should fire after ~3 heartbeat rounds (HeartbeatTimeout
	// = 50ms in NewCluster, so ~150ms). Allow generous slack.
	require.Eventually(t, func() bool {
		return !leader.IsLeader()
	}, 2*time.Second, 50*time.Millisecond,
		"isolated leader must step down via CheckQuorum")
}
