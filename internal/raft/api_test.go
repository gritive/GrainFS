package raft_test

import (
	"testing"

	v2 "github.com/gritive/GrainFS/internal/raft"
	"github.com/stretchr/testify/require"
)

func TestAPI_BootstrapIdempotent(t *testing.T) {
	n, err := v2.NewNode(v2.Config{ID: "n1"})
	require.NoError(t, err)
	require.NoError(t, n.Bootstrap())
	require.ErrorIs(t, n.Bootstrap(), v2.ErrAlreadyBootstrapped)
}

func TestAPI_ConfigurationReflectsConfig(t *testing.T) {
	n, err := v2.NewNode(v2.Config{ID: "n1", Peers: []string{"n2", "n3"}})
	require.NoError(t, err)
	cfg := n.Configuration()
	require.Len(t, cfg.Servers, 3)
	require.Equal(t, "n1", cfg.Servers[0].ID)
	require.Equal(t, v2.Voter, cfg.Servers[0].Suffrage)
}

// TestAPI_MembershipNotLeader exercises AddLearner / PromoteToVoter on a
// non-running follower: both surface ErrNotLeader. Happy paths live in
// learner_test.go (M6.0).
func TestAPI_MembershipNotLeader(t *testing.T) {
	n, err := v2.NewNode(v2.Config{ID: "n1"})
	require.NoError(t, err)
	require.ErrorIs(t, n.AddLearner("n2", "addr"), v2.ErrNotLeader)
	require.ErrorIs(t, n.PromoteToVoter("n2"), v2.ErrNotLeader)
}
