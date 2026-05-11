package raftv2_test

import (
	"testing"

	v2 "github.com/gritive/GrainFS/internal/raft/v2"
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

// TestAPI_MembershipStubsReturnErrNotImplemented covers the surfaces still
// awaiting implementation (AddLearner, PromoteToVoter).
// AddVoter / RemoveVoter are now wired through joint consensus and return
// ErrNotLeader from a non-running follower; their happy paths live in
// membership_test.go.
// TransferLeadership is now implemented; its error cases live in transfer_test.go.
func TestAPI_MembershipStubsReturnErrNotImplemented(t *testing.T) {
	n, err := v2.NewNode(v2.Config{ID: "n1"})
	require.NoError(t, err)
	require.ErrorIs(t, n.AddLearner("n2", "addr"), v2.ErrNotImplemented)
	require.ErrorIs(t, n.PromoteToVoter("n2"), v2.ErrNotImplemented)
}
