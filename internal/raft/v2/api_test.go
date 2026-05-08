package raftv2_test

import (
	"context"
	"testing"

	v2 "github.com/gritive/GrainFS/internal/raft/v2"
	"github.com/stretchr/testify/require"
)

func TestAPI_BootstrapIdempotent(t *testing.T) {
	n := v2.NewNode(v2.Config{ID: "n1"})
	require.NoError(t, n.Bootstrap())
	require.ErrorIs(t, n.Bootstrap(), v2.ErrAlreadyBootstrapped)
}

func TestAPI_ConfigurationReflectsConfig(t *testing.T) {
	n := v2.NewNode(v2.Config{ID: "n1", Peers: []string{"n2", "n3"}})
	cfg := n.Configuration()
	require.Len(t, cfg.Servers, 3)
	require.Equal(t, "n1", cfg.Servers[0].ID)
	require.Equal(t, v2.Voter, cfg.Servers[0].Suffrage)
}

func TestAPI_MembershipStubsReturnErrNotImplemented(t *testing.T) {
	n := v2.NewNode(v2.Config{ID: "n1"})
	require.ErrorIs(t, n.AddVoter("n2", "addr"), v2.ErrNotImplemented)
	require.ErrorIs(t, n.AddVoterCtx(context.Background(), "n2", "addr"), v2.ErrNotImplemented)
	require.ErrorIs(t, n.RemoveVoter("n2"), v2.ErrNotImplemented)
	require.ErrorIs(t, n.AddLearner("n2", "addr"), v2.ErrNotImplemented)
	require.ErrorIs(t, n.PromoteToVoter("n2"), v2.ErrNotImplemented)
	require.ErrorIs(t, n.TransferLeadership(), v2.ErrNotImplemented)
}
