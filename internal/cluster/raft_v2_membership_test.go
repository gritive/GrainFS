package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	raftv2 "github.com/gritive/GrainFS/internal/raft/v2"
)

// newV2LeaderForMembership builds a single-node v2 RaftNode (via raftV2Node
// adapter), bootstraps it, and waits for it to elect itself leader.
func newV2LeaderForMembership(t *testing.T) RaftNode {
	t.Helper()
	t.Setenv("GRAINFS_RAFT_V2", "cluster")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	rcfg := raft.DefaultConfig("membership-n1", nil)
	node, err := newRaftNode(rcfg, nil)
	require.NoError(t, err)
	node.SetTransport(noopRV, noopAE)
	node.Start()
	t.Cleanup(node.Close)
	go func() {
		for range node.ApplyCh() {
		}
	}()

	if err := node.Bootstrap(); err != nil && !errors.Is(err, raftv2.ErrAlreadyBootstrapped) {
		t.Fatalf("Bootstrap: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for !node.IsLeader() {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for v2 node to become leader")
		case <-time.After(10 * time.Millisecond):
		}
	}
	return node
}

// newV2FollowerForMembership builds a v2 RaftNode that stays Follower because
// it has peers configured (requires quorum to elect a leader) and uses noop
// transport (so no election succeeds). Useful for testing that membership
// calls return ErrNotLeader without touching the network.
func newV2FollowerForMembership(t *testing.T) RaftNode {
	t.Helper()
	t.Setenv("GRAINFS_RAFT_V2", "cluster")
	resetRaftV2FlagForTest()
	t.Cleanup(resetRaftV2FlagForTest)

	// Supply a peer so the node is NOT a solo voter (solo voters auto-elect).
	rcfg := raft.DefaultConfig("membership-follower", []string{"membership-peer"})
	node, err := newRaftNode(rcfg, nil)
	require.NoError(t, err)
	node.SetTransport(noopRV, noopAE)
	node.Start()
	t.Cleanup(node.Close)
	go func() {
		for range node.ApplyCh() {
		}
	}()
	// With noop transport, no quorum is formed and the node stays Follower.
	return node
}

// TestRaftV2Membership_AddVoter_PassesThroughToV2 verifies that AddVoter on a
// non-leader v2 node (via the adapter) surfaces ErrNotLeader rather than being
// silently nil-skipped. This confirms the call reaches v2 code rather than
// returning nil via the old nil-guard.
func TestRaftV2Membership_AddVoter_PassesThroughToV2(t *testing.T) {
	node := newV2FollowerForMembership(t)

	// A non-started node is in Follower state; AddVoter must return ErrNotLeader.
	err := node.AddVoter("n2", "addr-n2")
	require.Error(t, err, "AddVoter on a follower must return an error")
	assert.True(t,
		errors.Is(err, raftv2.ErrNotLeader),
		"expected ErrNotLeader, got: %v", err)
}

// TestRaftV2Membership_RemoveVoter_PassesThroughToV2 verifies that RemoveVoter
// on a non-leader v2 node surfaces ErrNotLeader.
func TestRaftV2Membership_RemoveVoter_PassesThroughToV2(t *testing.T) {
	node := newV2FollowerForMembership(t)

	err := node.RemoveVoter("n2")
	require.Error(t, err, "RemoveVoter on a follower must return an error")
	assert.True(t,
		errors.Is(err, raftv2.ErrNotLeader),
		"expected ErrNotLeader, got: %v", err)
}

// TestRaftV2Membership_AddLearner_ReturnsErrNotImplemented verifies that under
// v2, AddLearner surfaces ErrNotImplemented rather than a silent nil-skip.
func TestRaftV2Membership_AddLearner_ReturnsErrNotImplemented(t *testing.T) {
	node := newV2LeaderForMembership(t)

	err := node.AddLearner("n2", "addr-n2")
	require.Error(t, err, "AddLearner must return an error under v2")
	assert.True(t,
		errors.Is(err, raftv2.ErrNotImplemented),
		"expected ErrNotImplemented, got: %v", err)
}

// TestRaftV2Membership_TransferLeadership_PassesThroughToV2 verifies that
// under v2, TransferLeadership on a single-voter leader returns ErrNoPeers
// (no peer to transfer to), proving the call reaches v2 code.
func TestRaftV2Membership_TransferLeadership_PassesThroughToV2(t *testing.T) {
	node := newV2LeaderForMembership(t)

	err := node.TransferLeadership()
	require.Error(t, err, "TransferLeadership on single-voter leader must return an error")
	assert.True(t,
		errors.Is(err, raftv2.ErrNoPeers),
		"expected ErrNoPeers, got: %v", err)
}

// TestRaftV2Membership_PromoteToVoter_ReturnsErrNotImplemented verifies that
// under v2, PromoteToVoter surfaces ErrNotImplemented rather than a silent
// nil-skip.
func TestRaftV2Membership_PromoteToVoter_ReturnsErrNotImplemented(t *testing.T) {
	node := newV2LeaderForMembership(t)

	err := node.PromoteToVoter("n2")
	require.Error(t, err, "PromoteToVoter must return an error under v2")
	assert.True(t,
		errors.Is(err, raftv2.ErrNotImplemented),
		"expected ErrNotImplemented, got: %v", err)
}

// TestRaftV2Membership_ChangeMembership_SequencesAddsRemoves verifies the v2
// adapter's ChangeMembership sequences: empty adds+removes → nil (no-op), and
// a non-leader node → ErrNotLeader on the first RemoveVoter call (proving the
// sequencing bridge calls through rather than silently skipping).
func TestRaftV2Membership_ChangeMembership_SequencesAddsRemoves(t *testing.T) {
	t.Run("empty_is_noop", func(t *testing.T) {
		node := newV2LeaderForMembership(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := node.ChangeMembership(ctx, nil, nil)
		assert.NoError(t, err, "ChangeMembership with empty adds+removes must be a no-op")
	})

	t.Run("remove_on_follower_returns_not_leader", func(t *testing.T) {
		node := newV2FollowerForMembership(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Removes are processed after adds; with no adds, the first RemoveVoter
		// fires immediately and returns ErrNotLeader — proving the bridge calls through.
		err := node.ChangeMembership(ctx, nil, []string{"n2"})
		require.Error(t, err, "ChangeMembership with a remove on a follower must return an error")
		assert.True(t,
			errors.Is(err, raftv2.ErrNotLeader),
			"expected ErrNotLeader from sequenced RemoveVoter, got: %v", err)
	})

	t.Run("add_on_follower_returns_not_leader", func(t *testing.T) {
		node := newV2FollowerForMembership(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// The first AddVoterCtx fires for adds; a follower returns ErrNotLeader.
		adds := []raft.ServerEntry{{ID: "n2", Address: "addr-n2", Suffrage: raft.Voter}}
		err := node.ChangeMembership(ctx, adds, nil)
		require.Error(t, err, "ChangeMembership with an add on a follower must return an error")
		assert.True(t,
			errors.Is(err, raftv2.ErrNotLeader),
			"expected ErrNotLeader from sequenced AddVoterCtx, got: %v", err)
	})

	t.Run("ctx_cancelled_before_remove", func(t *testing.T) {
		node := newV2FollowerForMembership(t)
		// A pre-cancelled context is surfaced immediately for the remove step.
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // already cancelled
		err := node.ChangeMembership(ctx, nil, []string{"n2"})
		require.Error(t, err, "ChangeMembership with cancelled ctx must return an error")
		assert.ErrorIs(t, err, context.Canceled)
	})
}
