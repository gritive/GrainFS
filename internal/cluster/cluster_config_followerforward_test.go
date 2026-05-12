package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// TestClusterConfig_FollowerForward_E2E verifies that a ClusterConfigPatch
// proposed via a follower's MetaRaft.Propose is transparently forwarded to
// the meta-Raft leader (via the same MetaProposeForwardSender/Receiver path
// production uses) and replicated to every FSM at the same rev.
//
// Boot: 3-node meta-Raft ensemble using MetaTransportFake (same pattern as
// TestMetaRaft_ThreeNodeBootstrap_E2E and TestFullSharding_E2E). Each
// follower's forwardFn is wired to dial the leader's
// MetaProposeForwardReceiver in-process. The test then drives a follower's
// ClusterConfigProposer and waits for all three FSMs to observe the new value
// at rev=1.
func TestClusterConfig_FollowerForward_E2E(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tr := newMetaTransportFake()
	newMeta := func(id string, peers []string) *MetaRaft {
		t.Helper()
		m, err := NewMetaRaft(MetaRaftConfig{
			NodeID:    id,
			Peers:     peers,
			DataDir:   t.TempDir(),
			Transport: tr,
		})
		require.NoError(t, err)
		tr.register(id, m)
		return m
	}

	m0 := newMeta("node-0", nil)
	m1 := newMeta("node-1", []string{"node-0"})
	m2 := newMeta("node-2", []string{"node-0"})

	t.Cleanup(func() { _ = m0.Close(); _ = m1.Close(); _ = m2.Close() })

	require.NoError(t, m0.Bootstrap())
	require.NoError(t, m0.Start(ctx))
	require.Eventually(t, func() bool { return m0.node.State() == raft.Leader },
		3*time.Second, 20*time.Millisecond, "node-0 must become leader")

	require.NoError(t, m1.Bootstrap())
	require.NoError(t, m1.Start(ctx))
	require.NoError(t, m2.Bootstrap())
	require.NoError(t, m2.Start(ctx))

	joinCtx, joinCancel := context.WithTimeout(ctx, 5*time.Second)
	defer joinCancel()
	require.NoError(t, m0.Join(joinCtx, "node-1", "node-1"))
	require.NoError(t, m0.Join(joinCtx, "node-2", "node-2"))

	// Wire follower→leader forwarding in-process. Dialer routes peer="node-0"
	// (the only acceptable target — leader) to node-0's MetaProposeForwardReceiver,
	// matching the production sender→receiver contract.
	leaderReceiver := NewMetaProposeForwardReceiver(m0)
	dialer := func(peer string, payload []byte) ([]byte, error) {
		if peer != "node-0" {
			return encodeMetaForwardReply(raft.ErrNotLeader), nil
		}
		return leaderReceiver.Handle(&transport.Message{Payload: payload}).Payload, nil
	}
	sender := NewMetaProposeForwardSender(dialer)

	setForwarder := func(m *MetaRaft) {
		m.SetForwarder(func(ctx context.Context, data []byte) error {
			return sender.Send(ctx, []string{"node-0"}, data)
		})
	}
	setForwarder(m1)
	setForwarder(m2)

	// Pick a follower deterministically (m1 — node-0 is leader by construction).
	require.False(t, m1.IsLeader(), "m1 must be a follower")

	// Drive the proposer through the follower. This is the same call path
	// adminapi uses: PATCH handler → ClusterConfigProposer.ProposeClusterConfigPatch
	// → MetaRaft.Propose → proposeOrForward → forwardFn (since not leader).
	patch := ClusterConfigPatch{
		BalancerImbalanceTriggerPct: ptrFloat(42.5),
	}
	proposer := &ClusterConfigProposer{Propose: m1.Propose}
	require.NoError(t, proposer.ProposeClusterConfigPatch(patch))

	// Wait for raft replication to all three FSMs.
	require.Eventually(t, func() bool {
		for _, m := range []*MetaRaft{m0, m1, m2} {
			cfg := m.FSM().ClusterConfig()
			if cfg.Rev() != 1 {
				return false
			}
			if cfg.BalancerImbalanceTriggerPct() != 42.5 {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond,
		"all three FSMs must observe rev=1 with the patched balancer-imbalance-trigger-pct")
}
