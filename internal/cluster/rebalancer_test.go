package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockMetaRaftClient satisfies MetaRaftClient for Rebalancer tests.
type mockMetaRaftClient struct {
	leader        bool
	proposedPlans []RebalancePlan
	abortedPlans  []string
	fsm           *MetaFSM
}

func newMockMetaClient() *mockMetaRaftClient {
	return &mockMetaRaftClient{leader: true, fsm: NewMetaFSM()}
}

func (m *mockMetaRaftClient) IsLeader() bool { return m.leader }
func (m *mockMetaRaftClient) FSM() *MetaFSM  { return m.fsm }

func (m *mockMetaRaftClient) ProposeRebalancePlan(_ context.Context, p RebalancePlan) error {
	m.proposedPlans = append(m.proposedPlans, p)
	return m.fsm.applyCmd(makeRebalancePlanCmd(p))
}

func (m *mockMetaRaftClient) ProposeAbortPlan(_ context.Context, planID string) error {
	m.abortedPlans = append(m.abortedPlans, planID)
	return m.fsm.applyCmd(makeAbortCmd(planID))
}

func makeRebalancePlanCmd(plan RebalancePlan) []byte {
	data, _ := encodeMetaProposeRebalancePlanCmd(plan)
	cmd, _ := encodeMetaCmd(MetaCmdTypeProposeRebalancePlan, data)
	return cmd
}

func makeAbortCmd(planID string) []byte {
	data, _ := encodeMetaAbortPlanCmd(planID)
	cmd, _ := encodeMetaCmd(MetaCmdTypeAbortPlan, data)
	return cmd
}

func makeSetLoadSnapshotCmdDirect(entries []LoadStatEntry) []byte {
	data, _ := encodeMetaSetLoadSnapshotCmd(entries)
	cmd, _ := encodeMetaCmd(MetaCmdTypeSetLoadSnapshot, data)
	return cmd
}

func TestRebalancer_ProposesPlanWhenImbalanced(t *testing.T) {
	mc := newMockMetaClient()
	entries := []LoadStatEntry{
		{NodeID: "heavy", DiskUsedPct: 90.0},
		{NodeID: "light", DiskUsedPct: 20.0},
	}
	require.NoError(t, mc.fsm.applyCmd(makeSetLoadSnapshotCmdDirect(entries)))

	gm := NewDataGroupManager()
	gm.Add(NewDataGroup("group-0", []string{"heavy", "other"}))

	r := NewRebalancer("leader", mc, gm, DefaultRebalancerConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	r.tickOnce(ctx)

	require.Len(t, mc.proposedPlans, 1)
	assert.Equal(t, "heavy", mc.proposedPlans[0].FromNode)
	assert.Equal(t, "light", mc.proposedPlans[0].ToNode)
	assert.Equal(t, "group-0", mc.proposedPlans[0].GroupID)
}

func TestRebalancer_SkipsIfNotLeader(t *testing.T) {
	mc := newMockMetaClient()
	mc.leader = false
	r := NewRebalancer("follower", mc, NewDataGroupManager(), DefaultRebalancerConfig())
	r.tickOnce(context.Background())
	assert.Empty(t, mc.proposedPlans)
}

func TestRebalancer_SkipsIfActivePlan(t *testing.T) {
	mc := newMockMetaClient()
	existing := RebalancePlan{PlanID: "existing", GroupID: "g0", FromNode: "a", ToNode: "b", CreatedAt: time.Now()}
	require.NoError(t, mc.fsm.applyCmd(makeRebalancePlanCmd(existing)))

	entries := []LoadStatEntry{
		{NodeID: "heavy", DiskUsedPct: 90.0},
		{NodeID: "light", DiskUsedPct: 20.0},
	}
	require.NoError(t, mc.fsm.applyCmd(makeSetLoadSnapshotCmdDirect(entries)))

	r := NewRebalancer("leader", mc, NewDataGroupManager(), DefaultRebalancerConfig())
	r.tickOnce(context.Background())
	assert.Empty(t, mc.proposedPlans, "must not propose new plan while existing is active")
}

func TestRebalancer_AbortsPlanOnTimeout(t *testing.T) {
	mc := newMockMetaClient()
	old := RebalancePlan{
		PlanID:    "stale",
		GroupID:   "g0",
		FromNode:  "a",
		ToNode:    "b",
		CreatedAt: time.Now().Add(-11 * time.Minute),
	}
	require.NoError(t, mc.fsm.applyCmd(makeRebalancePlanCmd(old)))

	r := NewRebalancer("leader", mc, NewDataGroupManager(), DefaultRebalancerConfig())
	r.tickOnce(context.Background())

	require.Len(t, mc.abortedPlans, 1)
	assert.Equal(t, "stale", mc.abortedPlans[0])
}

func TestRebalancer_SkipsIfBalanced(t *testing.T) {
	mc := newMockMetaClient()
	entries := []LoadStatEntry{
		{NodeID: "n1", DiskUsedPct: 50.0},
		{NodeID: "n2", DiskUsedPct: 48.0},
	}
	require.NoError(t, mc.fsm.applyCmd(makeSetLoadSnapshotCmdDirect(entries)))

	gm := NewDataGroupManager()
	gm.Add(NewDataGroup("g0", []string{"n1", "n2"}))
	r := NewRebalancer("leader", mc, gm, DefaultRebalancerConfig())
	r.tickOnce(context.Background())
	assert.Empty(t, mc.proposedPlans)
}
