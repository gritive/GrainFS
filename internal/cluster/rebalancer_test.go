package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

// mockMetaRaftClient satisfies MetaRaftClient for Rebalancer tests.
type mockMetaRaftClient struct {
	leader        bool
	proposedPlans []RebalancePlan
	abortedPlans  []string
	abortReasons  []clusterpb.AbortPlanReason
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

func (m *mockMetaRaftClient) ProposeAbortPlan(_ context.Context, planID string, reason clusterpb.AbortPlanReason) error {
	m.abortedPlans = append(m.abortedPlans, planID)
	m.abortReasons = append(m.abortReasons, reason)
	return m.fsm.applyCmd(makeAbortCmd(planID))
}

func makeRebalancePlanCmd(plan RebalancePlan) []byte {
	data, _ := encodeMetaProposeRebalancePlanCmd(plan)
	cmd, _ := encodeMetaCmd(MetaCmdTypeProposeRebalancePlan, data)
	return cmd
}

func makeAbortCmd(planID string) []byte {
	data, _ := encodeMetaAbortPlanCmd(planID, clusterpb.AbortPlanReasonUnknown)
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

// TestRebalancer_NewNodeUnderUtilization verifies that an underutilized node
// (e.g., freshly joined with DiskUsedPct=0) triggers a voter migration plan
// when other nodes are above the imbalance threshold.
//
// findImbalance picks the lightest node as ToNode automatically; this test
// documents that behavior and guards against regression. Advisor flagged a
// concern that under-utilization required a new signal — this test demonstrates
// it does not.
func TestRebalancer_NewNodeUnderUtilization_TriggersMigration(t *testing.T) {
	mc := newMockMetaClient()
	entries := []LoadStatEntry{
		{NodeID: "n0", DiskUsedPct: 60.0},
		{NodeID: "n1", DiskUsedPct: 55.0},
		{NodeID: "n2", DiskUsedPct: 58.0},
		{NodeID: "fresh", DiskUsedPct: 0.0}, // newly joined, empty
	}
	require.NoError(t, mc.fsm.applyCmd(makeSetLoadSnapshotCmdDirect(entries)))

	gm := NewDataGroupManager()
	gm.Add(NewDataGroup("group-0", []string{"n0", "n1", "n2"}))

	r := NewRebalancer("leader", mc, gm, DefaultRebalancerConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	r.tickOnce(ctx)

	require.Len(t, mc.proposedPlans, 1, "expected migration plan toward fresh node")
	assert.Equal(t, "n0", mc.proposedPlans[0].FromNode, "heaviest must be FromNode")
	assert.Equal(t, "fresh", mc.proposedPlans[0].ToNode, "lightest (fresh) must be ToNode")
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
	require.Len(t, mc.abortReasons, 1)
	assert.Equal(t, clusterpb.AbortPlanReasonTimeout, mc.abortReasons[0])
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

func TestRebalancer_ExecutePlan_FailureAbortsplan(t *testing.T) {
	mc := newMockMetaClient()
	plan := RebalancePlan{
		PlanID:    "p-fail",
		GroupID:   "g0",
		FromNode:  "heavy",
		ToNode:    "light",
		CreatedAt: time.Now(),
	}
	require.NoError(t, mc.fsm.applyCmd(makeRebalancePlanCmd(plan)))

	mock := &MockGroupRebalancer{}
	mock.SetError(errors.New("move failed"))

	r := NewRebalancer("leader", mc, NewDataGroupManager(), DefaultRebalancerConfig())
	r.SetGroupRebalancer(mock)

	err := r.ExecutePlan(context.Background(), &plan)
	require.Error(t, err)
	require.Len(t, mc.abortedPlans, 1, "AbortPlan must be called on MoveReplica failure")
	assert.Equal(t, "p-fail", mc.abortedPlans[0])
	require.Len(t, mc.abortReasons, 1)
	assert.Equal(t, clusterpb.AbortPlanReasonExecutionFailed, mc.abortReasons[0])
}

func TestRebalancer_ExecutePlan_SuccessAbortsWithCompleted(t *testing.T) {
	mc := newMockMetaClient()
	plan := RebalancePlan{
		PlanID:    "p-ok",
		GroupID:   "g0",
		FromNode:  "heavy",
		ToNode:    "light",
		CreatedAt: time.Now(),
	}
	require.NoError(t, mc.fsm.applyCmd(makeRebalancePlanCmd(plan)))

	mock := &MockGroupRebalancer{} // no error → success

	r := NewRebalancer("leader", mc, NewDataGroupManager(), DefaultRebalancerConfig())
	r.SetGroupRebalancer(mock)

	err := r.ExecutePlan(context.Background(), &plan)
	require.NoError(t, err)
	require.Len(t, mc.abortedPlans, 1, "AbortPlan must be called on success as completion marker")
	assert.Equal(t, "p-ok", mc.abortedPlans[0])
	require.Len(t, mc.abortReasons, 1)
	assert.Equal(t, clusterpb.AbortPlanReasonCompleted, mc.abortReasons[0])
}
