package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestAutoRebalance_E2E_ProposeAndExecute는 3-node meta-Raft 클러스터에서
// 부하 불균형 시 Rebalancer가 RebalancePlan을 제안·커밋하고
// MockGroupRebalancer.MoveReplica가 호출되는지 검증한다.
//
// DataGroup 실제 Raft 노드 와이어링은 PR-E에서 추가되므로
// 이 테스트는 plan 제안/실행 경로만 검증한다.
func TestAutoRebalance_E2E_ProposeAndExecute(t *testing.T) {
	t.Parallel()
	tr := newMetaTransportFake()

	newNode := func(id string, peers []string) *MetaRaft {
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

	m0 := newNode("node-0", nil)
	m1 := newNode("node-1", []string{"node-0"})
	m2 := newNode("node-2", []string{"node-0"})

	t.Cleanup(func() {
		_ = m0.Close()
		_ = m1.Close()
		_ = m2.Close()
	})

	require.NoError(t, m0.Bootstrap())
	ctx := context.Background()
	require.NoError(t, m0.Start(ctx))
	require.Eventually(t, func() bool { return m0.node.State() == raft.Leader },
		3*time.Second, 20*time.Millisecond)

	require.NoError(t, m1.Bootstrap())
	require.NoError(t, m1.Start(ctx))
	require.NoError(t, m2.Bootstrap())
	require.NoError(t, m2.Start(ctx))

	joinCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, m0.Join(joinCtx, "node-1", "node-1"))
	require.NoError(t, m0.Join(joinCtx, "node-2", "node-2"))

	// 데이터 그룹 설정: node-0이 heavy group의 voter
	gm := NewDataGroupManager()
	gm.Add(NewDataGroup("group-0", []string{"node-0", "node-1"}))

	mockRebalancer := &MockGroupRebalancer{}

	cfg := DefaultRebalancerConfig()
	cfg.EvalInterval = 200 * time.Millisecond
	cfg.ImbalanceThresh = 30.0
	r := NewRebalancer("node-0", m0, gm, cfg)
	r.SetGroupRebalancer(mockRebalancer)

	m0.FSM().SetOnRebalancePlan(func(plan *RebalancePlan) {
		execCtx, execCancel := context.WithTimeout(ctx, 5*time.Second)
		go func() {
			defer execCancel()
			_ = r.ExecutePlan(execCtx, plan)
		}()
	})

	// 부하 불균형 주입: node-0=90%, node-1=20%, node-2=15%
	loadEntries := []LoadStatEntry{
		{NodeID: "node-0", DiskUsedPct: 90.0, DiskAvailBytes: 1000},
		{NodeID: "node-1", DiskUsedPct: 20.0, DiskAvailBytes: 9000},
		{NodeID: "node-2", DiskUsedPct: 15.0, DiskAvailBytes: 9500},
	}
	loadCtx, loadCancel := context.WithTimeout(ctx, 3*time.Second)
	defer loadCancel()
	require.NoError(t, m0.ProposeLoadSnapshot(loadCtx, loadEntries))

	rebalCtx, rebalCancel := context.WithTimeout(ctx, 5*time.Second)
	defer rebalCancel()
	go r.Run(rebalCtx)

	// plan commit 후 MoveReplica 호출까지 대기
	require.Eventually(t, func() bool {
		return len(mockRebalancer.Moves()) >= 1
	}, 3*time.Second, 50*time.Millisecond, "MoveReplica must be called within 3s")

	moves := mockRebalancer.Moves()
	require.Len(t, moves, 1)
	assert.Equal(t, "group-0", moves[0].GroupID)
	assert.Equal(t, "node-0", moves[0].FromNode)
	assert.Equal(t, "node-2", moves[0].ToNode) // lightest

	// plan 완료 후 activePlanID 지워짐 확인
	require.Eventually(t, func() bool {
		return m0.FSM().ActivePlanID() == ""
	}, 2*time.Second, 50*time.Millisecond, "activePlanID must be cleared after execution")
}
