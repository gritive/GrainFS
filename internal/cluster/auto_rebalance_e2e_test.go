package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/raft/chaos"
)

// TestAutoRebalance_E2E_ProposeAndExecute는 3-node meta-Raft 클러스터에서
// 부하 불균형 시 Rebalancer가 RebalancePlan을 제안·커밋하고
// DataGroupPlanExecutor(실 chaos data-Raft)가 voter 마이그레이션을 완료하는지 검증한다.
//
// localNodeID="node-0"; 부하 불균형 시 fromNode="node-1" (node-1=90%)이므로
// 자기 제거 가드가 발화하지 않는다 — 자기 제거 경로는 TestMoveReplica_TransfersLeadershipWhenFromNodeIsLocal 참조.
func TestAutoRebalance_E2E_ProposeAndExecute(t *testing.T) {
	t.Parallel()

	// — meta-Raft cluster (3 nodes, fake transport) —
	tr := newMetaTransportFake()
	newMetaNode := func(id string, peers []string) *MetaRaft {
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

	m0 := newMetaNode("node-0", nil)
	m1 := newMetaNode("node-1", []string{"node-0"})
	m2 := newMetaNode("node-2", []string{"node-0"})

	t.Cleanup(func() { _ = m0.Close(); _ = m1.Close(); _ = m2.Close() })

	ctx := context.Background()
	require.NoError(t, m0.Bootstrap())
	require.NoError(t, m0.Start(ctx))
	require.Eventually(t, func() bool { return m0.node.State() == raft.Leader },
		3*time.Second, 20*time.Millisecond)

	require.NoError(t, m1.Bootstrap())
	require.NoError(t, m1.Start(ctx))
	require.NoError(t, m2.Bootstrap())
	require.NoError(t, m2.Start(ctx))

	joinCtx, joinCancel := context.WithTimeout(ctx, 5*time.Second)
	defer joinCancel()
	require.NoError(t, m0.Join(joinCtx, "node-1", "node-1"))
	require.NoError(t, m0.Join(joinCtx, "node-2", "node-2"))

	// — chaos data-Raft cluster (node-0, node-1 as initial voters; node-2 non-voter) —
	// NewCluster(t,3)은 node-2를 voter로 만들어 AddLearner 시 already-voter config change로 leader가
	// step down하는 race가 생긴다. NewCluster(t,2)+AddNode("node-2")를 사용해 TestFullSharding_E2E와
	// 동일한 패턴을 유지한다.
	cl := chaos.NewCluster(t, 2) // node-0, node-1
	cl.StartAll()
	dataLeader := cl.WaitForLeader(5 * time.Second)
	require.NotNil(t, dataLeader, "data-Raft leader election timeout")

	cl.AddNode("node-2") // non-voter; MoveReplica가 AddLearner→PromoteToVoter 수행

	// 베이스라인 쓰기 (commitIndex 진행)
	for i := 0; i < 3; i++ {
		_, err := dataLeader.ProposeWait(ctx, []byte("baseline"))
		require.NoError(t, err)
	}

	// — DataGroup: group-0 voters = node-0, node-1 (node-2는 migration target) —
	gm := NewDataGroupManager()
	gm.Add(NewDataGroup("group-0", []string{"node-0", "node-1"}))

	// addrBook: chaos transport는 nodeID를 addr로 사용
	allIDs := append(cl.NodeIDs(), "node-2") // [node-0, node-1, node-2]
	addrBook := &autoRebalAddrBook{ids: allIDs}

	exec := NewDataGroupPlanExecutorForTest("node-0", gm, addrBook, m0,
		func(_ *DataGroup) DataRaftNode {
			return &autoRebalDataNode{n: dataLeader}
		},
	)

	cfg := DefaultRebalancerConfig()
	cfg.EvalInterval = 200 * time.Millisecond
	cfg.ImbalanceThresh = 30.0
	r := NewRebalancer("node-0", m0, gm, cfg)
	r.SetGroupRebalancer(exec)

	m0.FSM().SetOnRebalancePlan(func(plan *RebalancePlan) {
		execCtx, execCancel := context.WithTimeout(ctx, 10*time.Second)
		go func() {
			defer execCancel()
			if err := r.ExecutePlan(execCtx, plan); err != nil {
				t.Logf("ExecutePlan: %v", err)
			}
		}()
	})

	// 부하 불균형 주입: node-1=90% (group-0 voter, overloaded)
	// localNodeID="node-0"이므로 fromNode="node-1" ≠ localNodeID → 자기 제거 가드 미발화
	loadEntries := []LoadStatEntry{
		{NodeID: "node-0", DiskUsedPct: 20.0, DiskAvailBytes: 9000},
		{NodeID: "node-1", DiskUsedPct: 90.0, DiskAvailBytes: 1000},
		{NodeID: "node-2", DiskUsedPct: 15.0, DiskAvailBytes: 9500},
	}
	loadCtx, loadCancel := context.WithTimeout(ctx, 3*time.Second)
	defer loadCancel()
	require.NoError(t, m0.ProposeLoadSnapshot(loadCtx, loadEntries))

	rebalCtx, rebalCancel := context.WithTimeout(ctx, 15*time.Second)
	defer rebalCancel()
	go r.Run(rebalCtx)

	// 마이그레이션 완료 후 group-0에 node-2가 추가되고 node-1이 제거됨
	require.Eventually(t, func() bool {
		dg := gm.Get("group-0")
		if dg == nil {
			return false
		}
		peers := dg.PeerIDs()
		hasNode2, hasNode1 := false, false
		for _, p := range peers {
			switch p {
			case "node-2":
				hasNode2 = true
			case "node-1":
				hasNode1 = true
			}
		}
		return hasNode2 && !hasNode1
	}, 15*time.Second, 100*time.Millisecond, "group-0 must have node-2 and not node-1 after rebalance")

	// plan 완료 후 activePlanID 초기화
	require.Eventually(t, func() bool {
		return m0.FSM().ActivePlanID() == ""
	}, 3*time.Second, 50*time.Millisecond, "activePlanID must be cleared after migration")

	// 마이그레이션 후에도 data-Raft 쓰기 가능
	for i := 0; i < 3; i++ {
		_, err := dataLeader.ProposeWait(ctx, []byte("post-rebalance"))
		require.NoError(t, err)
	}

	// MetaFSM에 새 멤버십 반영 확인
	var sg *ShardGroupEntry
	for _, e := range m0.FSM().ShardGroups() {
		if e.ID == "group-0" {
			e := e
			sg = &e
			break
		}
	}
	require.NotNil(t, sg, "group-0 must exist in MetaFSM ShardGroups")
	assert.Contains(t, sg.PeerIDs, "node-2")
	assert.NotContains(t, sg.PeerIDs, "node-1")
}

// autoRebalAddrBook: chaos transport는 nodeID == addr
type autoRebalAddrBook struct{ ids []string }

func (b *autoRebalAddrBook) Nodes() []MetaNodeEntry {
	entries := make([]MetaNodeEntry, len(b.ids))
	for i, id := range b.ids {
		entries[i] = MetaNodeEntry{ID: id, Address: id}
	}
	return entries
}

// autoRebalDataNode wraps *raft.Node as DataRaftNode for this test.
type autoRebalDataNode struct{ n *raft.Node }

func (a *autoRebalDataNode) IsLeader() bool         { return a.n.IsLeader() }
func (a *autoRebalDataNode) CommittedIndex() uint64 { return a.n.CommittedIndex() }
func (a *autoRebalDataNode) PeerMatchIndex(pk string) (uint64, bool) {
	return a.n.PeerMatchIndex(pk)
}
func (a *autoRebalDataNode) AddLearner(id, _ string) error  { return a.n.AddLearner(id, "") }
func (a *autoRebalDataNode) PromoteToVoter(id string) error { return a.n.PromoteToVoter(id) }
func (a *autoRebalDataNode) RemoveVoter(id string) error    { return a.n.RemoveVoter(id) }
func (a *autoRebalDataNode) TransferLeadership() error      { return a.n.TransferLeadership() }
func (a *autoRebalDataNode) AddVoterCtx(ctx context.Context, id, addr string) error {
	return a.n.AddVoterCtx(ctx, id, addr)
}
func (a *autoRebalDataNode) ChangeMembership(ctx context.Context, adds []raft.ServerEntry, removes []string) error {
	return a.n.ChangeMembership(ctx, adds, removes)
}
