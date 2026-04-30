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

// TestFullSharding_E2E는 MetaRaft + Rebalancer + DataGroupPlanExecutor + chaos data-Raft를
// 완전히 연결해 end-to-end 재조정을 검증한다.
//
// 시나리오:
//  1. 3-node meta-Raft 클러스터 기동
//  2. 3-node chaos data-Raft 클러스터 기동 (node-0, node-1, node-2)
//  3. node-3 non-voter로 추가 (AddNode)
//  4. group-0 초기 멤버: node-0, node-1, node-2
//  5. node-1=90% (group-0 voter, overloaded) → Rebalancer가 node-1→node-3 마이그레이션 제안
//     (localNodeID="node-0"이므로 자기 제거 가드 미발화)
//  6. DataGroupPlanExecutor가 voter 마이그레이션 수행
//  7. 마이그레이션 후 group-0 멤버: node-0, node-2, node-3
//  8. data-Raft 클러스터가 여전히 쓰기 커밋 가능
func TestFullSharding_E2E(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// ── Meta-Raft 클러스터 (3-node) ──────────────────────────────────────────
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

	meta0 := newMeta("node-0", nil)
	meta1 := newMeta("node-1", []string{"node-0"})
	meta2 := newMeta("node-2", []string{"node-0"})

	t.Cleanup(func() { _ = meta0.Close(); _ = meta1.Close(); _ = meta2.Close() })

	require.NoError(t, meta0.Bootstrap())
	require.NoError(t, meta0.Start(ctx))
	require.Eventually(t, func() bool { return meta0.node.State() == raft.Leader },
		3*time.Second, 20*time.Millisecond, "meta-Raft leader election timeout")

	require.NoError(t, meta1.Bootstrap())
	require.NoError(t, meta1.Start(ctx))
	require.NoError(t, meta2.Bootstrap())
	require.NoError(t, meta2.Start(ctx))

	joinCtx, joinCancel := context.WithTimeout(ctx, 5*time.Second)
	defer joinCancel()
	require.NoError(t, meta0.Join(joinCtx, "node-1", "node-1"))
	require.NoError(t, meta0.Join(joinCtx, "node-2", "node-2"))

	// ── Chaos data-Raft 클러스터 ──────────────────────────────────────────────
	// 3 initial voters (node-0, node-1, node-2); node-3는 non-voter로 추가.
	// NewCluster(t, 4)를 사용하면 node-3이 이미 voter이므로 AddLearner가 실패한다.
	cl := chaos.NewCluster(t, 3) // node-0, node-1, node-2
	cl.StartAll()
	dataLeader := cl.WaitForLeader(5 * time.Second)
	require.NotNil(t, dataLeader, "data-Raft leader election timeout")

	node3 := cl.AddNode("node-3") // non-voter; MoveReplica가 AddLearner→PromoteToVoter 수행

	// 베이스라인 쓰기
	for i := 0; i < 5; i++ {
		_, err := dataLeader.ProposeWait(ctx, []byte("baseline"))
		require.NoError(t, err)
	}

	// ── DataGroup 설정 ────────────────────────────────────────────────────────
	gm := NewDataGroupManager()
	gm.Add(NewDataGroup("group-0", []string{"node-0", "node-1", "node-2"}))

	// addrBook: chaos transport는 nodeID == addr; node-3 포함
	allIDs := append(cl.NodeIDs(), "node-3")
	addrBook := &fullShardAddrBook{ids: allIDs}

	exec := NewDataGroupPlanExecutorForTest("node-0", gm, addrBook, meta0,
		func(_ *DataGroup) DataRaftNode {
			return &fullShardDataNode{n: dataLeader}
		},
	)

	// ── Rebalancer 설정 ───────────────────────────────────────────────────────
	cfg := DefaultRebalancerConfig()
	cfg.EvalInterval = 100 * time.Millisecond
	cfg.ImbalanceThresh = 20.0
	r := NewRebalancer("node-0", meta0, gm, cfg)
	r.SetGroupRebalancer(exec)

	meta0.FSM().SetOnRebalancePlan(func(plan *RebalancePlan) {
		execCtx, execCancel := context.WithTimeout(ctx, 15*time.Second)
		go func() {
			defer execCancel()
			if err := r.ExecutePlan(execCtx, plan); err != nil {
				t.Logf("ExecutePlan returned: %v", err)
			}
		}()
	})

	// ── 부하 불균형 주입 ──────────────────────────────────────────────────────
	// node-1=90% (group-0 voter, overloaded), node-3=10% (not in group yet).
	// localNodeID="node-0"이므로 fromNode="node-1" ≠ localNodeID → 자기 제거 가드 미발화.
	loadEntries := []LoadStatEntry{
		{NodeID: "node-0", DiskUsedPct: 30.0, DiskAvailBytes: 7000},
		{NodeID: "node-1", DiskUsedPct: 90.0, DiskAvailBytes: 500},
		{NodeID: "node-2", DiskUsedPct: 25.0, DiskAvailBytes: 7500},
		{NodeID: "node-3", DiskUsedPct: 10.0, DiskAvailBytes: 9000},
	}
	loadCtx, loadCancel := context.WithTimeout(ctx, 3*time.Second)
	defer loadCancel()
	require.NoError(t, meta0.ProposeLoadSnapshot(loadCtx, loadEntries))

	// ── Rebalancer 실행 ───────────────────────────────────────────────────────
	rebalCtx, rebalCancel := context.WithTimeout(ctx, 20*time.Second)
	defer rebalCancel()
	go r.Run(rebalCtx)

	// ── 검증: group-0에 node-3 추가, node-1 제거 ─────────────────────────────
	require.Eventually(t, func() bool {
		dg := gm.Get("group-0")
		if dg == nil {
			return false
		}
		peers := dg.PeerIDs()
		in3, out1 := false, true
		for _, p := range peers {
			if p == "node-3" {
				in3 = true
			}
			if p == "node-1" {
				out1 = false
			}
		}
		return in3 && out1
	}, 20*time.Second, 200*time.Millisecond, "group-0 voter migration node-1→node-3 must complete")

	// plan 완료 후 activePlanID 초기화
	require.Eventually(t, func() bool {
		return meta0.FSM().ActivePlanID() == ""
	}, 5*time.Second, 50*time.Millisecond, "activePlanID must be cleared")

	// 마이그레이션 후에도 data-Raft 쓰기 가능
	for i := 0; i < 5; i++ {
		_, err := dataLeader.ProposeWait(ctx, []byte("post-migration"))
		require.NoError(t, err)
	}

	// node-3이 commitIndex를 따라잡음
	require.Eventually(t, func() bool {
		return node3.CommittedIndex() == dataLeader.CommittedIndex()
	}, 10*time.Second, 100*time.Millisecond, "node-3 must catch up to leader commitIndex")

	// MetaFSM에 새 멤버십 반영
	var sg *ShardGroupEntry
	for _, e := range meta0.FSM().ShardGroups() {
		if e.ID == "group-0" {
			e := e
			sg = &e
			break
		}
	}
	require.NotNil(t, sg, "group-0 must exist in MetaFSM ShardGroups")
	assert.Contains(t, sg.PeerIDs, "node-3")
	assert.NotContains(t, sg.PeerIDs, "node-1")

	t.Logf("Final group-0 peers: %v", gm.Get("group-0").PeerIDs())
}

// fullShardAddrBook: chaos transport는 nodeID == addr
type fullShardAddrBook struct{ ids []string }

func (b *fullShardAddrBook) Nodes() []MetaNodeEntry {
	entries := make([]MetaNodeEntry, len(b.ids))
	for i, id := range b.ids {
		entries[i] = MetaNodeEntry{ID: id, Address: id}
	}
	return entries
}

// fullShardDataNode wraps *raft.Node as DataRaftNode for this test.
type fullShardDataNode struct{ n *raft.Node }

func (a *fullShardDataNode) IsLeader() bool         { return a.n.IsLeader() }
func (a *fullShardDataNode) CommittedIndex() uint64 { return a.n.CommittedIndex() }
func (a *fullShardDataNode) PeerMatchIndex(pk string) (uint64, bool) {
	return a.n.PeerMatchIndex(pk)
}
func (a *fullShardDataNode) AddLearner(id, _ string) error  { return a.n.AddLearner(id, "") }
func (a *fullShardDataNode) PromoteToVoter(id string) error { return a.n.PromoteToVoter(id) }
func (a *fullShardDataNode) RemoveVoter(id string) error    { return a.n.RemoveVoter(id) }
func (a *fullShardDataNode) TransferLeadership() error      { return a.n.TransferLeadership() }
func (a *fullShardDataNode) AddVoterCtx(ctx context.Context, id, addr string) error {
	return a.n.AddVoterCtx(ctx, id, addr)
}
