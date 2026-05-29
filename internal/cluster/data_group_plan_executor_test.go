package cluster_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/raft"
)

// fakeRaftNode implements cluster.DataRaftNode for testing.
type fakeRaftNode struct {
	isLeader     bool
	committed    uint64
	autoCatchup  bool // if true, AddLearner immediately sets matchIndexes[addr]=committed
	matchIndexes map[string]uint64
	addLearnerFn func(id, addr string) error
	promoteFn    func(id string) error
	removeFn     func(id string) error
	transferFn   func() error
	addedLearner [2]string // [id, addr]
	promoted     string
	removed      string

	// ChangeMembership tracking (PR-K2)
	changeMembershipFn    func(ctx context.Context, adds []raft.ServerEntry, removes []string) error
	changeMembershipCalls int
	lastAdds              []raft.ServerEntry
	lastRemoves           []string
	isLeaderFn            func() bool

	// config is the real raft Configuration returned by Configuration() (EvacuateVoter).
	config raft.Configuration
}

func (f *fakeRaftNode) Configuration() raft.Configuration { return f.config }

func (f *fakeRaftNode) IsLeader() bool {
	if f.isLeaderFn != nil {
		return f.isLeaderFn()
	}
	return f.isLeader
}
func (f *fakeRaftNode) CommittedIndex() uint64 { return f.committed }
func (f *fakeRaftNode) LastLogIndex() uint64   { return f.committed }
func (f *fakeRaftNode) TransferLeadership() error {
	if f.transferFn != nil {
		return f.transferFn()
	}
	return nil
}
func (f *fakeRaftNode) PeerMatchIndex(peerKey string) (uint64, bool) {
	if f.matchIndexes == nil {
		return 0, false
	}
	idx, ok := f.matchIndexes[peerKey]
	return idx, ok
}
func (f *fakeRaftNode) AddLearner(id, addr string) error {
	f.addedLearner = [2]string{id, addr}
	if f.matchIndexes == nil {
		f.matchIndexes = make(map[string]uint64)
	}
	// Mirror real raft.Node peerKey derivation: addr if non-empty, else id.
	peerKey := addr
	if peerKey == "" {
		peerKey = id
	}
	if f.autoCatchup {
		f.matchIndexes[peerKey] = f.committed
	} else {
		f.matchIndexes[peerKey] = 0
	}
	if f.addLearnerFn != nil {
		return f.addLearnerFn(id, addr)
	}
	return nil
}
func (f *fakeRaftNode) PromoteToVoter(id string) error {
	f.promoted = id
	if f.promoteFn != nil {
		return f.promoteFn(id)
	}
	return nil
}
func (f *fakeRaftNode) RemoveVoter(id string) error {
	f.removed = id
	if f.removeFn != nil {
		return f.removeFn(id)
	}
	return nil
}

func (f *fakeRaftNode) AddVoterCtx(ctx context.Context, id, addr string) error {
	if err := f.AddLearner(id, addr); err != nil {
		return err
	}
	// Simulate the watcher's catch-up wait: if autoCatchup is false, the learner
	// never catches up and AddVoter is bounded by ctx.
	if !f.autoCatchup {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.PromoteToVoter(id)
}

func (f *fakeRaftNode) ChangeMembership(ctx context.Context, adds []raft.ServerEntry, removes []string) error {
	f.changeMembershipCalls++
	f.lastAdds = adds
	f.lastRemoves = removes
	if f.changeMembershipFn != nil {
		return f.changeMembershipFn(ctx, adds, removes)
	}
	// Default: simulate catch-up wait when autoCatchup is false (parity with AddVoterCtx).
	if !f.autoCatchup && len(adds) > 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

type fakeAddrBook struct{ nodes []cluster.MetaNodeEntry }

func (f *fakeAddrBook) Nodes() []cluster.MetaNodeEntry { return f.nodes }

type fakeSGUpdater struct {
	proposed   []cluster.ShardGroupEntry
	err        error
	errForCall []error
}

func (f *fakeSGUpdater) ProposeShardGroup(_ context.Context, sg cluster.ShardGroupEntry) error {
	f.proposed = append(f.proposed, sg)
	if len(f.errForCall) >= len(f.proposed) {
		return f.errForCall[len(f.proposed)-1]
	}
	return f.err
}

// ProposeShardGroupForwarding delegates to ProposeShardGroup so existing
// recording assertions (proposed/err/errForCall) keep working unchanged.
func (f *fakeSGUpdater) ProposeShardGroupForwarding(ctx context.Context, sg cluster.ShardGroupEntry) error {
	return f.ProposeShardGroup(ctx, sg)
}

// newTestExecutor wires a DataGroupPlanExecutor with a fake raft node.
func newTestExecutor(t *testing.T, fakeNode *fakeRaftNode, nodes []cluster.MetaNodeEntry) (
	*cluster.DataGroupPlanExecutor, *fakeSGUpdater,
) {
	t.Helper()
	addrBook := &fakeAddrBook{nodes: nodes}
	sgUpdater := &fakeSGUpdater{}
	dgMgr := cluster.NewDataGroupManager()
	exec := cluster.NewDataGroupPlanExecutorForTest("local-node", dgMgr, addrBook, sgUpdater, func(dg *cluster.DataGroup) cluster.DataRaftNode {
		return fakeNode
	})
	return exec, sgUpdater
}

func TestMoveReplica_HappyPath(t *testing.T) {
	fakeNode := &fakeRaftNode{isLeader: true, committed: 10, autoCatchup: true}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-1", Address: "10.0.0.1:9001"},
		{ID: "node-2", Address: "10.0.0.2:9002"},
		{ID: "node-3", Address: "10.0.0.3:9003"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0", "node-1", "node-2"}, nil))

	err := exec.MoveReplica(context.Background(), "group-0", "node-0", "node-3")
	require.NoError(t, err)

	// PR-K2: single ChangeMembership call replaces the 5-step sequence.
	require.Equal(t, 1, fakeNode.changeMembershipCalls)
	require.Len(t, fakeNode.lastAdds, 1)
	require.Equal(t, "node-3", fakeNode.lastAdds[0].ID)
	require.Equal(t, "10.0.0.3:9003", fakeNode.lastAdds[0].Address)
	require.Equal(t, raft.Voter, fakeNode.lastAdds[0].Suffrage)
	// data-group raft keys voters by node id, so the remove is the node id.
	require.Equal(t, []string{"node-0"}, fakeNode.lastRemoves)

	require.Len(t, sgUpdater.proposed, 1)
	require.Equal(t, "group-0", sgUpdater.proposed[0].ID)
	require.Contains(t, sgUpdater.proposed[0].PeerIDs, "node-3")
	require.NotContains(t, sgUpdater.proposed[0].PeerIDs, "node-0")
}

func TestAddReplica_HappyPath(t *testing.T) {
	fakeNode := &fakeRaftNode{isLeader: true, committed: 10, autoCatchup: true}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-1", Address: "10.0.0.1:9001"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0"}, nil))

	err := exec.AddReplica(context.Background(), "group-0", "node-1")
	require.NoError(t, err)

	require.Equal(t, 1, fakeNode.changeMembershipCalls)
	require.Len(t, fakeNode.lastAdds, 1)
	require.Equal(t, "node-1", fakeNode.lastAdds[0].ID)
	require.Equal(t, "10.0.0.1:9001", fakeNode.lastAdds[0].Address)
	require.Equal(t, raft.Voter, fakeNode.lastAdds[0].Suffrage)
	require.Empty(t, fakeNode.lastRemoves)

	require.Len(t, sgUpdater.proposed, 2)
	require.Equal(t, "group-0", sgUpdater.proposed[0].ID)
	require.Equal(t, []string{"node-0", "node-1"}, sgUpdater.proposed[0].PeerIDs)
	require.Equal(t, []string{"node-0", "node-1"}, exec.DGMgr().Get("group-0").PeerIDs())
}

func TestAddReplica_PrePublishesDesiredPeersBeforeChangeMembership(t *testing.T) {
	fakeNode := &fakeRaftNode{isLeader: true, committed: 10, autoCatchup: true}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-1", Address: "10.0.0.1:9001"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0"}, nil))

	proposalsBeforeChange := -1
	fakeNode.changeMembershipFn = func(_ context.Context, _ []raft.ServerEntry, _ []string) error {
		proposalsBeforeChange = len(sgUpdater.proposed)
		return nil
	}

	err := exec.AddReplica(context.Background(), "group-0", "node-1")
	require.NoError(t, err)

	require.Equal(t, 1, proposalsBeforeChange, "desired peers must be published before raft membership change so the joining node starts the group")
	require.GreaterOrEqual(t, len(sgUpdater.proposed), 2)
	require.Equal(t, []string{"node-0", "node-1"}, sgUpdater.proposed[0].PeerIDs)
}

func TestAddReplica_NotLocalLeaderIsTypedAndDoesNotPublishDesiredPeers(t *testing.T) {
	fakeNode := &fakeRaftNode{isLeader: false, committed: 10, autoCatchup: true}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-1", Address: "10.0.0.1:9001"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0"}, nil))

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err := exec.AddReplica(ctx, "group-0", "node-1")
	require.ErrorIs(t, err, cluster.ErrDataGroupNotLocalLeader)
	require.Equal(t, 0, fakeNode.changeMembershipCalls)
	require.Empty(t, sgUpdater.proposed)
}

func TestAddReplica_PrePublishErrorDoesNotChangeMembership(t *testing.T) {
	sgErr := errors.New("meta apply timeout")
	fakeNode := &fakeRaftNode{isLeader: true, committed: 10, autoCatchup: true}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-1", Address: "10.0.0.1:9001"},
	}
	addrBook := &fakeAddrBook{nodes: nodes}
	sgUpdater := &fakeSGUpdater{err: sgErr}
	dgMgr := cluster.NewDataGroupManager()
	exec := cluster.NewDataGroupPlanExecutorForTest("local-node", dgMgr, addrBook, sgUpdater, func(dg *cluster.DataGroup) cluster.DataRaftNode {
		return fakeNode
	})
	dgMgr.Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0"}, nil))

	err := exec.AddReplica(context.Background(), "group-0", "node-1")
	require.ErrorIs(t, err, sgErr)
	require.Equal(t, 0, fakeNode.changeMembershipCalls)
	require.Equal(t, []string{"node-0"}, dgMgr.Get("group-0").PeerIDs())
}

func TestAddReplica_ChangeMembershipErrorRollsBackPrePublishedPeers(t *testing.T) {
	cmErr := errors.New("configuration change already in flight")
	fakeNode := &fakeRaftNode{
		isLeader:    true,
		committed:   10,
		autoCatchup: true,
		changeMembershipFn: func(context.Context, []raft.ServerEntry, []string) error {
			return cmErr
		},
	}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-1", Address: "10.0.0.1:9001"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0"}, nil))

	err := exec.AddReplica(context.Background(), "group-0", "node-1")
	require.ErrorIs(t, err, cmErr)

	require.Equal(t, 1, fakeNode.changeMembershipCalls)
	require.Len(t, sgUpdater.proposed, 2)
	require.Equal(t, []string{"node-0", "node-1"}, sgUpdater.proposed[0].PeerIDs)
	require.Equal(t, []string{"node-0"}, sgUpdater.proposed[1].PeerIDs)
	require.Equal(t, []string{"node-0"}, exec.DGMgr().Get("group-0").PeerIDs())
}

func TestAddReplica_SkipsExistingVoter(t *testing.T) {
	fakeNode := &fakeRaftNode{isLeader: true, committed: 10, autoCatchup: true}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-1", Address: "10.0.0.1:9001"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0", "node-1"}, nil))

	err := exec.AddReplica(context.Background(), "group-0", "node-1")
	require.NoError(t, err)
	require.Equal(t, 0, fakeNode.changeMembershipCalls)
	require.Empty(t, sgUpdater.proposed)
}

func TestAddReplica_WaitsForLocalLeadership(t *testing.T) {
	checks := 0
	fakeNode := &fakeRaftNode{committed: 10, autoCatchup: true}
	fakeNode.isLeaderFn = func() bool {
		checks++
		return checks >= 3
	}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-1", Address: "10.0.0.1:9001"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0"}, nil))

	err := exec.AddReplica(context.Background(), "group-0", "node-1")
	require.NoError(t, err)

	require.GreaterOrEqual(t, checks, 3)
	require.Equal(t, 1, fakeNode.changeMembershipCalls)
	require.Len(t, sgUpdater.proposed, 2)
	require.Equal(t, []string{"node-0", "node-1"}, sgUpdater.proposed[0].PeerIDs)
}

func TestMoveReplica_NotLeader(t *testing.T) {
	fakeNode := &fakeRaftNode{isLeader: false}
	exec, _ := newTestExecutor(t, fakeNode, nil)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-1"}, nil))

	err := exec.MoveReplica(context.Background(), "group-0", "node-1", "node-2")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not leader")
}

func TestMoveReplica_GroupNotFound(t *testing.T) {
	exec, _ := newTestExecutor(t, &fakeRaftNode{isLeader: true}, nil)
	err := exec.MoveReplica(context.Background(), "nonexistent", "a", "b")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestMoveReplica_ChangeMembershipError_Propagates(t *testing.T) {
	cmErr := errors.New("quorum unavailable")
	fakeNode := &fakeRaftNode{
		isLeader:    true,
		committed:   5,
		autoCatchup: true,
		changeMembershipFn: func(_ context.Context, _ []raft.ServerEntry, _ []string) error {
			return cmErr
		},
	}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-3", Address: "10.0.0.3:9003"},
	}
	exec, _ := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0"}, nil))

	err := exec.MoveReplica(context.Background(), "group-0", "node-0", "node-3")
	require.Error(t, err)
	require.ErrorIs(t, err, cmErr)
}

func TestMoveReplica_FromNodeNotInGroup(t *testing.T) {
	fakeNode := &fakeRaftNode{isLeader: true}
	exec, _ := newTestExecutor(t, fakeNode, nil)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0", "node-1", "node-2"}, nil))

	err := exec.MoveReplica(context.Background(), "group-0", "node-99", "node-3")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a voter")
}

func TestMoveReplica_BlocksUnresolvedLegacyPeer(t *testing.T) {
	fakeNode := &fakeRaftNode{isLeader: true, autoCatchup: true}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-3", Address: "10.0.0.3:9003"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0", "10.0.0.9:9009"}, nil))

	err := exec.MoveReplica(context.Background(), "group-0", "node-0", "node-3")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unresolved legacy peer")
	require.Equal(t, 0, fakeNode.changeMembershipCalls)
	require.Empty(t, sgUpdater.proposed)
}

func TestMoveReplica_ProposeShardGroupError_Propagates(t *testing.T) {
	sgErr := errors.New("meta-raft: not leader")
	fakeNode := &fakeRaftNode{isLeader: true, committed: 5, autoCatchup: true}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-3", Address: "10.0.0.3:9003"},
	}
	addrBook := &fakeAddrBook{nodes: nodes}
	sgUpdater := &fakeSGUpdater{err: sgErr}
	dgMgr := cluster.NewDataGroupManager()
	exec := cluster.NewDataGroupPlanExecutorForTest("node-1", dgMgr, addrBook, sgUpdater,
		func(_ *cluster.DataGroup) cluster.DataRaftNode { return fakeNode })
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0"}, nil))

	err := exec.MoveReplica(context.Background(), "group-0", "node-0", "node-3")
	require.Error(t, err)
	require.ErrorIs(t, err, sgErr)
	// Raft voter change committed via ChangeMembership; MetaFSM is stale.
	require.Equal(t, 1, fakeNode.changeMembershipCalls)
}

// Self-removal guard: when fromNode == localNodeID, MoveReplica calls
// TransferLeadership and returns ErrLeadershipTransferred so the new leader
// can retry the migration.
func TestMoveReplica_SelfRemoval_TransfersLeadership(t *testing.T) {
	transferred := false
	fakeNode := &fakeRaftNode{
		isLeader:  true,
		committed: 5,
		matchIndexes: map[string]uint64{
			"10.0.0.1:9001": 5,
		},
		transferFn: func() error {
			transferred = true
			return nil
		},
	}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-1", Address: "10.0.0.1:9001"},
		{ID: "node-2", Address: "10.0.0.2:9002"},
		{ID: "node-3", Address: "10.0.0.3:9003"},
	}
	addrBook := &fakeAddrBook{nodes: nodes}
	sgUpdater := &fakeSGUpdater{}
	dgMgr := cluster.NewDataGroupManager()
	exec := cluster.NewDataGroupPlanExecutorForTest("node-0", dgMgr, addrBook, sgUpdater,
		func(_ *cluster.DataGroup) cluster.DataRaftNode { return fakeNode })
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0", []string{"node-0", "node-1", "node-2"}, nil))

	err := exec.MoveReplica(context.Background(), "group-0", "node-0", "node-3")
	require.ErrorIs(t, err, cluster.ErrLeadershipTransferred,
		"self-removal must return ErrLeadershipTransferred")
	require.True(t, transferred, "TransferLeadership must be called before returning")
	require.Equal(t, 0, fakeNode.changeMembershipCalls, "ChangeMembership must not be called")
	require.Empty(t, sgUpdater.proposed, "ProposeShardGroup must not be called")
}

func TestMoveReplica_SelfRemovalRequiresCaughtUpTransferTarget(t *testing.T) {
	transferred := false
	fakeNode := &fakeRaftNode{
		isLeader:     true,
		committed:    5,
		matchIndexes: map[string]uint64{"10.0.0.1:9001": 4, "10.0.0.2:9002": 4},
		transferFn: func() error {
			transferred = true
			return nil
		},
	}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-1", Address: "10.0.0.1:9001"},
		{ID: "node-2", Address: "10.0.0.2:9002"},
		{ID: "node-3", Address: "10.0.0.3:9003"},
	}
	addrBook := &fakeAddrBook{nodes: nodes}
	sgUpdater := &fakeSGUpdater{}
	dgMgr := cluster.NewDataGroupManager()
	exec := cluster.NewDataGroupPlanExecutorForTest("node-0", dgMgr, addrBook, sgUpdater,
		func(_ *cluster.DataGroup) cluster.DataRaftNode { return fakeNode })
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0", []string{"node-0", "node-1", "node-2"}, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := exec.MoveReplica(ctx, "group-0", "node-0", "node-3")
	require.Error(t, err)
	require.ErrorContains(t, err, "no caught-up voter available for leadership transfer")
	require.False(t, transferred, "TransferLeadership must not run without a caught-up voter")
	require.Equal(t, 0, fakeNode.changeMembershipCalls, "ChangeMembership must not be called")
	require.Empty(t, sgUpdater.proposed, "ProposeShardGroup must not be called")
}

func TestMoveReplica_ContextCancelDuringCatchup(t *testing.T) {
	fakeNode := &fakeRaftNode{
		isLeader:    true,
		committed:   100,
		autoCatchup: false,
	}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-3", Address: "10.0.0.3:9003"},
	}
	exec, _ := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0"}, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := exec.MoveReplica(ctx, "group-0", "node-0", "node-3")
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// --- EvacuateVoter (P1-2) -------------------------------------------------

func voterConfig(addrs ...string) raft.Configuration {
	cfg := raft.Configuration{}
	for _, a := range addrs {
		cfg.Servers = append(cfg.Servers, raft.Server{ID: a, Suffrage: raft.Voter})
	}
	return cfg
}

func TestEvacuateVoter_AbsentFromRealConfig_ConvergesMirror(t *testing.T) {
	// Divergent state (review finding A): a prior remove committed to raft (revoked
	// already gone from the REAL config) but the follow-up PeerIDs forward failed,
	// so the mirror still lists it. The evacuator only reaches EvacuateVoter with
	// revoked still in the mirror, so this branch IS that divergence — it must
	// converge the mirror (a forward, no raft change), never a bare no-op that loops
	// forever.
	fakeNode := &fakeRaftNode{
		isLeader:  true,
		committed: 10,
		config:    voterConfig("10.0.0.1:9001", "10.0.0.2:9002"), // revoked already gone from raft
	}
	nodes := []cluster.MetaNodeEntry{
		{ID: "revoked", Address: "10.0.0.0:9000"},
		{ID: "r1", Address: "10.0.0.1:9001"},
		{ID: "r2", Address: "10.0.0.2:9002"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"revoked", "r1", "r2"}, nil)) // mirror still lists revoked

	err := exec.EvacuateVoter(context.Background(), "group-0", "revoked", "")
	require.NoError(t, err)
	require.Equal(t, 0, fakeNode.changeMembershipCalls) // already absent from raft: no membership change
	require.Len(t, sgUpdater.proposed, 1)               // but the mirror converges from the real config
	require.ElementsMatch(t, []string{"r1", "r2"}, sgUpdater.proposed[0].PeerIDs)
	require.NotContains(t, sgUpdater.proposed[0].PeerIDs, "revoked")
}

func TestEvacuateVoter_ShrinkWhenNoReplacement(t *testing.T) {
	fakeNode := &fakeRaftNode{
		isLeader:  true,
		committed: 10,
		config:    voterConfig("10.0.0.0:9000", "10.0.0.1:9001", "10.0.0.2:9002"),
	}
	nodes := []cluster.MetaNodeEntry{
		{ID: "revoked", Address: "10.0.0.0:9000"},
		{ID: "r1", Address: "10.0.0.1:9001"},
		{ID: "r2", Address: "10.0.0.2:9002"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"revoked", "r1", "r2"}, nil))

	err := exec.EvacuateVoter(context.Background(), "group-0", "revoked", "")
	require.NoError(t, err)
	require.Equal(t, 1, fakeNode.changeMembershipCalls)
	require.Empty(t, fakeNode.lastAdds)
	require.Equal(t, []string{"10.0.0.0:9000"}, fakeNode.lastRemoves)

	// PeerIDs mirror converges from the real config minus the revoked node.
	require.Len(t, sgUpdater.proposed, 1)
	require.ElementsMatch(t, []string{"r1", "r2"}, sgUpdater.proposed[0].PeerIDs)
	require.NotContains(t, sgUpdater.proposed[0].PeerIDs, "revoked")
}

func TestEvacuateVoter_FreshMove_DelegatesToMoveReplica(t *testing.T) {
	// survivors (real voters minus revoked) = {r1} = 1 < target 2 -> fresh move.
	fakeNode := &fakeRaftNode{
		isLeader:    true,
		committed:   10,
		autoCatchup: true,
		config:      voterConfig("10.0.0.0:9000", "10.0.0.1:9001"),
	}
	nodes := []cluster.MetaNodeEntry{
		{ID: "revoked", Address: "10.0.0.0:9000"},
		{ID: "r1", Address: "10.0.0.1:9001"},
		{ID: "newnode", Address: "10.0.0.3:9003"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"revoked", "r1"}, nil))

	err := exec.EvacuateVoter(context.Background(), "group-0", "revoked", "newnode")
	require.NoError(t, err)
	require.Equal(t, 1, fakeNode.changeMembershipCalls)
	require.Len(t, fakeNode.lastAdds, 1)
	require.Equal(t, "newnode", fakeNode.lastAdds[0].ID)
	// Move delegates to MoveReplica, which removes by node id (fromNode).
	require.Equal(t, []string{"revoked"}, fakeNode.lastRemoves)
	require.NotEmpty(t, sgUpdater.proposed)
}

func TestEvacuateVoter_RefusesLastVoter(t *testing.T) {
	fakeNode := &fakeRaftNode{
		isLeader:  true,
		committed: 10,
		config:    voterConfig("10.0.0.0:9000"),
	}
	nodes := []cluster.MetaNodeEntry{
		{ID: "revoked", Address: "10.0.0.0:9000"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"revoked"}, nil))

	err := exec.EvacuateVoter(context.Background(), "group-0", "revoked", "")
	require.ErrorIs(t, err, cluster.ErrRefuseLastVoter)
	require.Equal(t, 0, fakeNode.changeMembershipCalls)
	require.Empty(t, sgUpdater.proposed)
}

func TestEvacuateVoter_FullyAbsent_NoOp(t *testing.T) {
	// Revoked node is absent from BOTH the real config AND the mirror (e.g. a
	// double-fire after a complete eviction). No raft change, and the convergence
	// pass is a true no-op because the mirror already matches the real config
	// (samePeerSet skips the redundant proposal).
	fakeNode := &fakeRaftNode{
		isLeader:  true,
		committed: 10,
		config:    voterConfig("10.0.0.1:9001", "10.0.0.2:9002"),
	}
	nodes := []cluster.MetaNodeEntry{
		{ID: "r1", Address: "10.0.0.1:9001"},
		{ID: "r2", Address: "10.0.0.2:9002"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"r1", "r2"}, nil))

	err := exec.EvacuateVoter(context.Background(), "group-0", "revoked", "")
	require.NoError(t, err)
	require.Equal(t, 0, fakeNode.changeMembershipCalls)
	require.Empty(t, sgUpdater.proposed)
}

// TestEvacuateVoter_CrashedMove_DifferentReplacementPicked_RemoveOnly is the
// discriminating test: a prior move added node-X then crashed before removing
// revoked. The retry picks a DIFFERENT replacement (node-Y). A count-based impl
// must issue REMOVE-ONLY (adds empty) because survivors already >= target; a
// pick-based impl would add node-Y -> over-replication.
func TestEvacuateVoter_CrashedMove_DifferentReplacementPicked_RemoveOnly(t *testing.T) {
	// REAL config voters = {revoked, r1, r2, node-X} (4 voters, prior crashed move added node-X).
	fakeNode := &fakeRaftNode{
		isLeader:  true,
		committed: 10,
		config: voterConfig(
			"10.0.0.0:9000", // revoked
			"10.0.0.1:9001", // r1
			"10.0.0.2:9002", // r2
			"10.0.0.4:9004", // node-X (added by crashed move)
		),
	}
	nodes := []cluster.MetaNodeEntry{
		{ID: "revoked", Address: "10.0.0.0:9000"},
		{ID: "r1", Address: "10.0.0.1:9001"},
		{ID: "r2", Address: "10.0.0.2:9002"},
		{ID: "node-X", Address: "10.0.0.4:9004"},
		{ID: "node-Y", Address: "10.0.0.5:9005"},
	}
	exec, sgUpdater := newTestExecutor(t, fakeNode, nodes)
	// mirror dg.PeerIDs() = {revoked, r1, r2} => target = 3.
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"revoked", "r1", "r2"}, nil))

	// Retry picks a DIFFERENT replacement (node-Y, NOT a current voter).
	err := exec.EvacuateVoter(context.Background(), "group-0", "revoked", "node-Y")
	require.NoError(t, err)

	// survivors {r1,r2,X} = 3 >= target 3 -> remove-only. Must NOT add node-Y.
	require.Equal(t, 1, fakeNode.changeMembershipCalls)
	require.Empty(t, fakeNode.lastAdds, "must NOT add node-Y (pick-based would over-replicate)")
	require.Equal(t, []string{"10.0.0.0:9000"}, fakeNode.lastRemoves)

	// Mirror converges from real config minus revoked = {r1, r2, node-X}.
	require.Len(t, sgUpdater.proposed, 1)
	require.ElementsMatch(t, []string{"r1", "r2", "node-X"}, sgUpdater.proposed[0].PeerIDs)
	require.NotContains(t, sgUpdater.proposed[0].PeerIDs, "revoked")
	require.NotContains(t, sgUpdater.proposed[0].PeerIDs, "node-Y")
}
