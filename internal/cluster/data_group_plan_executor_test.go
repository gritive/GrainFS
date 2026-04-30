package cluster_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
}

func (f *fakeRaftNode) IsLeader() bool         { return f.isLeader }
func (f *fakeRaftNode) CommittedIndex() uint64 { return f.committed }
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
	proposed []cluster.ShardGroupEntry
	err      error
}

func (f *fakeSGUpdater) ProposeShardGroup(_ context.Context, sg cluster.ShardGroupEntry) error {
	f.proposed = append(f.proposed, sg)
	return f.err
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
	assert.Equal(t, "node-3", fakeNode.lastAdds[0].ID)
	assert.Equal(t, "10.0.0.3:9003", fakeNode.lastAdds[0].Address)
	assert.Equal(t, raft.Voter, fakeNode.lastAdds[0].Suffrage)
	assert.Equal(t, []string{"10.0.0.0:9000"}, fakeNode.lastRemoves)

	require.Len(t, sgUpdater.proposed, 1)
	assert.Equal(t, "group-0", sgUpdater.proposed[0].ID)
	assert.Contains(t, sgUpdater.proposed[0].PeerIDs, "node-3")
	assert.NotContains(t, sgUpdater.proposed[0].PeerIDs, "node-0")
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

// PR-K2: Self-removal no longer requires TransferLeadership pre-step. The
// joint commit-time step-down hook in raft.Node closes jointPromoteCh BEFORE
// state=Follower, so the caller wakes up nil even when removing self.
func TestMoveReplica_SelfRemoval_UsesChangeMembership(t *testing.T) {
	fakeNode := &fakeRaftNode{isLeader: true, committed: 5, autoCatchup: true}
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-0", Address: "10.0.0.0:9000"},
		{ID: "node-3", Address: "10.0.0.3:9003"},
	}
	addrBook := &fakeAddrBook{nodes: nodes}
	sgUpdater := &fakeSGUpdater{}
	dgMgr := cluster.NewDataGroupManager()
	// localNodeID == fromNode → self-removal scenario.
	exec := cluster.NewDataGroupPlanExecutorForTest("node-0", dgMgr, addrBook, sgUpdater,
		func(_ *cluster.DataGroup) cluster.DataRaftNode { return fakeNode })
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0", []string{"node-0"}, nil))

	err := exec.MoveReplica(context.Background(), "group-0", "node-0", "node-3")
	require.NoError(t, err, "self-removal returns nil via joint commit-time hook")
	require.Equal(t, 1, fakeNode.changeMembershipCalls)
	require.Equal(t, []string{"10.0.0.0:9000"}, fakeNode.lastRemoves)
	require.Len(t, fakeNode.lastAdds, 1)
	assert.Equal(t, "node-3", fakeNode.lastAdds[0].ID)
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
