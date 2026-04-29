package cluster_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
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
	addedLearner [2]string // [id, addr]
	promoted     string
	removed      string
}

func (f *fakeRaftNode) IsLeader() bool         { return f.isLeader }
func (f *fakeRaftNode) CommittedIndex() uint64 { return f.committed }
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
	if f.autoCatchup {
		f.matchIndexes[addr] = f.committed
	} else {
		f.matchIndexes[addr] = 0
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
	exec := cluster.NewDataGroupPlanExecutorForTest(dgMgr, addrBook, sgUpdater, func(dg *cluster.DataGroup) cluster.DataRaftNode {
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

	assert.Equal(t, "node-3", fakeNode.addedLearner[0])
	assert.Equal(t, "10.0.0.3:9003", fakeNode.addedLearner[1])
	assert.Equal(t, "node-3", fakeNode.promoted)
	assert.Equal(t, "10.0.0.0:9000", fakeNode.removed)

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

func TestMoveReplica_AddLearnerError_Propagates(t *testing.T) {
	addErr := errors.New("quorum unavailable")
	fakeNode := &fakeRaftNode{
		isLeader:     true,
		committed:    5,
		addLearnerFn: func(_, _ string) error { return addErr },
	}
	nodes := []cluster.MetaNodeEntry{{ID: "node-3", Address: "10.0.0.3:9003"}}
	exec, _ := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0"}, nil))

	err := exec.MoveReplica(context.Background(), "group-0", "node-0", "node-3")
	require.Error(t, err)
	require.ErrorIs(t, err, addErr)
}

func TestMoveReplica_ContextCancelDuringCatchup(t *testing.T) {
	fakeNode := &fakeRaftNode{
		isLeader:    true,
		committed:   100,
		autoCatchup: false,
	}
	nodes := []cluster.MetaNodeEntry{{ID: "node-3", Address: "10.0.0.3:9003"}}
	exec, _ := newTestExecutor(t, fakeNode, nodes)
	exec.DGMgr().Add(cluster.NewDataGroupWithBackend("group-0",
		[]string{"node-0"}, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := exec.MoveReplica(ctx, "group-0", "node-0", "node-3")
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
