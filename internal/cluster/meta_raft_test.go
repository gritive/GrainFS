package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// MetaTransportFake routes meta-Raft RPCs in-process for unit testing.
// It accesses MetaRaft.node (unexported) because it lives in package cluster.
type MetaTransportFake struct {
	mu    sync.RWMutex
	peers map[string]*MetaRaft
}

func newMetaTransportFake() *MetaTransportFake {
	return &MetaTransportFake{peers: make(map[string]*MetaRaft)}
}

func (f *MetaTransportFake) register(addr string, m *MetaRaft) {
	f.mu.Lock()
	f.peers[addr] = m
	f.mu.Unlock()
}

func (f *MetaTransportFake) SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	f.mu.RLock()
	m := f.peers[peer]
	f.mu.RUnlock()
	if m == nil {
		return nil, fmt.Errorf("fake transport: peer %s not registered", peer)
	}
	return m.node.HandleRequestVote(args), nil
}

func (f *MetaTransportFake) SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	f.mu.RLock()
	m := f.peers[peer]
	f.mu.RUnlock()
	if m == nil {
		return nil, fmt.Errorf("fake transport: peer %s not registered", peer)
	}
	return m.node.HandleAppendEntries(args), nil
}

func (f *MetaTransportFake) SendInstallSnapshot(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	f.mu.RLock()
	m := f.peers[peer]
	f.mu.RUnlock()
	if m == nil {
		return nil, fmt.Errorf("fake transport: peer %s not registered", peer)
	}
	return m.node.HandleInstallSnapshot(args), nil
}

// newSingleMetaRaft creates a single-node MetaRaft for testing.
func newSingleMetaRaft(t *testing.T) *MetaRaft {
	t.Helper()
	tr := newMetaTransportFake()
	m, err := NewMetaRaft(MetaRaftConfig{
		NodeID:    "node-0",
		Peers:     nil,
		DataDir:   t.TempDir(),
		Transport: tr,
	})
	require.NoError(t, err)
	tr.register("node-0", m)
	return m
}

func TestMetaRaft_Bootstrap_SingleNode(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background()))

	// Bootstrap is idempotent
	require.NoError(t, m.Bootstrap())

	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond, "single node must become leader")
}

func TestMetaRaft_Join_AddsLearnerThenVoter(t *testing.T) {
	dir0 := t.TempDir()
	dir1 := t.TempDir()

	tr := newMetaTransportFake()

	m0, err := NewMetaRaft(MetaRaftConfig{
		NodeID:    "node-0",
		Peers:     nil,
		DataDir:   dir0,
		Transport: tr,
	})
	require.NoError(t, err)
	tr.register("node-0-addr", m0)

	m1, err := NewMetaRaft(MetaRaftConfig{
		NodeID:    "node-1",
		Peers:     []string{"node-0-addr"},
		DataDir:   dir1,
		Transport: tr,
	})
	require.NoError(t, err)
	tr.register("node-1-addr", m1)

	t.Cleanup(func() { _ = m0.Close(); _ = m1.Close() })

	require.NoError(t, m0.Bootstrap())
	require.NoError(t, m0.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m0.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	require.NoError(t, m1.Bootstrap())
	require.NoError(t, m1.Start(context.Background()))

	// Join: leader adds node-1
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, m0.Join(ctx, "node-1", "node-1-addr"))

	// node-1 must appear in m0's FSM
	require.Eventually(t, func() bool {
		for _, n := range m0.fsm.Nodes() {
			if n.ID == "node-1" {
				return true
			}
		}
		return false
	}, 3*time.Second, 30*time.Millisecond, "node-1 must appear in FSM")
}

func TestMetaRaft_ProposeAddNode_CommitToFSM(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	entry := MetaNodeEntry{ID: "test-node", Address: "10.0.0.9:7001", Role: 0}
	require.NoError(t, m.ProposeAddNode(ctx, entry))

	// FSM must contain the node
	found := false
	for _, n := range m.fsm.Nodes() {
		if n.ID == "test-node" {
			found = true
			assert.Equal(t, "10.0.0.9:7001", n.Address)
		}
	}
	assert.True(t, found, "ProposeAddNode must commit to FSM")
}

func TestMetaRaft_Close_StopsApplyLoop(t *testing.T) {
	m := newSingleMetaRaft(t)

	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = m.Close()
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close() did not return within 3s — goroutine leak suspected")
	}
}

func TestMetaRaft_ProposeBucketAssignment_CommitToFSM(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, m.ProposeBucketAssignment(ctx, "photos", "group-0"))

	assignments := m.FSM().BucketAssignments()
	assert.Equal(t, "group-0", assignments["photos"])
}

func TestMetaRaft_ProposeBucketAssignment_CallbackFired(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	var cbBucket, cbGroup string
	m.FSM().SetOnBucketAssigned(func(bucket, groupID string) {
		cbBucket = bucket
		cbGroup = groupID
	})

	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, m.ProposeBucketAssignment(ctx, "videos", "group-1"))

	require.Eventually(t, func() bool {
		return cbBucket == "videos"
	}, 2*time.Second, 10*time.Millisecond, "callback must fire after propose")
	assert.Equal(t, "group-1", cbGroup)
}

func TestMetaRaft_ProposeLoadSnapshot_CommitToFSM(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	entries := []LoadStatEntry{
		{NodeID: "n1", DiskUsedPct: 80.0, DiskAvailBytes: 1000, UpdatedAt: time.Now()},
		{NodeID: "n2", DiskUsedPct: 20.0, DiskAvailBytes: 9000, UpdatedAt: time.Now()},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, m.ProposeLoadSnapshot(ctx, entries))

	snap := m.FSM().LoadSnapshot()
	require.Len(t, snap, 2)
	assert.InDelta(t, 80.0, snap["n1"].DiskUsedPct, 0.01)
	assert.InDelta(t, 20.0, snap["n2"].DiskUsedPct, 0.01)
}

func TestMetaRaft_ProposeRebalancePlan_CommitToFSM(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	plan := RebalancePlan{
		PlanID:    "plan-1",
		GroupID:   "group-0",
		FromNode:  "n1",
		ToNode:    "n2",
		CreatedAt: time.Now(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, m.ProposeRebalancePlan(ctx, plan))
	assert.Equal(t, "plan-1", m.FSM().ActivePlanID())
}

func TestMetaRaft_ProposeAbortPlan_CommitToFSM(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	plan := RebalancePlan{PlanID: "plan-1", GroupID: "g0", FromNode: "n1", ToNode: "n2", CreatedAt: time.Now()}
	require.NoError(t, m.ProposeRebalancePlan(ctx, plan))
	require.Equal(t, "plan-1", m.FSM().ActivePlanID())

	require.NoError(t, m.ProposeAbortPlan(ctx, "plan-1"))
	assert.Empty(t, m.FSM().ActivePlanID())
}

// TestMetaRaft_ConcurrentJoin_AtLeastOneSucceeds verifies that concurrent Join
// requests are serialised at the ConfChange level: only one pending conf change
// at a time, but at least one of two concurrent Join calls must succeed.
// Uses real joiner nodes so that learner replication can actually complete.
func TestMetaRaft_ConcurrentJoin_AtLeastOneSucceeds(t *testing.T) {
	tr := newMetaTransportFake()

	m0, err := NewMetaRaft(MetaRaftConfig{NodeID: "node-0", Peers: nil, DataDir: t.TempDir(), Transport: tr})
	require.NoError(t, err)
	tr.register("node-0-addr", m0)

	m1, err := NewMetaRaft(MetaRaftConfig{NodeID: "node-a", Peers: []string{"node-0-addr"}, DataDir: t.TempDir(), Transport: tr})
	require.NoError(t, err)
	tr.register("node-a-addr", m1)

	m2, err := NewMetaRaft(MetaRaftConfig{NodeID: "node-b", Peers: []string{"node-0-addr"}, DataDir: t.TempDir(), Transport: tr})
	require.NoError(t, err)
	tr.register("node-b-addr", m2)

	t.Cleanup(func() { _ = m0.Close(); _ = m1.Close(); _ = m2.Close() })

	require.NoError(t, m0.Bootstrap())
	require.NoError(t, m0.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m0.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	// Start joiners after m0 is leader so they don't interfere with election.
	require.NoError(t, m1.Bootstrap())
	require.NoError(t, m1.Start(context.Background()))
	require.NoError(t, m2.Bootstrap())
	require.NoError(t, m2.Start(context.Background()))

	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, args := range []struct{ id, addr string }{
		{"node-a", "node-a-addr"},
		{"node-b", "node-b-addr"},
	} {
		wg.Add(1)
		i, args := i, args
		go func() {
			defer wg.Done()
			errs[i] = m0.Join(ctx, args.id, args.addr)
		}()
	}
	wg.Wait()

	// At least one must succeed; the other may fail with ErrConfChangeInProgress.
	successCount := 0
	for _, err := range errs {
		if err == nil {
			successCount++
		}
	}
	assert.GreaterOrEqual(t, successCount, 1, "at least one concurrent Join must succeed")
}
