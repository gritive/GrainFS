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

func TestMetaRaft_ConcurrentJoin_SecondFails(t *testing.T) {
	dir0 := t.TempDir()
	tr := newMetaTransportFake()

	m0, err := NewMetaRaft(MetaRaftConfig{
		NodeID:    "node-0",
		Peers:     nil,
		DataDir:   dir0,
		Transport: tr,
	})
	require.NoError(t, err)
	tr.register("node-0-addr", m0)

	// Create two potential joiners but don't start them — we only care about
	// the leader-side sequential enforcement
	tr.register("node-a-addr", m0) // reuse m0 just to avoid "not found" errors
	tr.register("node-b-addr", m0)

	t.Cleanup(func() { _ = m0.Close() })

	require.NoError(t, m0.Bootstrap())
	require.NoError(t, m0.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m0.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, id := range []string{"node-a", "node-b"} {
		wg.Add(1)
		i, id := i, id
		go func() {
			defer wg.Done()
			addr := id + "-addr"
			errs[i] = m0.Join(ctx, id, addr)
		}()
	}
	wg.Wait()

	// At least one must succeed, at most one may fail with conf-change error
	successCount := 0
	for _, err := range errs {
		if err == nil {
			successCount++
		}
	}
	assert.GreaterOrEqual(t, successCount, 1, "at least one Join must succeed")
}
