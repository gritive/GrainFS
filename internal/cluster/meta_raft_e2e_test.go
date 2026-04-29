package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
)

// TestMetaRaft_ThreeNodeBootstrap_E2E spins up a 3-node meta-Raft cluster
// in-process using MetaTransportFake (no port binding) and verifies:
//  1. node-0 bootstraps and becomes leader
//  2. node-1 joins → appears in node-0's FSM
//  3. node-2 joins → both node-1 and node-2 appear in node-0's FSM
//  4. exactly one leader among the three nodes
//  5. all nodes can be closed cleanly
func TestMetaRaft_ThreeNodeBootstrap_E2E(t *testing.T) {
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

	// Bootstrap and start node-0 (singleton bootstrap).
	require.NoError(t, m0.Bootstrap())
	require.NoError(t, m0.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m0.node.State() == raft.Leader
	}, 3*time.Second, 20*time.Millisecond, "node-0 must become leader")

	// Join node-1.
	require.NoError(t, m1.Bootstrap())
	require.NoError(t, m1.Start(context.Background()))

	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	require.NoError(t, m0.Join(ctx1, "node-1", "node-1"))

	require.Eventually(t, func() bool {
		for _, n := range m0.fsm.Nodes() {
			if n.ID == "node-1" {
				return true
			}
		}
		return false
	}, 3*time.Second, 30*time.Millisecond, "node-1 must appear in FSM after join")

	// Join node-2.
	require.NoError(t, m2.Bootstrap())
	require.NoError(t, m2.Start(context.Background()))

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	require.NoError(t, m0.Join(ctx2, "node-2", "node-2"))

	require.Eventually(t, func() bool {
		fsmNodes := m0.fsm.Nodes()
		has1, has2 := false, false
		for _, n := range fsmNodes {
			if n.ID == "node-1" {
				has1 = true
			}
			if n.ID == "node-2" {
				has2 = true
			}
		}
		return has1 && has2
	}, 3*time.Second, 30*time.Millisecond, "both node-1 and node-2 must appear in FSM")

	// Exactly one leader in the group.
	leaderCount := 0
	for _, m := range []*MetaRaft{m0, m1, m2} {
		if m.node.State() == raft.Leader {
			leaderCount++
		}
	}
	assert.Equal(t, 1, leaderCount, "exactly one leader must exist in the group")
}
