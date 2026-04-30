package raft

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/transport"
)

// quicCluster wires up N Raft nodes communicating over real QUIC transport.
type quicCluster struct {
	nodes      []*Node
	transports []*transport.QUICTransport
	rpcs       []*QUICRPCTransport
}

func newQUICCluster(t *testing.T, n int) *quicCluster {
	t.Helper()
	ctx := context.Background()

	transports := make([]*transport.QUICTransport, n)
	for i := range transports {
		transports[i] = transport.NewQUICTransport()
		require.NoError(t, transports[i].Listen(ctx, "127.0.0.1:0"))
	}

	addrs := make([]string, n)
	for i, tr := range transports {
		addrs[i] = tr.LocalAddr()
	}

	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		peers := make([]string, 0, n-1)
		for j := 0; j < n; j++ {
			if i != j {
				peers = append(peers, addrs[j])
			}
		}
		config := Config{
			ID:               addrs[i],
			Peers:            peers,
			ElectionTimeout:  200 * time.Millisecond,
			HeartbeatTimeout: 50 * time.Millisecond,
		}
		nodes[i] = NewNode(config)
	}

	// Full mesh QUIC connections
	for i := range transports {
		for j := range transports {
			if i != j {
				require.NoError(t, transports[i].Connect(ctx, addrs[j]))
			}
		}
	}

	// Create QUIC RPC transports and wire them to nodes
	rpcs := make([]*QUICRPCTransport, n)
	for i := range nodes {
		rpcs[i] = NewQUICRPCTransport(transports[i], nodes[i])
		rpcs[i].SetTransport()
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.Stop()
		}
		for _, tr := range transports {
			tr.Close()
		}
	})

	return &quicCluster{nodes: nodes, transports: transports, rpcs: rpcs}
}

func (c *quicCluster) startAll() {
	for _, n := range c.nodes {
		n.Start()
	}
}

func (c *quicCluster) waitForLeader(timeout time.Duration) *Node {
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return nil
		default:
			for _, n := range c.nodes {
				if n.State() == Leader {
					return n
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// --- Integration Tests (real QUIC transport) ---

func TestIntegration_ThreeNodeQUIC_ElectsLeader(t *testing.T) {
	cluster := newQUICCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(5 * time.Second)
	require.NotNil(t, leader, "no leader elected in 3-node QUIC cluster")

	leaderCount := 0
	for _, n := range cluster.nodes {
		if n.State() == Leader {
			leaderCount++
		}
	}
	assert.Equal(t, 1, leaderCount, "exactly one leader")
}

func TestIntegration_ThreeNodeQUIC_FollowersKnowLeader(t *testing.T) {
	cluster := newQUICCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	time.Sleep(500 * time.Millisecond)

	for _, n := range cluster.nodes {
		if n.ID() == leader.ID() {
			continue
		}
		assert.Equal(t, Follower, n.State(), "node %s should be follower", n.ID())
		assert.Equal(t, leader.ID(), n.LeaderID(), "node %s should know leader", n.ID())
	}
}

func TestIntegration_ThreeNodeQUIC_Propose_Replicate(t *testing.T) {
	cluster := newQUICCluster(t, 3)
	cluster.startAll()

	leader := cluster.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	require.NoError(t, leader.Propose([]byte("quic-cmd-1")))
	require.NoError(t, leader.Propose([]byte("quic-cmd-2")))

	time.Sleep(1 * time.Second)

	for _, n := range cluster.nodes {
		n.mu.Lock()
		logLen := len(n.log)
		n.mu.Unlock()
		assert.GreaterOrEqual(t, logLen, 2, "node %s should have >= 2 entries", n.ID())
	}
}

func TestIntegration_ThreeNodeQUIC_LeaderFailover(t *testing.T) {
	cluster := newQUICCluster(t, 3)
	cluster.startAll()

	leader1 := cluster.waitForLeader(5 * time.Second)
	require.NotNil(t, leader1)

	require.NoError(t, leader1.Propose([]byte("before-failover")))
	time.Sleep(500 * time.Millisecond)

	leader1ID := leader1.ID()
	leader1.Stop()

	var leader2 *Node
	deadline := time.After(5 * time.Second)
	for leader2 == nil {
		select {
		case <-deadline:
			t.Fatal("no new leader elected after failover")
		default:
			for _, n := range cluster.nodes {
				if n.ID() != leader1ID && n.State() == Leader {
					leader2 = n
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	assert.NotEqual(t, leader1ID, leader2.ID())
	require.NoError(t, leader2.Propose([]byte("after-failover")))

	time.Sleep(500 * time.Millisecond)

	for _, n := range cluster.nodes {
		if n.ID() == leader1ID {
			continue
		}
		n.mu.Lock()
		logLen := len(n.log)
		n.mu.Unlock()
		assert.GreaterOrEqual(t, logLen, 2, "surviving node %s should have >= 2 entries", n.ID())
	}
}

func TestIntegration_PersistenceAndRecovery(t *testing.T) {
	dir := t.TempDir()

	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)

	entries := []LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
		{Term: 2, Index: 3, Command: []byte("cmd3")},
	}
	require.NoError(t, store.AppendEntries(entries))
	require.NoError(t, store.SaveState(2, "node-B"))
	require.NoError(t, store.SaveSnapshot(Snapshot{Index: 2, Term: 1, Data: []byte(`{"x":1}`)}))
	require.NoError(t, store.Close())

	store2, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	defer store2.Close()

	lastIdx, err := store2.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), lastIdx)

	term, votedFor, err := store2.LoadState()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), term)
	assert.Equal(t, "node-B", votedFor)

	snap2, err := store2.LoadSnapshot()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), snap2.Index)
	assert.Equal(t, uint64(1), snap2.Term)
	assert.Equal(t, `{"x":1}`, string(snap2.Data))

	for _, want := range entries {
		got, err := store2.GetEntry(want.Index)
		require.NoError(t, err)
		assert.Equal(t, string(want.Command), string(got.Command))
	}
}

func TestRestoreFromStore_LoadsSnapshotServers(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)

	snap := Snapshot{
		Index: 10,
		Term:  2,
		Data:  []byte("fsm-state"),
		Servers: []Server{
			{ID: "node-1", Suffrage: Voter},
			{ID: "node-2", Suffrage: Voter},
			{ID: "node-3", Suffrage: Voter},
		},
	}
	require.NoError(t, store.SaveSnapshot(snap))
	require.NoError(t, store.Close())

	store2, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { store2.Close() })

	cfg := DefaultConfig("node-1", nil)
	node := NewNode(cfg, store2)

	cfg2 := node.Configuration()
	ids := make(map[string]bool, len(cfg2.Servers))
	for _, s := range cfg2.Servers {
		ids[s.ID] = true
	}
	assert.True(t, ids["node-2"], "node-2 must be restored from snapshot")
	assert.True(t, ids["node-3"], "node-3 must be restored from snapshot")
	assert.True(t, ids["node-1"], "self (node-1) must be in configuration")

	node.mu.Lock()
	assert.Equal(t, uint64(10), node.lastApplied, "lastApplied must match snapshot index")
	assert.Equal(t, uint64(10), node.commitIndex, "commitIndex must match snapshot index")
	assert.Equal(t, uint64(2), node.currentTerm, "term must be restored from snapshot")
	node.mu.Unlock()
}
