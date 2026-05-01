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

// TestSnapshotPreservesClusterMembership verifies the full §2.3 fix:
// after a snapshot is taken and a follower restarts, the follower recovers
// its peer list from the snapshot (not from initial peers).
func TestSnapshotPreservesClusterMembership(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}

	ctx := context.Background()
	const numNodes = 3

	dirs := make([]string, numNodes)
	stores := make([]*BadgerLogStore, numNodes)
	for i := range dirs {
		dirs[i] = t.TempDir()
		var err error
		stores[i], err = NewBadgerLogStore(dirs[i])
		require.NoError(t, err)
	}

	transports := make([]*transport.QUICTransport, numNodes)
	for i := range transports {
		transports[i] = transport.NewQUICTransport()
		require.NoError(t, transports[i].Listen(ctx, "127.0.0.1:0"))
	}
	addrs := make([]string, numNodes)
	for i, tr := range transports {
		addrs[i] = tr.LocalAddr()
	}

	nodes := make([]*Node, numNodes)
	for i := 0; i < numNodes; i++ {
		peers := make([]string, 0, numNodes-1)
		for j := 0; j < numNodes; j++ {
			if i != j {
				peers = append(peers, addrs[j])
			}
		}
		cfg := Config{
			ID:               addrs[i],
			Peers:            peers,
			ElectionTimeout:  200 * time.Millisecond,
			HeartbeatTimeout: 50 * time.Millisecond,
		}
		nodes[i] = NewNode(cfg, stores[i])
	}

	for i := range transports {
		for j := range transports {
			if i != j {
				require.NoError(t, transports[i].Connect(ctx, addrs[j]))
			}
		}
	}

	rpcs := make([]*QUICRPCTransport, numNodes)
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
		for _, s := range stores {
			s.Close()
		}
	})

	for _, n := range nodes {
		n.Start()
	}
	cluster := &quicCluster{nodes: nodes, transports: transports, rpcs: rpcs}
	leader := cluster.waitForLeader(5 * time.Second)
	require.NotNil(t, leader, "cluster must elect a leader")

	leaderIdx := -1
	for i, n := range nodes {
		if n.State() == Leader {
			leaderIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, leaderIdx, 0, "leader index must be found")

	snapServers := nodes[leaderIdx].Configuration().Servers
	require.Len(t, snapServers, numNodes, "leader config must have all 3 nodes")

	snapToSave := Snapshot{
		Index:   5,
		Term:    leader.Term(),
		Data:    []byte("test-state"),
		Servers: snapServers,
	}
	require.NoError(t, stores[leaderIdx].SaveSnapshot(snapToSave))

	followerIdx := -1
	for i, n := range nodes {
		if n.State() != Leader {
			followerIdx = i
			break
		}
	}
	require.GreaterOrEqual(t, followerIdx, 0, "follower must exist")

	require.NoError(t, stores[followerIdx].SaveSnapshot(Snapshot{
		Index:   5,
		Term:    leader.Term(),
		Data:    []byte("test-state"),
		Servers: snapServers,
	}))

	nodes[followerIdx].Stop()
	time.Sleep(100 * time.Millisecond)

	followerAddr := addrs[followerIdx]
	allOtherPeers := make([]string, 0, numNodes-1)
	for j, a := range addrs {
		if j != followerIdx {
			allOtherPeers = append(allOtherPeers, a)
		}
	}
	restartCfg := Config{
		ID:               followerAddr,
		Peers:            nil, // intentionally empty — must restore from snapshot
		ElectionTimeout:  200 * time.Millisecond,
		HeartbeatTimeout: 50 * time.Millisecond,
	}
	restartedNode := NewNode(restartCfg, stores[followerIdx])
	nodes[followerIdx] = restartedNode
	rpcs[followerIdx] = NewQUICRPCTransport(transports[followerIdx], restartedNode)
	rpcs[followerIdx].SetTransport()
	restartedNode.Start()

	time.Sleep(300 * time.Millisecond)

	cfg := restartedNode.Configuration()
	peerIDs := make(map[string]bool)
	for _, s := range cfg.Servers {
		peerIDs[s.ID] = true
	}
	for _, addr := range allOtherPeers {
		assert.True(t, peerIDs[addr], "restarted follower must know peer %s from snapshot", addr)
	}
}

func TestRestoreFromStore_LegacySnapshot(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)

	// Legacy snapshot: servers field is nil.
	require.NoError(t, store.SaveSnapshot(Snapshot{
		Index: 7,
		Term:  3,
		Data:  []byte("legacy-fsm-state"),
	}))
	require.NoError(t, store.Close())

	store2, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { store2.Close() })

	cfg := DefaultConfig("node-1", []string{"node-2", "node-3"})
	node := NewNode(cfg, store2)

	node.mu.Lock()
	defer node.mu.Unlock()

	// Snapshot watermark must be applied even for legacy snapshots.
	assert.Equal(t, uint64(7), node.lastApplied, "lastApplied must match snapshot index")
	assert.Equal(t, uint64(7), node.commitIndex, "commitIndex must match snapshot index")
	assert.Equal(t, uint64(3), node.currentTerm, "term must be restored from snapshot")

	// Best-effort fallback: config falls back to initialPeers (no membership data in snapshot).
	assert.ElementsMatch(t, []string{"node-2", "node-3"}, node.config.Peers,
		"legacy snapshot must fall back to initialPeers")
}

func TestAddVoter_E2E_LearnerFirstThenPromote(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}

	const numNodes = 3
	cluster := newQUICCluster(t, numNodes)
	cluster.startAll()
	t.Cleanup(func() {
		for _, n := range cluster.nodes {
			n.Stop()
		}
		for _, tr := range cluster.transports {
			tr.Close()
		}
	})

	leader := cluster.waitForLeader(5 * time.Second)
	require.NotNil(t, leader, "must elect leader")

	// Drive commitIndex up so AddLearner can commit.
	for i := 0; i < 5; i++ {
		require.NoError(t, leader.Propose([]byte("entry")))
	}

	// Set high threshold so a brand-new learner with matchIndex=0 trivially passes.
	leader.SetLearnerCatchupThreshold(1_000_000)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := leader.AddVoterCtx(ctx, "fake-node", "127.0.0.1:65530")
	require.NoError(t, err, "AddVoter must complete")

	cfg := leader.Configuration()
	found := false
	for _, s := range cfg.Servers {
		if s.ID == "127.0.0.1:65530" && s.Suffrage == Voter {
			found = true
			break
		}
	}
	require.True(t, found, "fake-node must be voter in leader's config after AddVoter")
}

func TestAddVoter_E2E_LeaderChange_StillPromotes(t *testing.T) {
	t.Skip("flaky on real QUIC transport (timing-sensitive election + commit propagation). " +
		"Stable equivalent: TestChaos_LearnerFirst_LeaderChange_NewLeaderPromotes in " +
		"internal/raft/chaos/scenarios (in-memory, deterministic, 10/10 pass).")
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}

	const numNodes = 3
	cluster := newQUICCluster(t, numNodes)
	cluster.startAll()
	t.Cleanup(func() {
		for _, n := range cluster.nodes {
			if n != nil {
				n.Stop()
			}
		}
		for _, tr := range cluster.transports {
			tr.Close()
		}
	})

	leader := cluster.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)
	for i := 0; i < 5; i++ {
		require.NoError(t, leader.Propose([]byte("entry")))
	}

	for _, n := range cluster.nodes {
		n.SetLearnerCatchupThreshold(1_000_000)
	}

	// Step 1: AddLearner synchronously (commits on leader before we proceed).
	require.NoError(t, leader.AddLearner("fake-lc", "127.0.0.1:65531"))

	// Wait for AddLearner commit to propagate to all voter followers, otherwise
	// killing the leader strands the new leader at a stale commitIndex and the
	// AddLearner entry stays uncommitted (pendingConfChangeIndex never clears,
	// blocking the watcher from proposing Promote).
	leaderCommit := leader.CommittedIndex()
	require.Eventually(t, func() bool {
		for _, n := range cluster.nodes {
			if n.CommittedIndex() < leaderCommit {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond, "all followers must catch up to leader's commitIndex")

	// Step 2: Kill leader. New leader's watcher must propose Promote.
	leader.Stop()

	// Wait for a new leader to be elected first (separate concern from the
	// promote latency we want to measure).
	require.Eventually(t, func() bool {
		for _, n := range cluster.nodes {
			if n != leader && n.State() == Leader {
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "new leader must be elected after old leader stops")

	// Now wait for the new leader's watcher to propose + commit Promote.
	require.Eventually(t, func() bool {
		for _, n := range cluster.nodes {
			if n == leader || n.State() != Leader {
				continue
			}
			cfg := n.Configuration()
			for _, s := range cfg.Servers {
				if s.ID == "127.0.0.1:65531" && s.Suffrage == Voter {
					return true
				}
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond, "new leader's watcher must promote learner")
}

// Stage 4 — PR-K3 integration tests.

// TestManagedLearner_LogReplay_RestoresGuard verifies that when a node restarts
// from a store that has a snapshot followed by an AddLearner entry with
// ManagedByJoint=true, the jointManagedLearners set is restored via
// rebuildConfigFromLog (post-snapshot replay), and Guard 2 blocks auto-promotion.
func TestManagedLearner_LogReplay_RestoresGuard(t *testing.T) {
	dir := t.TempDir()
	store, err := NewBadgerLogStore(dir)
	require.NoError(t, err)

	// Snapshot at index 2 so restoreFromStore triggers rebuildConfigFromLog.
	require.NoError(t, store.SaveSnapshot(Snapshot{
		Index: 2, Term: 1, Data: []byte("fsm"),
		Servers: []Server{
			{ID: "self", Suffrage: Voter},
			{ID: "peer-1", Suffrage: Voter},
		},
	}))
	// AddLearner at index 3, after the snapshot — must be replayed.
	require.NoError(t, store.AppendEntries([]LogEntry{
		{Term: 1, Index: 3, Type: LogEntryConfChange,
			Command: encodeConfChange(ConfChangePayload{
				Op: ConfChangeAddLearner, ID: "managed-lrn", Address: "addr-m",
				ManagedByJoint: true,
			})},
	}))
	require.NoError(t, store.Close())

	// Reopen store — simulates process restart.
	store2, err := NewBadgerLogStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { store2.Close() })

	cfg := DefaultConfig("self", []string{"peer-1"})
	node := NewNode(cfg, store2)

	node.mu.Lock()
	_, inSet := node.jointManagedLearners["managed-lrn"]
	node.mu.Unlock()
	require.True(t, inSet, "managed learner must be in jointManagedLearners after post-snapshot log replay")

	// Guard 2: even with a high matchIndex, checkLearnerCatchup must NOT propose Promote.
	node.mu.Lock()
	node.state = Leader
	node.commitIndex = 100
	node.matchIndex["addr-m"] = 100
	node.checkLearnerCatchup()
	node.mu.Unlock()

	select {
	case p := <-node.proposalCh:
		cc := decodeConfChange(p.command)
		if cc.Op == ConfChangePromote && cc.ID == "managed-lrn" {
			t.Fatal("Guard 2 must block auto-promote for managed learner after restart")
		}
	default:
		// correct: no promote proposal
	}
}

// TestChangeMembership_ReturnsErrJointAborted verifies the end-to-end channel flow:
// ChangeMembership starts a joint transition, ForceAbortJoint is called concurrently,
// and ChangeMembership returns ErrJointAborted once the abort commits.
//
// The test controls the proposal pipeline directly (no real QUIC) to avoid
// timing races between JointLeave and JointAbort committing in a live cluster.
func TestChangeMembership_ReturnsErrJointAborted(t *testing.T) {
	n := &Node{
		id:                   "n1",
		state:                Leader,
		config:               Config{Peers: []string{"n2", "n3"}, HeartbeatTimeout: 50 * time.Millisecond},
		matchIndex:           make(map[string]uint64),
		nextIndex:            make(map[string]uint64),
		learnerIDs:           make(map[string]string),
		learnerPromoteCh:     make(map[string]chan struct{}),
		jointManagedLearners: make(map[string]struct{}),
		proposalCh:           make(chan proposal, 16),
		stopCh:               make(chan struct{}),
	}

	changeDone := make(chan error, 1)
	go func() {
		err := n.proposeJointConfChangeWait(context.Background(),
			[]ServerEntry{{ID: "n4", Address: "n4", Suffrage: Voter}}, nil)
		changeDone <- err
	}()

	// Fake pipeline goroutine: intercepts proposals, applies entries, signals doneCh.
	go func() {
		// 1. JointEnter proposal
		enterP := <-n.proposalCh
		n.mu.Lock()
		n.applyConfigChangeLocked(LogEntry{Index: 1, Term: 1, Type: LogEntryJointConfChange, Command: enterP.command})
		n.mu.Unlock()
		enterP.doneCh <- proposalResult{} // signal commit

		// 2. JointAbort proposal (from ForceAbortJoint)
		abortP := <-n.proposalCh
		n.mu.Lock()
		n.applyConfigChangeLocked(LogEntry{Index: 2, Term: 1, Type: LogEntryJointConfChange, Command: abortP.command})
		// Mirror raft.go apply loop: send ErrJointAborted to jointResultCh on abort commit.
		if n.jointResultCh != nil {
			n.jointResultCh <- ErrJointAborted
			n.jointResultCh = nil
		}
		n.mu.Unlock()
		abortP.doneCh <- proposalResult{} // signal commit
	}()

	// Wait until proposeJointConfChangeWait has installed jointResultCh and is waiting.
	require.Eventually(t, func() bool {
		n.mu.Lock()
		defer n.mu.Unlock()
		return n.jointPhase == JointEntering && n.jointResultCh != nil
	}, 2*time.Second, 5*time.Millisecond, "joint phase not entered")

	// Operator calls ForceAbortJoint — returns nil on successful abort commit.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, n.ForceAbortJoint(ctx), "ForceAbortJoint should return nil on success")

	// ChangeMembership must return ErrJointAborted.
	select {
	case err := <-changeDone:
		require.ErrorIs(t, err, ErrJointAborted,
			"ChangeMembership must return ErrJointAborted after ForceAbortJoint")
	case <-time.After(2 * time.Second):
		t.Fatal("ChangeMembership did not return")
	}

	// Config reverted to C_old.
	n.mu.Lock()
	defer n.mu.Unlock()
	require.Equal(t, JointNone, n.jointPhase)
	require.ElementsMatch(t, []string{"n2", "n3"}, n.config.Peers)
}
