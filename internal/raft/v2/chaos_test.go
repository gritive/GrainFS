package raftv2

// Chaos suite for raft/v2.
//
// TestChaos_Sustained runs a duration-bounded imperative chaos loop that
// randomly interleaves eight fault injection actions against a 3-voter cluster:
//
//  1. Propose          — submit a command to the current leader.
//  2. StepDownLeader   — inject a higher-term RequestVote to force step-down.
//  3. Partition        — isolate one node from the rest.
//  4. Heal             — remove all partitions.
//  5. SetDropRate      — configure probabilistic message drop (0–30%).
//  6. SetReorderDelay  — enable delivery delay up to 20ms.
//  7. KillFollower     — stop a non-leader node's actor goroutine.
//  8. RestartKilled    — restart a previously killed node from its BadgerDB stores.
//
// All six Raft safety+liveness invariants (PR 17+18) are checked after every
// action. The loop terminates when RAFT_CHAOS_DURATION elapses (default 30s for
// per-PR CI smoke; set RAFT_CHAOS_DURATION=30m for nightly runs via
// test-raft-v2-chaos Makefile target).
//
// KillNode/StartNode require persistent stores so the restarted node recovers
// its HardState and log. chaosCluster uses BadgerDB on t.TempDir() per node
// and re-opens the DB on StartNode, mirroring the recovery_test.go pattern.
//
// Design: imperative loop rather than rapid.StateMachine because:
//   - Duration-bounded runs cannot be usefully shrunk by rapid.
//   - A seeded rand makes failures reproducible without rapid's machinery.
//   - Simpler code; fewer moving parts.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// chaosCluster is a 3-voter cluster for the chaos test. Unlike propertyCluster,
// it uses BadgerDB-backed LogStore and StableStore per node so that KillNode /
// StartNode can close and reopen Badger, recovering the node's durable state.
type chaosCluster struct {
	ids  [3]string
	dirs [3]string // TempDir per node; persistent across kill/restart cycles

	mu    sync.Mutex
	nodes [3]*Node // element is nil while the node is dead
	dbs   [3]*badger.DB

	Net     *partitionNet
	ObsCh   chan nodeEntry
	stopCh  chan struct{}
	drained sync.WaitGroup
}

// newChaosCluster builds a fresh 3-voter cluster backed by BadgerDB stores.
func newChaosCluster(t *testing.T) *chaosCluster {
	t.Helper()

	ids := [3]string{"a", "b", "c"}
	var dirs [3]string
	for i := range dirs {
		dirs[i] = t.TempDir()
	}

	net := newPartitionNet()
	obs := make(chan nodeEntry, 4096)
	stopCh := make(chan struct{})

	cc := &chaosCluster{
		ids:    ids,
		dirs:   dirs,
		Net:    net,
		ObsCh:  obs,
		stopCh: stopCh,
	}

	for i, id := range ids {
		n, db := cc.openNode(t, i, id)
		cc.nodes[i] = n
		cc.dbs[i] = db
	}

	// Register transports and start nodes.
	for i, n := range cc.nodes {
		_ = i
		n.SetTransport(net.Register(n.ID(), n))
	}
	for i, n := range cc.nodes {
		n.Start()
		cc.drainNode(i)
	}

	return cc
}

// openNode creates a Node backed by BadgerDB at cc.dirs[idx]. Returns the Node
// and the open *badger.DB (caller must store db; it will be closed on kill/stop).
func (cc *chaosCluster) openNode(t *testing.T, idx int, id string) (*Node, *badger.DB) {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(cc.dirs[idx]).WithLogger(nil))
	require.NoError(t, err)

	logStore, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
	require.NoError(t, err)
	stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
	require.NoError(t, err)

	peers := make([]string, 0, 2)
	for _, p := range cc.ids {
		if p != id {
			peers = append(peers, p)
		}
	}
	et := propSlowElectionTimeout
	if idx == 0 {
		et = propFastElectionTimeout
	}
	n, err := NewNode(Config{
		ID:               id,
		Peers:            peers,
		ElectionTimeout:  et,
		HeartbeatTimeout: propHeartbeat,
		LogStore:         logStore,
		StableStore:      stable,
	})
	require.NoError(t, err)
	return n, db
}

// drainNode starts a background goroutine that forwards applyCh entries for
// cc.nodes[idx] to ObsCh until the node's applyCh closes or stopCh fires.
// Must be called after node.Start() and with the node alive.
func (cc *chaosCluster) drainNode(idx int) {
	cc.drained.Add(1)
	node := cc.nodes[idx]
	id := node.ID()
	applyCh := node.ApplyCh()
	stopCh := cc.stopCh
	obs := cc.ObsCh
	go func() {
		defer cc.drained.Done()
		for {
			select {
			case e, ok := <-applyCh:
				if !ok {
					return
				}
				select {
				case obs <- nodeEntry{nodeID: id, entry: e}:
				case <-stopCh:
					return
				}
			case <-stopCh:
				return
			}
		}
	}()
}

// Stop shuts down all live nodes, waits for drain goroutines, closes ObsCh,
// and closes all open Badger DBs.
func (cc *chaosCluster) Stop(t *testing.T) {
	t.Helper()
	close(cc.stopCh)

	cc.mu.Lock()
	for i, n := range cc.nodes {
		if n != nil {
			n.Stop()
			cc.nodes[i] = nil
		}
	}
	cc.mu.Unlock()

	cc.drained.Wait()
	close(cc.ObsCh)

	cc.mu.Lock()
	for i, db := range cc.dbs {
		if db != nil {
			_ = db.Close()
			cc.dbs[i] = nil
		}
	}
	cc.mu.Unlock()
}

// leader returns the current leader node, or nil if none.
func (cc *chaosCluster) leader() *Node {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for _, n := range cc.nodes {
		if n != nil && n.IsLeader() {
			return n
		}
	}
	return nil
}

// waitForLeader polls until any live node is Leader, up to timeout.
func (cc *chaosCluster) waitForLeader(timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := cc.leader(); l != nil {
			return l
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// KillNode stops node idx if it is alive, closes its BadgerDB, and marks it dead.
// The node must not be the current leader (use StepDownLeader first or call on a follower).
func (cc *chaosCluster) KillNode(t *testing.T, idx int) {
	t.Helper()
	cc.mu.Lock()
	n := cc.nodes[idx]
	db := cc.dbs[idx]
	if n == nil {
		cc.mu.Unlock()
		return // already dead
	}
	cc.nodes[idx] = nil
	cc.dbs[idx] = nil
	cc.mu.Unlock()

	n.Stop()
	if db != nil {
		require.NoError(t, db.Close())
	}
}

// StartNode restarts node idx from its persistent BadgerDB directory.
// The node must be dead (previously KillNode'd). After restart the node is
// live and registered in the network, and a new drain goroutine is started.
func (cc *chaosCluster) StartNode(t *testing.T, idx int) {
	t.Helper()
	cc.mu.Lock()
	if cc.nodes[idx] != nil {
		cc.mu.Unlock()
		return // already live
	}
	cc.mu.Unlock()

	id := cc.ids[idx]
	n, db := cc.openNode(t, idx, id)
	// Re-register in the net so existing nodes can route RPCs to this node.
	n.SetTransport(cc.Net.Register(id, n))
	n.Start()

	cc.mu.Lock()
	cc.nodes[idx] = n
	cc.dbs[idx] = db
	cc.mu.Unlock()

	cc.drainNode(idx)
}

// liveFollowers returns indices of live nodes that are not the current leader.
func (cc *chaosCluster) liveFollowers() []int {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	var result []int
	for i, n := range cc.nodes {
		if n != nil && !n.IsLeader() {
			result = append(result, i)
		}
	}
	return result
}

// deadNodes returns indices of nodes that are currently dead.
func (cc *chaosCluster) deadNodes() []int {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	var result []int
	for i, n := range cc.nodes {
		if n == nil {
			result = append(result, i)
		}
	}
	return result
}

// chaosObs wraps invObserver with cluster-aware observation for the chaos loop.
type chaosObs struct {
	inv            *invObserver
	proposeCounter int64
}

func newChaosObs() *chaosObs {
	return &chaosObs{inv: newInvObserver()}
}

// resetNodeHistory clears the applied-entry history for nodeID. Called when a
// node is restarted so its fresh apply stream is checked from index 1 again.
// This is safe: the restarted node replays the same entries (log/state-machine
// safety). Clearing prevents false-positive monotonicity violations where the
// observer sees old high-index entries then new low-index replay entries.
func (co *chaosObs) resetNodeHistory(nodeID string) {
	delete(co.inv.nodeApplied, nodeID)
}

// drain pulls pending observations from ObsCh into the invariant observer.
func (co *chaosObs) drain(obs chan nodeEntry) {
	co.inv.drainObsCh(obs)
}

// sampleLeaders samples each live node and records leader observations.
func (co *chaosObs) sampleLeaders(cc *chaosCluster) {
	cc.mu.Lock()
	nodes := cc.nodes // copy the array (3 pointers) by value
	cc.mu.Unlock()

	for _, n := range nodes {
		if n == nil {
			continue
		}
		rs := n.rs.Load()
		if rs.isLeader {
			co.inv.recordLeader(rs.term, rs.leaderID)
		}
	}
}

// checkAll asserts all six invariants. Returns an error string on violation.
func (co *chaosObs) checkAll(cc *chaosCluster, history []chaosActionRecord) error {
	co.sampleLeaders(cc)
	co.drain(cc.ObsCh)

	if err := checkElectionSafety(co.inv.leaderObs); err != nil {
		return err
	}
	if err := checkLeaderAppendOnly(co.inv.nodeApplied); err != nil {
		return err
	}
	if err := checkLogMatching(co.inv.nodeApplied); err != nil {
		return err
	}
	if err := checkLeaderCompleteness(co.inv.leaderObs, co.inv.nodeApplied); err != nil {
		return err
	}
	if err := checkStateMachineSafety(co.inv.nodeApplied); err != nil {
		return err
	}

	var maxCommitted uint64
	cc.mu.Lock()
	for _, n := range cc.nodes {
		if n != nil {
			if ci := n.CommittedIndex(); ci > maxCommitted {
				maxCommitted = ci
			}
		}
	}
	cc.mu.Unlock()

	return checkEventualCommitChaos(history, maxCommitted)
}

// chaosActionKind categorises chaos loop actions for liveness-suffix analysis.
type chaosActionKind int

const (
	chaosPropose chaosActionKind = iota
	chaosStepDown
	chaosPartition
	chaosHeal
	chaosSetDropRate
	chaosSetReorderDelay
	chaosKillFollower
	chaosRestartKilled
)

// chaosActionRecord captures what happened during one chaos loop action.
type chaosActionRecord struct {
	kind          chaosActionKind
	leaderExisted bool
	proposed      *proposedEntry // non-nil when Propose succeeded
}

// checkEventualCommitChaos is checkEventualCommit adapted for the chaos action
// set. It skips the liveness check if the stable suffix contains any
// destabilising action (partition, step-down, kill, drop-rate, reorder).
// This prevents false-positive liveness violations when the cluster is in
// a degraded state.
func checkEventualCommitChaos(history []chaosActionRecord, maxCommitted uint64) error {
	const K = 50
	if len(history) < K {
		return nil
	}
	suffix := history[len(history)-K:]

	for _, ar := range suffix {
		switch ar.kind {
		case chaosPartition, chaosStepDown, chaosKillFollower, chaosSetDropRate, chaosSetReorderDelay:
			return nil // not stable; skip
		}
	}
	if !suffix[0].leaderExisted {
		return nil // no leader at suffix start; skip
	}

	for i, ar := range suffix {
		if ar.proposed == nil {
			continue
		}
		if ar.proposed.index > maxCommitted {
			return fmt.Errorf(
				"chaos liveness violated: proposed entry at index %d (suffix action %d) not committed; max committed=%d",
				ar.proposed.index, i, maxCommitted,
			)
		}
	}
	return nil
}

// parseDuration reads RAFT_CHAOS_DURATION from the environment.
// Default: 30s (per-PR CI smoke). Nightly sets to 30m.
func parseDuration(envKey string, defaultDur time.Duration) time.Duration {
	val := os.Getenv(envKey)
	if val == "" {
		return defaultDur
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return defaultDur
	}
	return d
}

// TestChaos_Sustained runs a duration-bounded imperative chaos loop against a
// 3-voter cluster. Duration is controlled by RAFT_CHAOS_DURATION (default 30s).
// All six Raft invariants are asserted after each action.
//
// Per-PR CI: default 30s smoke run (runs as part of go test ./internal/raft/v2/).
// Nightly: RAFT_CHAOS_DURATION=30m make test-raft-v2-chaos.
func TestChaos_Sustained(t *testing.T) {
	duration := parseDuration("RAFT_CHAOS_DURATION", 30*time.Second)

	cc := newChaosCluster(t)
	t.Cleanup(func() { cc.Stop(t) })

	// Seed from env for reproducibility; default seed produces a fixed sequence.
	var seed int64 = 1234
	if s := os.Getenv("RAFT_CHAOS_SEED"); s != "" {
		if _, err := fmt.Sscanf(s, "%d", &seed); err != nil {
			seed = 1234
		}
	}
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // test code

	// Wait for initial leader before starting chaos.
	require.NotNil(t, cc.waitForLeader(3*time.Second), "cluster must elect a leader before chaos begins")

	obs := newChaosObs()
	var history []chaosActionRecord

	// 8 actions: Propose, StepDown, Partition, Heal, SetDropRate, SetReorderDelay,
	// KillFollower, RestartKilled.
	type actionFn func() chaosActionRecord

	actions := []actionFn{
		// 1. Propose
		func() chaosActionRecord {
			leader := cc.leader()
			ar := chaosActionRecord{kind: chaosPropose, leaderExisted: leader != nil}
			if leader != nil {
				obs.proposeCounter++
				cmd := []byte(fmt.Sprintf("chaos-cmd-%d", obs.proposeCounter))
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()
				if idx, err := leader.ProposeWait(ctx, cmd); err == nil {
					ar.proposed = &proposedEntry{index: idx}
				}
			}
			return ar
		},
		// 2. StepDownLeader
		func() chaosActionRecord {
			leader := cc.leader()
			ar := chaosActionRecord{kind: chaosStepDown, leaderExisted: leader != nil}
			if leader != nil {
				higherTerm := leader.Term() + 10
				leader.HandleRequestVote(&RequestVoteArgs{
					Term:         higherTerm,
					CandidateID:  "chaos-intruder",
					LastLogIndex: leader.CommittedIndex(),
					LastLogTerm:  higherTerm,
				})
			}
			return ar
		},
		// 3. Partition (isolate one node)
		func() chaosActionRecord {
			ar := chaosActionRecord{kind: chaosPartition, leaderExisted: cc.leader() != nil}
			cc.mu.Lock()
			idx := rng.Intn(3)
			id := cc.ids[idx]
			cc.mu.Unlock()
			cc.Net.Partition(id)
			return ar
		},
		// 4. Heal all partitions + reset drop/reorder
		func() chaosActionRecord {
			ar := chaosActionRecord{kind: chaosHeal, leaderExisted: cc.leader() != nil}
			cc.Net.Heal()
			cc.Net.SetDropRate(0)
			cc.Net.SetReorderDelay(0)
			return ar
		},
		// 5. SetDropRate (0–30%)
		func() chaosActionRecord {
			rate := rng.Float64() * 0.30
			cc.Net.SetDropRate(rate)
			return chaosActionRecord{kind: chaosSetDropRate, leaderExisted: cc.leader() != nil}
		},
		// 6. SetReorderDelay (0–20ms)
		func() chaosActionRecord {
			ms := time.Duration(rng.Intn(20)) * time.Millisecond
			cc.Net.SetReorderDelay(ms)
			return chaosActionRecord{kind: chaosSetReorderDelay, leaderExisted: cc.leader() != nil}
		},
		// 7. KillFollower (pick a live non-leader node)
		func() chaosActionRecord {
			ar := chaosActionRecord{kind: chaosKillFollower, leaderExisted: cc.leader() != nil}
			followers := cc.liveFollowers()
			if len(followers) == 0 {
				return ar // no follower to kill
			}
			pick := followers[rng.Intn(len(followers))]
			cc.KillNode(t, pick)
			return ar
		},
		// 8. RestartKilled (restart a dead node)
		func() chaosActionRecord {
			ar := chaosActionRecord{kind: chaosRestartKilled, leaderExisted: cc.leader() != nil}
			dead := cc.deadNodes()
			if len(dead) == 0 {
				return ar // no dead node
			}
			pick := dead[rng.Intn(len(dead))]
			// Drain ObsCh and reset the node's observation history before
			// restart. After restart the node replays its log from index 1
			// through applyCh; keeping old high-index entries in nodeApplied
			// would cause false-positive monotonicity violations.
			obs.drain(cc.ObsCh)
			obs.resetNodeHistory(cc.ids[pick])
			cc.StartNode(t, pick)
			// Give the restarted node a moment to reconnect to the cluster.
			time.Sleep(20 * time.Millisecond)
			return ar
		},
	}

	deadline := time.Now().Add(duration)
	actionCount := 0
	for time.Now().Before(deadline) {
		fn := actions[rng.Intn(len(actions))]
		ar := fn()
		history = append(history, ar)
		actionCount++

		if err := obs.checkAll(cc, history); err != nil {
			t.Fatalf("invariant violation after %d chaos actions: %v", actionCount, err)
		}
	}

	t.Logf("TestChaos_Sustained: %d actions in %s", actionCount, duration)
}
