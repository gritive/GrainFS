package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// partitionNet is a memNetwork wrapper that supports bidirectional partitioning,
// probabilistic packet drops, and delay-based reordering.
//
// Partition: When a peer is isolated all RPCs to/from that peer return ErrUnknownPeer.
// Drop rate: each message is dropped with probability dropRate (0.0 = none, 1.0 = all).
// Reorder: each message sleeps up to reorderMaxDelay before delivery, effectively
//
//	reordering messages that arrive in quick succession. This is the delay-based
//	approach (option A from the design): simpler shutdown semantics than a true
//	buffered queue while providing sufficient chaos for race detection.
//
// All three mechanisms are orthogonal and may be active simultaneously.
type partitionNet struct {
	inner *memNetwork

	// mu protects isolated, dropRate, reorderMaxDelay, and rng.
	// Transport send paths run from multiple goroutines concurrently; all
	// shared state must be read under this lock.
	mu              sync.Mutex
	isolated        map[string]bool
	dropRate        float64       // 0.0–1.0
	reorderMaxDelay time.Duration // 0 = disabled
	rng             *rand.Rand

	// snap is an atomic snapshot of isolated for partition-only hot-path reads.
	// Written under mu; readers load via atomic.
	snap atomic.Pointer[map[string]bool]
}

func newPartitionNet() *partitionNet {
	empty := make(map[string]bool)
	p := &partitionNet{
		inner:    newMemNetwork(),
		isolated: make(map[string]bool),
		rng:      rand.New(rand.NewSource(42)), //nolint:gosec // deterministic seed for reproducibility
	}
	p.snap.Store(&empty)
	return p
}

// SetDropRate configures the probability that each in-flight message is silently
// dropped before delivery. p must be in [0.0, 1.0]. Orthogonal to partition and
// reorder. Safe to call from any goroutine.
func (p *partitionNet) SetDropRate(rate float64) {
	p.mu.Lock()
	p.dropRate = rate
	p.mu.Unlock()
}

// SetReorderDelay configures the maximum random delay injected before each
// message delivery. Callers in distinct goroutines that both draw delays in
// [0, maxDelay) will be delivered in shuffled order relative to their call time.
// A zero duration disables reordering. Orthogonal to partition and drop.
// Safe to call from any goroutine.
func (p *partitionNet) SetReorderDelay(maxDelay time.Duration) {
	p.mu.Lock()
	p.reorderMaxDelay = maxDelay
	p.mu.Unlock()
}

// shouldDrop returns true if this message should be silently discarded.
// Must be called with mu held. Advances rng.
func (p *partitionNet) shouldDrop() bool {
	if p.dropRate <= 0 {
		return false
	}
	return p.rng.Float64() < p.dropRate
}

// reorderSleep sleeps for a random duration in [0, reorderMaxDelay) if
// reordering is enabled. Must be called WITHOUT mu held (sleeps outside the lock).
func (p *partitionNet) reorderSleep() {
	p.mu.Lock()
	d := p.reorderMaxDelay
	var delay time.Duration
	if d > 0 {
		delay = time.Duration(p.rng.Int63n(int64(d)))
	}
	p.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
}

// Register associates self → node and returns a partition-aware Transport.
func (p *partitionNet) Register(self string, node *Node) Transport {
	inner := p.inner.Register(self, node)
	return &partitionTransport{self: self, net: p, inner: inner}
}

// Partition marks the given peers as isolated. Bidirectional: both directions
// through the partitioned peer's transport return ErrUnknownPeer.
func (p *partitionNet) Partition(peers ...string) {
	p.mu.Lock()
	for _, peer := range peers {
		p.isolated[peer] = true
	}
	snap := make(map[string]bool, len(p.isolated))
	for k, v := range p.isolated {
		snap[k] = v
	}
	p.mu.Unlock()
	p.snap.Store(&snap)
}

// Heal removes all partitions, restoring full connectivity.
func (p *partitionNet) Heal() {
	p.mu.Lock()
	p.isolated = make(map[string]bool)
	empty := make(map[string]bool)
	p.mu.Unlock()
	p.snap.Store(&empty)
}

func (p *partitionNet) isIsolated(id string) bool {
	snap := p.snap.Load()
	return (*snap)[id]
}

// partitionTransport wraps a memTransport and blocks RPCs to/from isolated peers,
// applies probabilistic drop, and injects reorder delays. All three mechanisms
// are checked on every send.
type partitionTransport struct {
	self  string
	net   *partitionNet
	inner Transport
}

// chaosCheck gates a send: returns ErrUnknownPeer if the message should not be
// delivered (partition or probabilistic drop), otherwise applies any reorder
// delay and returns nil so the caller may proceed to deliver.
func (t *partitionTransport) chaosCheck(peer string) error {
	// Check partition and drop rate under a single lock so both share the same rng.
	t.net.mu.Lock()
	partitioned := t.net.isolated[t.self] || t.net.isolated[peer]
	drop := !partitioned && t.net.shouldDrop()
	t.net.mu.Unlock()

	if partitioned || drop {
		return ErrUnknownPeer
	}
	// Reorder delay runs outside the lock to avoid holding it during sleep.
	t.net.reorderSleep()
	return nil
}

func (t *partitionTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	if err := t.chaosCheck(peer); err != nil {
		return nil, err
	}
	return t.inner.SendRequestVote(peer, args)
}

func (t *partitionTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if err := t.chaosCheck(peer); err != nil {
		return nil, err
	}
	return t.inner.SendAppendEntries(peer, args)
}

func (t *partitionTransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	if err := t.chaosCheck(peer); err != nil {
		return nil, err
	}
	return t.inner.SendInstallSnapshot(peer, args)
}

func (t *partitionTransport) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	if err := t.chaosCheck(peer); err != nil {
		return nil, err
	}
	return t.inner.SendTimeoutNow(peer, args)
}

// propertyCluster is a 3-voter cluster set up for property tests. It exposes
// the partitionNet directly so state machine commands can call Partition/Heal.
// ApplyCh from each node is drained by a per-node goroutine that forwards
// entries to a shared observations channel — no lock needed in the drain loop
// (single writer per node goroutine).
type propertyCluster struct {
	Nodes   []*Node
	Net     *partitionNet
	ObsCh   chan nodeEntry // buffered observations channel; drained by invariant checks
	stopCh  chan struct{}  // closed on teardown to stop drain goroutines
	drained sync.WaitGroup
}

// nodeEntry records a (nodeID, LogEntry) pair for invariant tracking.
type nodeEntry struct {
	nodeID string
	entry  LogEntry
}

const (
	propFastElectionTimeout = 80 * time.Millisecond
	propSlowElectionTimeout = 400 * time.Millisecond
	propHeartbeat           = 30 * time.Millisecond
)

// newPropertyCluster builds a 3-voter cluster wired through a partitionNet.
// ids must have length 3. The first id gets the fast election timeout so it
// wins elections deterministically under normal conditions.
func newPropertyCluster(t testing.TB, ids [3]string) *propertyCluster {
	t.Helper()

	net := newPartitionNet()
	// 4096: generous buffer; Check drains every action so steady-state depth
	// is well under 100. Larger size absorbs burst-apply windows during
	// post-partition catch-up without blocking applyLoop.
	obs := make(chan nodeEntry, 4096)
	stopCh := make(chan struct{})
	pc := &propertyCluster{
		Nodes:  make([]*Node, 3),
		Net:    net,
		ObsCh:  obs,
		stopCh: stopCh,
	}

	for i, id := range ids {
		peers := make([]string, 0, 2)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		et := propSlowElectionTimeout
		if i == 0 {
			et = propFastElectionTimeout
		}
		n, err := NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  et,
			HeartbeatTimeout: propHeartbeat,
		})
		require.NoError(t, err)
		pc.Nodes[i] = n
	}

	// Register transports before Start so initial RequestVote routes immediately.
	for _, n := range pc.Nodes {
		n.SetTransport(net.Register(n.ID(), n))
	}

	for _, n := range pc.Nodes {
		n.Start()
		id := n.ID()
		applyCh := n.ApplyCh()
		pc.drained.Add(1)
		go func() {
			defer pc.drained.Done()
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

	return pc
}

// Stop shuts down all nodes, drains observation goroutines, and closes ObsCh.
func (pc *propertyCluster) Stop() {
	close(pc.stopCh)
	for _, n := range pc.Nodes {
		n.Stop()
	}
	pc.drained.Wait()
	close(pc.ObsCh)
}

// waitForLeader polls until any node reports itself as Leader, up to timeout.
// Returns the leader Node or nil if none emerges.
func (pc *propertyCluster) waitForLeader(timeout time.Duration) *Node {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range pc.Nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

// leader returns the current leader node, or nil if there is none.
func (pc *propertyCluster) leader() *Node {
	for _, n := range pc.Nodes {
		if n.IsLeader() {
			return n
		}
	}
	return nil
}
