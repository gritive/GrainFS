package raftv2

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// partitionNet is a memNetwork wrapper that supports bidirectional partitioning.
// When a peer is isolated, all RPCs to/from that peer return ErrUnknownPeer,
// simulating a network partition. The partition state is stored as an atomic
// pointer to a string set so reads from hot-path goroutines need no lock.
type partitionNet struct {
	inner *memNetwork

	// isolated is the set of peer IDs currently partitioned off.
	// Written under mu; readers load via the atomic snapshot.
	mu       sync.Mutex
	isolated map[string]bool
	snap     atomic.Pointer[map[string]bool]
}

func newPartitionNet() *partitionNet {
	empty := make(map[string]bool)
	p := &partitionNet{
		inner:    newMemNetwork(),
		isolated: make(map[string]bool),
	}
	p.snap.Store(&empty)
	return p
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

// partitionTransport wraps a memTransport and blocks RPCs to/from isolated peers.
type partitionTransport struct {
	self  string
	net   *partitionNet
	inner Transport
}

func (t *partitionTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	if t.net.isIsolated(t.self) || t.net.isIsolated(peer) {
		return nil, ErrUnknownPeer
	}
	return t.inner.SendRequestVote(peer, args)
}

func (t *partitionTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if t.net.isIsolated(t.self) || t.net.isIsolated(peer) {
		return nil, ErrUnknownPeer
	}
	return t.inner.SendAppendEntries(peer, args)
}

func (t *partitionTransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	if t.net.isIsolated(t.self) || t.net.isIsolated(peer) {
		return nil, ErrUnknownPeer
	}
	return t.inner.SendInstallSnapshot(peer, args)
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
