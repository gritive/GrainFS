package chaos

import (
	"errors"
	"sync"

	"github.com/gritive/GrainFS/internal/raft"
)

// edge identifies a directional message lane for DropMessage.
type edge struct {
	from, to string
}

// ChaosTransport routes RPC callbacks between in-memory raft.Node instances.
// All Driver primitives (Partition, DropMessage) are gated here.
type ChaosTransport struct {
	mu          sync.Mutex
	nodes       map[string]*raft.Node
	partitioned map[string]bool
	dropCounts  map[edge]int
}

// NewChaosTransport creates an empty transport. Register nodes before Wire.
func NewChaosTransport() *ChaosTransport {
	return &ChaosTransport{
		nodes:       make(map[string]*raft.Node),
		partitioned: make(map[string]bool),
		dropCounts:  make(map[edge]int),
	}
}

// Register adds a node to the routing table by its ID.
func (c *ChaosTransport) Register(n *raft.Node) {
	c.mu.Lock()
	c.nodes[n.ID()] = n
	c.mu.Unlock()
}

// lookup returns the registered node for an ID, or nil.
func (c *ChaosTransport) lookup(id string) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// shouldDeliver returns true if a message from→to should be delivered.
// False if either endpoint is partitioned or there is a pending drop counter.
func (c *ChaosTransport) shouldDeliver(from, to string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.partitioned[from] || c.partitioned[to] {
		return false
	}
	e := edge{from, to}
	if cnt := c.dropCounts[e]; cnt > 0 {
		c.dropCounts[e] = cnt - 1
		return false
	}
	return true
}

// PartitionPeer blocks all messages in and out of nodeID.
func (c *ChaosTransport) PartitionPeer(nodeID string) {
	c.mu.Lock()
	c.partitioned[nodeID] = true
	c.mu.Unlock()
}

// HealPartition restores message delivery for nodeID.
func (c *ChaosTransport) HealPartition(nodeID string) {
	c.mu.Lock()
	delete(c.partitioned, nodeID)
	c.mu.Unlock()
}

// DropMessage drops the next n outbound messages from→to. Counter persists
// until exhausted, then normal delivery resumes. Direction is one-way.
func (c *ChaosTransport) DropMessage(from, to string, n int) {
	c.mu.Lock()
	c.dropCounts[edge{from, to}] += n
	c.mu.Unlock()
}

// errPartitioned is returned by gated callbacks when a message is dropped.
var errPartitioned = errors.New("chaos: partitioned")

// Wire installs the chaos-routed transport callbacks on n. n must already be
// Registered.
func (c *ChaosTransport) Wire(n *raft.Node) {
	from := n.ID()

	sendVote := func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
		if !c.shouldDeliver(from, peer) {
			return nil, errPartitioned
		}
		target := c.lookup(peer)
		if target == nil {
			return nil, errors.New("chaos: peer not registered: " + peer)
		}
		return target.HandleRequestVote(args), nil
	}

	sendAppend := func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
		if !c.shouldDeliver(from, peer) {
			return nil, errPartitioned
		}
		target := c.lookup(peer)
		if target == nil {
			return nil, errors.New("chaos: peer not registered: " + peer)
		}
		return target.HandleAppendEntries(args), nil
	}

	n.SetTransport(sendVote, sendAppend)
}
