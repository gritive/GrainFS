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

// RequestVoteHookFn intercepts an outbound RequestVote RPC before delivery.
// Return (nil, true) to drop the message, or (args, false) to deliver
// (possibly modified) args. Keyed by the destination node ID.
type RequestVoteHookFn func(from, to string, args *raft.RequestVoteArgs) (*raft.RequestVoteArgs, bool)

// ChaosTransport routes RPC callbacks between in-memory raft.Node instances.
// All Driver primitives (Partition, DropMessage) are gated here.
type ChaosTransport struct {
	mu          sync.Mutex
	nodes       map[string]*raft.Node
	partitioned map[string]bool
	dropCounts  map[edge]int
	rvHooks     map[string]RequestVoteHookFn
}

// NewChaosTransport creates an empty transport. Register nodes before Wire.
func NewChaosTransport() *ChaosTransport {
	return &ChaosTransport{
		nodes:       make(map[string]*raft.Node),
		partitioned: make(map[string]bool),
		dropCounts:  make(map[edge]int),
		rvHooks:     make(map[string]RequestVoteHookFn),
	}
}

// Register adds a node to the routing table by its ID.
func (c *ChaosTransport) Register(n *raft.Node) {
	c.mu.Lock()
	c.nodes[n.ID()] = n
	c.mu.Unlock()
}

// resolveDelivery atomically checks delivery policy AND returns the target node.
// Returns (nil, false) if either endpoint is partitioned, a drop counter fires,
// or the target is not registered. A single lock acquisition prevents a TOCTOU
// race where PartitionPeer fires between the delivery check and the lookup.
func (c *ChaosTransport) resolveDelivery(from, to string) (*raft.Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.partitioned[from] || c.partitioned[to] {
		return nil, false
	}
	e := edge{from, to}
	if cnt := c.dropCounts[e]; cnt > 0 {
		c.dropCounts[e] = cnt - 1
		return nil, false
	}
	target := c.nodes[to]
	return target, target != nil
}

// shouldDeliver returns true if a message from→to should be delivered.
// Used only in tests for targeted assertions. Wire callbacks use resolveDelivery.
//
//nolint:unused // package tests assert partition/drop behaviour directly.
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

// SetRequestVoteHook installs a hook that fires before each RequestVote
// delivery to toNodeID. A nil fn removes the hook.
func (c *ChaosTransport) SetRequestVoteHook(toNodeID string, fn RequestVoteHookFn) {
	c.mu.Lock()
	if fn == nil {
		delete(c.rvHooks, toNodeID)
	} else {
		c.rvHooks[toNodeID] = fn
	}
	c.mu.Unlock()
}

// applyRVHook applies the per-destination hook (if any) for a RequestVote
// from→to. Must be called without c.mu held.
// Returns (modified args, drop). drop=true means the caller must not deliver.
func (c *ChaosTransport) applyRVHook(from, to string, args *raft.RequestVoteArgs) (*raft.RequestVoteArgs, bool) {
	c.mu.Lock()
	fn := c.rvHooks[to]
	c.mu.Unlock()
	if fn == nil {
		return args, false
	}
	return fn(from, to, args)
}

// errPartitioned is returned by gated callbacks when a message is dropped.
var errPartitioned = errors.New("chaos: partitioned")

// Wire installs the chaos-routed transport callbacks on n. n must already be
// Registered.
func (c *ChaosTransport) Wire(n *raft.Node) {
	from := n.ID()

	sendVote := func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
		target, ok := c.resolveDelivery(from, peer)
		if !ok {
			return nil, errPartitioned
		}
		modifiedArgs, drop := c.applyRVHook(from, peer, args)
		if drop {
			return nil, errPartitioned
		}
		return target.HandleRequestVote(modifiedArgs), nil
	}

	sendAppend := func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
		target, ok := c.resolveDelivery(from, peer)
		if !ok {
			return nil, errPartitioned
		}
		return target.HandleAppendEntries(args), nil
	}

	sendTimeoutNow := func(peer string) error {
		target, ok := c.resolveDelivery(from, peer)
		if !ok {
			return errPartitioned
		}
		target.HandleTimeoutNow()
		return nil
	}

	n.SetTransport(sendVote, sendAppend)
	n.SetTimeoutNowTransport(sendTimeoutNow)
}
