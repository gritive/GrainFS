package chaos

import (
	"fmt"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/raft"
)

// Cluster is an in-memory N-node Raft test harness. It owns the transport,
// node configs, and lifecycle. Implements Driver.
var _ Driver = (*Cluster)(nil)

type Cluster struct {
	t         *testing.T
	transport *ChaosTransport
	configs   map[string]raft.Config
	nodes     map[string]*raft.Node
	ids       []string
}

// NewCluster constructs N nodes named "node-0".."node-(N-1)" with default
// timeouts and a fully-meshed peer list. Nodes are NOT started; call StartAll.
// Cleanup (Close all nodes) is registered via t.Cleanup.
func NewCluster(t *testing.T, n int) *Cluster {
	t.Helper()

	c := &Cluster{
		t:         t,
		transport: NewChaosTransport(),
		configs:   make(map[string]raft.Config, n),
		nodes:     make(map[string]*raft.Node, n),
		ids:       make([]string, n),
	}

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node-%d", i)
		c.ids[i] = id
	}

	for i, id := range c.ids {
		peers := make([]string, 0, n-1)
		for j, pid := range c.ids {
			if i != j {
				peers = append(peers, pid)
			}
		}
		cfg := raft.Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  200 * time.Millisecond,
			HeartbeatTimeout: 50 * time.Millisecond,
			MaxEntriesPerAE:  512,
			TrailingLogs:     1024,
		}
		c.configs[id] = cfg

		node := raft.NewNode(cfg)
		c.nodes[id] = node
		c.transport.Register(node)
		c.transport.Wire(node)
	}

	t.Cleanup(func() {
		for _, nd := range c.nodes {
			nd.Close()
		}
	})

	return c
}

// StartAll starts every node in the cluster.
func (c *Cluster) StartAll() {
	for _, n := range c.nodes {
		n.Start()
	}
}

// WaitForLeader polls until a leader is elected or timeout elapses.
// Returns nil on timeout.
func (c *Cluster) WaitForLeader(timeout time.Duration) *raft.Node {
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return nil
		default:
			for _, n := range c.nodes {
				if n.IsLeader() {
					return n
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// CurrentLeader returns the current leader, or nil if none.
func (c *Cluster) CurrentLeader() *raft.Node {
	for _, n := range c.nodes {
		if n.IsLeader() {
			return n
		}
	}
	return nil
}

// Nodes returns all nodes (order is not guaranteed).
func (c *Cluster) Nodes() []*raft.Node {
	out := make([]*raft.Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		out = append(out, n)
	}
	return out
}

// NodeIDs returns the cluster node IDs in creation order.
func (c *Cluster) NodeIDs() []string {
	out := make([]string, len(c.ids))
	copy(out, c.ids)
	return out
}

// NodeByID returns the node with the given ID, or nil if not found.
func (c *Cluster) NodeByID(id string) *raft.Node {
	return c.nodes[id]
}

// AddNode creates a new raft node with the given id, registers it in the
// transport (so the leader can replicate to it), and starts it. The caller is
// responsible for proposing AddVoter to the leader to include the node in the
// configuration. t.Cleanup will Close the node when the test ends.
func (c *Cluster) AddNode(id string) *raft.Node {
	c.t.Helper()
	cfg := raft.Config{
		ID:               id,
		Peers:            nil, // learns peers via AppendEntries from leader
		ElectionTimeout:  200 * time.Millisecond,
		HeartbeatTimeout: 50 * time.Millisecond,
		MaxEntriesPerAE:  512,
		TrailingLogs:     1024,
	}
	c.configs[id] = cfg
	node := raft.NewNode(cfg)
	c.nodes[id] = node
	c.ids = append(c.ids, id)
	c.transport.Register(node)
	c.transport.Wire(node)
	node.Start()
	c.t.Cleanup(func() { node.Close() })
	return node
}

// StopNode calls Close on the named node. The node remains in the routing
// table so that partitioned-peer tests still see it as a registered target;
// use PartitionPeer to silence it.
func (c *Cluster) StopNode(nodeID string) {
	c.t.Helper()
	if n, ok := c.nodes[nodeID]; ok {
		n.Close()
	}
}

// Driver primitive delegations.

func (c *Cluster) PartitionPeer(nodeID string)        { c.transport.PartitionPeer(nodeID) }
func (c *Cluster) HealPartition(nodeID string)        { c.transport.HealPartition(nodeID) }
func (c *Cluster) DropMessage(from, to string, n int) { c.transport.DropMessage(from, to, n) }
func (c *Cluster) SetRequestVoteHook(toNodeID string, fn RequestVoteHookFn) {
	c.transport.SetRequestVoteHook(toNodeID, fn)
}

// InjectRequestVote calls HandleRequestVote directly on targetNodeID,
// bypassing all chaos transport gating (partition, drop, hooks).
// Used to test disrupting-prevention at the handler level.
func (c *Cluster) InjectRequestVote(targetNodeID string, args *raft.RequestVoteArgs) *raft.RequestVoteReply {
	c.t.Helper()
	n, ok := c.nodes[targetNodeID]
	if !ok {
		c.t.Fatalf("InjectRequestVote: unknown node %q", targetNodeID)
	}
	return n.HandleRequestVote(args)
}

// RestartNode performs Close() on the named node, then constructs a fresh
// *raft.Node with the original Config, re-wires the transport, and Starts it.
// Per-node BadgerDB stores are not preserved (in-memory state only). If a
// scenario needs durable restart, extend NewCluster to accept a store factory.
func (c *Cluster) RestartNode(nodeID string) {
	c.t.Helper()

	old, ok := c.nodes[nodeID]
	if !ok {
		c.t.Fatalf("RestartNode: unknown node %q", nodeID)
	}

	old.Close()

	cfg := c.configs[nodeID]
	newNode := raft.NewNode(cfg)
	c.nodes[nodeID] = newNode
	c.transport.Register(newNode) // overwrite registry entry
	c.transport.Wire(newNode)
	newNode.Start()
}
