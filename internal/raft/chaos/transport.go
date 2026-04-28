package chaos

import (
	"errors"
	"sync"

	"github.com/gritive/GrainFS/internal/raft"
)

// ChaosTransport routes RPC callbacks between in-memory raft.Node instances.
// All Driver primitives (Partition, DropMessage) are gated here.
type ChaosTransport struct {
	mu    sync.Mutex
	nodes map[string]*raft.Node
}

// NewChaosTransport creates an empty transport. Register nodes before Wire.
func NewChaosTransport() *ChaosTransport {
	return &ChaosTransport{nodes: make(map[string]*raft.Node)}
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

// Wire installs the chaos-routed transport callbacks on n. n must already be
// Registered (so peers can route back to it).
func (c *ChaosTransport) Wire(n *raft.Node) {
	sendVote := func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
		target := c.lookup(peer)
		if target == nil {
			return nil, errors.New("chaos: peer not registered: " + peer)
		}
		return target.HandleRequestVote(args), nil
	}

	sendAppend := func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
		target := c.lookup(peer)
		if target == nil {
			return nil, errors.New("chaos: peer not registered: " + peer)
		}
		return target.HandleAppendEntries(args), nil
	}

	n.SetTransport(sendVote, sendAppend)
}
