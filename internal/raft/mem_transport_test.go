package raft

import (
	"errors"
	"sync"
)

// ErrUnknownPeer is returned by memTransport when the destination peer is not
// registered in the in-process network. Production transports return their
// own network errors.
var ErrUnknownPeer = errors.New("raftv2: unknown peer")

// memNetwork is a process-local Transport substrate for tests. It dispatches
// calls directly to the destination Node's Handle* methods, bypassing
// serialization. Construct one network per test scenario via newMemNetwork
// and Register each Node to obtain its per-node Transport view.
type memNetwork struct {
	// nodes maps peer-id → Node so SendRequestVote / SendAppendEntries can
	// route. RWMutex-protected because tests may register nodes concurrently
	// with dispatch; the cost is negligible vs. the actor channel hop.
	mu    sync.RWMutex
	nodes map[string]*Node
}

// memTransport is the per-Node view of memNetwork.
type memTransport struct {
	self string
	net  *memNetwork
}

func newMemNetwork() *memNetwork {
	return &memNetwork{nodes: make(map[string]*Node)}
}

// Register associates self → node in the network and returns a Transport that
// routes outbound RPCs through the network. Re-registering the same id
// overwrites the previous binding (useful for restart-style tests).
func (n *memNetwork) Register(self string, node *Node) Transport {
	n.mu.Lock()
	n.nodes[self] = node
	n.mu.Unlock()
	return &memTransport{self: self, net: n}
}

func (n *memNetwork) lookup(peer string) *Node {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.nodes[peer]
}

func (t *memTransport) SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	dst := t.net.lookup(peer)
	if dst == nil {
		return nil, ErrUnknownPeer
	}
	return dst.HandleRequestVote(args), nil
}

func (t *memTransport) SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	dst := t.net.lookup(peer)
	if dst == nil {
		return nil, ErrUnknownPeer
	}
	return dst.HandleAppendEntries(args), nil
}

func (t *memTransport) SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	dst := t.net.lookup(peer)
	if dst == nil {
		return nil, ErrUnknownPeer
	}
	return dst.HandleInstallSnapshot(args), nil
}

func (t *memTransport) SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error) {
	dst := t.net.lookup(peer)
	if dst == nil {
		return nil, ErrUnknownPeer
	}
	return dst.HandleTimeoutNow(args), nil
}
