package raftv2

import (
	"errors"
	"sync"
)

// ErrUnknownPeer is returned by memTransport when the destination peer is not
// registered in the in-process network. Production transports return their
// own network errors.
var ErrUnknownPeer = errors.New("raftv2: unknown peer")

// Transport sends RPCs to peer nodes. Implementations are responsible for
// network details (QUIC, TCP, etc). The actor goroutine never calls Transport
// methods directly; outbound RPCs are sent by separate worker goroutines that
// the Transport implementation owns. PR 4 only defines the interface; the
// outbound side is wired in PR 5.
type Transport interface {
	SendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	// SendInstallSnapshot ships a snapshot blob to a peer whose nextIndex
	// has fallen below the leader's compaction floor (Raft §6.3 / §7).
	// PR 15 sends the entire snapshot in one call; chunked transmission is
	// out of scope.
	SendInstallSnapshot(peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
	// SendTimeoutNow triggers an immediate election on the transfer target
	// (Raft §3.10). The leader calls this after selecting the most-caught-up
	// peer and steps down regardless of whether the RPC succeeds.
	SendTimeoutNow(peer string, args *TimeoutNowArgs) (*TimeoutNowReply, error)
}

// memNetwork is a process-local Transport substrate for tests. It dispatches
// calls directly to the destination Node's Handle* methods, bypassing
// serialization. Construct one network per test scenario via newMemNetwork
// and Register each Node to obtain its per-node Transport view.
//
// Placement note: kept in production code path (not _test.go) so future
// in-process bring-up paths (e.g. embedded single-process clusters in tests
// outside this package) can reuse it. It carries no production responsibility
// beyond being a reference Transport implementation.
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
