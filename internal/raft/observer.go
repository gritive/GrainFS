package raft

import (
	"sync"

	"github.com/gritive/GrainFS/internal/pool"
)

// EventType identifies the kind of observation emitted by a Raft node.
type EventType int

const (
	// EventLeaderChange fires when this node transitions to or from Leader.
	EventLeaderChange EventType = iota
	// EventFailedHeartbeat fires when an AppendEntries RPC to a peer fails.
	EventFailedHeartbeat
)

// Event is the unit delivered to registered observers.
// Only fields relevant to the EventType are populated.
type Event struct {
	Type EventType

	// EventLeaderChange fields
	IsLeader bool
	LeaderID string
	Term     uint64

	// EventFailedHeartbeat fields
	PeerID string
}

// RegisterObserver registers ch to receive Raft events. Delivery is
// non-blocking: a full channel drops the event rather than blocking Raft.
//
// The caller owns ch. Call DeregisterObserver before closing ch; sending
// on a closed channel will panic.
func (n *Node) RegisterObserver(ch chan<- Event) {
	n.observerMu.Lock()
	defer n.observerMu.Unlock()
	cur := n.observers.Load()
	next := make([]chan<- Event, len(cur)+1)
	copy(next, cur)
	next[len(cur)] = ch
	n.observers.Store(next)
}

// DeregisterObserver removes ch from the observer list. No-op if not registered.
func (n *Node) DeregisterObserver(ch chan<- Event) {
	n.observerMu.Lock()
	defer n.observerMu.Unlock()
	cur := n.observers.Load()
	next := make([]chan<- Event, 0, len(cur))
	for _, obs := range cur {
		if obs != ch {
			next = append(next, obs)
		}
	}
	n.observers.Store(next)
}

// notifyObservers delivers e to all registered observers non-blockingly.
// Read path is lock-free via atomic.Value COW; observerMu only serializes writes.
func (n *Node) notifyObservers(e Event) {
	obs := n.observers.Load()
	for _, ch := range obs {
		select {
		case ch <- e:
		default:
		}
	}
}

// observerState holds the observer fields embedded in Node.
type observerState struct {
	observers  pool.AtomicValue[[]chan<- Event] // read lock-free via COW
	observerMu sync.Mutex                       // serializes Register/Deregister only
}
