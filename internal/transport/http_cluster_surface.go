package transport

import (
	"context"
)

// This file completes the transport.ClusterTransport surface for HTTPTransport:
// the Transport gossip methods (Connect/Send/Receive) and the conn-recycle /
// traffic-admission methods. The compile-time assertion below forces the full
// surface so boot can hold an HTTPTransport wherever it holds a ClusterTransport.
var _ ClusterTransport = (*HTTPTransport)(nil)

// Connect is a no-op: Call/Send dial on demand via the Hertz client pool. Kept to
// satisfy Transport (gossip calls Connect before Send); reachability errors surface
// at Send/Call instead. Mirrors TCPTransport.Connect.
func (t *HTTPTransport) Connect(ctx context.Context, addr string) error { return nil }

// Send delivers a fire-and-forget message (gossip). It POSTs the message and
// ignores the reply body. The peer's handleRPC routes a message with no per-type
// handler to its inbox (Receive), returning an empty 200.
func (t *HTTPTransport) Send(ctx context.Context, addr string, msg *Message) error {
	_, _, err := t.doRPC(ctx, addr, msg, nil, false)
	return err
}

// Receive returns the channel of fire-and-forget inbound messages (gossip). Mirrors
// TCPTransport.Receive.
func (t *HTTPTransport) Receive() <-chan *ReceivedMessage { return t.inbox }

// RecycleConns closes the Hertz client's idle connections so peers re-handshake
// under the current presented identity (the HTTP analogue of TCPTransport's conn
// recycling after a rotation/flip; the server side re-reads identity per handshake
// via GetConfigForClient).
func (t *HTTPTransport) RecycleConns() {
	t.mu.RLock()
	c := t.client
	t.mu.RUnlock()
	if c != nil {
		c.CloseIdleConnections()
	}
}

// ClosePeer drops cached connections for one peer. The Hertz client has no per-host
// close, so this closes all idle conns (coarser than TCP's per-addr eviction but
// correct: a revoked identity stops using any pre-revocation pooled conn).
func (t *HTTPTransport) ClosePeer(addr string) { t.RecycleConns() }

// SetTrafficLimits rebuilds the inbound admission limiter (applied in handleRPC by
// StreamType class). Mirrors TCPTransport.SetTrafficLimits.
func (t *HTTPTransport) SetTrafficLimits(l TrafficLimits) {
	t.mu.Lock()
	t.traffic = NewTrafficLimiter(l)
	t.mu.Unlock()
}
