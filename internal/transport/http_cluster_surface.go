package transport

import (
	"context"
	"errors"
)

// This file completes the transport.ClusterTransport surface for HTTPTransport
// (S8-3): the Transport gossip methods (Connect/Send/Receive), the mux-carrier
// stubs (the HTTP transport runs with raft mux DISABLED — see the S8-3 design —
// so these are never invoked), and the conn-recycle / traffic-admission methods.
// The compile-time assertion below forces the full surface so boot can hold an
// HTTPTransport wherever it holds a ClusterTransport (S8-4 wiring).
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

// SetMuxConnHandler is a stub: the HTTP transport runs with raft mux disabled, so no
// mux connections are ever accepted. Never invoked (boot does not EnableMux for HTTP).
func (t *HTTPTransport) SetMuxConnHandler(h MuxConnHandler) {}

// GetOrConnectMux is unsupported on the HTTP transport (mux disabled). Returning an
// error rather than a nil carrier makes accidental use loud rather than a nil panic.
func (t *HTTPTransport) GetOrConnectMux(ctx context.Context, addr string) (MuxCarrier, error) {
	return nil, errors.New("http transport: mux is unsupported (raft runs over Call)")
}

// EvictMux is a stub (no mux carriers exist on the HTTP transport).
func (t *HTTPTransport) EvictMux(addr string, carrier MuxCarrier) {}

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
