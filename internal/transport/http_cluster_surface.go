package transport

// This file completes the transport.ClusterTransport surface for HTTPTransport:
// the conn-recycle / traffic-admission methods. The compile-time assertion below
// forces the full surface so boot can hold an HTTPTransport wherever it holds a
// ClusterTransport.
var _ ClusterTransport = (*HTTPTransport)(nil)

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

// SetTrafficLimits rebuilds the inbound admission limiter (applied per native
// route by the family's internal StreamType class).
func (t *HTTPTransport) SetTrafficLimits(l TrafficLimits) {
	t.mu.Lock()
	t.traffic = NewTrafficLimiter(l)
	t.mu.Unlock()
}
