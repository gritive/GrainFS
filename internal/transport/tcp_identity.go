package transport

import (
	"crypto/tls"
	"net"
	"time"
)

// Cluster identity / rotation surface for TCPTransport (S5a), mirroring the
// prior QUIC identity surface. Each mutator delegates to the
// shared identityComposer, whose swap closure atomically restores a fresh
// IdentitySnapshot into t.identity; the per-handshake TLS builders read it live.
// RecycleConns/ClosePeer close live conns so peers re-handshake under the new
// identity — the TCP analogue of QUIC's conn recycling, adapted to the pool +
// accepted-conn set + mux carriers.

// SwapIdentity atomically replaces the active identity snapshot. snap must be
// non-nil with at least one accept entry (caller's invariant). Mirror of QUIC.
func (t *TCPTransport) SwapIdentity(snap *IdentitySnapshot) {
	t.identity.Store(snap)
}

// UpdateRegistryAccept feeds peer-registry per-node SPKIs into the composer as a
// delta; the composer recomputes base ∪ rotation ∪ registry.
func (t *TCPTransport) UpdateRegistryAccept(spkis [][32]byte) {
	t.composer.setRegistry(spkis)
}

// SeedInitialPeerSPKIs populates the registry accept-set before Listen so a joiner
// accepts incumbents' per-node certs from the first inbound handshake. Empty input
// is a no-op (rolling-upgrade compat).
func (t *TCPTransport) SeedInitialPeerSPKIs(spkis [][32]byte) {
	if len(spkis) == 0 {
		return
	}
	t.UpdateRegistryAccept(spkis)
}

// ApplyRotation routes one rotation-phase change (window + present cert, and an
// optional new base) through the composer as a single atomic recompute.
func (t *TCPTransport) ApplyRotation(window [][32]byte, present tls.Certificate, presentSPKI [32]byte, newBase *[32]byte) {
	t.composer.applyRotation(window, present, presentSPKI, newBase)
}

// FlipPresent pins this transport's PRESENTED identity to its per-node cert.
func (t *TCPTransport) FlipPresent(cert tls.Certificate, spki [32]byte) {
	t.composer.setPinPresent(cert, spki)
}

// SetDropped removes ALL cluster-key-derived SPKIs (base + rotation window) from
// the accept-set (post cluster-key-drop).
func (t *TCPTransport) SetDropped() {
	t.composer.setDropped()
}

// SetTrafficLimits rebuilds the inbound admission limiter. Mirror of QUIC.
func (t *TCPTransport) SetTrafficLimits(l TrafficLimits) {
	t.mu.Lock()
	t.traffic = NewTrafficLimiter(l)
	t.mu.Unlock()
}

// RecycleConns closes every live conn — pooled outbound, accepted inbound, and mux
// carriers (inbound + outbound) — so peers re-handshake under the current presented
// identity. Bounded jitter avoids a synchronized cluster-wide re-handshake storm.
// Mirrors QUIC's RecycleConns; the caller orchestrates (e.g. SetDropped then
// RecycleConns), the method itself does not mutate identity.
func (t *TCPTransport) RecycleConns() {
	// Bump the pool generation FIRST so any data-plane conn checked out before this
	// call is discarded (not re-pooled) when it checks in, even if it is still
	// in-flight here. closeAll below drains the already-idle conns.
	t.pool.bumpGlobalGen()

	t.mu.Lock()
	inbound := make([]net.Conn, 0, len(t.conns))
	for c := range t.conns {
		inbound = append(inbound, c)
		delete(t.conns, c)
	}
	muxIn := make([]*tcpInboundMuxCarrier, 0, len(t.muxInbound))
	for sid, c := range t.muxInbound {
		muxIn = append(muxIn, c)
		delete(t.muxInbound, sid)
	}
	muxOut := make([]*tcpOutboundMuxCarrier, 0, len(t.muxOutbound))
	for c := range t.muxOutbound {
		muxOut = append(muxOut, c)
		delete(t.muxOutbound, c)
	}
	t.mu.Unlock()

	// Drain the per-peer data-plane pool (its own lock) outside t.mu.
	t.pool.closeAll()

	for _, c := range inbound {
		c := c
		go t.closeJittered(func() { _ = c.Close() })
	}
	for _, c := range muxIn {
		c := c
		go t.closeJittered(func() { _ = c.Close(nil) })
	}
	for _, c := range muxOut {
		c := c
		go t.closeJittered(func() { _ = c.Close(nil) })
	}
}

// ClosePeer evicts and closes cached OUTBOUND connections for one peer (pooled
// data-plane conns + any mux carrier to that addr) so a revoked identity stops
// using a pre-revocation connection. Inbound conns are not addr-keyed on TCP
// (matching QUIC, which only closes its addr-keyed outbound caches).
func (t *TCPTransport) ClosePeer(addr string) {
	// Bump the peer generation FIRST so an in-flight conn to this peer is discarded
	// on checkin (not re-pooled); closePeer drains the already-idle conns.
	t.pool.bumpPeerGen(addr)
	t.pool.closePeer(addr)

	t.mu.Lock()
	var carriers []*tcpOutboundMuxCarrier
	for c := range t.muxOutbound {
		if c.RemoteAddr() == addr {
			carriers = append(carriers, c)
			delete(t.muxOutbound, c)
		}
	}
	t.mu.Unlock()
	for _, c := range carriers {
		_ = c.Close(nil)
	}
}

// closeJittered runs close after a bounded random delay (or immediately if the
// transport is shutting down), de-synchronizing a cluster-wide reconnect storm.
func (t *TCPTransport) closeJittered(close func()) {
	if j := recycleJitter(); j > 0 {
		select {
		case <-time.After(j):
		case <-t.ctx.Done():
		}
	}
	close()
}
