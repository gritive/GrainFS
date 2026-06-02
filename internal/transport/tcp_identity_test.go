package transport

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTCPIdentity_UpdateRegistryAcceptTakesEffectOnNewDial proves the server reads
// its accept-set FRESH per inbound handshake (live-swap), not from a config
// captured at construction. A peer with an out-of-cluster identity is rejected by
// a full data-plane Call (the proven SPKI-rejection surface — TLS 1.3 defers
// server-side client-cert rejection past the client handshake), then accepted
// after UpdateRegistryAccept — with NO listener restart.
func TestTCPIdentity_UpdateRegistryAcceptTakesEffectOnNewDial(t *testing.T) {
	srv := startTCP(t, "tcp-identity-server-key")
	srv.Handle(StreamAdmin, func(req *Message) *Message { return NewResponse(req, nil) })
	_, serverSPKI, err := DeriveClusterIdentity("tcp-identity-server-key")
	require.NoError(t, err)

	// The outsider has a DIFFERENT cluster identity. It accepts the server (so the
	// CLIENT side never rejects — isolating the SERVER-side accept-set under test).
	outsider := MustNewTCPTransport("tcp-identity-outsider-key")
	t.Cleanup(func() { _ = outsider.Close() })
	_, outsiderSPKI, err := DeriveClusterIdentity("tcp-identity-outsider-key")
	require.NoError(t, err)
	outsider.UpdateRegistryAccept([][32]byte{serverSPKI})

	call := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, e := outsider.Call(ctx, srv.LocalAddr(), &Message{Type: StreamAdmin})
		return e
	}

	// Before: the outsider's SPKI is not in the server accept-set → rejected.
	require.Error(t, call(), "outsider must be rejected before UpdateRegistryAccept")

	// Live swap: add the outsider SPKI to the server accept-set.
	srv.UpdateRegistryAccept([][32]byte{outsiderSPKI})

	// After: a NEW handshake reads the updated accept-set → accepted.
	require.NoError(t, call(), "outsider must be accepted after UpdateRegistryAccept (fresh per-handshake read)")
}

// TestTCPIdentity_FlipPresentChangesPresentedCert proves FlipPresent swaps the
// cert the server PRESENTS, surfaced via a client that accepts ONLY the post-flip
// (per-node) cert — so the pre-flip handshake is client-rejected and the post-flip
// one succeeds, with no restart.
func TestTCPIdentity_FlipPresentChangesPresentedCert(t *testing.T) {
	srv := startTCP(t, "flip-server-key")
	srv.Handle(StreamAdmin, func(req *Message) *Message { return NewResponse(req, nil) })

	perNodeCert, perNodeSPKI, err := GenerateNodeIdentity("cid", "srv-per-node")
	require.NoError(t, err)

	cli := MustNewTCPTransport("flip-client-key")
	t.Cleanup(func() { _ = cli.Close() })
	_, cliSPKI, err := DeriveClusterIdentity("flip-client-key")
	require.NoError(t, err)
	// Client accepts ONLY the flipped per-node SPKI (plus its own base) — NOT the
	// server's original PSK-derived present cert.
	cli.UpdateRegistryAccept([][32]byte{perNodeSPKI})
	srv.UpdateRegistryAccept([][32]byte{cliSPKI}) // server accepts the client

	call := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, e := cli.Call(ctx, srv.LocalAddr(), &Message{Type: StreamAdmin})
		return e
	}

	require.Error(t, call(), "client must reject the pre-flip (PSK-derived) server cert")
	srv.FlipPresent(perNodeCert, perNodeSPKI)
	require.NoError(t, call(), "client must accept the flipped per-node server cert")
}

// TestTCPIdentity_SetDroppedRemovesBaseAccept proves SetDropped removes the
// cluster-key-derived base SPKI from the accept-set: a same-identity peer accepted
// before the drop is rejected on a NEW dial after it.
func TestTCPIdentity_SetDroppedRemovesBaseAccept(t *testing.T) {
	srv := startTCP(t, "dropped-key")
	srv.Handle(StreamAdmin, func(req *Message) *Message { return NewResponse(req, nil) })
	cli := MustNewTCPTransport("dropped-key") // same cluster identity
	t.Cleanup(func() { _ = cli.Close() })

	call := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, e := cli.Call(ctx, srv.LocalAddr(), &Message{Type: StreamAdmin})
		return e
	}

	require.NoError(t, call(), "same-identity peer accepted before drop")
	srv.SetDropped()
	// Recycle so the next Call re-handshakes (the pooled conn predates the drop).
	srv.RecycleConns()
	cli.RecycleConns()
	require.Error(t, call(), "base SPKI dropped → same-identity peer rejected on new dial")
}

// TestTCPIdentity_RecycleConnsClosesPooledAndInbound proves the recycle actually
// drops live conns (not just that a setter ran): a pooled idle OUTBOUND conn is
// drained + closed, and a live INBOUND conn the server tracked is dropped + closed
// — so no conn opened under the old identity survives to serve post-flip traffic.
// (Data-plane Call is connection-per-RPC, so we establish persistent conns directly
// via dial+checkin and a held-open inbound handshake.)
func TestTCPIdentity_RecycleConnsClosesPooledAndInbound(t *testing.T) {
	srv := startTCP(t, "recycle-key")
	cli := MustNewTCPTransport("recycle-key")
	t.Cleanup(func() { _ = cli.Close() })
	addr := srv.LocalAddr()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Outbound: dial a conn under the current identity and park it idle in the pool.
	outConn, err := cli.dial(ctx, addr)
	require.NoError(t, err)
	cli.pool.checkin(addr, outConn)
	cli.pool.mu.Lock()
	require.Len(t, cli.pool.idle[addr], 1, "expected one pooled idle conn")
	cli.pool.mu.Unlock()

	// Inbound: the dial above also produced an accepted conn the server tracks
	// (its serveConn blocks on Decode, holding it in t.conns).
	require.Eventually(t, func() bool {
		srv.mu.RLock()
		defer srv.mu.RUnlock()
		return len(srv.conns) > 0
	}, 3*time.Second, 10*time.Millisecond, "server should track the inbound conn")

	cli.RecycleConns()
	srv.RecycleConns()

	// Outbound pool drained synchronously (pool.closeAll) + the conn is closed.
	cli.pool.mu.Lock()
	idleAfter := len(cli.pool.idle[addr])
	cli.pool.mu.Unlock()
	assert.Equal(t, 0, idleAfter, "client pool must be drained after RecycleConns")
	_ = outConn.SetReadDeadline(time.Now().Add(time.Second))
	_, rerr := outConn.Read(make([]byte, 1))
	assert.Error(t, rerr, "the recycled pooled conn must be closed")

	// Inbound conns dropped from tracking (closed off-lock with jitter).
	require.Eventually(t, func() bool {
		srv.mu.RLock()
		defer srv.mu.RUnlock()
		return len(srv.conns) == 0
	}, 3*time.Second, 10*time.Millisecond, "server must drop tracked inbound conns on recycle")
}

// TestTCPIdentity_ClosePeerDropsPeerPool proves ClosePeer drains+closes the pooled
// conns for one peer (the revoked-peer case), leaving other peers untouched.
func TestTCPIdentity_ClosePeerDropsPeerPool(t *testing.T) {
	srv := startTCP(t, "closepeer-key")
	other := startTCP(t, "closepeer-key")
	cli := MustNewTCPTransport("closepeer-key")
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peer := srv.LocalAddr()
	otherAddr := other.LocalAddr()
	peerConn, err := cli.dial(ctx, peer)
	require.NoError(t, err)
	cli.pool.checkin(peer, peerConn)
	otherConn, err := cli.dial(ctx, otherAddr)
	require.NoError(t, err)
	cli.pool.checkin(otherAddr, otherConn)

	cli.ClosePeer(peer)

	cli.pool.mu.Lock()
	peerIdle := len(cli.pool.idle[peer])
	otherIdle := len(cli.pool.idle[otherAddr])
	cli.pool.mu.Unlock()
	assert.Equal(t, 0, peerIdle, "ClosePeer must drain the peer's pooled conns")
	assert.Equal(t, 1, otherIdle, "ClosePeer must NOT touch other peers")

	_ = peerConn.SetReadDeadline(time.Now().Add(time.Second))
	_, rerr := peerConn.Read(make([]byte, 1))
	assert.Error(t, rerr, "ClosePeer must close the peer's pooled conn")
}
