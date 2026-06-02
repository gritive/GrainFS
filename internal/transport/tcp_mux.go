package transport

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// muxSessionID groups the N TCP conns of one outbound mux carrier so the acceptor
// can demux them into a single inbound carrier. It is per-carrier (a fresh dialer
// carrier => a fresh id), NOT per-peer, so the transient two-dial race produces two
// distinct sessions that never collide.
type muxSessionID [16]byte

// muxSessionInitMagic prefixes every mux conn's first frame so the acceptor can
// reject a stray (non-mux) conn that reached the mux ALPN. ALPN already gates this;
// the magic is defense in depth + a clean framing boundary.
var muxSessionInitMagic = [4]byte{'g', 'm', 'x', '1'}

// muxSessionInitLen is the fixed on-wire size of the session-init frame
// (magic + session id). Read with io.ReadFull — exactly these bytes, no read-ahead.
const muxSessionInitLen = 4 + 16

func newMuxSessionID() muxSessionID {
	var id muxSessionID
	// crypto/rand.Read never returns a short read without an error.
	if _, err := rand.Read(id[:]); err != nil {
		panic(fmt.Sprintf("transport: mux session id: %v", err))
	}
	return id
}

// writeMuxSessionInit sends the fixed-size [magic|sessionID] frame on a freshly
// dialed mux conn so the acceptor can group it. These are the FIRST bytes on the
// conn, ahead of the raft opStreamInit frame.
func writeMuxSessionInit(w io.Writer, id muxSessionID) error {
	buf := make([]byte, muxSessionInitLen)
	copy(buf[0:4], muxSessionInitMagic[:])
	copy(buf[4:], id[:])
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("write mux session init: %w", err)
	}
	return nil
}

// readMuxSessionInit reads EXACTLY muxSessionInitLen bytes (io.ReadFull — no
// buffered reader, no read-ahead) and returns the session id. The conn is left
// positioned at the next frame (the raft opStreamInit), which RaftConn skips.
func readMuxSessionInit(r io.Reader) (muxSessionID, error) {
	buf := make([]byte, muxSessionInitLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return muxSessionID{}, fmt.Errorf("read mux session init: %w", err)
	}
	var magic [4]byte
	copy(magic[:], buf[0:4])
	if magic != muxSessionInitMagic {
		return muxSessionID{}, fmt.Errorf("mux session init: bad magic %x", buf[0:4])
	}
	var id muxSessionID
	copy(id[:], buf[4:])
	return id, nil
}

var (
	errMuxCarrierClosed  = errors.New("transport: mux carrier closed")
	errMuxOutboundAccept = errors.New("transport: AcceptStream unsupported on outbound mux carrier")
	errMuxInboundOpen    = errors.New("transport: OpenStream unsupported on inbound mux carrier")
)

// tcpOutboundMuxCarrier is the dialer side of a TCP mux carrier. Each OpenStream
// dials a fresh conn to addr and stamps it with this carrier's session id, so the
// acceptor groups them. It owns ONLY its own conns (fresh-per-GetOrConnectMux), so
// a race-loser closing it never tears down another carrier's live conns.
type tcpOutboundMuxCarrier struct {
	t      *TCPTransport
	addr   string
	sid    muxSessionID
	mu     sync.Mutex
	conns  []net.Conn
	closed bool
}

func (c *tcpOutboundMuxCarrier) OpenStream(ctx context.Context) (io.ReadWriteCloser, error) {
	conn, err := c.t.dialMux(ctx, c.addr)
	if err != nil {
		return nil, err
	}
	if err := writeMuxSessionInit(conn, c.sid); err != nil {
		_ = conn.Close()
		return nil, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = conn.Close()
		return nil, errMuxCarrierClosed
	}
	c.conns = append(c.conns, conn)
	c.mu.Unlock()
	return conn, nil
}

// AcceptStream is unsupported on the dialer side: the raft driver only ever calls
// AcceptStream on the inbound (acceptor) carrier.
func (c *tcpOutboundMuxCarrier) AcceptStream(ctx context.Context) (io.ReadWriteCloser, error) {
	return nil, errMuxOutboundAccept
}

func (c *tcpOutboundMuxCarrier) RemoteAddr() string { return c.addr }

func (c *tcpOutboundMuxCarrier) Close(cause error) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conns := c.conns
	c.conns = nil
	c.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	c.t.dropOutboundMux(c) // gate-check #4: cleaned on Close (and via EvictMux on break/race-loss)
	return nil
}

var _ MuxCarrier = (*tcpOutboundMuxCarrier)(nil)

// tcpInboundMuxCarrier is the acceptor side of a TCP mux carrier: the conns of one
// session id, delivered to AcceptStream in arrival order. Created on the FIRST conn
// of a session (which also invokes the mux handler once); subsequent same-session
// conns are pushed onto the queue.
type tcpInboundMuxCarrier struct {
	t        *TCPTransport
	sid      muxSessionID
	addr     string
	incoming chan net.Conn
	mu       sync.Mutex
	conns    []net.Conn
	closed   bool
	done     chan struct{}
}

func (c *tcpInboundMuxCarrier) OpenStream(ctx context.Context) (io.ReadWriteCloser, error) {
	return nil, errMuxInboundOpen
}

func (c *tcpInboundMuxCarrier) AcceptStream(ctx context.Context) (io.ReadWriteCloser, error) {
	select {
	case conn, ok := <-c.incoming:
		if !ok {
			return nil, errMuxCarrierClosed
		}
		// A select with both incoming and done ready picks one at random; if Close
		// already ran, the popped conn is in c.conns and was closed there. Re-check
		// so AcceptStream returns an error, never an already-dead conn. (incoming is
		// never closed — a concurrent routeInboundMuxConn send would panic — so this
		// post-pop check is the safe way to tighten the closed-at-pop window.)
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if closed {
			_ = conn.Close()
			return nil, errMuxCarrierClosed
		}
		return conn, nil
	case <-c.done:
		return nil, errMuxCarrierClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *tcpInboundMuxCarrier) RemoteAddr() string { return c.addr }

func (c *tcpInboundMuxCarrier) Close(cause error) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	conns := c.conns
	c.conns = nil
	c.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	c.t.dropInboundMux(c.sid, c)
	return nil
}

var _ MuxCarrier = (*tcpInboundMuxCarrier)(nil)

// GetOrConnectMux returns a FRESH outbound mux carrier for addr (no addr-cache).
// Each carrier owns its own session + conns, so two concurrent dials to the same
// peer (the raft muxConnFor race) produce independent carriers; the race-loser's
// Close tears down only its own conns. The actual dialing is lazy (OpenStream).
func (t *TCPTransport) GetOrConnectMux(ctx context.Context, addr string) (MuxCarrier, error) {
	c := &tcpOutboundMuxCarrier{t: t, addr: addr, sid: newMuxSessionID()}
	t.mu.Lock()
	if t.ctx.Err() != nil {
		t.mu.Unlock()
		return nil, errMuxCarrierClosed
	}
	t.muxOutbound[c] = struct{}{}
	t.mu.Unlock()
	return c, nil
}

// EvictMux drops an outbound carrier from the reap-set. internal/raft calls this on
// break (OnBroken) and on the race-loss path (rc.Close()→OnBroken). addr is part of
// the interface signature but unused: outbound carriers are identity-keyed, not
// addr-keyed (multiple carriers can transiently share an addr).
func (t *TCPTransport) EvictMux(addr string, carrier MuxCarrier) {
	if oc, ok := carrier.(*tcpOutboundMuxCarrier); ok {
		t.dropOutboundMux(oc)
	}
}

func (t *TCPTransport) dropOutboundMux(c *tcpOutboundMuxCarrier) {
	t.mu.Lock()
	delete(t.muxOutbound, c)
	t.mu.Unlock()
}

// SetMuxConnHandler registers the callback that owns each accepted mux session (one
// invocation per session id). internal/raft registers it to wrap the carrier in a
// RaftConn. nil (the default) rejects mux conns at accept time.
func (t *TCPTransport) SetMuxConnHandler(h MuxConnHandler) {
	t.mu.Lock()
	t.muxHandler = h
	t.mu.Unlock()
}

// routeInboundMuxConn reads the session-init frame (deadline-bounded) and groups the
// conn into its session's inbound carrier, invoking the mux handler once per new
// session. Called from serveConn after the handshake routes a mux-ALPN conn here.
// On any error the conn is closed (serveConn already untracked it).
func (t *TCPTransport) routeInboundMuxConn(conn net.Conn) {
	// gate-check #3: bound the session-init read so a silent peer can't pin us.
	_ = conn.SetReadDeadline(time.Now().Add(t.cfg.ServerIdleTimeout))
	sid, err := readMuxSessionInit(conn) // gate-check #2: io.ReadFull, exact bytes
	if err != nil {
		_ = conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{}) // clear before handing to RaftConn (long-lived)

	t.mu.Lock()
	if t.ctx.Err() != nil || t.muxHandler == nil {
		t.mu.Unlock()
		_ = conn.Close()
		return
	}
	carrier := t.muxInbound[sid]
	isNew := false
	if carrier == nil {
		carrier = &tcpInboundMuxCarrier{
			t: t, sid: sid, addr: conn.RemoteAddr().String(),
			incoming: make(chan net.Conn, 8), done: make(chan struct{}),
		}
		t.muxInbound[sid] = carrier
		isNew = true
	}
	h := t.muxHandler
	t.mu.Unlock()

	// Track the conn under the carrier so Close reaps it.
	carrier.mu.Lock()
	if carrier.closed {
		carrier.mu.Unlock()
		_ = conn.Close()
		return
	}
	carrier.conns = append(carrier.conns, conn)
	carrier.mu.Unlock()

	// Deliver to AcceptStream (buffered; block on done/ctx if full).
	select {
	case carrier.incoming <- conn:
	case <-carrier.done:
		_ = conn.Close()
		return
	case <-t.ctx.Done():
		_ = conn.Close()
		return
	}

	if isNew {
		go h(t.ctx, carrier) // gate-check #6: handler invoked ONCE per session
	}
}

func (t *TCPTransport) dropInboundMux(sid muxSessionID, c *tcpInboundMuxCarrier) {
	t.mu.Lock()
	if t.muxInbound[sid] == c {
		delete(t.muxInbound, sid)
	}
	t.mu.Unlock()
}
