package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// tcpALPN is the single ALPN advertised by the TCP cluster transport (S1).
// QUIC's dual-ALPN (legacy/mux) split is replaced in S2 (RaftConn restructure);
// S1 speaks one protocol over connection-per-RPC.
const tcpALPN = "grainfs-tcp-v1"

// tcpMuxALPN is the ALPN for the raft control-plane mux carrier (S2b-2). An accepted
// conn negotiating it routes into the mux demux instead of the data-plane request
// loop. The data plane keeps speaking tcpALPN only; a dialer offers exactly one of
// the two ALPNs (data-plane clientTLS vs mux muxClientTLS).
const tcpMuxALPN = "grainfs-tcp-mux-v1"

// tcpInboundBulkAcquireTimeout bounds inbound admission for Data/Bulk classes so a
// saturated class fails fast with StatusOverloaded instead of queueing (parity with
// QUIC acquireInboundTraffic). Control/Meta classes block on t.ctx (no overload).
const tcpInboundBulkAcquireTimeout = 25 * time.Millisecond

var (
	errOverloaded         = errors.New("transport: server overloaded")
	errNilHandlerResponse = errors.New("transport: handler returned no response")
)

// TCPTransport implements the transport-agnostic cluster RPC surface
// (transport.Transport + Call*/Handle*) over TLS 1.3 on TCP. The data plane uses
// a pooled, chunk-framed model (S3a); S3b adds resource bounds (read deadlines,
// shutdown reaping), an elastic conn pool, socket tuning, and inbound admission.
//
// S1/S2/S3 are DORMANT: TCPTransport is not wired into boot (QUIC stays live), so
// the bounds below are not yet load-bearing in production — they become operative
// when S4/S5 wire it in.
//
// Identity is STATIC SPKI pinning (one IdentitySnapshot). The dynamic rotation/
// registry surface defers to the wiring/join slice and is intentionally absent.
//
// Framing invariant (RST avoidance): bulk bodies are length-prefixed chunk streams
// terminated by a zero-length chunk (S3a), so request/response frames are
// self-delimiting and no path relies on a conn close to delimit data.
type TCPTransport struct {
	mu        sync.RWMutex
	listener  net.Listener
	inbox     chan *ReceivedMessage
	codec     *BinaryCodec
	router    *StreamRouter
	localAddr string
	ctx       context.Context
	cancel    context.CancelFunc

	streamHandler StreamHandler // catch-all for types with no per-type handler

	snap         *IdentitySnapshot
	serverTLS    *tls.Config
	clientTLS    *tls.Config
	muxClientTLS *tls.Config // mux-ONLY dialer config (NextProtos = [tcpMuxALPN])

	cfg     TCPTransportConfig
	pool    *connPool             // per-peer elastic data-plane conn pool (S3b)
	traffic *TrafficLimiter       // inbound admission (nil-safe = unlimited)
	connSem chan struct{}         // bounds concurrent serveConn goroutines (nil = unlimited)
	conns   map[net.Conn]struct{} // accepted, in-flight conns; reaped on Close
	dpSeq   uint64                // monotonic data-plane request ID (desync detection)

	// Mux carrier state (S2b-2, dormant — set only when internal/raft registers a
	// mux handler; not wired into boot, which still selects QUIC).
	muxHandler  MuxConnHandler                         // nil = reject mux conns at accept
	muxInbound  map[muxSessionID]*tcpInboundMuxCarrier // accepted mux sessions (demux); reaped on Close
	muxOutbound map[*tcpOutboundMuxCarrier]struct{}    // dialed carriers; reaped on Close
}

// NewTCPTransport derives the cluster identity from psk and builds a transport
// pinned to it. Mirrors NewQUICTransport's empty-PSK contract (D6=B). Resource
// bounds and pool policy use the package defaults (see TCPTransportConfig).
func NewTCPTransport(psk string) (*TCPTransport, error) {
	if psk == "" {
		return nil, ErrEmptyClusterKey
	}
	cert, spki, err := DeriveClusterIdentity(psk)
	if err != nil {
		return nil, fmt.Errorf("derive cluster identity: %w", err)
	}
	snap := NewIdentitySnapshot([][32]byte{spki}, cert, spki)

	ctx, cancel := context.WithCancel(context.Background())
	cfg := TCPTransportConfig{MaxConnsPerPeer: defaultMaxConnsPerPeer}.withDefaults()
	t := &TCPTransport{
		inbox:       make(chan *ReceivedMessage, 256),
		codec:       &BinaryCodec{},
		router:      NewStreamRouter(),
		ctx:         ctx,
		cancel:      cancel,
		snap:        snap,
		conns:       make(map[net.Conn]struct{}),
		muxInbound:  make(map[muxSessionID]*tcpInboundMuxCarrier),
		muxOutbound: make(map[*tcpOutboundMuxCarrier]struct{}),
	}
	t.applyConfig(cfg)
	// Server pins the dialer's cert SPKI; ClientAuth forces the dialer to present
	// one. crypto/tls runs VerifyPeerCertificate during the handshake and fails
	// closed on mismatch — unlike quic-go, no app-layer re-check is needed.
	t.serverTLS = &tls.Config{
		MinVersion:            tls.VersionTLS13,
		Certificates:          []tls.Certificate{snap.PresentCert},
		ClientAuth:            tls.RequireAnyClientCert,
		NextProtos:            []string{tcpALPN},
		VerifyPeerCertificate: pinAcceptedSPKI(snap),
	}
	// Dialer presents the cluster cert and pins the server's SPKI. InsecureSkipVerify
	// disables default CA/hostname verification; the real check is the SPKI pin in
	// VerifyPeerCertificate (same pattern as quic.go buildClientTLSConfig; repo
	// golangci excludes G402).
	t.clientTLS = &tls.Config{
		MinVersion:            tls.VersionTLS13,
		InsecureSkipVerify:    true,
		Certificates:          []tls.Certificate{snap.PresentCert},
		NextProtos:            []string{tcpALPN},
		VerifyPeerCertificate: pinAcceptedSPKI(snap),
	}
	// Advertise BOTH ALPNs on the listener: data-plane (tcpALPN) and mux (tcpMuxALPN).
	// Inbound conns route by negotiated protocol in serveConn (S2b-2).
	t.serverTLS.NextProtos = []string{tcpMuxALPN, tcpALPN}
	// Mux dials use a SEPARATE config offering ONLY the mux ALPN, so a data-plane dial
	// can never negotiate mux (gate-check #1) and a non-mux peer fails a mux dial cleanly.
	t.muxClientTLS = t.clientTLS.Clone()
	t.muxClientTLS.NextProtos = []string{tcpMuxALPN}
	return t, nil
}

// applyConfig (re)builds the config-derived components (pool, traffic limiter,
// accept semaphore). Used by NewTCPTransport and setConfigForTest.
func (t *TCPTransport) applyConfig(cfg TCPTransportConfig) {
	t.cfg = cfg
	t.pool = newConnPool(cfg.MaxConnsPerPeer, cfg.PoolIdleTimeout)
	t.traffic = NewTrafficLimiter(cfg.TrafficLimits)
	t.connSem = makeLimit(cfg.MaxConcurrentConns)
}

// MustNewTCPTransport panics on error. Test setup only — production must surface
// the error to the operator.
func MustNewTCPTransport(psk string) *TCPTransport {
	t, err := NewTCPTransport(psk)
	if err != nil {
		panic(fmt.Sprintf("MustNewTCPTransport: %v", err))
	}
	return t
}

// nextDataPlaneID returns a unique nonzero request ID for pooled data-plane RPCs,
// used to detect a desynced pooled conn (the response must echo req.ID).
func (t *TCPTransport) nextDataPlaneID() uint64 {
	return atomic.AddUint64(&t.dpSeq, 1)
}

// Receive returns the channel of fire-and-forget inbound messages (gossip).
func (t *TCPTransport) Receive() <-chan *ReceivedMessage { return t.inbox }

// LocalAddr returns the bound listen address.
func (t *TCPTransport) LocalAddr() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.localAddr
}

// SetStreamHandler registers a catch-all handler for request types that have no
// per-type handler (mirrors QUICTransport). Boot wires the cluster stream router
// here, and StreamData shard RPCs reach the shard service only via this catch-all.
func (t *TCPTransport) SetStreamHandler(h StreamHandler) {
	t.mu.Lock()
	t.streamHandler = h
	t.mu.Unlock()
}

// Handle registers a per-type request/response handler.
func (t *TCPTransport) Handle(st StreamType, h StreamHandler) { t.router.Handle(st, h) }

// HandleBody registers a per-type handler that receives the request frame plus the
// chunked body stream. The handler reads the body via the provided io.Reader; the
// transport drains any un-read remainder to the terminator so the pooled conn is
// left clean (S3a serveOne drain).
func (t *TCPTransport) HandleBody(st StreamType, h StreamBodyHandler) { t.router.HandleBody(st, h) }

// HandleRead registers a per-type handler that returns a framed metadata response
// followed by a streamed (chunked) response body.
func (t *TCPTransport) HandleRead(st StreamType, h StreamReadHandler) { t.router.HandleRead(st, h) }

// Connect is a no-op for the pooled TCP transport: Call/CallWithBody dial or check
// out conns on demand. Kept to satisfy transport.Transport (gossip calls Connect
// before Send); peer-reachability errors surface at Send/Call instead.
func (t *TCPTransport) Connect(ctx context.Context, addr string) error { return nil }

// Listen binds a TCP listener wrapped in the server TLS config and serves each
// accepted connection in a persistent request loop (conns are pooled by clients).
func (t *TCPTransport) Listen(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	t.mu.Lock()
	t.listener = tls.NewListener(ln, t.serverTLS)
	t.localAddr = ln.Addr().String()
	t.mu.Unlock()
	go t.acceptLoop()
	return nil
}

func (t *TCPTransport) acceptLoop() {
	t.mu.RLock()
	ln := t.listener
	t.mu.RUnlock()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener closed
		}
		// Bound concurrent serveConn goroutines (accept-rate / FD ceiling). A full
		// semaphore applies natural backpressure on accept.
		if t.connSem != nil {
			select {
			case t.connSem <- struct{}{}:
			case <-t.ctx.Done():
				_ = conn.Close()
				return
			}
		}
		// Track before launch so a concurrent Close() reaps this conn (tracking inside
		// serveConn would leave an accept-vs-Close window where the snapshot misses it).
		t.trackConn(conn)
		go func() {
			defer func() {
				if t.connSem != nil {
					<-t.connSem
				}
			}()
			t.serveConn(conn)
		}()
	}
}

// trackConn / untrackConn manage the accepted-conn set so Close can reap in-flight
// conns (an idle Decode otherwise survives shutdown).
func (t *TCPTransport) trackConn(c net.Conn) {
	t.mu.Lock()
	t.conns[c] = struct{}{}
	t.mu.Unlock()
}

func (t *TCPTransport) untrackConn(c net.Conn) {
	t.mu.Lock()
	delete(t.conns, c)
	t.mu.Unlock()
}

// serveConn runs the TLS handshake (SPKI pinning) then serves requests on conn in
// a persistent loop, so a pooled (reused) client conn carries many transfers. A
// read deadline bounds idle waits and stalled body transfers (S3b resource bound).
func (t *TCPTransport) serveConn(conn net.Conn) {
	defer t.untrackConn(conn) // trackConn ran in acceptLoop before launch (reap-on-Close)
	needClose := true
	defer func() {
		if needClose {
			_ = conn.Close()
		}
	}()
	t.tuneTCP(conn)
	if tc, ok := conn.(*tls.Conn); ok {
		_ = tc.SetDeadline(time.Now().Add(t.cfg.ServerIdleTimeout)) // bound the handshake (read+write)
		if err := tc.HandshakeContext(t.ctx); err != nil {
			return
		}
		// Clear the handshake WRITE deadline. SetDeadline above set an ABSOLUTE
		// (handshakeStart + idle) write deadline, but the steady-state loop re-arms
		// only the READ deadline — so leaving it would make every response Encode on a
		// conn reused past that instant fail with i/o timeout. Only writeChunkedBody
		// (the hasRead body egress) re-arms its own write deadline.
		_ = tc.SetWriteDeadline(time.Time{})
		// Route by negotiated ALPN: a mux carrier conn goes to the demux (grouped by
		// session id into one inbound carrier); everything else is the data plane. The
		// mux carrier takes over the conn lifetime, so suppress the deferred close.
		if tc.ConnectionState().NegotiatedProtocol == tcpMuxALPN {
			needClose = false
			t.routeInboundMuxConn(conn)
			return
		}
	}
	from := conn.RemoteAddr().String()
	for {
		// Idle bound: a pooled conn waiting for its next request must not pin a
		// goroutine forever. A stalled/dead peer trips this and the loop exits.
		_ = conn.SetReadDeadline(time.Now().Add(t.cfg.ServerIdleTimeout))
		req, err := t.codec.Decode(conn)
		if err != nil {
			return // EOF (client closed), framing error, or idle deadline → drop
		}
		// The body phase may run long; widen the read deadline to bound only a stall
		// (covers the handler body read AND the serveOne drain). The next loop
		// iteration re-arms a fresh idle bound.
		_ = conn.SetReadDeadline(time.Now().Add(t.cfg.ServerBodyTimeout))
		if !t.serveOne(conn, from, req) {
			return // dispatch left the conn in an unknown state → drop it
		}
	}
}

// serveOne handles one request on a (possibly reused) conn. Returns true iff the
// conn is left clean (positioned at the next frame) and may serve another request.
func (t *TCPTransport) serveOne(conn net.Conn, from string, req *Message) bool {
	bodyHandler, hasBody := t.router.LookupBody(req.Type)
	readHandler, hasRead := t.router.LookupRead(req.Type)
	typeHandler, hasType := t.router.Lookup(req.Type)

	// Inbound admission (parity with QUIC acquireInboundTraffic): gate per-type
	// handlers; the catch-all/inbox (gossip) path is not class-limited.
	if hasBody || hasRead || hasType {
		release, aerr := t.acquireInboundTraffic(req.Type)
		if aerr != nil {
			return t.rejectOverloaded(conn, req, hasBody)
		}
		defer release()
	}

	switch {
	case hasBody:
		// Request body is a chunk stream terminated by a zero-length chunk. Drain
		// any remainder to the terminator so the conn is left clean for the next
		// request (the handler is not required to drain on every path).
		cbr := &chunkedBodyReader{r: conn}
		resp := bodyHandler(req, cbr)
		if !cbr.done {
			if _, err := io.Copy(io.Discard, cbr); err != nil {
				return false // could not reach terminator → conn dirty
			}
			if !cbr.done {
				// io.Copy can return nil on a mid-chunk underlying EOF, which is NOT
				// the terminator → conn is dirty.
				return false
			}
		}
		if resp == nil {
			return t.writeNilRespError(conn, req)
		}
		return t.writeResp(conn, req, resp)
	case hasRead:
		resp, body := readHandler(req)
		if resp == nil {
			if body != nil {
				_ = body.Close()
			}
			return t.writeNilRespError(conn, req)
		}
		resp.ID = req.ID // echo for client desync detection
		if err := t.codec.Encode(conn, resp); err != nil {
			if body != nil {
				_ = body.Close()
			}
			return false
		}
		if body != nil {
			defer body.Close()
			// Egress bound: a slow-reading client must not pin this goroutine on the
			// response-body Write. Cleared before the next idle wait.
			_ = conn.SetWriteDeadline(time.Now().Add(t.cfg.ServerBodyTimeout))
			if err := writeChunkedBody(conn, body); err != nil {
				return false
			}
			_ = conn.SetWriteDeadline(time.Time{})
		}
		return true
	case hasType:
		resp := typeHandler(req)
		if resp == nil {
			return t.writeNilRespError(conn, req)
		}
		return t.writeResp(conn, req, resp)
	default:
		// No per-type handler: try the catch-all (boot routes StreamData shard RPCs
		// through it via SetStreamHandler), then fall back to the inbox for
		// fire-and-forget (gossip Receive path). Mirrors QUIC handleStream order.
		t.mu.RLock()
		catchAll := t.streamHandler
		t.mu.RUnlock()
		if catchAll != nil {
			if resp := catchAll(req); resp != nil {
				return t.writeResp(conn, req, resp)
			}
		}
		// Fire-and-forget (gossip): no response; conn stays clean for reuse.
		select {
		case t.inbox <- &ReceivedMessage{From: from, Message: req}:
		case <-t.ctx.Done():
		}
		return true
	}
}

// writeResp echoes the request ID into resp (desync detection) and encodes it as a
// single self-delimiting frame, leaving the conn clean.
func (t *TCPTransport) writeResp(conn net.Conn, req, resp *Message) bool {
	resp.ID = req.ID
	if err := t.codec.Encode(conn, resp); err != nil {
		return false
	}
	return true
}

// writeNilRespError turns a misbehaving per-type handler's nil response into a
// deterministic StatusError frame, so the client gets a structured error instead
// of hanging until its ctx deadline. The conn stays clean/reusable.
func (t *TCPTransport) writeNilRespError(conn net.Conn, req *Message) bool {
	resp := NewErrorResponse(req, StatusError, errNilHandlerResponse)
	return t.writeResp(conn, req, resp)
}

// acquireInboundTraffic bounds Data/Bulk admission with a short timeout (fail fast
// to StatusOverloaded when saturated) and blocks Control/Meta on t.ctx. Mirrors
// QUICTransport.acquireInboundTraffic for cross-transport parity.
func (t *TCPTransport) acquireInboundTraffic(st StreamType) (func(), error) {
	switch ClassOf(st) {
	case StreamClassData, StreamClassBulk:
		ctx, cancel := context.WithTimeout(t.ctx, tcpInboundBulkAcquireTimeout)
		release, err := t.traffic.Acquire(ctx, st)
		cancel()
		return release, err
	default:
		return t.traffic.Acquire(t.ctx, st)
	}
}

// rejectOverloaded handles an inbound admission rejection. For a body RPC the
// client is mid-streaming a body and will not read a response until its own write
// completes, and draining a refused body is amplification — so the conn is dropped
// (mirrors QUIC's stream cancel on bulk overload). For a non-body RPC a single
// StatusOverloaded frame is written and the conn stays clean/reusable.
func (t *TCPTransport) rejectOverloaded(conn net.Conn, req *Message, hasBody bool) bool {
	if hasBody {
		return false
	}
	resp := NewErrorResponse(req, StatusOverloaded, errOverloaded)
	return t.writeResp(conn, req, resp)
}

// tuneTCP applies NODELAY + optional buffer sizes to the underlying *net.TCPConn.
// Best-effort: failures are non-fatal. A wrapped tls.Conn exposes NetConn().
func (t *TCPTransport) tuneTCP(c net.Conn) {
	tcp, ok := underlyingTCP(c)
	if !ok {
		return
	}
	_ = tcp.SetNoDelay(true)
	if t.cfg.ReadBufferBytes > 0 {
		_ = tcp.SetReadBuffer(t.cfg.ReadBufferBytes)
	}
	if t.cfg.WriteBufferBytes > 0 {
		_ = tcp.SetWriteBuffer(t.cfg.WriteBufferBytes)
	}
}

func underlyingTCP(c net.Conn) (*net.TCPConn, bool) {
	switch v := c.(type) {
	case *net.TCPConn:
		return v, true
	case *tls.Conn:
		tcp, ok := v.NetConn().(*net.TCPConn)
		return tcp, ok
	}
	return nil, false
}

// Close shuts the transport down: cancels in-flight context, drops pooled
// data-plane conns, closes the listener, and reaps accepted (in-flight) conns so a
// stalled peer's serveConn goroutine/FD does not survive shutdown.
func (t *TCPTransport) Close() error {
	t.cancel()
	t.pool.closeAll()
	t.mu.Lock()
	ln := t.listener
	conns := make([]net.Conn, 0, len(t.conns))
	for c := range t.conns {
		conns = append(conns, c)
	}
	// Reap mux carriers (dialed + accepted sessions) so a dormant mux session does
	// not survive shutdown. Collected under the lock; closed outside it (carrier
	// Close re-locks via drop{Inbound,Outbound}Mux).
	outbound := make([]*tcpOutboundMuxCarrier, 0, len(t.muxOutbound))
	for c := range t.muxOutbound {
		outbound = append(outbound, c)
	}
	inbound := make([]*tcpInboundMuxCarrier, 0, len(t.muxInbound))
	for _, c := range t.muxInbound {
		inbound = append(inbound, c)
	}
	t.mu.Unlock()
	for _, c := range outbound {
		_ = c.Close(nil)
	}
	for _, c := range inbound {
		_ = c.Close(nil)
	}
	for _, c := range conns {
		_ = c.Close() // unblocks serveConn's Decode → untrackConn
	}
	if ln != nil {
		return ln.Close()
	}
	return nil
}

// TCPTransport satisfies the transport-agnostic Transport surface (gossip uses
// Connect/Send/Receive). The dynamic identity + mux surface of ClusterTransport is
// intentionally NOT implemented (see type doc).
var _ Transport = (*TCPTransport)(nil)
