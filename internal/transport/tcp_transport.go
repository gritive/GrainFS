package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
)

// tcpALPN is the single ALPN advertised by the TCP cluster transport (S1).
// QUIC's dual-ALPN (legacy/mux) split is replaced in S2 (RaftConn restructure);
// S1 speaks one protocol over connection-per-RPC.
const tcpALPN = "grainfs-tcp-v1"

// TCPTransport implements the transport-agnostic cluster RPC surface
// (transport.Transport + Call*/Handle*) over TLS 1.3 on TCP, using a
// connection-per-RPC model: every RPC dials a fresh conn, exchanges one
// request/response (CloseWrite delimits bulk bodies), then closes.
//
// S1 is DORMANT and CORRECTNESS-ONLY: it is not wired into boot (QUIC stays the
// live transport) and the per-RPC TLS handshake is intentionally un-pooled.
// Throughput comes from the S2 (control-plane mux) and S3 (data-plane elastic
// pool) slices; do NOT benchmark S1 as a transport baseline.
//
// Identity is STATIC SPKI pinning (one IdentitySnapshot). The dynamic rotation/
// registry surface (SwapIdentity/ApplyRotation/UpdateRegistryAccept/...) defers
// to the wiring/join slice and is intentionally absent here.
//
// Framing invariant (RST avoidance): CloseWrite (close_notify) is sent ONLY by
// the side that just wrote an EOF-delimited body — the client in CallWithBody
// and the server in the HandleRead branch. Request/response frames are length-
// prefixed and self-delimiting, so no other path CloseWrites; the body reader
// consumes the close_notify, leaving no unread data to force an RST on close.
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

	snap      *IdentitySnapshot
	serverTLS *tls.Config
	clientTLS *tls.Config
	pool      *connPool // per-peer idle data-plane conns (S3a checkout/checkin)
}

// NewTCPTransport derives the cluster identity from psk and builds a transport
// pinned to it. Mirrors NewQUICTransport's empty-PSK contract (D6=B).
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
	t := &TCPTransport{
		inbox:  make(chan *ReceivedMessage, 256),
		codec:  &BinaryCodec{},
		router: NewStreamRouter(),
		ctx:    ctx,
		cancel: cancel,
		snap:   snap,
		pool:   newConnPool(),
	}
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
	// VerifyPeerCertificate (same pattern as quic.go buildClientTLSConfig:1565; repo
	// golangci excludes G402).
	t.clientTLS = &tls.Config{
		MinVersion:            tls.VersionTLS13,
		InsecureSkipVerify:    true,
		Certificates:          []tls.Certificate{snap.PresentCert},
		NextProtos:            []string{tcpALPN},
		VerifyPeerCertificate: pinAcceptedSPKI(snap),
	}
	return t, nil
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
// here, and StreamData shard RPCs reach the shard service only via this catch-all
// — so a faithful TCP drop-in must implement it even though no use-site role
// interface requires it. (Remaining ClusterTransport gap = the mux-connection
// methods, which are the S2 RaftConn restructure's job.)
func (t *TCPTransport) SetStreamHandler(h StreamHandler) {
	t.mu.Lock()
	t.streamHandler = h
	t.mu.Unlock()
}

// Handle registers a per-type request/response handler.
func (t *TCPTransport) Handle(st StreamType, h StreamHandler) { t.router.Handle(st, h) }

// HandleBody registers a per-type handler that receives the request frame plus
// raw body bytes (read to EOF / peer CloseWrite) on the same conn. The handler
// MUST drain the body to io.EOF before returning: under connection-per-RPC TCP,
// leaving unread client data means the server's subsequent Close emits an RST
// that truncates the response mid-flight (framing invariant). This is stronger
// than QUIC's HandleBody contract — S2/S3 body handlers must honor it.
func (t *TCPTransport) HandleBody(st StreamType, h StreamBodyHandler) { t.router.HandleBody(st, h) }

// HandleRead registers a per-type handler that returns a framed metadata
// response followed by a streamed response body.
func (t *TCPTransport) HandleRead(st StreamType, h StreamReadHandler) { t.router.HandleRead(st, h) }

// Connect is a no-op for the connection-per-RPC TCP transport: each Send/Call
// dials a fresh conn, so there is no persistent connection to establish. Kept to
// satisfy transport.Transport (gossip calls Connect before Send); peer-reachability
// errors surface at Send instead. Pooling slices (S2/S3) may give this meaning.
func (t *TCPTransport) Connect(ctx context.Context, addr string) error { return nil }

// Listen binds a TCP listener wrapped in the server TLS config and serves each
// accepted connection as a single RPC (connection-per-RPC).
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
		go t.serveConn(conn)
	}
}

// serveConn runs the TLS handshake (SPKI pinning) then serves requests on conn
// in a persistent loop, so a pooled (reused) client conn carries many transfers.
// The loop ends when the client closes the conn (Decode gets EOF) or a dispatch
// leaves the conn in an unknown state.
func (t *TCPTransport) serveConn(conn net.Conn) {
	defer conn.Close()
	if tc, ok := conn.(*tls.Conn); ok {
		if err := tc.HandshakeContext(t.ctx); err != nil {
			return
		}
	}
	from := conn.RemoteAddr().String()
	for {
		req, err := t.codec.Decode(conn)
		if err != nil {
			return // client closed the conn (clean EOF) or a framing error
		}
		if !t.serveOne(conn, from, req) {
			return // dispatch left the conn in an unknown state → drop it
		}
	}
}

// serveOne handles one request on a (possibly reused) conn. It returns true iff
// the conn is left clean (positioned at the next frame) and may serve another
// request; false means the conn must be dropped.
func (t *TCPTransport) serveOne(conn net.Conn, from string, req *Message) bool {
	bodyHandler, hasBody := t.router.LookupBody(req.Type)
	readHandler, hasRead := t.router.LookupRead(req.Type)
	typeHandler, hasType := t.router.Lookup(req.Type)

	switch {
	case hasBody:
		// Request body is a chunk stream terminated by a zero-length chunk; the
		// handler reads it via a chunkedBodyReader. Drain any remainder to the
		// terminator so the conn is left clean for the next request.
		cbr := &chunkedBodyReader{r: conn}
		resp := bodyHandler(req, cbr)
		if !cbr.done {
			if _, err := io.Copy(io.Discard, cbr); err != nil {
				return false // could not reach terminator → conn dirty
			}
		}
		// The response — OK or StatusError — is a single self-delimiting frame with
		// NO chunk stream after it, so the conn is left clean either way. This is
		// what makes the client treat a StatusError as a reusable cycle. If a future
		// body handler ever streams a partial body and THEN errors, that property
		// (and the pool-reuse-on-error assumption) breaks.
		if resp != nil {
			if err := t.codec.Encode(conn, resp); err != nil {
				return false
			}
		}
		return true
	case hasRead:
		resp, body := readHandler(req)
		if resp != nil {
			if err := t.codec.Encode(conn, resp); err != nil {
				if body != nil {
					_ = body.Close()
				}
				return false
			}
		}
		if body != nil {
			defer body.Close()
			// Response body as a chunk stream + terminator: the client's
			// chunkedBodyReader stops at the terminator and the conn stays reusable.
			if err := writeChunkedBody(conn, body); err != nil {
				return false
			}
		}
		return true
	case hasType:
		resp := typeHandler(req)
		if resp != nil {
			if err := t.codec.Encode(conn, resp); err != nil {
				return false
			}
		}
		return true
	default:
		// No per-type handler: try the catch-all (boot routes StreamData shard
		// RPCs through it via SetStreamHandler), then fall back to the inbox for
		// fire-and-forget (gossip Receive path). Mirrors QUIC handleStream order.
		t.mu.RLock()
		catchAll := t.streamHandler
		t.mu.RUnlock()
		if catchAll != nil {
			if resp := catchAll(req); resp != nil {
				if err := t.codec.Encode(conn, resp); err != nil {
					return false
				}
				return true
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

// Close shuts the transport down: cancels in-flight context, drops pooled
// data-plane conns, and closes the listener.
func (t *TCPTransport) Close() error {
	t.cancel()
	t.pool.closeAll()
	t.mu.Lock()
	ln := t.listener
	t.mu.Unlock()
	if ln != nil {
		return ln.Close()
	}
	return nil
}

// TCPTransport satisfies the transport-agnostic Transport surface (gossip uses
// Connect/Send/Receive). The dynamic identity + mux surface of ClusterTransport
// is intentionally NOT implemented in S1 (see type doc).
var _ Transport = (*TCPTransport)(nil)
