package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// QUIC connection timeouts.
const (
	quicMaxIdleTimeout  = 10 * time.Second
	quicKeepAlivePeriod = 3 * time.Second
	quicMaxRPCStreams   = 4096
	// quicAppErrCode is the application-level error code reported on
	// graceful CloseWithError. Picked to be non-zero so peers can distinguish
	// "we rejected your ALPN" from idle-timeout closes.
	quicAppErrCode = 0x1
)

// StreamHandler processes an incoming request message and returns a response.
type StreamHandler func(req *Message) *Message

// StreamBodyHandler processes a framed request followed by raw body bytes on
// the same QUIC stream. The handler must read body before returning a response.
type StreamBodyHandler func(req *Message, body io.Reader) *Message

// StreamRouter routes incoming messages to different handlers based on StreamType.
type StreamRouter struct {
	handlers     map[StreamType]StreamHandler
	bodyHandlers map[StreamType]StreamBodyHandler
}

// NewStreamRouter creates a router that dispatches by StreamType.
func NewStreamRouter() *StreamRouter {
	return &StreamRouter{
		handlers:     make(map[StreamType]StreamHandler),
		bodyHandlers: make(map[StreamType]StreamBodyHandler),
	}
}

// Handle registers a handler for a specific stream type.
func (r *StreamRouter) Handle(st StreamType, h StreamHandler) {
	r.handlers[st] = h
}

// HandleBody registers a handler that receives the framed request payload plus
// any remaining bytes on the same stream as a streaming body.
func (r *StreamRouter) HandleBody(st StreamType, h StreamBodyHandler) {
	r.bodyHandlers[st] = h
}

// Dispatch finds the handler for the message's stream type and calls it.
func (r *StreamRouter) Dispatch(req *Message) *Message {
	h, ok := r.handlers[req.Type]
	if !ok {
		return nil
	}
	return h(req)
}

// Lookup returns the handler for the given stream type, if registered.
func (r *StreamRouter) Lookup(st StreamType) (StreamHandler, bool) {
	h, ok := r.handlers[st]
	return h, ok
}

// LookupBody returns the streaming body handler for the stream type, if any.
func (r *StreamRouter) LookupBody(st StreamType) (StreamBodyHandler, bool) {
	h, ok := r.bodyHandlers[st]
	return h, ok
}

// MuxConnHandler is invoked once per accepted mux QUIC connection (ALPN
// "grainfs-mux-v1-..."). The handler owns the conn lifetime: it must arrange
// for stream accept/read and conn close. The handler is registered by
// internal/raft (the only consumer of mux connections); the transport package
// does not interpret mux frames.
type MuxConnHandler func(conn *quic.Conn)

// QUICTransport implements Transport using QUIC for node-to-node communication.
type QUICTransport struct {
	mu            sync.RWMutex
	listener      *quic.Listener
	conns         map[string]*quic.Conn // legacy ALPN: addr -> conn (Send/Call/CallFlatBuffer)
	muxConns      map[string]*quic.Conn // mux ALPN: addr -> conn (raft RPC, owned by internal/raft)
	inbox         chan *ReceivedMessage
	codec         *BinaryCodec
	tlsConfig     *tls.Config
	localAddr     string
	ctx           context.Context
	cancel        context.CancelFunc
	router        *StreamRouter // per-type bidirectional handlers (takes priority)
	streamHandler StreamHandler // catch-all bidirectional handler (backward compat)
	psk           string        // pre-shared key for peer authentication (empty = no auth)
	muxHandler    MuxConnHandler
}

// NewQUICTransport creates a new QUIC-based transport.
// If psk is non-empty, connections are authenticated using the shared key.
func NewQUICTransport(psk ...string) *QUICTransport {
	ctx, cancel := context.WithCancel(context.Background())
	t := &QUICTransport{
		conns:    make(map[string]*quic.Conn),
		muxConns: make(map[string]*quic.Conn),
		inbox:    make(chan *ReceivedMessage, 256),
		codec:    &BinaryCodec{},
		router:   NewStreamRouter(),
		ctx:      ctx,
		cancel:   cancel,
	}
	if len(psk) > 0 {
		t.psk = psk[0]
	}
	return t
}

// pskALPN returns the legacy ALPN protocol string, incorporating PSK hash for authentication.
// Used by Send / Call / CallFlatBuffer (per-message stream open/close).
func (t *QUICTransport) pskALPN() string {
	if t.psk == "" {
		return "grainfs"
	}
	h := sha256.Sum256([]byte(t.psk))
	return "grainfs-" + hex.EncodeToString(h[:8])
}

// muxALPN returns the multiplexed-stream ALPN protocol string used by raft RPC
// connections. Same PSK hash as pskALPN, plus a "-mux-v1" version tag so peers
// running older binaries fall back cleanly to the legacy ALPN.
func (t *QUICTransport) muxALPN() string {
	if t.psk == "" {
		return "grainfs-mux-v1"
	}
	h := sha256.Sum256([]byte(t.psk))
	return "grainfs-mux-v1-" + hex.EncodeToString(h[:8])
}

func defaultQUICConfig() *quic.Config {
	return &quic.Config{
		KeepAlivePeriod:    quicKeepAlivePeriod,
		MaxIdleTimeout:     quicMaxIdleTimeout,
		MaxIncomingStreams: quicMaxRPCStreams,
	}
}

// SetMuxConnHandler registers the callback that owns accepted mux QUIC
// connections. internal/raft sets this during process startup if --quic-mux is
// enabled. If unset, mux connections are rejected at accept time.
func (t *QUICTransport) SetMuxConnHandler(h MuxConnHandler) {
	t.mu.Lock()
	t.muxHandler = h
	t.mu.Unlock()
}

// Listen starts accepting incoming QUIC connections.
//
// The listener advertises both the legacy ALPN ("grainfs-<pskhash>") and the
// mux ALPN ("grainfs-mux-v1-<pskhash>"). Inbound connections are routed by
// negotiated ALPN: legacy → handleInboundConnection (existing path);
// mux → muxHandler (set by internal/raft if --quic-mux is enabled).
func (t *QUICTransport) Listen(ctx context.Context, addr string) error {
	tlsConf, err := generateTLSConfig()
	if err != nil {
		return fmt.Errorf("generate TLS config: %w", err)
	}
	// Mux ALPN listed first so peers preferring mux complete handshake on it.
	tlsConf.NextProtos = []string{t.muxALPN(), t.pskALPN()}
	t.tlsConfig = tlsConf

	listener, err := quic.ListenAddr(addr, tlsConf, defaultQUICConfig())
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	t.listener = listener
	t.localAddr = listener.Addr().String()

	go t.acceptLoop()
	return nil
}

// LocalAddr returns the address the transport is listening on.
func (t *QUICTransport) LocalAddr() string {
	return t.localAddr
}

func (t *QUICTransport) acceptLoop() {
	for {
		conn, err := t.listener.Accept(t.ctx)
		if err != nil {
			return // listener closed
		}
		// Route by negotiated ALPN. Mux connections go to the mux handler
		// (registered by internal/raft); legacy connections go to the
		// per-message stream handler. Unknown ALPN is rejected.
		state := conn.ConnectionState()
		alpn := state.TLS.NegotiatedProtocol
		switch {
		case alpn == t.muxALPN():
			t.mu.RLock()
			h := t.muxHandler
			t.mu.RUnlock()
			if h == nil {
				_ = conn.CloseWithError(quicAppErrCode, "mux ALPN unsupported on this peer")
				continue
			}
			go h(conn)
		case alpn == t.pskALPN():
			go t.handleInboundConnection(conn)
		default:
			_ = conn.CloseWithError(quicAppErrCode, "unknown ALPN: "+alpn)
		}
	}
}

// handleInboundConnection serves an accepted (inbound) QUIC connection.
// Inbound connections are NOT stored in the conns map — they use an ephemeral
// remote port, so they cannot be addressed by service addr via Call/evict.
// Cleanup is automatic: context cancellation or listener.Close unblocks AcceptStream.
func (t *QUICTransport) handleInboundConnection(conn *quic.Conn) {
	from := conn.RemoteAddr().String()
	for {
		stream, err := conn.AcceptStream(t.ctx)
		if err != nil {
			return
		}
		go t.handleStream(from, stream)
	}
}

// handleConnection serves an outbound (dialed) QUIC connection.
// It is always called after the connection has been stored in conns by Connect(),
// and removes the entry on exit so the next Call triggers a fresh dial.
func (t *QUICTransport) handleConnection(conn *quic.Conn) {
	remoteAddr := conn.RemoteAddr().String()

	defer func() {
		t.mu.Lock()
		if t.conns[remoteAddr] == conn {
			delete(t.conns, remoteAddr)
		}
		t.mu.Unlock()
	}()

	for {
		stream, err := conn.AcceptStream(t.ctx)
		if err != nil {
			return
		}
		go t.handleStream(remoteAddr, stream)
	}
}

func (t *QUICTransport) handleStream(from string, stream *quic.Stream) {
	defer stream.Close()

	msg, err := t.codec.Decode(stream)
	if err != nil {
		return
	}

	// Per-type handler takes priority over catch-all.
	t.mu.RLock()
	typeHandler, hasTypeHandler := t.router.Lookup(msg.Type)
	bodyHandler, hasBodyHandler := t.router.LookupBody(msg.Type)
	catchAll := t.streamHandler
	t.mu.RUnlock()

	if hasBodyHandler {
		resp := bodyHandler(msg, stream)
		if resp != nil {
			_ = t.codec.Encode(stream, resp)
		}
		return
	}
	if hasTypeHandler {
		resp := typeHandler(msg)
		if resp != nil {
			_ = t.codec.Encode(stream, resp)
		}
		return
	}
	if catchAll != nil {
		resp := catchAll(msg)
		if resp != nil {
			_ = t.codec.Encode(stream, resp)
		}
		return
	}

	// No handler: fire-and-forget mode (put in inbox).
	select {
	case t.inbox <- &ReceivedMessage{From: from, Message: msg}:
	case <-t.ctx.Done():
	}
}

// GetOrConnectMux dials (or returns the cached) mux QUIC connection for addr.
// Used by internal/raft to wrap the conn in a RaftConn for raft RPC traffic.
// The returned *quic.Conn is owned by the transport; callers must not close it.
// Eviction is automatic on idle timeout / connection failure.
func (t *QUICTransport) GetOrConnectMux(ctx context.Context, addr string) (*quic.Conn, error) {
	t.mu.RLock()
	conn, ok := t.muxConns[addr]
	t.mu.RUnlock()
	if ok {
		return conn, nil
	}

	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		// Advertise mux first; if peer is older it falls back to legacy
		// ALPN — caller (internal/raft) handles that case by evicting and
		// using the legacy path.
		NextProtos: []string{t.muxALPN(), t.pskALPN()},
	}
	dialed, err := quic.DialAddr(ctx, addr, tlsConf, defaultQUICConfig())
	if err != nil {
		return nil, fmt.Errorf("dial mux %s: %w", addr, err)
	}
	state := dialed.ConnectionState()
	if state.TLS.NegotiatedProtocol != t.muxALPN() {
		// Peer does not support mux; close and report so caller can fall back.
		negotiated := state.TLS.NegotiatedProtocol
		_ = dialed.CloseWithError(quicAppErrCode, "peer does not speak mux ALPN")
		return nil, fmt.Errorf("peer at %s negotiated %q (expected %q)", addr, negotiated, t.muxALPN())
	}

	t.mu.Lock()
	if existing, exists := t.muxConns[addr]; exists {
		t.mu.Unlock()
		_ = dialed.CloseWithError(quicAppErrCode, "duplicate mux connection")
		return existing, nil
	}
	t.muxConns[addr] = dialed
	t.mu.Unlock()
	return dialed, nil
}

// EvictMux removes the mux connection for addr from the cache. internal/raft
// calls this when its RaftConn becomes broken so the next dial creates a fresh
// connection.
func (t *QUICTransport) EvictMux(addr string, conn *quic.Conn) {
	t.mu.Lock()
	if t.muxConns[addr] == conn {
		delete(t.muxConns, addr)
	}
	t.mu.Unlock()
}

// MuxALPN returns the mux ALPN string for tests / diagnostics.
func (t *QUICTransport) MuxALPN() string { return t.muxALPN() }

// LegacyALPN returns the legacy ALPN string for tests / diagnostics.
func (t *QUICTransport) LegacyALPN() string { return t.pskALPN() }

// Connect opens a QUIC connection to a remote peer.
func (t *QUICTransport) Connect(ctx context.Context, addr string) error {
	t.mu.RLock()
	_, exists := t.conns[addr]
	t.mu.RUnlock()
	if exists {
		return nil
	}

	tlsConf := &tls.Config{
		InsecureSkipVerify: true, // self-signed certs; PSK via ALPN provides authentication
		NextProtos:         []string{t.pskALPN()},
	}

	conn, err := quic.DialAddr(ctx, addr, tlsConf, defaultQUICConfig())
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}

	t.mu.Lock()
	if _, exists = t.conns[addr]; exists {
		// Another goroutine dialled the same peer concurrently; close the duplicate.
		conn.CloseWithError(0, "duplicate connection")
		t.mu.Unlock()
		return nil
	}
	t.conns[addr] = conn
	t.mu.Unlock()

	go t.handleConnection(conn)
	return nil
}

// Send sends a message to a peer. Opens a new stream per message.
func (t *QUICTransport) Send(ctx context.Context, addr string, msg *Message) error {
	t.mu.RLock()
	conn, ok := t.conns[addr]
	t.mu.RUnlock()
	if !ok {
		return fmt.Errorf("not connected to %s", addr)
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("open stream to %s: %w", addr, err)
	}

	if err := t.codec.Encode(stream, msg); err != nil {
		stream.Close()
		return fmt.Errorf("encode message: %w", err)
	}
	stream.Close()
	return nil
}

// Receive returns the channel of incoming messages.
func (t *QUICTransport) Receive() <-chan *ReceivedMessage {
	return t.inbox
}

// SetStreamHandler registers a catch-all handler for bidirectional request-response streams.
// Per-type handlers registered via Handle take priority over this handler.
func (t *QUICTransport) SetStreamHandler(h StreamHandler) {
	t.mu.Lock()
	t.streamHandler = h
	t.mu.Unlock()
}

// Handle registers a per-type handler for a specific StreamType.
// Messages of this type are dispatched here before the catch-all SetStreamHandler.
// Messages handled here never reach the inbox channel.
func (t *QUICTransport) Handle(st StreamType, h StreamHandler) {
	t.mu.Lock()
	t.router.Handle(st, h)
	t.mu.Unlock()
}

// HandleBody registers a per-type handler for a request frame followed by raw
// body bytes on the same bidirectional stream.
func (t *QUICTransport) HandleBody(st StreamType, h StreamBodyHandler) {
	t.mu.Lock()
	t.router.HandleBody(st, h)
	t.mu.Unlock()
}

// Call sends a request message and waits for a response (bidirectional stream).
// Unlike Send, this opens a stream, writes the request, reads the response, and returns it.
// If the cached connection is stale (idle-timed-out), it is evicted and re-dialled once.
func (t *QUICTransport) Call(ctx context.Context, addr string, req *Message) (*Message, error) {
	conn, err := t.getOrConnect(ctx, addr)
	if err != nil {
		return nil, err
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		if !shouldEvictOpenStreamError(ctx, err) {
			return nil, err
		}
		// Evict the stale connection and reconnect once before giving up.
		t.evict(addr, conn)
		conn, err = t.getOrConnect(ctx, addr)
		if err != nil {
			return nil, fmt.Errorf("reconnect to %s: %w", addr, err)
		}
		if stream, err = conn.OpenStreamSync(ctx); err != nil {
			return nil, fmt.Errorf("open stream to %s: %w", addr, err)
		}
	}
	defer stream.Close()

	// Write request (length-prefixed, so server knows the boundary)
	if err := t.codec.Encode(stream, req); err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}

	// Read response
	resp, err := t.codec.Decode(stream)
	if err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}

	return resp, nil
}

// CallWithBody sends a framed request, streams body bytes behind it, half-closes
// the write side, then waits for a framed response from the peer.
func (t *QUICTransport) CallWithBody(ctx context.Context, addr string, req *Message, body io.Reader) (*Message, error) {
	conn, err := t.getOrConnect(ctx, addr)
	if err != nil {
		return nil, err
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		if !shouldEvictOpenStreamError(ctx, err) {
			return nil, err
		}
		t.evict(addr, conn)
		conn, err = t.getOrConnect(ctx, addr)
		if err != nil {
			return nil, fmt.Errorf("reconnect to %s: %w", addr, err)
		}
		if stream, err = conn.OpenStreamSync(ctx); err != nil {
			return nil, fmt.Errorf("open stream to %s: %w", addr, err)
		}
	}
	defer stream.Close()
	cancelDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			stream.CancelRead(quic.StreamErrorCode(quicAppErrCode))
			stream.CancelWrite(quic.StreamErrorCode(quicAppErrCode))
		case <-cancelDone:
		}
	}()
	defer close(cancelDone)

	if err := t.codec.Encode(stream, req); err != nil {
		return nil, fmt.Errorf("encode request: %w", err)
	}
	if body != nil {
		if _, err := io.Copy(stream, body); err != nil {
			return nil, fmt.Errorf("stream body to %s: %w", addr, err)
		}
	}
	if err := stream.Close(); err != nil {
		return nil, fmt.Errorf("close request stream to %s: %w", addr, err)
	}

	resp, err := t.codec.Decode(stream)
	if err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}
	return resp, nil
}

// CallFlatBuffer sends a FlatBuffers message and waits for a response.
// Builder must remain alive until this returns; caller is responsible for returning it to pool.
// Unlike Call, this uses EncodeWriterTo to write directly from Builder.FinishedBytes() — no make+copy.
func (t *QUICTransport) CallFlatBuffer(ctx context.Context, addr string, fw *FlatBuffersWriter) (*Message, error) {
	conn, err := t.getOrConnect(ctx, addr)
	if err != nil {
		return nil, err
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		if !shouldEvictOpenStreamError(ctx, err) {
			return nil, err
		}
		t.evict(addr, conn)
		conn, err = t.getOrConnect(ctx, addr)
		if err != nil {
			return nil, fmt.Errorf("reconnect to %s: %w", addr, err)
		}
		if stream, err = conn.OpenStreamSync(ctx); err != nil {
			return nil, fmt.Errorf("open stream to %s: %w", addr, err)
		}
	}
	defer stream.Close()

	if err := t.codec.EncodeWriterTo(stream, fw); err != nil {
		return nil, fmt.Errorf("encode flatbuffer: %w", err)
	}

	resp, err := t.codec.Decode(stream)
	if err != nil {
		return nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}
	return resp, nil
}

func shouldEvictOpenStreamError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
		return false
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// evict removes conn from the active connection map if it is still the current entry for addr.
// It also closes the connection so that any goroutines blocked on OpenStreamSync for this
// connection unblock immediately and can reconnect on their next attempt.
func (t *QUICTransport) evict(addr string, conn *quic.Conn) {
	t.mu.Lock()
	shouldClose := t.conns[addr] == conn
	if shouldClose {
		delete(t.conns, addr)
	}
	t.mu.Unlock()
	if shouldClose {
		conn.CloseWithError(0, "evicted: stale connection")
	}
}

// getOrConnect returns an existing connection or lazily connects to the peer.
func (t *QUICTransport) getOrConnect(ctx context.Context, addr string) (*quic.Conn, error) {
	t.mu.RLock()
	conn, ok := t.conns[addr]
	t.mu.RUnlock()
	if ok {
		return conn, nil
	}

	// Lazy connect
	if err := t.Connect(ctx, addr); err != nil {
		return nil, err
	}

	t.mu.RLock()
	conn = t.conns[addr]
	t.mu.RUnlock()
	return conn, nil
}

// Close shuts down the transport.
func (t *QUICTransport) Close() error {
	t.cancel()

	t.mu.Lock()
	defer t.mu.Unlock()

	for addr, conn := range t.conns {
		conn.CloseWithError(0, "transport closing")
		delete(t.conns, addr)
	}

	if t.listener != nil {
		return t.listener.Close()
	}
	return nil
}

// generateTLSConfig creates a self-signed TLS config for QUIC.
func generateTLSConfig() (*tls.Config, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}

	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
	}, nil
}
