package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"golang.org/x/crypto/hkdf"
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

// StreamReadHandler processes a framed request and returns a framed metadata
// response followed by a raw response body on the same QUIC stream.
type StreamReadHandler func(req *Message) (*Message, io.ReadCloser)

type TrafficLimits struct {
	Control int
	Meta    int
	Data    int
	Bulk    int
}

type TrafficLimiter struct {
	control chan struct{}
	meta    chan struct{}
	data    chan struct{}
	bulk    chan struct{}
}

func NewTrafficLimiter(l TrafficLimits) *TrafficLimiter {
	return &TrafficLimiter{
		control: makeLimit(l.Control),
		meta:    makeLimit(l.Meta),
		data:    makeLimit(l.Data),
		bulk:    makeLimit(l.Bulk),
	}
}

func makeLimit(n int) chan struct{} {
	if n <= 0 {
		return nil
	}
	return make(chan struct{}, n)
}

func (l *TrafficLimiter) Acquire(ctx context.Context, st StreamType) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	var ch chan struct{}
	switch ClassOf(st) {
	case StreamClassControl:
		ch = l.control
	case StreamClassMeta:
		ch = l.meta
	case StreamClassData:
		ch = l.data
	case StreamClassBulk:
		ch = l.bulk
	}
	if ch == nil {
		return func() {}, nil
	}
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// StreamRouter routes incoming messages to different handlers based on StreamType.
type StreamRouter struct {
	handlers     map[StreamType]StreamHandler
	bodyHandlers map[StreamType]StreamBodyHandler
	readHandlers map[StreamType]StreamReadHandler
}

// NewStreamRouter creates a router that dispatches by StreamType.
func NewStreamRouter() *StreamRouter {
	return &StreamRouter{
		handlers:     make(map[StreamType]StreamHandler),
		bodyHandlers: make(map[StreamType]StreamBodyHandler),
		readHandlers: make(map[StreamType]StreamReadHandler),
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

// HandleRead registers a handler that writes a framed metadata response and
// then streams any returned body bytes on the same stream.
func (r *StreamRouter) HandleRead(st StreamType, h StreamReadHandler) {
	r.readHandlers[st] = h
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

// LookupRead returns the streaming read handler for the stream type, if any.
func (r *StreamRouter) LookupRead(st StreamType) (StreamReadHandler, bool) {
	h, ok := r.readHandlers[st]
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
	psk           string        // pre-shared key for peer authentication
	muxHandler    MuxConnHandler
	traffic       *TrafficLimiter

	// Cluster identity, computed once at construction. D7=A: stable for the
	// life of the transport so handshakes don't pay HKDF/ECDSA cost per dial.
	identityCert tls.Certificate
	expectedSPKI [32]byte
}

// NewQUICTransport constructs a QUIC transport pinned to the cluster identity
// derived from psk. D6=B: returns error on empty PSK (refuse to start cluster
// mode); short-but-non-empty PSK proceeds (caller logs warning via
// ValidateClusterKey).
func NewQUICTransport(psk string) (*QUICTransport, error) {
	if psk == "" {
		return nil, ErrEmptyClusterKey
	}

	cert, spki, err := deriveClusterIdentity(psk)
	if err != nil {
		return nil, fmt.Errorf("derive cluster identity: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := &QUICTransport{
		conns:        make(map[string]*quic.Conn),
		muxConns:     make(map[string]*quic.Conn),
		inbox:        make(chan *ReceivedMessage, 256),
		codec:        &BinaryCodec{},
		router:       NewStreamRouter(),
		ctx:          ctx,
		cancel:       cancel,
		psk:          psk,
		identityCert: cert,
		expectedSPKI: spki,
	}
	return t, nil
}

// MustNewQUICTransport is NewQUICTransport that panics on error. Intended only
// for test setup where a configuration mistake should fail loud and fast. NOT
// for production code paths — production must surface the error to the operator.
func MustNewQUICTransport(psk string) *QUICTransport {
	t, err := NewQUICTransport(psk)
	if err != nil {
		panic(fmt.Sprintf("MustNewQUICTransport: %v", err))
	}
	return t
}

func (t *QUICTransport) SetTrafficLimits(l TrafficLimits) {
	t.mu.Lock()
	t.traffic = NewTrafficLimiter(l)
	t.mu.Unlock()
}

func (t *QUICTransport) acquireTraffic(ctx context.Context, st StreamType) (func(), error) {
	t.mu.RLock()
	lim := t.traffic
	t.mu.RUnlock()
	if lim == nil {
		return func() {}, nil
	}
	return lim.Acquire(ctx, st)
}

func (t *QUICTransport) acquireInboundTraffic(st StreamType) (func(), error) {
	switch ClassOf(st) {
	case StreamClassData, StreamClassBulk:
		ctx, cancel := context.WithTimeout(t.ctx, 25*time.Millisecond)
		release, err := t.acquireTraffic(ctx, st)
		cancel()
		return release, err
	default:
		return t.acquireTraffic(t.ctx, st)
	}
}

// pskALPN returns the legacy per-message ALPN protocol string. After T2,
// authentication moved to TLS SPKI pinning (see deriveClusterIdentity); the
// ALPN string no longer carries any PSK material. Kept as a method (not a
// constant) so future routing/versioning can be added without API churn.
func (t *QUICTransport) pskALPN() string {
	return "grainfs"
}

// muxALPN returns the multiplexed-stream ALPN used by raft RPC connections.
// Same rationale as pskALPN — static, no PSK material in the protocol string.
func (t *QUICTransport) muxALPN() string {
	return "grainfs-mux-v1"
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
// The listener advertises both the legacy ALPN ("grainfs") and the mux ALPN
// ("grainfs-mux-v1"). Inbound connections are routed by negotiated ALPN:
// legacy → handleInboundConnection (existing path);
// mux → muxHandler (set by internal/raft if --quic-mux is enabled).
//
// Authentication is via SPKI pinning (D5): both client and server present
// the cluster identity cert; both verify the peer's cert SPKI matches the
// expected cluster identity.
func (t *QUICTransport) Listen(ctx context.Context, addr string) error {
	tlsConf := t.buildServerTLSConfig()
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
		// D5: verify the peer's TLS cert SPKI matches the cluster identity.
		// quic-go does NOT reliably enforce tls.Config.ClientAuth on its own,
		// so we re-check here at the application layer. tls.Config still sets
		// ClientAuth to ensure the cert is requested; the actual gate is here.
		state := conn.ConnectionState()
		if err := verifyPeerSPKI(state.TLS.PeerCertificates, t.expectedSPKI); err != nil {
			_ = conn.CloseWithError(quicAppErrCode, "peer cert rejected: "+err.Error())
			continue
		}
		// Route by negotiated ALPN. Mux connections go to the mux handler
		// (registered by internal/raft); legacy connections go to the
		// per-message stream handler. Unknown ALPN is rejected.
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

// verifyPeerSPKI checks that the peer presented a cert whose SPKI hash
// matches `expected`. Used in acceptLoop because quic-go does not honor
// tls.Config.ClientAuth reliably enough for cluster security to depend on.
func verifyPeerSPKI(certs []*x509.Certificate, expected [32]byte) error {
	if len(certs) == 0 {
		return errors.New("no peer cert presented")
	}
	spki := sha256.Sum256(certs[0].RawSubjectPublicKeyInfo)
	if subtle.ConstantTimeCompare(spki[:], expected[:]) != 1 {
		return errors.New("peer cert SPKI does not match cluster identity")
	}
	return nil
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

	release, err := t.acquireInboundTraffic(msg.Type)
	if err != nil {
		_ = t.writeErrorResponse(stream, msg, StatusOverloaded, err)
		return
	}
	defer release()

	// Per-type handler takes priority over catch-all.
	t.mu.RLock()
	typeHandler, hasTypeHandler := t.router.Lookup(msg.Type)
	bodyHandler, hasBodyHandler := t.router.LookupBody(msg.Type)
	readHandler, hasReadHandler := t.router.LookupRead(msg.Type)
	catchAll := t.streamHandler
	t.mu.RUnlock()

	if hasBodyHandler {
		resp := bodyHandler(msg, stream)
		if resp != nil {
			_ = t.codec.Encode(stream, resp)
		}
		return
	}
	if hasReadHandler {
		resp, body := readHandler(msg)
		if resp != nil {
			if err := t.codec.Encode(stream, resp); err != nil {
				if body != nil {
					_ = body.Close()
				}
				return
			}
		}
		if body != nil {
			defer body.Close()
			_, _ = io.Copy(stream, body)
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

func (t *QUICTransport) writeErrorResponse(stream *quic.Stream, req *Message, status MessageStatus, err error) error {
	return t.codec.Encode(stream, NewErrorResponse(req, status, err))
}

func checkResponseStatus(addr string, resp *Message) (*Message, error) {
	if resp == nil {
		return nil, errors.New("transport: nil response")
	}
	if resp.Status == StatusOK {
		return resp, nil
	}
	return nil, fmt.Errorf("transport response from %s status %d: %s", addr, resp.Status, string(resp.Payload))
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

	tlsConf := t.buildClientTLSConfig()
	// Mux ALPN listed first so peers preferring mux complete handshake on it.
	// buildClientTLSConfig already sets NextProtos in this order.
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

	// Legacy Connect path: per-message Send/Call streams. Force the legacy
	// ALPN so the server routes to handleInboundConnection rather than the
	// mux handler. Identity is verified via SPKI pinning, same as the mux
	// dial path.
	tlsConf := t.buildClientTLSConfig()
	tlsConf.NextProtos = []string{t.pskALPN()}

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
	release, err := t.acquireTraffic(ctx, msg.Type)
	if err != nil {
		return err
	}
	defer release()

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

// HandleRead registers a per-type handler for a request frame that returns a
// framed metadata response followed by raw response body bytes.
func (t *QUICTransport) HandleRead(st StreamType, h StreamReadHandler) {
	t.mu.Lock()
	t.router.HandleRead(st, h)
	t.mu.Unlock()
}

// Call sends a request message and waits for a response (bidirectional stream).
// Unlike Send, this opens a stream, writes the request, reads the response, and returns it.
// If the cached connection is stale (idle-timed-out), it is evicted and re-dialled once.
func (t *QUICTransport) Call(ctx context.Context, addr string, req *Message) (*Message, error) {
	release, err := t.acquireTraffic(ctx, req.Type)
	if err != nil {
		return nil, err
	}
	defer release()

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

	return checkResponseStatus(addr, resp)
}

// CallWithBody sends a framed request, streams body bytes behind it, half-closes
// the write side, then waits for a framed response from the peer.
func (t *QUICTransport) CallWithBody(ctx context.Context, addr string, req *Message, body io.Reader) (*Message, error) {
	release, err := t.acquireTraffic(ctx, req.Type)
	if err != nil {
		return nil, err
	}
	defer release()

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
	return checkResponseStatus(addr, resp)
}

type quicReadCloser struct {
	stream  *quic.Stream
	release func()
	once    sync.Once
}

func (r *quicReadCloser) Read(p []byte) (int, error) {
	return r.stream.Read(p)
}

func (r *quicReadCloser) Close() error {
	var err error
	r.once.Do(func() {
		r.stream.CancelRead(quic.StreamErrorCode(quicAppErrCode))
		err = r.stream.Close()
		r.release()
	})
	return err
}

// CallRead sends a framed request, reads a framed metadata response, then
// returns the remaining stream bytes as the response body. The caller must
// close the returned body to release traffic-limiter capacity.
func (t *QUICTransport) CallRead(ctx context.Context, addr string, req *Message) (*Message, io.ReadCloser, error) {
	release, err := t.acquireTraffic(ctx, req.Type)
	if err != nil {
		return nil, nil, err
	}
	released := false
	releaseOnce := func() {
		if !released {
			released = true
			release()
		}
	}
	defer func() {
		if !released && err != nil {
			releaseOnce()
		}
	}()

	conn, err := t.getOrConnect(ctx, addr)
	if err != nil {
		return nil, nil, err
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		if !shouldEvictOpenStreamError(ctx, err) {
			return nil, nil, err
		}
		t.evict(addr, conn)
		conn, err = t.getOrConnect(ctx, addr)
		if err != nil {
			return nil, nil, fmt.Errorf("reconnect to %s: %w", addr, err)
		}
		if stream, err = conn.OpenStreamSync(ctx); err != nil {
			return nil, nil, fmt.Errorf("open stream to %s: %w", addr, err)
		}
	}

	cancelDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			stream.CancelRead(quic.StreamErrorCode(quicAppErrCode))
			stream.CancelWrite(quic.StreamErrorCode(quicAppErrCode))
		case <-cancelDone:
		}
	}()

	closeStreamOnError := true
	defer func() {
		if closeStreamOnError {
			close(cancelDone)
			stream.CancelRead(quic.StreamErrorCode(quicAppErrCode))
			_ = stream.Close()
		}
	}()

	if err = t.codec.Encode(stream, req); err != nil {
		return nil, nil, fmt.Errorf("encode request: %w", err)
	}

	resp, err := t.codec.Decode(stream)
	if err != nil {
		return nil, nil, fmt.Errorf("decode response from %s: %w", addr, err)
	}
	resp, err = checkResponseStatus(addr, resp)
	if err != nil {
		return nil, nil, err
	}

	close(cancelDone)
	closeStreamOnError = false
	rc := &quicReadCloser{
		stream:  stream,
		release: releaseOnce,
	}
	return resp, rc, nil
}

// CallFlatBuffer sends a FlatBuffers message and waits for a response.
// Builder must remain alive until this returns; caller is responsible for returning it to pool.
// Unlike Call, this uses EncodeWriterTo to write directly from Builder.FinishedBytes() — no make+copy.
func (t *QUICTransport) CallFlatBuffer(ctx context.Context, addr string, fw *FlatBuffersWriter) (*Message, error) {
	release, err := t.acquireTraffic(ctx, fw.Typ)
	if err != nil {
		return nil, err
	}
	defer release()

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
	return checkResponseStatus(addr, resp)
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

// deriveClusterIdentity deterministically derives an ECDSA P-256 keypair from
// psk via HKDF, then issues a self-signed certificate. The keypair (and
// therefore the SPKI hash) is stable for a given PSK; the certificate
// signature uses crypto/rand, so cert DER bytes vary per call. This avoids
// feeding HKDF output into ECDSA signature nonce generation, which would
// create a footgun if Go runtime semantics change across versions.
//
// Returns (cert, spki, err). spki is sha256(RawSubjectPublicKeyInfo).
func deriveClusterIdentity(psk string) (tls.Certificate, [32]byte, error) {
	if psk == "" {
		return tls.Certificate{}, [32]byte{}, ErrEmptyClusterKey
	}

	// 1) Derive private scalar from psk via HKDF.
	reader := hkdf.New(sha256.New, []byte(psk), nil, []byte("grainfs-cluster-identity-v1"))
	priv, err := derivePrivKeyFromHKDF(reader, elliptic.P256())
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("derive cluster keypair: %w", err)
	}

	// 2) Self-signed cert with crypto/rand for signature.
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "grainfs-cluster"},
		NotBefore:    time.Unix(0, 0),
		NotAfter:     time.Unix(0, 0).Add(100 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("create cluster cert: %w", err)
	}

	// 3) SPKI hash for pinning.
	parsed, err := x509.ParseCertificate(certDER)
	if err != nil {
		return tls.Certificate{}, [32]byte{}, fmt.Errorf("parse cluster cert: %w", err)
	}
	spki := sha256.Sum256(parsed.RawSubjectPublicKeyInfo)

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  priv,
		Leaf:        parsed,
	}, spki, nil
}

// derivePrivKeyFromHKDF reads scalar bytes from r, reduces modulo curve order N,
// rejects zero. Re-reads on rejection (HKDF Expand can produce up to
// 255*HashLen bytes for SHA-256, far more than enough for a few retries).
func derivePrivKeyFromHKDF(r io.Reader, curve elliptic.Curve) (*ecdsa.PrivateKey, error) {
	params := curve.Params()
	N := params.N
	byteLen := (N.BitLen() + 7) / 8
	buf := make([]byte, byteLen)

	for attempt := 0; attempt < 8; attempt++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("hkdf expand: %w", err)
		}
		k := new(big.Int).SetBytes(buf)
		k.Mod(k, N)
		if k.Sign() == 0 {
			continue
		}
		priv := new(ecdsa.PrivateKey)
		priv.PublicKey.Curve = curve
		priv.D = k
		priv.PublicKey.X, priv.PublicKey.Y = curve.ScalarBaseMult(k.Bytes())
		return priv, nil
	}
	return nil, errors.New("hkdf produced 8 consecutive zero scalars (impossible without a broken PSK)")
}

// pinExpectedSPKI returns a VerifyPeerCertificate callback that accepts only
// peers whose leaf cert SPKI hash matches `expected`. Used identically on both
// sides (D5): client verifies server identity, server verifies client identity.
// Constant-time compare avoids leaking SPKI through timing.
func pinExpectedSPKI(expected [32]byte) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("no peer cert presented")
		}
		cert, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parse peer cert: %w", err)
		}
		spki := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
		if subtle.ConstantTimeCompare(spki[:], expected[:]) != 1 {
			return errors.New("peer cert SPKI does not match cluster identity")
		}
		return nil
	}
}

// buildClientTLSConfig returns the TLS config used when this transport DIALS
// a peer. The dialer presents the cluster identity cert (so the remote
// server's ClientAuth: RequireAnyClientCert is satisfied) AND verifies the
// server's cert SPKI matches the expected cluster identity.
func (t *QUICTransport) buildClientTLSConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify:    true, // quic-go requires; real check is VerifyPeerCertificate
		NextProtos:            []string{t.muxALPN(), t.pskALPN()},
		Certificates:          []tls.Certificate{t.identityCert},
		VerifyPeerCertificate: pinExpectedSPKI(t.expectedSPKI),
	}
}

// buildServerTLSConfig returns the TLS config used when this transport
// LISTENS for incoming peers. ClientAuth: RequireAnyClientCert forces dialers
// to present a cert; VerifyPeerCertificate then pins the cert SPKI to the
// cluster identity. Without this pairing, server-side identity check is dead
// code (D5 regression target).
func (t *QUICTransport) buildServerTLSConfig() *tls.Config {
	return &tls.Config{
		Certificates:          []tls.Certificate{t.identityCert},
		ClientAuth:            tls.RequireAnyClientCert,
		NextProtos:            []string{t.muxALPN(), t.pskALPN()},
		VerifyPeerCertificate: pinExpectedSPKI(t.expectedSPKI),
	}
}
