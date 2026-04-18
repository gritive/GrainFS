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
	"fmt"
	"math/big"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
)

// StreamHandler processes an incoming request message and returns a response.
type StreamHandler func(req *Message) *Message

// StreamRouter routes incoming messages to different handlers based on StreamType.
type StreamRouter struct {
	handlers map[StreamType]StreamHandler
}

// NewStreamRouter creates a router that dispatches by StreamType.
func NewStreamRouter() *StreamRouter {
	return &StreamRouter{handlers: make(map[StreamType]StreamHandler)}
}

// Handle registers a handler for a specific stream type.
func (r *StreamRouter) Handle(st StreamType, h StreamHandler) {
	r.handlers[st] = h
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

// QUICTransport implements Transport using QUIC for node-to-node communication.
type QUICTransport struct {
	mu            sync.RWMutex
	listener      *quic.Listener
	conns         map[string]*quic.Conn // addr -> connection
	inbox         chan *ReceivedMessage
	codec         *BinaryCodec
	tlsConfig     *tls.Config
	localAddr     string
	ctx           context.Context
	cancel        context.CancelFunc
	router        *StreamRouter // per-type bidirectional handlers (takes priority)
	streamHandler StreamHandler // catch-all bidirectional handler (backward compat)
	psk           string        // pre-shared key for peer authentication (empty = no auth)
}

// NewQUICTransport creates a new QUIC-based transport.
// If psk is non-empty, connections are authenticated using the shared key.
func NewQUICTransport(psk ...string) *QUICTransport {
	ctx, cancel := context.WithCancel(context.Background())
	t := &QUICTransport{
		conns:  make(map[string]*quic.Conn),
		inbox:  make(chan *ReceivedMessage, 256),
		codec:  &BinaryCodec{},
		router: NewStreamRouter(),
		ctx:    ctx,
		cancel: cancel,
	}
	if len(psk) > 0 {
		t.psk = psk[0]
	}
	return t
}

// pskALPN returns the ALPN protocol string, incorporating PSK hash for authentication.
func (t *QUICTransport) pskALPN() string {
	if t.psk == "" {
		return "grainfs"
	}
	h := sha256.Sum256([]byte(t.psk))
	return "grainfs-" + hex.EncodeToString(h[:8])
}

// Listen starts accepting incoming QUIC connections.
func (t *QUICTransport) Listen(ctx context.Context, addr string) error {
	tlsConf, err := generateTLSConfig()
	if err != nil {
		return fmt.Errorf("generate TLS config: %w", err)
	}
	tlsConf.NextProtos = []string{t.pskALPN()}
	t.tlsConfig = tlsConf

	listener, err := quic.ListenAddr(addr, tlsConf, &quic.Config{})
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
		go t.handleConnection(conn)
	}
}

func (t *QUICTransport) handleConnection(conn *quic.Conn) {
	remoteAddr := conn.RemoteAddr().String()

	t.mu.Lock()
	t.conns[remoteAddr] = conn
	t.mu.Unlock()

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
	catchAll := t.streamHandler
	t.mu.RUnlock()

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

	conn, err := quic.DialAddr(ctx, addr, tlsConf, &quic.Config{})
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}

	t.mu.Lock()
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

// Call sends a request message and waits for a response (bidirectional stream).
// Unlike Send, this opens a stream, writes the request, reads the response, and returns it.
func (t *QUICTransport) Call(ctx context.Context, addr string, req *Message) (*Message, error) {
	conn, err := t.getOrConnect(ctx, addr)
	if err != nil {
		return nil, err
	}

	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("open stream to %s: %w", addr, err)
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
