package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
)

// QUICTransport implements Transport using QUIC for node-to-node communication.
type QUICTransport struct {
	mu        sync.RWMutex
	listener  *quic.Listener
	conns     map[string]*quic.Conn // addr -> connection
	inbox     chan *ReceivedMessage
	codec     *BinaryCodec
	tlsConfig *tls.Config
	localAddr string
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewQUICTransport creates a new QUIC-based transport.
func NewQUICTransport() *QUICTransport {
	ctx, cancel := context.WithCancel(context.Background())
	return &QUICTransport{
		conns:  make(map[string]*quic.Conn),
		inbox:  make(chan *ReceivedMessage, 256),
		codec:  &BinaryCodec{},
		ctx:    ctx,
		cancel: cancel,
	}
}

// Listen starts accepting incoming QUIC connections.
func (t *QUICTransport) Listen(ctx context.Context, addr string) error {
	tlsConf, err := generateTLSConfig()
	if err != nil {
		return fmt.Errorf("generate TLS config: %w", err)
	}
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

	for {
		msg, err := t.codec.Decode(stream)
		if err != nil {
			return
		}
		select {
		case t.inbox <- &ReceivedMessage{From: from, Message: msg}:
		case <-t.ctx.Done():
			return
		}
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
		InsecureSkipVerify: true,
		NextProtos:         []string{"grainfs"},
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
		NextProtos:   []string{"grainfs"},
	}, nil
}
