package transport

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// joinDrainTimeout bounds the leader-side post-handler drain so a misbehaving
// joiner that never closes its write side cannot pin the handler goroutine.
const joinDrainTimeout = 5 * time.Second

// tcpJoinStream adapts a *tls.Conn to the join JoinHandler's io.ReadWriteCloser
// with the HALF-CLOSE contract (see JoinHandler): Close() closes only the WRITE
// direction via tls.Conn.CloseWrite(), matching *quic.Stream.Close() so the
// shared consumer's `write → stream.Close() → read reply` choreography works. The
// full connection teardown is owned by the listener (server) / the closer
// returned by DialJoinTCP (client), NOT by this Close.
type tcpJoinStream struct{ conn *tls.Conn }

func (s tcpJoinStream) Read(p []byte) (int, error)  { return s.conn.Read(p) }
func (s tcpJoinStream) Write(p []byte) (int, error) { return s.conn.Write(p) }
func (s tcpJoinStream) Close() error                { return s.conn.CloseWrite() }

var _ io.ReadWriteCloser = tcpJoinStream{}

// TCPJoinListener is the TCP/TLS twin of JoinListener: a dedicated JoinALPN
// listener for the Zero-CA invite-join handshake. It DELIBERATELY accepts any
// client cert — a brand-new joiner's self-signed SPKI is in nobody's accept-set —
// and captures the peer SPKI from the accepted connection's TLS state. The join's
// security rests on (a) app-layer invite-transcript validation, (b) the RFC5705
// channel binding tying the transcript to this exact TLS session, and (c) the
// dialer's client-side SPKI pin (relay/MITM defense) — NOT on server-side cert
// verification — so permissive accept is correct here, exactly as on QUIC.
//
// Dormant (S4): not wired into boot until the boot flip (S5); boot keeps the QUIC
// JoinListener. On TCP the join "stream" IS the connection (one request per
// connection), so the handler receives a half-close wrapper over the *tls.Conn.
type TCPJoinListener struct {
	listener net.Listener
	handler  JoinHandler
	spki     [32]byte
	addr     string
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewTCPJoinListener starts a TLS-over-TCP join listener on addr using cert as the
// stable server identity. Mirrors NewJoinListener (QUIC).
func NewTCPJoinListener(addr string, cert tls.Certificate, handler JoinHandler) (*TCPJoinListener, error) {
	if handler == nil {
		return nil, errors.New("tcp join listener: nil handler")
	}
	spki, err := certSPKI(cert)
	if err != nil {
		return nil, fmt.Errorf("tcp join listener: server cert SPKI: %w", err)
	}
	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		// RequireAnyClientCert so ConnectionState().PeerCertificates is populated
		// for the SPKI capture; the join handler re-validates via the invite
		// transcript at the app layer.
		ClientAuth:            tls.RequireAnyClientCert,
		NextProtos:            []string{JoinALPN},
		MinVersion:            tls.VersionTLS13,
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: func([][]byte, [][]*x509.Certificate) error { return nil },
	}
	ln, err := tls.Listen("tcp", addr, tlsConf)
	if err != nil {
		return nil, fmt.Errorf("tcp join listener: listen %s: %w", addr, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &TCPJoinListener{
		listener: ln,
		handler:  handler,
		spki:     spki,
		addr:     ln.Addr().String(),
		ctx:      ctx,
		cancel:   cancel,
	}
	go l.acceptLoop()
	return l, nil
}

// Addr returns the address the join listener is bound to.
func (l *TCPJoinListener) Addr() string { return l.addr }

// SPKI returns sha256(server cert RawSubjectPublicKeyInfo) so callers can publish
// it as the SeedSPKI joiners must pin.
func (l *TCPJoinListener) SPKI() [32]byte { return l.spki }

// Close stops the accept loop and closes the underlying listener.
func (l *TCPJoinListener) Close() error {
	l.cancel()
	return l.listener.Close()
}

func (l *TCPJoinListener) acceptLoop() {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			return // listener closed
		}
		go l.handleConn(conn)
	}
}

func (l *TCPJoinListener) handleConn(conn net.Conn) {
	tc, ok := conn.(*tls.Conn)
	if !ok {
		_ = conn.Close()
		return
	}
	// Complete the handshake so ConnectionState (peer cert + exporter) is ready.
	if err := tc.HandshakeContext(l.ctx); err != nil {
		_ = tc.Close()
		return
	}
	state := tc.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		_ = tc.Close()
		return
	}
	peerSPKI := sha256.Sum256(state.PeerCertificates[0].RawSubjectPublicKeyInfo)
	bind, err := ExportJoinBinding(state)
	if err != nil {
		_ = tc.Close()
		return
	}
	// The conn IS the stream; the handler gets the half-close wrapper and owns the
	// request/reply exchange. The handler's Close() is a half-close (CloseWrite).
	l.handler(l.ctx, peerSPKI, bind, tcpJoinStream{conn: tc})

	// Leader-side drain (RST-robustness): the joiner CloseWrote right after its
	// request, so its TLS close_notify is already buffered here. Draining it to EOF
	// (immediate in the normal flow) leaves no unread bytes, so the full close
	// below sends a clean FIN rather than an RST that could truncate the joiner's
	// in-flight reply read. The deadline only bounds a misbehaving joiner.
	_ = tc.SetReadDeadline(time.Now().Add(joinDrainTimeout))
	_, _ = io.Copy(io.Discard, tc)
	_ = tc.Close() // full teardown, reaps the FD
}

// DialJoinTCP dials a TCPJoinListener, presenting clientCert as the joiner's node
// identity and pinning the leader by SPKI (relay/MITM defense). It returns the
// connection as the io.ReadWriteCloser stream (HALF-CLOSE contract: stream.Close()
// closes only the write side; use the returned closer for full teardown), the
// RFC5705 channel binding for this session, and the full-teardown closer. Mirrors
// DialJoin (QUIC).
func DialJoinTCP(ctx context.Context, addr string, expectedServerSPKI [32]byte, clientCert tls.Certificate) (io.ReadWriteCloser, []byte, func() error, error) {
	clientTLS := &tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		NextProtos:         []string{JoinALPN},
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, // we pin the server by SPKI ourselves below
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("tcp join dial: server presented no cert")
			}
			cert, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("tcp join dial: parse server cert: %w", err)
			}
			if sha256.Sum256(cert.RawSubjectPublicKeyInfo) != expectedServerSPKI {
				return errors.New("tcp join dial: server SPKI does not match expected leader identity")
			}
			return nil
		},
	}
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("tcp join dial %s: %w", addr, err)
	}
	conn := tls.Client(raw, clientTLS)
	// Complete the handshake before deriving the binding (mirror dialMux).
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, nil, nil, fmt.Errorf("tcp join dial %s: handshake: %w", addr, err)
	}
	bind, err := ExportJoinBinding(conn.ConnectionState())
	if err != nil {
		_ = conn.Close()
		return nil, nil, nil, fmt.Errorf("tcp join dial %s: channel binding: %w", addr, err)
	}
	closeConn := func() error { return conn.Close() }
	return tcpJoinStream{conn: conn}, bind, closeConn, nil
}
