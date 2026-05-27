package transport

// JoinListener is the dedicated Zero-CA join transport: a QUIC listener that
// DELIBERATELY bypasses the two production accept-set SPKI gates so a brand-new
// joiner — whose self-signed SPKI is in NObody's accept-set — can complete a
// QUIC handshake and reach the join handler.
//
// Production gates this listener does NOT run (contrast with quic.go):
//   - buildServerTLSConfig wires VerifyPeerCertificate: pinAcceptedSPKI (the
//     TLS-layer accept-set pin). This listener uses a permissive
//     VerifyPeerCertificate that returns nil — the peer cert is captured, not
//     pinned, so the handshake completes for any client cert.
//   - acceptLoop does an explicit post-accept SPKI check that closes unknown
//     peers with "peer cert rejected" BEFORE ALPN routing (the comment there
//     notes quic-go does NOT reliably enforce ClientAuth server-side). This
//     listener runs its OWN minimal accept loop with NO accept-set rejection,
//     and captures the peer SPKI from conn.ConnectionState().TLS rather than
//     from VerifyPeerCertificate (same reliability reason).
//
// Relay/MITM defense lives on the CLIENT side: DialJoin pins the expected
// server SPKI in its own VerifyPeerCertificate (client-side verification IS
// reliably enforced by quic-go), so a joiner only delivers its request to the
// leader it was told to trust.
//
// Identity is stable: NewJoinListener takes a caller-provided tls.Certificate
// (W9/W10 persist a deterministic leader cert and publish SPKI() as SeedSPKI).
// This listener never generates an ephemeral cert internally.
//
// Wire framing is length-prefixed binary (NO JSON), lifted from the de-risk
// spike.

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/quic-go/quic-go"
)

// JoinALPN is the single, isolated ALPN for the Zero-CA join transport. It must
// not collide with the production psk/mux ALPNs so the join listener can never
// be reached through the normal cluster listener and vice versa.
const JoinALPN = "grainfs-join-v1"

// joinMaxFrame bounds a single length-prefixed field on the join wire.
const joinMaxFrame = 1 << 20

// JoinHandler is invoked once per accepted join connection with the peer's
// captured SPKI (sha256 of the presented leaf cert's RawSubjectPublicKeyInfo)
// and the connection's first accepted stream. The handler owns the stream.
type JoinHandler func(ctx context.Context, peerSPKI [32]byte, stream *quic.Stream)

// JoinListener is a dedicated QUIC listener for the Zero-CA join handshake.
type JoinListener struct {
	listener *quic.Listener
	handler  JoinHandler
	spki     [32]byte
	addr     string
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewJoinListener starts a QUIC listener on addr using cert as the stable server
// identity. The server TLS config is permissive (InsecureSkipVerify + a
// VerifyPeerCertificate that returns nil) and requires any client cert; the peer
// SPKI is captured from the accepted connection's TLS state, not from
// verification. The listener runs its own minimal accept loop in the background.
func NewJoinListener(addr string, cert tls.Certificate, handler JoinHandler) (*JoinListener, error) {
	if handler == nil {
		return nil, errors.New("join listener: nil handler")
	}
	spki, err := certSPKI(cert)
	if err != nil {
		return nil, fmt.Errorf("join listener: server cert SPKI: %w", err)
	}

	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAnyClientCert,
		NextProtos:   []string{JoinALPN},
		MinVersion:   tls.VersionTLS13,
		// quic-go requires InsecureSkipVerify when a custom VerifyPeerCertificate
		// is set. We return nil to let any client cert through; the join handler
		// (W7) re-validates the joiner via the invite transcript at the app layer.
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: func([][]byte, [][]*x509.Certificate) error { return nil },
	}

	ln, err := quic.ListenAddr(addr, tlsConf, defaultQUICConfig())
	if err != nil {
		return nil, fmt.Errorf("join listener: listen %s: %w", addr, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	jl := &JoinListener{
		listener: ln,
		handler:  handler,
		spki:     spki,
		addr:     ln.Addr().String(),
		ctx:      ctx,
		cancel:   cancel,
	}
	go jl.acceptLoop()
	return jl, nil
}

// Addr returns the address the join listener is bound to.
func (l *JoinListener) Addr() string { return l.addr }

// SPKI returns sha256(server cert RawSubjectPublicKeyInfo) so callers (W10) can
// publish it as the SeedSPKI joiners must pin.
func (l *JoinListener) SPKI() [32]byte { return l.spki }

// Close stops the accept loop and closes the underlying listener.
func (l *JoinListener) Close() error {
	l.cancel()
	return l.listener.Close()
}

// acceptLoop is the join listener's OWN minimal accept loop: NO accept-set
// rejection (the production acceptLoop gate is bypassed). It captures the peer
// SPKI from the accepted connection's TLS state and routes the first stream to
// the handler.
func (l *JoinListener) acceptLoop() {
	for {
		conn, err := l.listener.Accept(l.ctx)
		if err != nil {
			return // listener closed
		}
		go l.handleConn(conn)
	}
}

func (l *JoinListener) handleConn(conn *quic.Conn) {
	// Capture the peer SPKI from the ACCEPTED connection (quic-go server-side
	// VerifyPeerCertificate is unreliable — mirror the production acceptLoop's
	// post-accept read of ConnectionState().TLS.PeerCertificates).
	state := conn.ConnectionState()
	if len(state.TLS.PeerCertificates) == 0 {
		_ = conn.CloseWithError(quicAppErrCode, "no peer cert")
		return
	}
	peerSPKI := sha256.Sum256(state.TLS.PeerCertificates[0].RawSubjectPublicKeyInfo)

	stream, err := conn.AcceptStream(l.ctx)
	if err != nil {
		_ = conn.CloseWithError(quicAppErrCode, "accept stream failed")
		return
	}
	l.handler(l.ctx, peerSPKI, stream)
}

// DialJoin dials a JoinListener at addr, presenting clientCert as the joiner's
// node identity, and pins the leader: the client TLS VerifyPeerCertificate
// REJECTS unless sha256(leaf.RawSubjectPublicKeyInfo) == expectedServerSPKI
// (relay/MITM defense — client-side verification is reliably enforced). It
// returns the opened stream plus a func that closes the connection; the caller
// owns both.
func DialJoin(ctx context.Context, addr string, expectedServerSPKI [32]byte, clientCert tls.Certificate) (*quic.Stream, func() error, error) {
	clientTLS := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		NextProtos:   []string{JoinALPN},
		MinVersion:   tls.VersionTLS13,
		// We pin the server by SPKI ourselves, so disable the default chain
		// verification and enforce the pin in VerifyPeerCertificate.
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("join dial: server presented no cert")
			}
			cert, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("join dial: parse server cert: %w", err)
			}
			if sha256.Sum256(cert.RawSubjectPublicKeyInfo) != expectedServerSPKI {
				return errors.New("join dial: server SPKI does not match expected leader identity")
			}
			return nil
		},
	}

	conn, err := quic.DialAddr(ctx, addr, clientTLS, defaultQUICConfig())
	if err != nil {
		return nil, nil, fmt.Errorf("join dial %s: %w", addr, err)
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(quicAppErrCode, "open stream failed")
		return nil, nil, fmt.Errorf("join dial %s: open stream: %w", addr, err)
	}
	closeConn := func() error { return conn.CloseWithError(quicAppErrCode, "join done") }
	return stream, closeConn, nil
}

// certSPKI returns sha256 of the certificate leaf's RawSubjectPublicKeyInfo.
func certSPKI(cert tls.Certificate) ([32]byte, error) {
	var out [32]byte
	leaf := cert.Leaf
	if leaf == nil {
		if len(cert.Certificate) == 0 {
			return out, errors.New("certificate has no leaf DER")
		}
		parsed, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return out, fmt.Errorf("parse leaf: %w", err)
		}
		leaf = parsed
	}
	return sha256.Sum256(leaf.RawSubjectPublicKeyInfo), nil
}

// --- length-prefixed binary wire framing (NO JSON) -------------------------
//
// Every field is a big-endian uint32 length followed by that many bytes. These
// are the production join-wire helpers (lifted from the Zero-CA de-risk spike);
// W7 builds the invite request/reply messages on top of them.

// joinPutField appends a length-prefixed field to buf.
func joinPutField(buf []byte, f []byte) []byte {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(f)))
	buf = append(buf, hdr[:]...)
	return append(buf, f...)
}

// joinReadFields reads exactly n length-prefixed fields from r.
func joinReadFields(r io.Reader, n int) ([][]byte, error) {
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		var hdr [4]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil, err
		}
		sz := binary.BigEndian.Uint32(hdr[:])
		if sz > joinMaxFrame {
			return nil, fmt.Errorf("join field too large: %d", sz)
		}
		body := make([]byte, sz)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
		out[i] = body
	}
	return out, nil
}
