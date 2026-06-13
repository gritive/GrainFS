package transport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	"sync"

	"github.com/cloudwego/hertz/pkg/app"
	hzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// joinPath is the single HTTP route for the Zero-CA invite handshake.
const joinPath = "/_grainfs/join"

// JoinRequestHandler is the consumer surface for the HTTP join listener: given
// the TLS-captured peer SPKI, the RFC5705 channel binding, and the request
// body, it returns the reply body. It replaces the stream-based JoinHandler —
// HTTP framing carries the single request/reply, so there is no manual
// length-prefix framing, half-close, or leader drain.
type JoinRequestHandler func(ctx context.Context, peerSPKI [32]byte, bind []byte, req []byte) ([]byte, error)

// HTTPJoinListener is the HTTP/TLS twin of the (deleted) TCP join listener: a
// dedicated JoinALPN Hertz server on the join port for the Zero-CA invite
// handshake. Like the TCP twin it DELIBERATELY accepts any client cert (a new
// joiner's self-signed SPKI is in nobody's accept-set) and captures the peer
// SPKI + RFC5705 channel binding from the accepted connection's TLS state. The
// join's security rests on (a) app-layer invite-transcript validation, (b) the
// channel binding tying the transcript to this exact TLS session, and (c) the
// dialer's client-side SPKI pin — NOT on server-side cert verification — so
// permissive accept is correct here, exactly as on the TCP twin and on QUIC
// before it.
type HTTPJoinListener struct {
	srv     *hzserver.Hertz
	handler JoinRequestHandler
	spki    [32]byte
	addr    string
	mu      sync.Mutex
	closed  bool
}

// NewHTTPJoinListener starts a TLS-over-HTTP join listener on addr using cert as
// the stable server identity. The bind happens synchronously (net.Listen before
// return), so callers may advertise Addr() immediately. Mirrors the prior
// TCPJoinListener contract (Addr/SPKI/Close).
func NewHTTPJoinListener(addr string, cert tls.Certificate, handler JoinRequestHandler) (*HTTPJoinListener, error) {
	if handler == nil {
		return nil, errors.New("http join listener: nil handler")
	}
	spki, err := certSPKI(cert)
	if err != nil {
		return nil, fmt.Errorf("http join listener: server cert SPKI: %w", err)
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
	// Synchronous bind (G5): net.Listen now, advertise the resolved addr. We
	// pass the PLAIN listener + hzserver.WithTLS(tlsConf) (NOT a pre-wrapped
	// tls.NewListener) so Hertz's standard transport wraps each conn as a
	// *standard.TLSConn — that is the ONLY shape whose rc.GetConn() satisfies
	// network.ConnTLSer, which the handler needs for the per-request peer-cert
	// SPKI + RFC5705 channel binding. Pre-wrapping with tls.NewListener leaves
	// t.tls nil, so Hertz wraps a plain *standard.Conn and the handler loses
	// ConnectionState. The main transport gets away with tls.NewListener only
	// because its handlers never read per-request TLS state. The join cert is
	// stable (no rotation), so a static WithTLS config is correct here.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("http join listener: listen %s: %w", addr, err)
	}

	l := &HTTPJoinListener{handler: handler, spki: spki, addr: ln.Addr().String()}
	srv := hzserver.New(
		hzserver.WithListener(ln),
		hzserver.WithTLS(tlsConf),
		hzserver.WithTransport(standard.NewTransporter),
		hzserver.WithHostPorts(""),
		// Cap the request body at the same 1 MiB the TCP framing enforced: join
		// accept is permissive to arbitrary client certs, so an unauthenticated
		// peer must not be able to make us buffer an unbounded body (G2).
		hzserver.WithMaxRequestBodySize(joinMaxFrame),
	)
	srv.POST(joinPath, l.handle)
	l.srv = srv
	go func() { _ = srv.Run() }()
	return l, nil
}

func (l *HTTPJoinListener) handle(c context.Context, rc *app.RequestContext) {
	conn, ok := rc.GetConn().(network.ConnTLSer)
	if !ok {
		rc.SetStatusCode(consts.StatusInternalServerError)
		return
	}
	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		rc.SetStatusCode(consts.StatusUnauthorized)
		return
	}
	peerSPKI := sha256.Sum256(state.PeerCertificates[0].RawSubjectPublicKeyInfo)
	bind, err := ExportJoinBinding(state)
	if err != nil {
		rc.SetStatusCode(consts.StatusInternalServerError)
		return
	}
	reply, herr := l.handler(c, peerSPKI, bind, rc.Request.Body())
	if herr != nil {
		rc.SetStatusCode(consts.StatusInternalServerError)
		rc.SetBodyString(herr.Error())
		return
	}
	rc.SetStatusCode(consts.StatusOK)
	rc.Response.SetBody(reply)
}

// Addr returns the address the join listener is bound to.
func (l *HTTPJoinListener) Addr() string { return l.addr }

// SPKI returns sha256(server cert RawSubjectPublicKeyInfo) so callers can
// publish it as the SeedSPKI joiners must pin.
func (l *HTTPJoinListener) SPKI() [32]byte { return l.spki }

// Close stops the join server. Like HTTPTransport.Close it uses the immediate
// Close (not graceful Shutdown, which can hang on keep-alives).
func (l *HTTPJoinListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if l.srv != nil {
		return l.srv.Close()
	}
	return nil
}

// DialJoinHTTP dials the HTTP join listener, pins the server by SPKI, computes
// the RFC5705 channel binding from the established session, lets buildReq build
// the request body FROM that binding, POSTs it to joinPath, and returns the
// reply body. One-shot over a single fresh conn: dial → handshake → binding →
// buildReq(binding) → single HTTP/1.1 round-trip via stdlib (req.Write +
// http.ReadResponse + a bounded full read of the body). buildReq is injected
// (rather than the request passed in) because the binding must exist before the
// body it is folded into — the joiner's transcript is bound to this session.
// A non-200 reply surfaces as an error carrying the capped response text.
func DialJoinHTTP(ctx context.Context, addr string, expectedServerSPKI [32]byte, clientCert tls.Certificate, buildReq func(bind []byte) ([]byte, error)) ([]byte, error) {
	clientTLS := &tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		NextProtos:         []string{JoinALPN},
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, // we pin the server by SPKI ourselves below
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("http join dial: server presented no cert")
			}
			cert, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return fmt.Errorf("http join dial: parse server cert: %w", err)
			}
			if sha256.Sum256(cert.RawSubjectPublicKeyInfo) != expectedServerSPKI {
				return errors.New("http join dial: server SPKI does not match expected leader identity")
			}
			return nil
		},
	}
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("http join dial %s: %w", addr, err)
	}
	conn := tls.Client(raw, clientTLS)
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("http join dial %s: handshake: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}

	bind, err := ExportJoinBinding(conn.ConnectionState())
	if err != nil {
		return nil, fmt.Errorf("http join dial %s: channel binding: %w", addr, err)
	}
	body, err := buildReq(bind)
	if err != nil {
		return nil, fmt.Errorf("http join dial %s: build request: %w", addr, err)
	}

	httpReq, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, "https://"+addr+joinPath, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("http join dial %s: build http request: %w", addr, err)
	}
	httpReq.ContentLength = int64(len(body))
	if err := httpReq.Write(conn); err != nil {
		return nil, fmt.Errorf("http join dial %s: write request: %w", addr, err)
	}
	resp, err := nethttp.ReadResponse(bufio.NewReader(conn), httpReq)
	if err != nil {
		return nil, fmt.Errorf("http join dial %s: read response: %w", addr, err)
	}
	defer func() { _ = resp.Body.Close() }()
	// Bounded FULL read of the body (G4): http.ReadResponse only parses
	// status/headers; the reply is not received until the body is drained.
	reply, err := io.ReadAll(io.LimitReader(resp.Body, joinMaxFrame+1))
	if err != nil {
		return nil, fmt.Errorf("http join dial %s: read reply: %w", addr, err)
	}
	if len(reply) > joinMaxFrame {
		return nil, fmt.Errorf("http join dial %s: reply exceeds max %d", addr, joinMaxFrame)
	}
	if resp.StatusCode != nethttp.StatusOK {
		return nil, fmt.Errorf("http join dial %s: status %d: %s", addr, resp.StatusCode, reply)
	}
	return reply, nil
}
