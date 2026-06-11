package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	hzclient "github.com/cloudwego/hertz/pkg/app/client"
	hzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// httpALPN is the ALPN protocol for the Phase 8 HTTP cluster transport, distinct
// from tcpALPN/tcpMuxALPN so an HTTP dial can never negotiate the legacy TCP
// transport and vice-versa.
const httpALPN = "grainfs-http-v1"

// httpPingPath is the scaffold-only liveness route that proves the secure HTTP
// channel end-to-end (S8-1). S8-2 replaces it with the /shard data-plane routes.
const httpPingPath = "/_grainfs/ping"

// HTTPTransport is the dormant Phase 8 node-to-node transport: a Hertz HTTP server
// and Hertz HTTP client over the SAME zero-CA SPKI-pinned mTLS + live identity
// rotation the TCP transport uses (transport_shared.go). S8-1 carries only the
// secure substrate — handshake, SPKI pin, identity rotation, and one liveness
// round-trip; data-plane shard PUT/GET stream over HTTP in S8-2. It is NOT wired
// into boot, so the production default transport is unchanged.
type HTTPTransport struct {
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	localAddr string

	// Live-swappable identity (mirrors TCPTransport S5a): the composer owns
	// accept-set/present-cert mutations and atomically stores a fresh
	// IdentitySnapshot into identity; the server (via GetConfigForClient) and the
	// client dialer rebuild their tls.Config per handshake/dial from
	// identity.Load() so a post-Listen rotation/flip takes effect on NEW
	// connections without a restart.
	identity atomic.Pointer[IdentitySnapshot]
	composer *identityComposer

	srv    *hzserver.Hertz
	client *hzclient.Client
}

// NewHTTPTransport derives the cluster identity from psk and builds a transport
// pinned to it. Mirrors NewTCPTransport's empty-PSK contract.
func NewHTTPTransport(psk string) (*HTTPTransport, error) {
	if psk == "" {
		return nil, ErrEmptyClusterKey
	}
	cert, spki, err := DeriveClusterIdentity(psk)
	if err != nil {
		return nil, fmt.Errorf("derive cluster identity: %w", err)
	}
	snap := NewIdentitySnapshot([][32]byte{spki}, cert, spki)

	ctx, cancel := context.WithCancel(context.Background())
	t := &HTTPTransport{ctx: ctx, cancel: cancel}
	// Seed the live identity (base PSK accepted, present = PSK cert), then hand
	// ownership to the composer whose swap closure atomically restores it — exactly
	// as NewTCPTransport does — so rotation/flip mutations recompute the snapshot.
	t.identity.Store(snap)
	t.composer = newIdentityComposer(spki, func(s *IdentitySnapshot) { t.identity.Store(s) })
	t.composer.setPresent(cert, spki)
	return t, nil
}

// MustNewHTTPTransport panics on error. Test setup only.
func MustNewHTTPTransport(psk string) *HTTPTransport {
	t, err := NewHTTPTransport(psk)
	if err != nil {
		panic(fmt.Sprintf("MustNewHTTPTransport: %v", err))
	}
	return t
}

// --- Identity / rotation surface (delegates to the shared composer; mirrors
// tcp_identity.go so the HTTP transport rotates identically to the TCP one). ---

// SwapIdentity atomically replaces the active identity snapshot.
func (t *HTTPTransport) SwapIdentity(snap *IdentitySnapshot) { t.identity.Store(snap) }

// UpdateRegistryAccept feeds peer-registry per-node SPKIs into the composer as a
// delta; the composer recomputes base ∪ rotation ∪ registry.
func (t *HTTPTransport) UpdateRegistryAccept(spkis [][32]byte) { t.composer.setRegistry(spkis) }

// SeedInitialPeerSPKIs populates the registry accept-set before Listen. Empty
// input is a no-op (rolling-upgrade compat).
func (t *HTTPTransport) SeedInitialPeerSPKIs(spkis [][32]byte) {
	if len(spkis) == 0 {
		return
	}
	t.UpdateRegistryAccept(spkis)
}

// ApplyRotation routes one rotation-phase change (window + present cert, optional
// new base) through the composer as a single atomic recompute.
func (t *HTTPTransport) ApplyRotation(window [][32]byte, present tls.Certificate, presentSPKI [32]byte, newBase *[32]byte) {
	t.composer.applyRotation(window, present, presentSPKI, newBase)
}

// FlipPresent pins this transport's PRESENTED identity to its per-node cert.
func (t *HTTPTransport) FlipPresent(cert tls.Certificate, spki [32]byte) {
	t.composer.setPinPresent(cert, spki)
}

// SetDropped removes ALL cluster-key-derived SPKIs (base + rotation window) from
// the accept-set (post cluster-key-drop).
func (t *HTTPTransport) SetDropped() { t.composer.setDropped() }

// --- TLS config builders (read FRESH from identity.Load(); mirror
// tcp_transport.go buildServerTLS/buildClientTLS). ---

// buildServerTLS returns the inbound-handshake config, read fresh per handshake
// via Listen's GetConfigForClient. Server pins the dialer's cert SPKI.
func (t *HTTPTransport) buildServerTLS() *tls.Config {
	snap := t.identity.Load()
	return &tls.Config{
		MinVersion:            tls.VersionTLS13,
		Certificates:          []tls.Certificate{snap.PresentCert},
		ClientAuth:            tls.RequireAnyClientCert,
		NextProtos:            []string{httpALPN},
		VerifyPeerCertificate: pinAcceptedSPKI(snap),
	}
}

// buildClientTLS returns the dialer config, read fresh per dial. InsecureSkipVerify
// disables default CA/hostname checks; the real check is the SPKI pin in
// VerifyPeerCertificate (same as tcp_transport.go; repo golangci excludes G402).
func (t *HTTPTransport) buildClientTLS() *tls.Config {
	snap := t.identity.Load()
	return &tls.Config{
		MinVersion:            tls.VersionTLS13,
		InsecureSkipVerify:    true,
		Certificates:          []tls.Certificate{snap.PresentCert},
		NextProtos:            []string{httpALPN},
		VerifyPeerCertificate: pinAcceptedSPKI(snap),
	}
}

// Listen binds a TCP listener wrapped in the server TLS config (GetConfigForClient
// reads the live IdentitySnapshot per inbound handshake, mirroring tcp_transport.go)
// and serves it with a Hertz server. WithStreamBody(true) is enabled now so S8-2
// can stream large shard bodies without buffering.
func (t *HTTPTransport) Listen(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	base := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		ClientAuth:         tls.RequireAnyClientCert,
		NextProtos:         []string{httpALPN},
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return t.buildServerTLS(), nil },
	}
	tlsLn := tls.NewListener(ln, base)

	srv := hzserver.New(
		hzserver.WithListener(tlsLn),
		hzserver.WithTransport(standard.NewTransporter),
		hzserver.WithHostPorts(""),
		hzserver.WithStreamBody(true),
	)
	srv.GET(httpPingPath, func(c context.Context, rc *app.RequestContext) {
		rc.SetStatusCode(consts.StatusOK)
	})

	t.mu.Lock()
	t.srv = srv
	t.localAddr = ln.Addr().String()
	t.mu.Unlock()

	go func() { _ = srv.Run() }()
	return nil
}

// LocalAddr returns the bound listen address.
func (t *HTTPTransport) LocalAddr() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.localAddr
}

// httpFreshDialer wraps the Hertz standard dialer and injects a FRESH client
// tls.Config (read from identity.Load()) on every dial, ignoring the static config
// the host client caches. This is the per-dial fresh-read seam — equivalent to
// net/http's DialTLSContext — so a post-Listen rotation/flip reaches new client
// conns. (newTLSConn is unexported, so we wrap rather than reimplement.)
type httpFreshDialer struct {
	inner network.Dialer
	build func() *tls.Config
}

func (d *httpFreshDialer) DialConnection(n, address string, timeout time.Duration, _ *tls.Config) (network.Conn, error) {
	return d.inner.DialConnection(n, address, timeout, d.build())
}

func (d *httpFreshDialer) DialTimeout(n, address string, timeout time.Duration, _ *tls.Config) (net.Conn, error) {
	return d.inner.DialTimeout(n, address, timeout, d.build())
}

func (d *httpFreshDialer) AddTLS(conn network.Conn, _ *tls.Config) (network.Conn, error) {
	return d.inner.AddTLS(conn, d.build())
}

// httpClient lazily builds the Hertz client. The custom dialer supplies a fresh
// SPKI-pinned tls.Config per dial; WithResponseBodyStream(true) is set for S8-2.
func (t *HTTPTransport) httpClient() (*hzclient.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.client != nil {
		return t.client, nil
	}
	c, err := hzclient.NewClient(
		hzclient.WithDialer(&httpFreshDialer{inner: standard.NewDialer(), build: t.buildClientTLS}),
		hzclient.WithResponseBodyStream(true),
	)
	if err != nil {
		return nil, fmt.Errorf("http client: %w", err)
	}
	t.client = c
	return c, nil
}

// Ping does one liveness round-trip over the SPKI-pinned mTLS HTTP channel
// (scaffold-only; S8-2 adds the real Call methods). A handshake failure (SPKI
// mismatch) or non-200 returns an error.
func (t *HTTPTransport) Ping(ctx context.Context, addr string) error {
	c, err := t.httpClient()
	if err != nil {
		return err
	}
	status, _, err := c.Get(ctx, nil, "https://"+addr+httpPingPath)
	if err != nil {
		return fmt.Errorf("http ping %s: %w", addr, err)
	}
	if status != consts.StatusOK {
		return fmt.Errorf("http ping %s: status %d", addr, status)
	}
	return nil
}

// Close shuts the transport down: cancels the context, shuts the Hertz server, and
// closes idle client conns. Idempotent.
func (t *HTTPTransport) Close() error {
	t.cancel()
	t.mu.Lock()
	srv := t.srv
	client := t.client
	t.mu.Unlock()
	if srv != nil {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}
	if client != nil {
		client.CloseIdleConnections()
	}
	return nil
}
