package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	hzclient "github.com/cloudwego/hertz/pkg/app/client"
	"github.com/cloudwego/hertz/pkg/app/client/retry"
	"github.com/cloudwego/hertz/pkg/app/middlewares/server/recovery"
	hzserver "github.com/cloudwego/hertz/pkg/app/server"
	errs "github.com/cloudwego/hertz/pkg/common/errors"
	"github.com/cloudwego/hertz/pkg/network"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/cloudwego/hertz/pkg/protocol"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// httpALPN is the ALPN protocol for the HTTP cluster transport. It was kept
// distinct from the legacy QUIC/TCP-mux ALPNs (now deleted) so a dial could
// never cross-negotiate transports; it is the only cluster-transport ALPN today.
const httpALPN = "grainfs-http-v1"

// httpPingPath is the liveness route that proves the secure HTTP channel
// end-to-end (S8-1). It remains a cheap reachability probe alongside the
// native per-family data-plane routes.
const httpPingPath = "/_grainfs/ping"

// defaultClientBodyTimeout is the idle bound (reset per Read) armed on the client
// response-body read so a stalled mid-body server cannot pin a client goroutine +
// pooled conn forever. Generous for 16MiB+ shard bodies over LAN. See idleReadConn.
const defaultClientBodyTimeout = 5 * time.Minute

// HTTPTransport is the sole node-to-node cluster transport (Phase 8): a Hertz
// HTTP server and Hertz HTTP client over zero-CA SPKI-pinned mTLS + live
// identity rotation (transport_shared.go). Every cluster RPC family rides a
// native per-family route (typed shard/forward/append-segment surfaces plus the
// generic buffered-Call and gossip primitives); there is no generic envelope
// tunnel on the wire.
type HTTPTransport struct {
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	localAddr string

	// Live-swappable identity (mirrors HTTPTransport S5a): the composer owns
	// accept-set/present-cert mutations and atomically stores a fresh
	// IdentitySnapshot into identity; the server (via GetConfigForClient) and the
	// client dialer rebuild their tls.Config per handshake/dial from
	// identity.Load() so a post-Listen rotation/flip takes effect on NEW
	// connections without a restart.
	identity atomic.Pointer[IdentitySnapshot]
	composer *identityComposer

	// traffic is the nil-safe inbound admission limiter, applied per native
	// route by the family's internal StreamType class.
	traffic *TrafficLimiter

	// Native route surfaces (Phase 8 N6+). Consumer-registered per-family
	// handlers; atomic so registration may follow Listen (boot ordering).
	shardWriteHandler atomic.Pointer[ShardWriteHandler]
	nativeShardWrites atomic.Uint64
	shardReadHandler  atomic.Pointer[ShardReadHandler]
	nativeShardReads  atomic.Uint64

	forwardWriteHandler atomic.Pointer[ForwardWriteHandler]
	forwardReadHandler  atomic.Pointer[ForwardReadHandler]
	nativeForwardWrites atomic.Uint64
	nativeForwardReads  atomic.Uint64

	appendSegReadHandler atomic.Pointer[AppendSegmentReadHandler]
	nativeAppendSegReads atomic.Uint64

	// Generic native primitives (Phase 8 N7-3): buffered-Call routes and
	// gossip routes, keyed by path. The maps are built at construction and
	// immutable afterwards; per-route handler/counter fields are atomic.
	bufferedByPath map[string]*bufferedRouteState
	gossipByPath   map[string]*gossipRouteState

	srv    *hzserver.Hertz
	client *hzclient.Client

	// clientBodyTimeout is the reset-per-Read IDLE bound armed on the client
	// response-body read (mirrors tcp_config.go ClientBodyTimeout). The Hertz
	// client sets NO read deadline of its own here (calcTimeout returns 0 with
	// neither WithClientReadTimeout nor a per-request RequestTimeout set), so
	// without this a streaming-read body read on a stalled peer would pin the
	// client goroutine + pooled conn forever. Set before the first call (the
	// client builds lazily); 0 disables the bound. See idleReadConn.
	clientBodyTimeout time.Duration
}

// NewHTTPTransport derives the cluster identity from psk and builds a transport
// pinned to it. Mirrors NewHTTPTransport's empty-PSK contract.
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
	t := &HTTPTransport{
		ctx:            ctx,
		cancel:         cancel,
		bufferedByPath: newBufferedRouteStates(),
		gossipByPath:   newGossipRouteStates(),
	}
	// Seed the live identity (base PSK accepted, present = PSK cert), then hand
	// ownership to the composer whose swap closure atomically restores it — exactly
	// as NewHTTPTransport does — so rotation/flip mutations recompute the snapshot.
	t.identity.Store(snap)
	t.composer = newIdentityComposer(spki, func(s *IdentitySnapshot) { t.identity.Store(s) })
	t.composer.setPresent(cert, spki)
	t.clientBodyTimeout = defaultClientBodyTimeout
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
	// Panic containment: hzserver.New (unlike server.Default) ships NO recovery
	// middleware, so a panic in any route handler — e.g. a lazy FlatBuffers
	// accessor indexing a corrupt payload from an authenticated peer — would
	// kill the whole process. Recovery converts it to a 500 on that request;
	// the native-route clients already map non-200 to a Go error, so consumers
	// degrade to a per-RPC failure instead of a node crash.
	srv.Use(recovery.Recovery())
	srv.GET(httpPingPath, func(c context.Context, rc *app.RequestContext) {
		rc.SetStatusCode(consts.StatusOK)
	})
	// Inbound admission (TrafficLimiter) runs as per-route middleware ahead of
	// each handler — see admissionMiddleware. Each route's StreamType is its
	// admission class.
	srv.POST(httpShardWritePath, t.admissionMiddleware(StreamShardWriteBody), t.handleShardWrite)
	srv.GET(httpShardReadPath, t.admissionMiddleware(StreamShardReadBody), t.handleShardRead)
	srv.POST(httpForwardWritePath, t.admissionMiddleware(StreamGroupForwardBody), t.handleForwardWrite)
	srv.GET(httpForwardReadPath, t.admissionMiddleware(StreamGroupForwardRead), t.handleForwardRead)
	srv.GET(httpAppendSegmentReadPath, t.admissionMiddleware(StreamReadAppendSegment), t.handleAppendSegmentRead)
	// Generic native primitives (N7-3): EVERY declared buffered/gossip route is
	// live from Listen; a family whose handler has not registered answers 503.
	for path, rs := range t.bufferedByPath {
		srv.POST(path, t.handleBufferedRoute(rs))
	}
	for path, rs := range t.gossipByPath {
		srv.POST(path, t.handleGossipRoute(rs))
	}

	t.mu.Lock()
	t.srv = srv
	t.localAddr = ln.Addr().String()
	t.mu.Unlock()

	go func() { _ = srv.Run() }()
	return nil
}

// admissionMiddleware applies inbound traffic admission for one StreamType
// class ahead of the route handler, replacing the per-handler
// limiter.Acquire/503/defer-release boilerplate the STREAMING routes (shard
// read/write, forward, append-segment) used to repeat. Those routes already
// acquired before reading their large request body, so middleware (acquire
// before the handler) is equivalent. release fires after ctx.Next returns — the
// same point the handlers' defer release() did (handler-return; for streaming
// responses that is before-flush, unchanged). t.traffic is nil-safe.
//
// The buffered/gossip routes do NOT use this — they acquire in-handler AFTER
// reading their bounded payload (acquireAdmission), preserving the property
// that a slow-body peer cannot hold a slot during the read.
func (t *HTTPTransport) admissionMiddleware(st StreamType) app.HandlerFunc {
	return func(c context.Context, ctx *app.RequestContext) {
		release, aerr := t.acquireAdmission(c, st)
		if aerr != nil {
			ctx.AbortWithMsg("overloaded: "+aerr.Error(), consts.StatusServiceUnavailable)
			return
		}
		defer release()
		ctx.Next(c)
	}
}

// acquireAdmission acquires an inbound traffic slot for st, returning the
// release func (always callable; t.traffic is nil-safe). Single admission entry
// point shared by admissionMiddleware (streaming routes) and the
// buffered/gossip handlers (which acquire after reading their bounded payload).
func (t *HTTPTransport) acquireAdmission(c context.Context, st StreamType) (func(), error) {
	t.mu.RLock()
	limiter := t.traffic
	t.mu.RUnlock()
	return limiter.Acquire(c, st)
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
	idle  time.Duration // per-Read idle bound armed on the dialed conn (0 = none)
}

func (d *httpFreshDialer) DialConnection(n, address string, timeout time.Duration, _ *tls.Config) (network.Conn, error) {
	conn, err := d.inner.DialConnection(n, address, timeout, d.build())
	if err != nil {
		return nil, err
	}
	return wrapIdleReadConn(conn, d.idle), nil
}

func (d *httpFreshDialer) DialTimeout(n, address string, timeout time.Duration, _ *tls.Config) (net.Conn, error) {
	return d.inner.DialTimeout(n, address, timeout, d.build())
}

func (d *httpFreshDialer) AddTLS(conn network.Conn, _ *tls.Config) (network.Conn, error) {
	c, err := d.inner.AddTLS(conn, d.build())
	if err != nil {
		return nil, err
	}
	return wrapIdleReadConn(c, d.idle), nil
}

// wrapIdleReadConn wraps conn in an idleReadConn when idle > 0, else returns it
// unchanged (no behavior change when the bound is disabled).
func wrapIdleReadConn(conn network.Conn, idle time.Duration) network.Conn {
	if idle <= 0 {
		return conn
	}
	return &idleReadConn{Conn: conn, idle: idle}
}

// idleReadConn arms a reset-per-Read IDLE read deadline before every blocking
// network read on the Hertz client's dialed connection — the HTTP analogue of the
// TCP transport's tcpReadCloser (S3b-cbd). Post-flip every production shard read is
// a native streaming route (ShardRead/ForwardRead/AppendSegmentRead) whose response body streams through this conn, and the Hertz
// client sets NO read deadline of its own (calcTimeout returns 0 when neither
// WithClientReadTimeout nor a per-request RequestTimeout is set — both unset here),
// so a peer that stalls mid-body would otherwise pin this client goroutine + the
// pooled conn forever. SetReadTimeout(idle) before each read makes a stall surface
// as a timeout IN THE SAME GOROUTINE — not the cross-goroutine CloseBodyStream
// watchdog that was the S8-2 BLOCKER (Hertz forbids Close concurrent with Read). A
// read that makes progress returns and the next read re-arms, so a slow-but-
// progressing transfer is never aborted (idle, not total). Because the client never
// sets a shorter deadline, the arm clobbers nothing — it only replaces "unbounded"
// with the idle bound.
//
// All four network-blocking read entry points are overridden: Go embedding has no
// virtual dispatch, so standard.Conn.ReadByte/ReadBinary call the EMBEDDED conn's
// Peek (not ours) — overriding only Peek would miss reads entering via those. One
// arm per entry point is sufficient (it bounds the blocking syscall the read drives;
// stream.go reads fixed-length bodies via Read and chunked via Peek).
//
// ConnTLSer and ErrorNormalization are delegated explicitly because embedding the
// network.Conn INTERFACE hides the optional interfaces the Hertz client asserts on
// the conn (client.go:564 Handshake, :625/:696 ToHertzError for ErrConnectionClosed
// detection on pooled-conn reuse). These two are the complete client-side optional
// set in Hertz v0.10.4 (StatefulConn is server-only); a Hertz bump must re-check.
type idleReadConn struct {
	network.Conn
	idle time.Duration
}

func (c *idleReadConn) arm() { _ = c.Conn.SetReadTimeout(c.idle) }

func (c *idleReadConn) Read(b []byte) (int, error)       { c.arm(); return c.Conn.Read(b) }
func (c *idleReadConn) Peek(n int) ([]byte, error)       { c.arm(); return c.Conn.Peek(n) }
func (c *idleReadConn) ReadByte() (byte, error)          { c.arm(); return c.Conn.ReadByte() }
func (c *idleReadConn) ReadBinary(n int) ([]byte, error) { c.arm(); return c.Conn.ReadBinary(n) }

func (c *idleReadConn) Handshake() error {
	if tc, ok := c.Conn.(network.ConnTLSer); ok {
		return tc.Handshake()
	}
	return nil
}

func (c *idleReadConn) ConnectionState() tls.ConnectionState {
	if tc, ok := c.Conn.(network.ConnTLSer); ok {
		return tc.ConnectionState()
	}
	return tls.ConnectionState{}
}

func (c *idleReadConn) ToHertzError(err error) error {
	if e, ok := c.Conn.(network.ErrorNormalization); ok {
		return e.ToHertzError(err)
	}
	return err
}

// httpClient lazily builds the Hertz client. The custom dialer supplies a fresh
// SPKI-pinned tls.Config per dial; WithResponseBodyStream(true) keeps large
// shard/forward bodies streaming.
//
// Retry policy (httpRetryIf): the client retries ONCE on ErrBadPoolConn. HTTP keep-alive
// pools connections, and a peer routinely reaps an idle pooled conn ("Apache and nginx
// usually do this", per Hertz); the TCP→HTTP default flip makes this production-relevant
// (TCP's connection-per-RPC Call never pooled, so it never hit a reaped conn). ErrBadPoolConn
// is raised when the pooled conn was found closed BEFORE delivery, so the retry is a first
// delivery on a fresh conn — provably replay-safe for every buffered RPC family (raft RPCs,
// reads, AND the non-idempotent proposal forwards CallBuffered carries). Retry refuses
// IsBodyStream requests, so a streamed-body request (ShardWrite/ForwardWrite) one-shot pipe
// body is never re-sent (the S3b "retry-after-body" landmine); buffered payloads use SetBody
// (rewindable). The retry delay is 0 (no DelayPolicy) so both attempts fit the caller's RPC
// ctx. This retry does NOT fix the pre-existing node-id==raft-addr join deadlock (TODOS.md §6)
// — that is a transport-independent boot-ordering bug, unrelated to keep-alive. httpRetryIf's
// body-stream refusal is pinned by TestHTTPDataPlane_RetryIf_RefusesBodyStream.
func (t *HTTPTransport) httpClient() (*hzclient.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.client != nil {
		return t.client, nil
	}
	c, err := hzclient.NewClient(
		hzclient.WithDialer(&httpFreshDialer{inner: standard.NewDialer(), build: t.buildClientTLS, idle: t.clientBodyTimeout}),
		hzclient.WithResponseBodyStream(true),
		hzclient.WithRetryConfig(retry.WithMaxAttemptTimes(2)), // 1 retry; delay 0 (no DelayPolicy)
	)
	if err != nil {
		return nil, fmt.Errorf("http client: %w", err)
	}
	c.SetRetryIfFunc(httpRetryIf)
	t.client = c
	return c, nil
}

// httpRetryIf decides whether to retry a failed request. It retries idempotent
// raft/control RPCs when a POOLED keep-alive connection was closed by the peer
// before the request was delivered, but NEVER retries a streamed request body
// (CallWithBody) — re-sending an exhausted one-shot stream is the S3b
// "retry-after-body" landmine.
func httpRetryIf(req *protocol.Request, _ *protocol.Response, err error) bool {
	if req.IsBodyStream() {
		return false
	}
	if err == nil {
		return false
	}
	// ONLY ErrBadPoolConn: Hertz raises it when a pooled keep-alive conn was found closed
	// BEFORE the request was delivered, so the retry is a first delivery on a fresh conn —
	// provably replay-safe for ALL Call-path RPC types, including the non-idempotent proposal
	// forwards CallPooled carries (ShardService.SendRequest forwards every PUT's index/group
	// proposal). We deliberately do NOT match Hertz's errConnectionClosed ("server closed
	// connection before returning the first response byte"): that fires AFTER the request was
	// written, so the server may have processed it before closing, and replaying a proposal
	// forward there could double-propose. That ambiguous case stays a transient RPC error
	// (raft retries next tick; the S3 client retries the PUT/GET) — the safe behavior.
	return errors.Is(err, errs.ErrBadPoolConn)
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

// Close shuts the transport down: cancels the context, closes the Hertz server
// listener, and closes idle client conns. Idempotent.
//
// Close is IMMEDIATE (TCP-parity), not a graceful drain. The TCP transport closes
// its listener and conns at once, and the cluster tolerates abrupt peer loss, so
// Close must not block node shutdown. A graceful srv.Shutdown would wait up to
// ExitWaitTimeout for idle keep-alive conns held open by REMOTE clients to drain
// (the standard transport waits for active==0 and never force-closes them), so it
// just adds shutdown latency without benefit. srv.Close() closes the listener now;
// client.CloseIdleConnections() drops the conns THIS node holds to peers, which
// unblocks their servers' read loops in turn.
func (t *HTTPTransport) Close() error {
	t.cancel()
	t.mu.Lock()
	srv := t.srv
	client := t.client
	t.mu.Unlock()
	if srv != nil {
		_ = srv.Close()
	}
	if client != nil {
		client.CloseIdleConnections()
	}
	return nil
}
