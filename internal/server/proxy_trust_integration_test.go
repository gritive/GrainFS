// §5 T45 review: integration test for (*Server).authoritativeClientIP.
//
// proxy_trust_test.go covers the algorithm with hand-crafted strings. THIS
// file covers the wiring: it constructs a real Hertz *app.RequestContext,
// sets headers via c.Request.Header.Set (byte→string conversion happens on
// the Hertz side) and a remote address via a faked network.Conn, then drives
// (*Server).authoritativeClientIP end-to-end.
//
// The scaffold is intentionally minimal: a stub *Server with only the
// proxyTrust field populated, since that is the only field
// authoritativeClientIP consults.
package server

import (
	"net"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/test/mock"
	"github.com/cloudwego/hertz/pkg/network"
)

// remoteAddrConn wraps mock.Conn and overrides RemoteAddr so tests can plug
// in arbitrary peer addresses. Embedding gives us the full network.Conn
// interface (SetReadTimeout, Reader, Writer, etc.) for free.
type remoteAddrConn struct {
	*mock.Conn
	addr net.Addr
}

func (r *remoteAddrConn) RemoteAddr() net.Addr { return r.addr }

// newContextWithRemote builds an *app.RequestContext whose RemoteAddr resolves
// to the given host:port and whose request headers carry the three forwarding
// headers under test. host must be a host:port string (matches what a real
// TCP listener would produce).
func newContextWithRemote(t *testing.T, hostPort, forwarded, xfProto, xfFor string) *app.RequestContext {
	t.Helper()
	c := app.NewContext(0)

	addr, err := net.ResolveTCPAddr("tcp", hostPort)
	if err != nil {
		t.Fatalf("ResolveTCPAddr(%q): %v", hostPort, err)
	}
	conn := &remoteAddrConn{Conn: mock.NewConn(""), addr: addr}
	// Assert the embed satisfies network.Conn so a Hertz upgrade can't break
	// this test silently.
	var _ network.Conn = conn
	c.SetConn(conn)

	if forwarded != "" {
		c.Request.Header.Set("Forwarded", forwarded)
	}
	if xfProto != "" {
		c.Request.Header.Set("X-Forwarded-Proto", xfProto)
	}
	if xfFor != "" {
		c.Request.Header.Set("X-Forwarded-For", xfFor)
	}
	return c
}

// stubServer returns a *Server with only proxyTrust set. authoritativeClientIP
// is the only method under test and it consults only s.proxyTrust.
func stubServer(trusted []string) *Server {
	return &Server{proxyTrust: NewProxyTrust(trusted)}
}

func TestAuthoritativeClientIP_UntrustedRemoteIgnoresSpoofyHeaders(t *testing.T) {
	s := stubServer([]string{"10.0.0.0/8"})
	c := newContextWithRemote(t, "203.0.113.7:54321",
		`for=192.0.2.99;proto=https`, // spoofy
		"https", "192.0.2.99")
	got := s.authoritativeClientIP(c)
	if got != "203.0.113.7" {
		t.Fatalf("untrusted remote: got %q, want %q (raw remote, port stripped)", got, "203.0.113.7")
	}
}

func TestAuthoritativeClientIP_TrustedForwardedAccepted(t *testing.T) {
	s := stubServer([]string{"10.0.0.0/8"})
	c := newContextWithRemote(t, "10.0.0.5:7777",
		`for=198.51.100.1;proto=https`, "", "")
	got := s.authoritativeClientIP(c)
	if got != "198.51.100.1" {
		t.Fatalf("trusted + Forwarded: got %q, want %q", got, "198.51.100.1")
	}
}

func TestAuthoritativeClientIP_TrustedForwardedWrongProtoFallsBack(t *testing.T) {
	// Contract: Authoritative returns (_, false) when proto != https. The
	// helper then falls back to the raw remote (documented in its godoc:
	// audit/policy still need a non-empty SourceIP; header-driven
	// 400-rejection is a follow-up). Lock that behavior in here so a future
	// change to "return empty on reject" can't silently regress logging.
	s := stubServer([]string{"10.0.0.0/8"})
	c := newContextWithRemote(t, "10.0.0.5:7777",
		`for=198.51.100.2;proto=http`, "", "")
	got := s.authoritativeClientIP(c)
	if got != "10.0.0.5" {
		t.Fatalf("trusted + wrong-proto Forwarded: got %q, want %q (fallback to remote)", got, "10.0.0.5")
	}
}

func TestAuthoritativeClientIP_TrustedXForwardedForAccepted(t *testing.T) {
	s := stubServer([]string{"10.0.0.0/8"})
	c := newContextWithRemote(t, "10.0.0.5:7777", "",
		"https", "198.51.100.3, 10.0.0.5")
	got := s.authoritativeClientIP(c)
	if got != "198.51.100.3" {
		t.Fatalf("trusted + XFF: got %q, want %q", got, "198.51.100.3")
	}
}

func TestAuthoritativeClientIP_TrustedAllTrustedChainFallsBack(t *testing.T) {
	// Same fallback contract as wrong-proto: Authoritative rejects, helper
	// degrades to remote so audit/policy still have a SourceIP.
	s := stubServer([]string{"10.0.0.0/8"})
	c := newContextWithRemote(t, "10.0.0.5:7777", "",
		"https", "10.0.0.7, 10.0.0.5")
	got := s.authoritativeClientIP(c)
	if got != "10.0.0.5" {
		t.Fatalf("trusted + all-trusted XFF: got %q, want %q (fallback to remote)", got, "10.0.0.5")
	}
}

func TestAuthoritativeClientIP_TrustedForwardedIPv6Bracketed(t *testing.T) {
	s := stubServer([]string{"10.0.0.0/8"})
	c := newContextWithRemote(t, "10.0.0.5:7777",
		`for="[2001:db8::1]";proto=https`, "", "")
	got := s.authoritativeClientIP(c)
	if got != "2001:db8::1" {
		t.Fatalf("trusted + Forwarded IPv6: got %q, want %q", got, "2001:db8::1")
	}
}

// Explicitly assert the helper strips host:port from c.RemoteAddr() before
// consulting the CIDR set. If this regresses we'd start ignoring the
// trusted-proxy chain because the host:port string never matches a /8.
func TestAuthoritativeClientIP_StripsRemotePort(t *testing.T) {
	s := stubServer([]string{"10.0.0.0/8"})
	c := newContextWithRemote(t, "10.0.0.5:54321",
		`for=198.51.100.1;proto=https`, "", "")
	got := s.authoritativeClientIP(c)
	if got != "198.51.100.1" {
		t.Fatalf("host:port remote not stripped before CIDR match: got %q, want %q",
			got, "198.51.100.1")
	}
}

// Nil proxyTrust (construction-only test fixtures) must still produce a
// usable SourceIP — the raw remote with port stripped.
func TestAuthoritativeClientIP_NilProxyTrustReturnsBareRemote(t *testing.T) {
	s := &Server{} // no proxyTrust wired
	c := newContextWithRemote(t, "203.0.113.7:54321",
		`for=evil;proto=https`, "https", "evil")
	got := s.authoritativeClientIP(c)
	if got != "203.0.113.7" {
		t.Fatalf("nil proxyTrust: got %q, want %q", got, "203.0.113.7")
	}
}
