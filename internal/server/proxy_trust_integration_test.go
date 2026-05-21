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

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/test/mock"
	"github.com/cloudwego/hertz/pkg/network"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
func newContextWithRemote(hostPort, forwarded, xfProto, xfFor string) *app.RequestContext {
	c := app.NewContext(0)

	addr, err := net.ResolveTCPAddr("tcp", hostPort)
	Expect(err).NotTo(HaveOccurred())
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

var _ = Describe("Proxy trust integration", func() {
	It("ignores spoofed headers from untrusted remotes", func() {
		s := stubServer([]string{"10.0.0.0/8"})
		c := newContextWithRemote("203.0.113.7:54321",
			`for=192.0.2.99;proto=https`,
			"https", "192.0.2.99")

		Expect(s.authoritativeClientIP(c)).To(Equal("203.0.113.7"))
	})

	It("accepts trusted Forwarded headers", func() {
		s := stubServer([]string{"10.0.0.0/8"})
		c := newContextWithRemote("10.0.0.5:7777",
			`for=198.51.100.1;proto=https`, "", "")

		Expect(s.authoritativeClientIP(c)).To(Equal("198.51.100.1"))
	})

	It("falls back to remote address for wrong Forwarded proto", func() {
		s := stubServer([]string{"10.0.0.0/8"})
		c := newContextWithRemote("10.0.0.5:7777",
			`for=198.51.100.2;proto=http`, "", "")

		Expect(s.authoritativeClientIP(c)).To(Equal("10.0.0.5"))
	})

	It("accepts trusted X-Forwarded-For headers", func() {
		s := stubServer([]string{"10.0.0.0/8"})
		c := newContextWithRemote("10.0.0.5:7777", "",
			"https", "198.51.100.3, 10.0.0.5")

		Expect(s.authoritativeClientIP(c)).To(Equal("198.51.100.3"))
	})

	It("falls back when every X-Forwarded-For hop is trusted", func() {
		s := stubServer([]string{"10.0.0.0/8"})
		c := newContextWithRemote("10.0.0.5:7777", "",
			"https", "10.0.0.7, 10.0.0.5")

		Expect(s.authoritativeClientIP(c)).To(Equal("10.0.0.5"))
	})

	It("accepts bracketed IPv6 in trusted Forwarded headers", func() {
		s := stubServer([]string{"10.0.0.0/8"})
		c := newContextWithRemote("10.0.0.5:7777",
			`for="[2001:db8::1]";proto=https`, "", "")

		Expect(s.authoritativeClientIP(c)).To(Equal("2001:db8::1"))
	})

	It("strips the remote port before CIDR matching", func() {
		s := stubServer([]string{"10.0.0.0/8"})
		c := newContextWithRemote("10.0.0.5:54321",
			`for=198.51.100.1;proto=https`, "", "")

		Expect(s.authoritativeClientIP(c)).To(Equal("198.51.100.1"))
	})

	It("returns the bare remote address when proxy trust is nil", func() {
		s := &Server{}
		c := newContextWithRemote("203.0.113.7:54321",
			`for=evil;proto=https`, "https", "evil")

		Expect(s.authoritativeClientIP(c)).To(Equal("203.0.113.7"))
	})
})
