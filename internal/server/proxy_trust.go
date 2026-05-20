// §5 T45: ProxyTrust — RFC 7239 Forwarded / X-Forwarded-* validation.
//
// Determines the authoritative client IP when the server sits behind a trusted
// L7/L4 proxy (nginx, ALB, etc). The algorithm:
//
//  1. If the immediate remote (the TCP peer) is NOT in any trusted CIDR, the
//     forwarding headers are IGNORED and the remote wins. This prevents IP
//     spoofing by direct clients.
//  2. Otherwise the request came from a trusted upstream and must carry proof
//     of TLS termination plus the original client:
//     a. RFC 7239 Forwarded header is preferred. Requires `proto=https` and a
//     `for=` token. If the `for=` value is itself in a trusted CIDR (all-
//     trusted chain) the request is rejected — we have no untrusted leaf.
//     b. Otherwise X-Forwarded-Proto must be "https" and X-Forwarded-For is
//     walked left-to-right; the first untrusted IP is the authoritative
//     client. An all-trusted chain is rejected.
//  3. No valid header path returns (empty, false) → caller rejects.
//
// Limitation: parseForwarded only supports a single `;`-separated key=value
// pair set with simple comma-separated chains. Quoted-string handling is
// minimal (brackets / surrounding quotes stripped). Multi-pair lists with
// embedded quoted commas are not parsed; in those exotic deployments operators
// should instead rely on X-Forwarded-* which is unambiguous.
package server

import (
	"net"
	"strings"
	"sync/atomic"

	"github.com/cloudwego/hertz/pkg/app"
)

// ProxyTrust validates forwarding headers against a trusted-CIDR allowlist.
// The CIDR set is held in an atomic.Pointer so SetCIDRs (driven by the
// trusted-proxy.cidr reload hook) can swap it without locking the hot path.
type ProxyTrust struct {
	cidrs atomic.Pointer[[]*net.IPNet]
}

// NewProxyTrust constructs a ProxyTrust seeded with cidrSpec. Invalid or empty
// entries are silently dropped; an empty trusted set means "trust nobody, the
// remote IP is always authoritative".
func NewProxyTrust(cidrSpec []string) *ProxyTrust {
	p := &ProxyTrust{}
	p.SetCIDRs(cidrSpec)
	return p
}

// SetCIDRs replaces the trusted CIDR set. Safe to call concurrently — the
// reload hook on trusted-proxy.cidr drives this. Bare IPs (no `/mask`) are
// accepted and treated as host CIDRs (/32 or /128).
func (p *ProxyTrust) SetCIDRs(cidrSpec []string) {
	parsed := make([]*net.IPNet, 0, len(cidrSpec))
	for _, s := range cidrSpec {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, n, err := net.ParseCIDR(s); err == nil {
			parsed = append(parsed, n)
			continue
		}
		// Bare IP fallback — treat as a /32 (IPv4) or /128 (IPv6) host net.
		if ip := net.ParseIP(s); ip != nil {
			bits := net.IPv6len * 8
			if v4 := ip.To4(); v4 != nil {
				ip = v4
				bits = net.IPv4len * 8
			}
			parsed = append(parsed, &net.IPNet{
				IP:   ip,
				Mask: net.CIDRMask(bits, bits),
			})
		}
	}
	p.cidrs.Store(&parsed)
}

// inTrusted reports whether ip (as a host string, no port) falls in any
// trusted CIDR. Returns false on parse error or empty trusted set.
func (p *ProxyTrust) inTrusted(ip string) bool {
	ip = stripBracketsAndPort(ip)
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	ptr := p.cidrs.Load()
	if ptr == nil {
		return false
	}
	for _, n := range *ptr {
		if n.Contains(parsed) {
			return true
		}
	}
	return false
}

// Authoritative returns the IP to attribute to this request and whether the
// request should be allowed. See the package doc-comment for the algorithm.
//
// Inputs are stringly typed so callers can plug in Hertz / net/http / test
// fixtures uniformly. remoteIP is the immediate TCP peer (host or host:port).
func (p *ProxyTrust) Authoritative(remoteIP, forwarded, xfProto, xfFor string) (string, bool) {
	remote := stripBracketsAndPort(remoteIP)

	// (1) Untrusted source — headers ignored.
	if !p.inTrusted(remote) {
		return remote, true
	}

	// (2a) Forwarded header preferred.
	if forwarded != "" {
		proto, forIP := parseForwarded(forwarded)
		if proto == "https" && forIP != "" {
			leaf := stripBracketsAndPort(forIP)
			if leaf != "" && !p.inTrusted(leaf) {
				return leaf, true
			}
			// All-trusted chain (the one for= we parsed is itself trusted) →
			// reject. parseForwarded only returns one for=, so this is the
			// MVP form of "leftmost untrusted" with the chain truncated.
			return "", false
		}
		// Forwarded present but missing proto=https or for= → reject.
		return "", false
	}

	// (2b) X-Forwarded-* fallback.
	if strings.EqualFold(strings.TrimSpace(xfProto), "https") && xfFor != "" {
		for _, raw := range strings.Split(xfFor, ",") {
			ip := stripBracketsAndPort(strings.TrimSpace(raw))
			if ip == "" {
				continue
			}
			if !p.inTrusted(ip) {
				return ip, true
			}
		}
		// Walked the whole chain, every hop trusted → reject.
		return "", false
	}

	// (3) Trusted source but no valid header path — reject.
	return "", false
}

// parseForwarded extracts proto and the first `for=` value from an RFC 7239
// Forwarded header. Case-insensitive parameter names per RFC 7239 §4.
//
// MVP: handles `proto=https;for=192.0.2.1` and `for="[2001:db8::1]:1234"`.
// Does NOT handle multi-element lists (`Forwarded: for=a, for=b`) or
// quoted-strings containing commas/semicolons. Operators with that complexity
// should use X-Forwarded-* instead.
func parseForwarded(h string) (proto, forIP string) {
	// Take the first element of a comma-separated list (the closest proxy).
	if i := strings.IndexByte(h, ','); i >= 0 {
		h = h[:i]
	}
	for _, kv := range strings.Split(h, ";") {
		kv = strings.TrimSpace(kv)
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(kv[:eq]))
		val := strings.TrimSpace(kv[eq+1:])
		val = strings.Trim(val, "\"")
		switch key {
		case "proto":
			proto = strings.ToLower(val)
		case "for":
			forIP = val
		}
	}
	return proto, forIP
}

// authoritativeClientIP returns the IP we attribute to a Hertz request. It
// consults the wired ProxyTrust; when ProxyTrust is absent (test fixtures) or
// rejects the request (e.g. trusted-source with bad headers), it falls back to
// the raw peer host. The returned value is the SourceIP that feeds audit logs
// and policy.RequestContext.SourceIP — only the data-plane gateway path uses
// this. UDS / localhost / peer-cred gates intentionally keep using
// c.RemoteAddr() because they want the actual TCP peer, not a proxy-asserted
// client identity.
func (s *Server) authoritativeClientIP(c *app.RequestContext) string {
	remote := ""
	if addr := c.RemoteAddr(); addr != nil {
		remote = addr.String()
	}
	if s == nil || s.proxyTrust == nil {
		return stripBracketsAndPort(remote)
	}
	ip, ok := s.proxyTrust.Authoritative(
		remote,
		string(c.GetHeader("Forwarded")),
		string(c.GetHeader("X-Forwarded-Proto")),
		string(c.GetHeader("X-Forwarded-For")),
	)
	if !ok {
		// Reject path: ProxyTrust said the request should be refused (e.g.
		// trusted-source missing valid headers). Audit/policy still need a
		// non-empty SourceIP; fall back to the raw peer so logs are coherent.
		// Header-driven request rejection (returning a 400 / 403) is a
		// follow-up; the spec only mandates the IP computation here.
		return stripBracketsAndPort(remote)
	}
	return ip
}

// stripBracketsAndPort accepts a host, host:port, [ipv6], or [ipv6]:port and
// returns the bare host. Returns the input unchanged on parse failure so the
// caller can still attempt net.ParseIP.
func stripBracketsAndPort(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Bracketed IPv6 with optional port.
	if s[0] == '[' {
		if end := strings.IndexByte(s, ']'); end > 0 {
			return s[1:end]
		}
	}
	// host:port — but only split if exactly one colon (IPv4) OR if the
	// rightmost colon is preceded by a digit/letter and there are no other
	// colons (excludes bare IPv6).
	if strings.Count(s, ":") == 1 {
		if host, _, err := net.SplitHostPort(s); err == nil {
			return host
		}
	}
	return s
}
