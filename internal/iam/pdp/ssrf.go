package pdp

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
)

// errSSRFBlocked marks a dial rejected by the SSRF egress filter. The decorator
// detects it (errors.Is) and HARD-DENIES — never falls through to failure_policy.
var errSSRFBlocked = fmt.Errorf("pdp: ssrf: dial blocked by egress filter")

var (
	cgnat       = netip.MustParsePrefix("100.64.0.0/10") // RFC 6598; IsPrivate does NOT cover it
	benchmarkV4 = netip.MustParsePrefix("198.18.0.0/15") // RFC 2544
	reservedV4  = netip.MustParsePrefix("240.0.0.0/4")   // RFC 1112 (incl. broadcast)
	testNet1    = netip.MustParsePrefix("192.0.2.0/24")
	testNet2    = netip.MustParsePrefix("198.51.100.0/24")
	testNet3    = netip.MustParsePrefix("203.0.113.0/24")
)

// isForbiddenForHTTPS reports whether addr is in a range an https remote PDP must
// never resolve to.
func isForbiddenForHTTPS(addr netip.Addr) bool {
	addr = addr.Unmap()
	if addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() ||
		addr.IsPrivate() || addr.IsUnspecified() || addr.IsMulticast() ||
		addr.IsInterfaceLocalMulticast() {
		return true
	}
	if cgnat.Contains(addr) || benchmarkV4.Contains(addr) || reservedV4.Contains(addr) ||
		testNet1.Contains(addr) || testNet2.Contains(addr) || testNet3.Contains(addr) {
		return true
	}
	return false
}

// validateLiteralAddr / checkDialAddr policy:
//   - http  → addr MUST be loopback (local sidecar only); everything else blocked.
//   - https → addr must NOT be forbidden. allow_private relaxes ONLY loopback +
//     RFC1918/ULA private + CGNAT; link-local (incl. metadata), multicast,
//     unspecified, benchmark/TEST-NET/reserved stay blocked.
func validateLiteralAddr(scheme string, addr netip.Addr, allowPrivate bool) error {
	addr = addr.Unmap()
	if scheme == "http" {
		if !addr.IsLoopback() {
			return fmt.Errorf("iam.pdp: http endpoint must target loopback, got %s", addr)
		}
		return nil
	}
	// https
	if !isForbiddenForHTTPS(addr) {
		return nil
	}
	if allowPrivate {
		if addr.IsLoopback() || addr.IsPrivate() || cgnat.Contains(addr) {
			return nil
		}
		return fmt.Errorf("iam.pdp: https endpoint resolves to a blocked address %s (link-local/metadata/multicast/special-use are not relaxable by allow_private)", addr)
	}
	if addr.IsLoopback() {
		return fmt.Errorf("iam.pdp: https endpoint must not target loopback, got %s", addr)
	}
	return fmt.Errorf("iam.pdp: https endpoint resolves to a private/blocked address %s (set ssrf.allow_private to override)", addr)
}

// checkDialAddr is the net.Dialer.Control hook. ipPort is "host:port" with host
// already a numeric IP (Control runs after DNS). Returns errSSRFBlocked on reject.
func checkDialAddr(scheme, ipPort string, allowPrivate bool) error {
	host, _, err := net.SplitHostPort(ipPort)
	if err != nil {
		host = ipPort
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("%w: unparseable dial addr %q", errSSRFBlocked, ipPort)
	}
	if verr := validateLiteralAddr(scheme, addr, allowPrivate); verr != nil {
		return fmt.Errorf("%w: %v", errSSRFBlocked, verr)
	}
	return nil
}

// isPlausibleDNSName rejects obfuscated IP literals masquerading as hosts. Valid
// DNS name: dot-separated [A-Za-z0-9-] labels, at least one non-numeric label
// (rejects 2130706433 / 1.2.3 / 0177...), no empty label, no trailing/leading dot.
func isPlausibleDNSName(host string) bool {
	if host == "" || strings.HasSuffix(host, ".") || strings.HasPrefix(host, ".") {
		return false
	}
	if host == "localhost" {
		return true
	}
	labels := strings.Split(host, ".")
	sawAlpha := false
	for _, l := range labels {
		if l == "" {
			return false
		}
		for _, r := range l {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
				sawAlpha = true
			case r >= '0' && r <= '9':
			case r == '-':
			default:
				return false
			}
		}
	}
	return sawAlpha
}
