// §5 T45: ProxyTrust test matrix.
//
// Five spec tests + edge cases. Drives the helper directly with synthesized
// header values — cheaper than threading through the full Hertz stack and the
// audit pipeline. Integration via authoritativeClientIP() is covered by the
// build itself plus the existing audit/policy paths.
package server

import (
	"testing"
)

func TestProxyTrust_ForwardedAccepted(t *testing.T) {
	p := NewProxyTrust([]string{"10.0.0.0/8"})
	got, ok := p.Authoritative(
		"10.0.0.5", // trusted proxy hop
		`for=192.0.2.1;proto=https`,
		"", "",
	)
	if !ok || got != "192.0.2.1" {
		t.Fatalf("Forwarded accepted: got (%q, %v), want (\"192.0.2.1\", true)", got, ok)
	}
}

func TestProxyTrust_XForwardedForAccepted(t *testing.T) {
	p := NewProxyTrust([]string{"10.0.0.0/8"})
	got, ok := p.Authoritative(
		"10.0.0.5",
		"",
		"https",
		"192.0.2.1, 10.0.0.7", // client, then trusted hop
	)
	if !ok || got != "192.0.2.1" {
		t.Fatalf("XFF accepted: got (%q, %v), want (\"192.0.2.1\", true)", got, ok)
	}
}

func TestProxyTrust_AllTrustedChainRejected(t *testing.T) {
	p := NewProxyTrust([]string{"10.0.0.0/8"})

	// Forwarded path: the single for= is itself trusted.
	if got, ok := p.Authoritative("10.0.0.5", `for=10.0.0.9;proto=https`, "", ""); ok {
		t.Fatalf("Forwarded all-trusted: got (%q, %v), want rejected", got, ok)
	}
	// XFF path: every hop trusted.
	if got, ok := p.Authoritative("10.0.0.5", "", "https", "10.0.0.7, 10.0.0.9"); ok {
		t.Fatalf("XFF all-trusted: got (%q, %v), want rejected", got, ok)
	}
}

func TestProxyTrust_MissingProtoRejected(t *testing.T) {
	p := NewProxyTrust([]string{"10.0.0.0/8"})

	// Forwarded with proto=http → reject (no TLS upstream proof).
	if got, ok := p.Authoritative("10.0.0.5", `for=192.0.2.1;proto=http`, "", ""); ok {
		t.Fatalf("Forwarded proto=http: got (%q, %v), want rejected", got, ok)
	}
	// Forwarded with no proto at all → reject.
	if got, ok := p.Authoritative("10.0.0.5", `for=192.0.2.1`, "", ""); ok {
		t.Fatalf("Forwarded no proto: got (%q, %v), want rejected", got, ok)
	}
	// X-Forwarded-For with no X-Forwarded-Proto → reject.
	if got, ok := p.Authoritative("10.0.0.5", "", "", "192.0.2.1"); ok {
		t.Fatalf("XFF no proto: got (%q, %v), want rejected", got, ok)
	}
	// X-Forwarded-Proto: http → reject.
	if got, ok := p.Authoritative("10.0.0.5", "", "http", "192.0.2.1"); ok {
		t.Fatalf("XFF proto=http: got (%q, %v), want rejected", got, ok)
	}
}

func TestProxyTrust_UntrustedSourceIgnored(t *testing.T) {
	p := NewProxyTrust([]string{"10.0.0.0/8"})

	// Remote is NOT in trusted CIDR — headers are ignored, remote wins.
	// Even with spoofy headers claiming a different IP, we MUST NOT honor them.
	got, ok := p.Authoritative(
		"203.0.113.7", // direct client, not behind trusted proxy
		`for=192.0.2.1;proto=https`,
		"https",
		"192.0.2.1",
	)
	if !ok || got != "203.0.113.7" {
		t.Fatalf("untrusted source: got (%q, %v), want (\"203.0.113.7\", true)", got, ok)
	}
}

// Edge: empty trusted set means trust nobody. Every remote is "untrusted" so
// headers are always ignored and the remote IP wins.
func TestProxyTrust_EmptyTrustedSet(t *testing.T) {
	p := NewProxyTrust(nil)
	got, ok := p.Authoritative("10.0.0.5", `for=192.0.2.1;proto=https`, "https", "192.0.2.1")
	if !ok || got != "10.0.0.5" {
		t.Fatalf("empty trusted set: got (%q, %v), want (\"10.0.0.5\", true)", got, ok)
	}

	// SetCIDRs with [""] should also clear (defensive against strings.Split("")).
	p.SetCIDRs([]string{""})
	got, ok = p.Authoritative("10.0.0.5", `for=192.0.2.1;proto=https`, "", "")
	if !ok || got != "10.0.0.5" {
		t.Fatalf("[\"\"] CIDR: got (%q, %v), want (\"10.0.0.5\", true)", got, ok)
	}
}

// Edge: remote IP carries :port; should still match the trusted CIDR.
func TestProxyTrust_RemoteWithPort(t *testing.T) {
	p := NewProxyTrust([]string{"10.0.0.0/8"})
	got, ok := p.Authoritative("10.0.0.5:54321", `for=192.0.2.1;proto=https`, "", "")
	if !ok || got != "192.0.2.1" {
		t.Fatalf("remote with port: got (%q, %v), want (\"192.0.2.1\", true)", got, ok)
	}
}

// Edge: IPv6 `for=` value bracketed with port.
func TestProxyTrust_ForwardedIPv6(t *testing.T) {
	p := NewProxyTrust([]string{"10.0.0.0/8"})
	got, ok := p.Authoritative("10.0.0.5", `for="[2001:db8::1]:1234";proto=https`, "", "")
	if !ok || got != "2001:db8::1" {
		t.Fatalf("Forwarded IPv6: got (%q, %v), want (\"2001:db8::1\", true)", got, ok)
	}
}

// Edge: bare IP in trusted spec (no /mask) is accepted as host net.
func TestProxyTrust_SetCIDRs_BareIP(t *testing.T) {
	p := NewProxyTrust([]string{"10.0.0.5"})
	got, ok := p.Authoritative("10.0.0.5", `for=192.0.2.1;proto=https`, "", "")
	if !ok || got != "192.0.2.1" {
		t.Fatalf("bare IP CIDR: got (%q, %v), want (\"192.0.2.1\", true)", got, ok)
	}
	// A different remote in the same /8 must NOT be trusted (only the exact host).
	got, ok = p.Authoritative("10.0.0.6", `for=192.0.2.1;proto=https`, "", "")
	if !ok || got != "10.0.0.6" {
		t.Fatalf("bare IP CIDR neighbor: got (%q, %v), want (\"10.0.0.6\", true)", got, ok)
	}
}

// Edge: SetCIDRs is idempotent and replaces (not appends).
func TestProxyTrust_SetCIDRs_Replaces(t *testing.T) {
	p := NewProxyTrust([]string{"10.0.0.0/8"})
	p.SetCIDRs([]string{"192.168.0.0/16"})
	// 10.x is no longer trusted.
	got, ok := p.Authoritative("10.0.0.5", `for=192.0.2.1;proto=https`, "", "")
	if !ok || got != "10.0.0.5" {
		t.Fatalf("SetCIDRs replace: got (%q, %v), want (\"10.0.0.5\", true)", got, ok)
	}
	// 192.168.x is.
	got, ok = p.Authoritative("192.168.1.5", `for=192.0.2.1;proto=https`, "", "")
	if !ok || got != "192.0.2.1" {
		t.Fatalf("SetCIDRs new range: got (%q, %v), want (\"192.0.2.1\", true)", got, ok)
	}
}

// Edge: parseForwarded direct unit test for parameter parsing.
func TestParseForwarded(t *testing.T) {
	cases := []struct {
		name      string
		in        string
		wantProto string
		wantForIP string
	}{
		{"basic", `for=192.0.2.1;proto=https`, "https", "192.0.2.1"},
		{"order_reversed", `proto=https;for=192.0.2.1`, "https", "192.0.2.1"},
		{"case_insensitive_keys", `For=192.0.2.1;Proto=HTTPS`, "https", "192.0.2.1"},
		{"quoted_ipv6", `for="[2001:db8::1]:1234";proto=https`, "https", "[2001:db8::1]:1234"},
		{"list_takes_first", `for=192.0.2.1;proto=https, for=10.0.0.9;proto=http`, "https", "192.0.2.1"},
		{"missing_for", `proto=https`, "https", ""},
		{"missing_proto", `for=192.0.2.1`, "", "192.0.2.1"},
		{"empty", ``, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotProto, gotFor := parseForwarded(tc.in)
			if gotProto != tc.wantProto || gotFor != tc.wantForIP {
				t.Fatalf("parseForwarded(%q) = (%q, %q), want (%q, %q)",
					tc.in, gotProto, gotFor, tc.wantProto, tc.wantForIP)
			}
		})
	}
}
