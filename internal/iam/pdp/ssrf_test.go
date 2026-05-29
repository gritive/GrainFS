package pdp

import (
	"net/netip"
	"testing"
)

func TestValidateLiteralAddr(t *testing.T) {
	mk := func(s string) netip.Addr { a, _ := netip.ParseAddr(s); return a }
	cases := []struct {
		name         string
		scheme       string
		addr         string
		allowPrivate bool
		wantErr      bool
	}{
		{"https public ok", "https", "93.184.216.34", false, false},
		{"https loopback blocked", "https", "127.0.0.1", false, true},
		{"https ::1 blocked", "https", "::1", false, true},
		{"https rfc1918 blocked", "https", "10.1.2.3", false, true},
		{"https rfc1918 allow_private", "https", "10.1.2.3", true, false},
		{"https loopback relaxed by allow_private", "https", "127.0.0.1", true, false},
		{"https link-local NOT relaxed by allow_private", "https", "169.254.1.1", true, true},
		{"https metadata NOT relaxed by allow_private", "https", "169.254.169.254", true, true},
		{"https link-local blocked", "https", "169.254.1.1", false, true},
		{"https metadata blocked", "https", "169.254.169.254", false, true},
		{"https ULA blocked", "https", "fd00::1", false, true},
		{"https CGNAT blocked", "https", "100.64.0.1", false, true},
		{"https unspecified blocked", "https", "0.0.0.0", false, true},
		{"https benchmark blocked", "https", "198.18.0.1", false, true},
		{"https v4-mapped private blocked", "https", "::ffff:10.0.0.1", false, true},
		{"http loopback ok", "http", "127.0.0.1", false, false},
		{"http ::1 ok", "http", "::1", false, false},
		{"http non-loopback blocked", "http", "93.184.216.34", false, true},
		{"http private blocked (not loopback)", "http", "10.0.0.1", false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateLiteralAddr(c.scheme, mk(c.addr), c.allowPrivate)
			if (err != nil) != c.wantErr {
				t.Fatalf("scheme=%s addr=%s allowPrivate=%v: err=%v wantErr=%v", c.scheme, c.addr, c.allowPrivate, err, c.wantErr)
			}
		})
	}
}

func TestIsPlausibleDNSName(t *testing.T) {
	ok := []string{"pdp.example.com", "pdp-1.internal", "localhost", "a.b.c"}
	bad := []string{"0177.0.0.1", "2130706433", "0", "pdp.example.", "1.2.3", "999.999.999.999"}
	for _, h := range ok {
		if !isPlausibleDNSName(h) {
			t.Errorf("%q should be a plausible DNS name", h)
		}
	}
	for _, h := range bad {
		if isPlausibleDNSName(h) {
			t.Errorf("%q should NOT be a plausible DNS name", h)
		}
	}
}

func TestCheckDialAddr(t *testing.T) {
	if err := checkDialAddr("https", "10.0.0.1:443", false); err == nil {
		t.Fatal("https private should be blocked")
	}
	if err := checkDialAddr("http", "127.0.0.1:8181", false); err != nil {
		t.Fatalf("http loopback should pass: %v", err)
	}
}
