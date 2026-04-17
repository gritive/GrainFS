package server

import "testing"

func TestIsLocalhostAddr(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want bool
	}{
		{"IPv4 loopback", "127.0.0.1:9000", true},
		{"IPv6 loopback", "[::1]:9000", true},
		{"IPv4-mapped IPv6 loopback", "[::ffff:127.0.0.1]:9000", true},
		{"literal localhost", "localhost:9000", true},
		{"external IPv4", "192.168.1.5:9000", false},
		{"external IPv6", "[2001:db8::1]:9000", false},
		{"other IPv4 loopback not matched", "127.0.0.2:9000", false}, // only 127.0.0.1 matched
		{"empty", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isLocalhostAddr(tt.addr); got != tt.want {
				t.Errorf("isLocalhostAddr(%q) = %v, want %v", tt.addr, got, tt.want)
			}
		})
	}
}
