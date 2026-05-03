package transport

import (
	"testing"
)

func TestPSKAlpn_Static(t *testing.T) {
	t1 := &QUICTransport{psk: "psk-A-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	t2 := &QUICTransport{psk: "psk-B-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}

	if got := t1.pskALPN(); got != "grainfs" {
		t.Fatalf("pskALPN: want %q, got %q", "grainfs", got)
	}
	if got := t1.muxALPN(); got != "grainfs-mux-v1" {
		t.Fatalf("muxALPN: want %q, got %q", "grainfs-mux-v1", got)
	}
	// PSK no longer affects ALPN.
	if t1.pskALPN() != t2.pskALPN() || t1.muxALPN() != t2.muxALPN() {
		t.Fatalf("ALPN must be static across PSKs after T2")
	}
}
