package transport

import (
	"crypto/tls"
	"testing"
)

// TestQUICTransport_ComposerWiring verifies that ApplyRotation (rotation window)
// and UpdateRegistryAccept (peer registry) both feed the SAME composer, so
// neither delta clobbers the other or the base PSK SPKI (spec §6 D-rev3 step 3).
func TestQUICTransport_ComposerWiring(t *testing.T) {
	tr := MustNewQUICTransport(longPSK("c"))
	_, base, err := DeriveClusterIdentity(longPSK("c"))
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	// Initial snapshot must present the real PSK cert, not a zero cert.
	if got := tr.identity.Load(); len(got.PresentCert.Certificate) == 0 {
		t.Fatal("initial composer recompute lost the present cert")
	}

	tr.ApplyRotation([][32]byte{base, sp(2)}, tls.Certificate{}, base, nil)
	tr.UpdateRegistryAccept([][32]byte{sp(3)})

	snap := tr.identity.Load()
	for _, w := range [][32]byte{base, sp(2), sp(3)} {
		if !snap.Accepts(w) {
			t.Fatalf("composer wiring lost %x", w)
		}
	}
}
