package transport

import (
	"crypto/tls"
	"testing"
)

func sp(b byte) [32]byte { var s [32]byte; s[0] = b; return s }

func TestComposer_UnionAndNoClobber(t *testing.T) {
	var swapped *IdentitySnapshot
	c := newIdentityComposer(sp(1), func(snap *IdentitySnapshot) { swapped = snap })
	c.setPresent(tls.Certificate{}, sp(1))
	if !swapped.Accepts(sp(1)) {
		t.Fatal("base PSK must be accepted")
	}
	c.applyRotation([][32]byte{sp(1), sp(2)}, tls.Certificate{}, sp(1), nil)
	if !swapped.Accepts(sp(1)) || !swapped.Accepts(sp(2)) {
		t.Fatal("rotation window must not clobber base")
	}
	c.setRegistry([][32]byte{sp(3)})
	for _, want := range []byte{1, 2, 3} {
		if !swapped.Accepts(sp(want)) {
			t.Fatalf("union lost sp(%d)", want)
		}
	}
	c.setRegistry(nil)
	if swapped.Accepts(sp(3)) {
		t.Fatal("registry removal must drop sp(3)")
	}
	if !swapped.Accepts(sp(1)) || !swapped.Accepts(sp(2)) {
		t.Fatal("base/rotation lost on registry delta")
	}
}
