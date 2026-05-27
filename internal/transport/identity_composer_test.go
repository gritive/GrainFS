package transport

import (
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
)

func sp(b byte) [32]byte { var s [32]byte; s[0] = b; return s }

func captureSwap(last **IdentitySnapshot) func(*IdentitySnapshot) {
	return func(s *IdentitySnapshot) { *last = s }
}

func spkiN(b byte) [32]byte {
	var s [32]byte
	for i := range s {
		s[i] = b
	}
	return s
}

func acceptsContains(snap *IdentitySnapshot, want [32]byte) bool {
	for _, s := range snap.AcceptSPKIs {
		if s == want {
			return true
		}
	}
	return false
}

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

func TestComposer_PinPresent_SurvivesRotation(t *testing.T) {
	base := spkiN(1)
	var snap *IdentitySnapshot
	c := newIdentityComposer(base, captureSwap(&snap))

	perNode := spkiN(9)
	c.setPinPresent(tls.Certificate{}, perNode)
	require.Equal(t, perNode, snap.PresentSPKI, "pin sets present SPKI")

	rotSPKI := spkiN(5)
	c.applyRotation([][32]byte{spkiN(2), spkiN(3)}, tls.Certificate{}, rotSPKI, nil)
	require.Equal(t, perNode, snap.PresentSPKI, "rotation must not override pinned present")
	require.True(t, acceptsContains(snap, spkiN(2)), "rotation window still applied")
	require.True(t, acceptsContains(snap, spkiN(3)), "rotation window still applied")
}

func TestComposer_Dropped_ExcludesBase(t *testing.T) {
	base := spkiN(1)
	var snap *IdentitySnapshot
	c := newIdentityComposer(base, captureSwap(&snap))
	c.setRegistry([][32]byte{spkiN(7)})
	require.True(t, acceptsContains(snap, base), "base accepted before drop")

	c.setDropped()
	require.False(t, acceptsContains(snap, base), "dropped excludes base")
	require.True(t, acceptsContains(snap, spkiN(7)), "registry still accepted after drop")
}

func TestComposer_NotPinned_RotationStillOverridesPresent(t *testing.T) {
	base := spkiN(1)
	var snap *IdentitySnapshot
	c := newIdentityComposer(base, captureSwap(&snap))
	rotSPKI := spkiN(5)
	c.applyRotation(nil, tls.Certificate{}, rotSPKI, nil)
	require.Equal(t, rotSPKI, snap.PresentSPKI, "unpinned: rotation sets present")
}
