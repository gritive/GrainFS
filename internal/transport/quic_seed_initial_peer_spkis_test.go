package transport

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSeedInitialPeerSPKIs_BeforeListen(t *testing.T) {
	tr, err := NewQUICTransport("test-psk-32-bytes-aaaaaaaaaaaaaaaa")
	require.NoError(t, err)
	defer tr.Close()

	spkiA := [32]byte{0xAA}
	spkiB := [32]byte{0xBB}

	tr.SeedInitialPeerSPKIs([][32]byte{spkiA, spkiB})

	snap := tr.identity.Load()
	require.NotNil(t, snap)
	require.True(t, snap.Accepts(spkiA), "seeded SPKI A must be in accept-set")
	require.True(t, snap.Accepts(spkiB), "seeded SPKI B must be in accept-set")
}

func TestSeedInitialPeerSPKIs_Empty_NoChange(t *testing.T) {
	tr, err := NewQUICTransport("test-psk-32-bytes-aaaaaaaaaaaaaaaa")
	require.NoError(t, err)
	defer tr.Close()

	before := tr.identity.Load()
	tr.SeedInitialPeerSPKIs(nil) // empty → no-op
	after := tr.identity.Load()
	require.Equal(t, before.AcceptSPKIs, after.AcceptSPKIs)
}
