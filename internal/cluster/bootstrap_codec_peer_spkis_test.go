package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBootstrapSecretsPayload_PeerSPKIs_RoundTrip(t *testing.T) {
	encKey := make([]byte, 32)
	psk := make([]byte, 32)
	a := [32]byte{0xAA}
	b := [32]byte{0xBB}

	blob := EncodeBootstrapSecretsPayloadWithCutover(encKey, nil, psk, [][32]byte{a, b}, false)
	gotEnc, _, gotPSK, gotSPKIs, gotDropped, err := DecodeBootstrapSecretsPayloadWithCutover(blob)
	require.NoError(t, err)
	require.Equal(t, encKey, gotEnc)
	require.Equal(t, psk, gotPSK)
	require.False(t, gotDropped)
	require.ElementsMatch(t, [][32]byte{a, b}, gotSPKIs)
}

func TestBootstrapSecretsPayload_EmptyPeerSPKIs_RoundTrip(t *testing.T) {
	encKey := make([]byte, 32)
	psk := make([]byte, 32)

	blob := EncodeBootstrapSecretsPayloadWithCutover(encKey, nil, psk, nil, false)
	_, _, _, gotSPKIs, _, err := DecodeBootstrapSecretsPayloadWithCutover(blob)
	require.NoError(t, err)
	require.Empty(t, gotSPKIs)
}
