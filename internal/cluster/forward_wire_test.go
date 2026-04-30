package cluster

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncodeGroupForwardPayload_RoundTrip(t *testing.T) {
	gid := "group-7"
	data := []byte("propose-bytes")
	payload := encodeGroupForwardPayload(gid, data)
	gotGid, gotData, err := decodeGroupForwardPayload(payload)
	require.NoError(t, err)
	require.Equal(t, gid, gotGid)
	require.True(t, bytes.Equal(data, gotData))
}

func TestEncodeGroupForwardPayload_EmptyGroupID(t *testing.T) {
	// Empty groupID is technically allowed by the wire — caller decides whether
	// to treat as an error.
	payload := encodeGroupForwardPayload("", []byte("x"))
	gid, data, err := decodeGroupForwardPayload(payload)
	require.NoError(t, err)
	require.Equal(t, "", gid)
	require.Equal(t, []byte("x"), data)
}

func TestDecodeGroupForwardPayload_TooShort(t *testing.T) {
	_, _, err := decodeGroupForwardPayload([]byte{0, 0, 0}) // only 3 bytes, need 4 for groupIDLen
	require.Error(t, err)
}

func TestDecodeGroupForwardPayload_GidLenExceedsPayload(t *testing.T) {
	// Claim groupIDLen=100 but only have 4 bytes total.
	payload := []byte{0, 0, 0, 100}
	_, _, err := decodeGroupForwardPayload(payload)
	require.Error(t, err)
}

// REGRESSION: legacy StreamProposeForward (0x06) wire format must remain unchanged.
// v0.0.6.x ↔ v0.0.7.0 rolling upgrade depends on this.
//
// The legacy wire format is: payload IS the raw propose data (no length prefix,
// no groupID). The new StreamProposeGroupForward (0x08) wire is independent:
// different stream type, different payload layout. They cannot collide.
func TestLegacyProposeForwardWire_Unchanged(t *testing.T) {
	// Verify legacy raw payload does NOT decode as group-forward layout.
	// "lega" bytes (0x6c 0x65 0x67 0x61) interpreted as big-endian uint32 =
	// ~1.8B which exceeds any realistic payload, so decode must error.
	rawData := []byte("lega-cmd-bytes")
	_, _, err := decodeGroupForwardPayload(rawData)
	require.Error(t, err, "legacy payload must NOT decode as group-forward layout — wire must remain disjoint")
}
