package cluster

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFSMValueFrameV2RoundTrip(t *testing.T) {
	ct := bytes.Repeat([]byte{0xA5}, 32)
	frame := encodeFSMValueFrameV2(7, ct)

	gen, got, ok, err := decodeFSMValueFrameV2(frame)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(7), gen)
	require.Equal(t, ct, got)
	require.NotContains(t, string(frame), "plain-value")
}

func TestFSMValueFrameV2RejectsTruncated(t *testing.T) {
	_, _, ok, err := decodeFSMValueFrameV2([]byte("GFMV\x02\x00"))
	require.Error(t, err)
	require.True(t, ok, "GFMV prefix means this is a v2 frame attempt")
}

func TestFSMValueFrameV2PlaintextIsNotFrame(t *testing.T) {
	gen, got, ok, err := decodeFSMValueFrameV2([]byte("plain"))
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, uint32(0), gen)
	require.Nil(t, got)
}
