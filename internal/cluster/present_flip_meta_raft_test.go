package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

func TestProposePreparePresentFlip_EncodesCmdType(t *testing.T) {
	var got []byte
	fakeProposer := func(_ context.Context, payload []byte, _ time.Duration) error {
		got = payload
		return nil
	}
	err := ProposePreparePresentFlip(context.Background(), fakeProposer,
		PresentFlipStamp{Voters: []string{"A", "B"}, ConfigIndex: 42}, time.Second)
	require.NoError(t, err)
	require.NotEmpty(t, got)

	cmdType, _ := decodeMetaCmdEnvelope(t, got)
	require.Equal(t, MetaCmdTypePreparePresentFlip, cmdType)
}

func TestProposeBeginPresentFlip_EncodesCmdType(t *testing.T) {
	var got []byte
	fakeProposer := func(_ context.Context, payload []byte, _ time.Duration) error {
		got = payload
		return nil
	}
	err := ProposeBeginPresentFlip(context.Background(), fakeProposer,
		PresentFlipStamp{Voters: []string{"A", "B"}, ConfigIndex: 42}, time.Second)
	require.NoError(t, err)
	cmdType, _ := decodeMetaCmdEnvelope(t, got)
	require.Equal(t, MetaCmdTypeBeginPresentFlip, cmdType)
}

func decodeMetaCmdEnvelope(t *testing.T, payload []byte) (MetaCmdType, []byte) {
	t.Helper()
	mc := clusterpb.GetRootAsMetaCmd(payload, 0)
	return MetaCmdType(mc.Type()), mc.DataBytes()
}

func TestPresentFlipStamp_RoundTrip(t *testing.T) {
	in := PresentFlipStamp{Voters: []string{"node-A", "node-B", "node-C"}, ConfigIndex: 12345}
	payload, err := encodePresentFlipStampCmd(in)
	require.NoError(t, err)
	out, err := decodePresentFlipStampCmd(payload)
	require.NoError(t, err)
	require.Equal(t, in, out)
}
