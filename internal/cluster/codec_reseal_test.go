package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResealFSMValuesCmd_RoundTrip(t *testing.T) {
	in := ResealFSMValuesCmd{Keys: []string{"policy:b1", "obj:b1/k/v"}, ActiveGen: 2}
	data, err := EncodeCommand(CmdResealFSMValues, in)
	require.NoError(t, err)
	cmd, err := DecodeCommand(data)
	require.NoError(t, err)
	require.Equal(t, CmdResealFSMValues, cmd.Type)
	got, err := decodeResealFSMValuesCmd(cmd.Data)
	require.NoError(t, err)
	require.Equal(t, in.Keys, got.Keys)
	require.Equal(t, uint32(2), got.ActiveGen)
}

func TestResealFSMValuesCmd_EmptyKeys(t *testing.T) {
	in := ResealFSMValuesCmd{Keys: nil, ActiveGen: 1}
	data, err := EncodeCommand(CmdResealFSMValues, in)
	require.NoError(t, err)
	cmd, err := DecodeCommand(data)
	require.NoError(t, err)
	got, err := decodeResealFSMValuesCmd(cmd.Data)
	require.NoError(t, err)
	require.Empty(t, got.Keys)
	require.Equal(t, uint32(1), got.ActiveGen)
}
