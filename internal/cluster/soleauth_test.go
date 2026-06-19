package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSoleAuthKeyAndConsts(t *testing.T) {
	// stateKeyspace{} is the empty (identity) keyspace — Key(raw) returns raw unchanged.
	// Mirror keyspace_test.go usage of newStateKeyspaceEmpty().
	ks := newStateKeyspaceEmpty()
	require.Equal(t, "soleauth:b", string(ks.BucketSoleAuthKey("b")))
	require.Equal(t, "off", soleAuthOff)
	require.Equal(t, "pending", soleAuthPending)
	require.Equal(t, "on", soleAuthOn)
	_ = SetBucketSoleAuthorityCmd{Bucket: "b", State: soleAuthOn}
}

func TestSetBucketSoleAuthorityCmd_RoundTrip(t *testing.T) {
	raw, err := EncodeCommand(CmdSetBucketSoleAuthority, SetBucketSoleAuthorityCmd{Bucket: "b", State: soleAuthPending})
	require.NoError(t, err)
	cmd, err := DecodeCommand(raw) // returns {Type, Data} only — NO typed Payload (fsm.go:355)
	require.NoError(t, err)
	require.Equal(t, CmdSetBucketSoleAuthority, cmd.Type)
	got, err := decodeSetBucketSoleAuthorityCmd(cmd.Data) // decode the payload bytes
	require.NoError(t, err)
	require.Equal(t, "b", got.Bucket)
	require.Equal(t, soleAuthPending, got.State)
}
