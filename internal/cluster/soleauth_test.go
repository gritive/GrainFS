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
