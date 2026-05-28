package iam

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccessKey_SecretKeyDEKGenZeroByDefault(t *testing.T) {
	k := AccessKey{AccessKey: "x", SAID: "sa-1"}
	require.Equal(t, uint32(0), k.SecretKeyDEKGen, "zero-value should be 0 (matches FB default)")
}

func TestBucketUpstream_SecretKeyDEKGenZeroByDefault(t *testing.T) {
	u := BucketUpstream{Bucket: "b"}
	require.Equal(t, uint32(0), u.SecretKeyDEKGen)
}

func TestAccessKey_SecretKeyDEKGenAssignable(t *testing.T) {
	k := AccessKey{SecretKeyDEKGen: 7}
	require.Equal(t, uint32(7), k.SecretKeyDEKGen)
}
