package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A zero-value DistributedBackend is safe here because the policy branch of
// notifyOnApply early-returns before touching registry/logger/store.
func TestNotifyOnApplyFiresBucketPolicyInvalidate(t *testing.T) {
	b := &DistributedBackend{}
	var got []string
	b.SetOnBucketPolicyApply(func(bucket string) { got = append(got, bucket) })

	setRaw, err := EncodeCommand(CmdSetBucketPolicy, SetBucketPolicyCmd{Bucket: "b1", PolicyJSON: []byte(`{}`)})
	require.NoError(t, err)
	delRaw, err := EncodeCommand(CmdDeleteBucketPolicy, DeleteBucketPolicyCmd{Bucket: "b2"})
	require.NoError(t, err)

	b.notifyOnApply(setRaw)
	b.notifyOnApply(delRaw)

	require.Equal(t, []string{"b1", "b2"}, got)
}
