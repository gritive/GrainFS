package storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/policy"
)

func TestOperationsBucketPolicyPersistsAndUpdatesCache(t *testing.T) {
	backend := &policyBackendFake{}
	store := policy.NewCompiledPolicyStore()
	ops := NewOperations(backend, WithPolicyStore(store))
	body := []byte(`{"Version":"2012-10-17","Statement":[]}`)

	require.NoError(t, ops.SetBucketPolicy("b", body))
	got, err := ops.GetBucketPolicy("b")
	require.NoError(t, err)
	require.Equal(t, body, got)
	require.Equal(t, body, store.GetRaw("b"))

	require.NoError(t, ops.DeleteBucketPolicy("b"))
	require.Nil(t, store.GetRaw("b"))
	require.Equal(t, []string{"set:b", "delete:b"}, backend.calls)
}

func TestOperationsBucketPolicyUsesCacheWhenBackendUnsupported(t *testing.T) {
	store := policy.NewCompiledPolicyStore()
	ops := NewOperations(&aclNoCapabilityBackend{}, WithPolicyStore(store))
	body := []byte(`{"Version":"2012-10-17","Statement":[]}`)

	require.NoError(t, ops.SetBucketPolicy("b", body))
	got, err := ops.GetBucketPolicy("b")
	require.NoError(t, err)
	require.Equal(t, body, got)

	require.NoError(t, ops.DeleteBucketPolicy("b"))
	_, err = ops.GetBucketPolicy("b")
	requireUnsupportedOp(t, err, "GetBucketPolicy", UnsupportedReasonNoAdapter)
}

func TestOperationsBucketPolicyUnsupportedWithoutBackendOrCache(t *testing.T) {
	ops := NewOperations(&aclNoCapabilityBackend{})

	requireUnsupportedOp(t, ops.SetBucketPolicy("b", []byte(`{}`)), "SetBucketPolicy", UnsupportedReasonNoAdapter)
	_, err := ops.GetBucketPolicy("b")
	requireUnsupportedOp(t, err, "GetBucketPolicy", UnsupportedReasonNoAdapter)
	requireUnsupportedOp(t, ops.DeleteBucketPolicy("b"), "DeleteBucketPolicy", UnsupportedReasonNoAdapter)
}

type policyBackendFake struct {
	Backend
	calls []string
	raw   []byte
}

func (b *policyBackendFake) SetBucketPolicy(bucket string, policyJSON []byte) error {
	b.calls = append(b.calls, "set:"+bucket)
	b.raw = append([]byte(nil), policyJSON...)
	return nil
}

func (b *policyBackendFake) GetBucketPolicy(bucket string) ([]byte, error) {
	b.calls = append(b.calls, "get:"+bucket)
	return append([]byte(nil), b.raw...), nil
}

func (b *policyBackendFake) DeleteBucketPolicy(bucket string) error {
	b.calls = append(b.calls, "delete:"+bucket)
	b.raw = nil
	return nil
}
