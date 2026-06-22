package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/policy"
	"github.com/gritive/GrainFS/internal/s3auth"
)

// mapPolicyBackend serves a settable policy map and returns ErrBucketNotFound for
// absent buckets, so loadCommittedBucketPolicy's not-found normalization is
// exercised (the existing policyBackendFake returns nil error on a miss).
type mapPolicyBackend struct {
	Backend
	policies map[string][]byte
}

func (m *mapPolicyBackend) GetBucketPolicy(bucket string) ([]byte, error) {
	if p, ok := m.policies[bucket]; ok {
		return p, nil
	}
	return nil, ErrBucketNotFound
}
func (m *mapPolicyBackend) SetBucketPolicy(bucket string, p []byte) error {
	m.policies[bucket] = p
	return nil
}
func (m *mapPolicyBackend) DeleteBucketPolicy(bucket string) error {
	delete(m.policies, bucket)
	return nil
}

func TestOperationsAllowPullsCommittedPolicyOnColdCache(t *testing.T) {
	be := &mapPolicyBackend{policies: map[string][]byte{
		"b": []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":["*"]},"Action":["s3:PutObject"],"Resource":["arn:aws:s3:::b/*"]}]}`),
	}}
	store := policy.NewCompiledPolicyStore()
	_ = NewOperations(be, WithPolicyStore(store)) // installs the pull-on-miss loader

	in := s3auth.PermCheckInput{Action: s3auth.PutObject,
		Principal: s3auth.Principal{AccessKey: "AKIA"},
		Resource:  s3auth.ResourceRef{Bucket: "b", Key: "k"}}
	require.False(t, store.Allow(context.Background(), in)) // cold store + committed Deny ⇒ deny

	in.Resource.Bucket = "free"
	require.True(t, store.Allow(context.Background(), in)) // no committed policy ⇒ allow (negative-cache)
}

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
