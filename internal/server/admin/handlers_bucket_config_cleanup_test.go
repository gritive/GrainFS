package admin_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// fakeLifecycleDeleteProp records ProposeLifecycleDelete calls and returns errOnDelete.
type fakeLifecycleDeleteProp struct {
	called      []string
	errOnDelete error
}

func (f *fakeLifecycleDeleteProp) ProposeLifecycleDelete(_ context.Context, bucket string) error {
	f.called = append(f.called, bucket)
	return f.errOnDelete
}

// fakeUpstreamDeleteProp records ProposeBucketUpstreamDelete calls and returns errOnDelete.
type fakeUpstreamDeleteProp struct {
	called      []string
	errOnDelete error
}

func (f *fakeUpstreamDeleteProp) ProposeBucketUpstreamDelete(_ context.Context, bucket string) error {
	f.called = append(f.called, bucket)
	return f.errOnDelete
}

// On a successful delete, both config proposers are invoked with the bucket name.
func TestAdminDeleteBucket_CascadesConfigOnSuccess(t *testing.T) {
	fake := newFakeBucketOps()
	fake.buckets["b"] = true
	lc := &fakeLifecycleDeleteProp{}
	up := &fakeUpstreamDeleteProp{}
	d := &admin.Deps{Buckets: fake, LifecycleDeleteProp: lc, BucketUpstreamDeleteProp: up}

	err := admin.AdminDeleteBucket(context.Background(), d, "b", false)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, lc.called)
	assert.Equal(t, []string{"b"}, up.called)
}

// On force delete, the cascade still runs.
func TestAdminDeleteBucket_CascadesConfigOnForce(t *testing.T) {
	fake := newFakeBucketOps()
	fake.buckets["b"] = true
	lc := &fakeLifecycleDeleteProp{}
	up := &fakeUpstreamDeleteProp{}
	d := &admin.Deps{Buckets: fake, LifecycleDeleteProp: lc, BucketUpstreamDeleteProp: up}

	err := admin.AdminDeleteBucket(context.Background(), d, "b", true)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, lc.called)
	assert.Equal(t, []string{"b"}, up.called)
}

// When the bucket is already gone, the cascade still runs (idempotent reconcile)
// and the handler reports not_found.
func TestAdminDeleteBucket_CascadesConfigOnAlreadyMissing(t *testing.T) {
	lc := &fakeLifecycleDeleteProp{}
	up := &fakeUpstreamDeleteProp{}
	d := &admin.Deps{Buckets: newFakeBucketOps(), LifecycleDeleteProp: lc, BucketUpstreamDeleteProp: up}

	err := admin.AdminDeleteBucket(context.Background(), d, "ghost", false)
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "not_found", ae.Code)
	assert.Equal(t, []string{"ghost"}, lc.called)
	assert.Equal(t, []string{"ghost"}, up.called)
}

// A non-empty bucket survives the delete attempt, so its config MUST NOT be touched.
func TestAdminDeleteBucket_NoCascadeWhenBucketSurvives(t *testing.T) {
	lc := &fakeLifecycleDeleteProp{}
	up := &fakeUpstreamDeleteProp{}
	d := &admin.Deps{Buckets: &fakeBucketOpsNotEmpty{}, LifecycleDeleteProp: lc, BucketUpstreamDeleteProp: up}

	err := admin.AdminDeleteBucket(context.Background(), d, "b", false)
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "conflict", ae.Code)
	assert.Empty(t, lc.called)
	assert.Empty(t, up.called)
}

// A lifecycle-delete error after a successful bucket delete surfaces as an
// internal error (so the operator retries; the retry re-runs the idempotent
// cascade), and the upstream proposer is STILL called — the cascade must not
// short-circuit on the first failure (errors.Join semantics).
func TestAdminDeleteBucket_ConfigCascadeErrorSurfaces(t *testing.T) {
	fake := newFakeBucketOps()
	fake.buckets["b"] = true
	lc := &fakeLifecycleDeleteProp{errOnDelete: errors.New("meta-raft unavailable")}
	up := &fakeUpstreamDeleteProp{}
	d := &admin.Deps{Buckets: fake, LifecycleDeleteProp: lc, BucketUpstreamDeleteProp: up}

	err := admin.AdminDeleteBucket(context.Background(), d, "b", false)
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "internal", ae.Code)
	assert.Contains(t, ae.Message, "lifecycle")
	assert.Equal(t, []string{"b"}, up.called, "upstream proposer must still run after a lifecycle failure")
}

// When BOTH config proposers fail, the joined error names both subsystems.
func TestAdminDeleteBucket_BothConfigProposersFail(t *testing.T) {
	fake := newFakeBucketOps()
	fake.buckets["b"] = true
	lc := &fakeLifecycleDeleteProp{errOnDelete: errors.New("lc down")}
	up := &fakeUpstreamDeleteProp{errOnDelete: errors.New("iam down")}
	d := &admin.Deps{Buckets: fake, LifecycleDeleteProp: lc, BucketUpstreamDeleteProp: up}

	err := admin.AdminDeleteBucket(context.Background(), d, "b", false)
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "internal", ae.Code)
	assert.Contains(t, ae.Message, "lifecycle")
	assert.Contains(t, ae.Message, "IAM bucket-upstream")
}

// Nil proposers (unwired) → no panic, behavior identical to before.
func TestAdminDeleteBucket_NilConfigProposers_NoPanic(t *testing.T) {
	fake := newFakeBucketOps()
	fake.buckets["b"] = true
	d := &admin.Deps{Buckets: fake} // both config proposers nil

	err := admin.AdminDeleteBucket(context.Background(), d, "b", false)
	require.NoError(t, err)
	assert.False(t, fake.buckets["b"])
}
