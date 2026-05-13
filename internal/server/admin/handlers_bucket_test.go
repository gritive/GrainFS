package admin_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/gritive/GrainFS/internal/storage"
)

type fakeBucketOps struct {
	buckets map[string]bool
	counts  map[string]int64
}

func newFakeBucketOps() *fakeBucketOps {
	return &fakeBucketOps{
		buckets: map[string]bool{},
		counts:  map[string]int64{},
	}
}

func (f *fakeBucketOps) CreateBucket(_ context.Context, bucket string) error {
	if f.buckets[bucket] {
		return storage.ErrBucketAlreadyExists
	}
	f.buckets[bucket] = true
	return nil
}
func (f *fakeBucketOps) HeadBucket(_ context.Context, bucket string) error {
	if !f.buckets[bucket] {
		return storage.ErrBucketNotFound
	}
	return nil
}
func (f *fakeBucketOps) DeleteBucket(_ context.Context, bucket string) error {
	if !f.buckets[bucket] {
		return storage.ErrBucketNotFound
	}
	delete(f.buckets, bucket)
	return nil
}
func (f *fakeBucketOps) ListBuckets(_ context.Context) ([]string, error) {
	out := make([]string, 0, len(f.buckets))
	for b := range f.buckets {
		out = append(out, b)
	}
	return out, nil
}
func (f *fakeBucketOps) ForceDeleteBucket(_ context.Context, bucket string) error {
	if !f.buckets[bucket] {
		return storage.ErrBucketNotFound
	}
	delete(f.buckets, bucket)
	return nil
}

func (f *fakeBucketOps) CountObjects(_ context.Context, bucket string) (int64, error) {
	if !f.buckets[bucket] {
		return 0, storage.ErrBucketNotFound
	}
	return f.counts[bucket], nil
}

func (f *fakeBucketOps) GetBucketPolicy(bucket string) ([]byte, error) {
	return nil, storage.UnsupportedOperationError{Op: "GetBucketPolicy", Reason: storage.UnsupportedReasonNoAdapter}
}
func (f *fakeBucketOps) SetBucketPolicy(bucket string, policyJSON []byte) error {
	return storage.UnsupportedOperationError{Op: "SetBucketPolicy", Reason: storage.UnsupportedReasonNoAdapter}
}
func (f *fakeBucketOps) DeleteBucketPolicy(bucket string) error {
	return storage.UnsupportedOperationError{Op: "DeleteBucketPolicy", Reason: storage.UnsupportedReasonNoAdapter}
}
func (f *fakeBucketOps) GetBucketVersioning(bucket string) (string, error) {
	return "", storage.UnsupportedOperationError{Op: "GetBucketVersioning", Reason: storage.UnsupportedReasonNoAdapter}
}
func (f *fakeBucketOps) SetBucketVersioning(bucket, state string) error {
	return storage.UnsupportedOperationError{Op: "SetBucketVersioning", Reason: storage.UnsupportedReasonNoAdapter}
}

func TestAdminCreateBucket(t *testing.T) {
	fake := newFakeBucketOps()
	d := &admin.Deps{Buckets: fake}
	ctx := context.Background()

	_, err := admin.AdminCreateBucket(ctx, d, admin.CreateBucketAdminReq{Name: "my-bucket"})
	require.NoError(t, err)
	assert.True(t, fake.buckets["my-bucket"])

	// 중복 생성 → conflict
	_, err = admin.AdminCreateBucket(ctx, d, admin.CreateBucketAdminReq{Name: "my-bucket"})
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "conflict", ae.Code)
}

func TestAdminListBuckets(t *testing.T) {
	fake := newFakeBucketOps()
	fake.buckets["a"] = true
	fake.buckets["__grainfs_internal"] = true // internal bucket should be filtered
	d := &admin.Deps{Buckets: fake}

	resp, err := admin.AdminListBuckets(context.Background(), d)
	require.NoError(t, err)
	require.Len(t, resp.Buckets, 1)
	assert.Equal(t, "a", resp.Buckets[0].Name)
}

func TestAdminDeleteBucket_Force(t *testing.T) {
	fake := newFakeBucketOps()
	fake.buckets["b"] = true
	d := &admin.Deps{Buckets: fake}

	err := admin.AdminDeleteBucket(context.Background(), d, "b", true)
	require.NoError(t, err)
	assert.False(t, fake.buckets["b"])
}

func TestAdminDeleteBucket_NotFound(t *testing.T) {
	d := &admin.Deps{Buckets: newFakeBucketOps()}
	err := admin.AdminDeleteBucket(context.Background(), d, "nonexistent", false)
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "not_found", ae.Code)
}

func TestAdminDeleteBucket_NotEmpty_NoForce(t *testing.T) {
	fake := &fakeBucketOpsNotEmpty{}
	d := &admin.Deps{Buckets: fake}
	err := admin.AdminDeleteBucket(context.Background(), d, "b", false)
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "conflict", ae.Code)
	assert.Contains(t, ae.Message, "--force")
}

func TestAdminDeleteBucket_NotEmpty_Force_Retry(t *testing.T) {
	// force=true인데도 concurrent write로 ErrBucketNotEmpty가 발생하면
	// "use --force" 대신 503 retry 메시지를 반환해야 한다.
	fake := &fakeBucketOpsNotEmpty{}
	d := &admin.Deps{Buckets: fake}
	err := admin.AdminDeleteBucket(context.Background(), d, "b", true)
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "retry", ae.Code)
	assert.NotContains(t, ae.Message, "--force")
}

// fakeBucketOpsNotEmpty는 ForceDeleteBucket/DeleteBucket이 항상 ErrBucketNotEmpty를 반환한다.
type fakeBucketOpsNotEmpty struct{ fakeBucketOps }

func (f *fakeBucketOpsNotEmpty) DeleteBucket(_ context.Context, _ string) error {
	return storage.ErrBucketNotEmpty
}
func (f *fakeBucketOpsNotEmpty) ForceDeleteBucket(_ context.Context, _ string) error {
	return storage.ErrBucketNotEmpty
}
func (f *fakeBucketOpsNotEmpty) HeadBucket(_ context.Context, _ string) error   { return nil }
func (f *fakeBucketOpsNotEmpty) CreateBucket(_ context.Context, _ string) error { return nil }
func (f *fakeBucketOpsNotEmpty) ListBuckets(_ context.Context) ([]string, error) {
	return nil, nil
}

func TestAdminGetBucket(t *testing.T) {
	fake := newFakeBucketOps()
	fake.buckets["my-bucket"] = true
	fake.counts["my-bucket"] = 42
	d := &admin.Deps{Buckets: fake}

	info, err := admin.AdminGetBucket(context.Background(), d, "my-bucket")
	require.NoError(t, err)
	assert.Equal(t, "my-bucket", info.Name)
	require.NotNil(t, info.ObjectCount)
	assert.Equal(t, int64(42), *info.ObjectCount)
}

func TestAdminGetBucket_NotFound(t *testing.T) {
	fake := newFakeBucketOps()
	d := &admin.Deps{Buckets: fake}

	_, err := admin.AdminGetBucket(context.Background(), d, "missing")
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "not_found", ae.Code)
}

func TestAdminGetBucket_InternalBucketForbidden(t *testing.T) {
	fake := newFakeBucketOps()
	fake.buckets["__grainfs_internal"] = true
	d := &admin.Deps{Buckets: fake}

	_, err := admin.AdminGetBucket(context.Background(), d, "__grainfs_internal")
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "forbidden", ae.Code)
}

// fakeIAMUpstream implements IAMService.GetBucketUpstream and ListBucketUpstreams only.
type fakeIAMUpstream struct {
	admin.IAMService
	upstreams []iam.BucketUpstreamItem
}

func (s *fakeIAMUpstream) GetBucketUpstream(_ context.Context, bucket string) (iam.BucketUpstreamItem, error) {
	for _, u := range s.upstreams {
		if u.Bucket == bucket {
			return u, nil
		}
	}
	return iam.BucketUpstreamItem{}, &adminapi.Error{Code: "not_found", Message: "not found"}
}

func (s *fakeIAMUpstream) ListBucketUpstreams(_ context.Context) ([]iam.BucketUpstreamItem, error) {
	return s.upstreams, nil
}

func TestAdminGetBucket_HasUpstreamTrue(t *testing.T) {
	fake := newFakeBucketOps()
	fake.buckets["my-bucket"] = true
	fake.counts["my-bucket"] = 5
	iamFake := &fakeIAMUpstream{
		upstreams: []iam.BucketUpstreamItem{{Bucket: "my-bucket"}},
	}
	d := &admin.Deps{Buckets: fake, IAM: iamFake}

	info, err := admin.AdminGetBucket(context.Background(), d, "my-bucket")
	require.NoError(t, err)
	assert.True(t, info.HasUpstream)
}

func TestAdminGetBucket_HasUpstreamFalse(t *testing.T) {
	fake := newFakeBucketOps()
	fake.buckets["my-bucket"] = true
	fake.counts["my-bucket"] = 0
	iamFake := &fakeIAMUpstream{upstreams: nil}
	d := &admin.Deps{Buckets: fake, IAM: iamFake}

	info, err := admin.AdminGetBucket(context.Background(), d, "my-bucket")
	require.NoError(t, err)
	assert.False(t, info.HasUpstream)
}

func TestAdminGetBucket_VersioningUnsupported(t *testing.T) {
	// GetBucketVersioning이 UnsupportedOperationError를 반환하면 Versioning은 빈 문자열이어야 한다.
	fake := newFakeBucketOps()
	fake.buckets["my-bucket"] = true
	d := &admin.Deps{Buckets: fake}

	info, err := admin.AdminGetBucket(context.Background(), d, "my-bucket")
	require.NoError(t, err)
	assert.Equal(t, "", info.Versioning)
}

func TestAdminListBuckets_HasUpstream(t *testing.T) {
	fake := newFakeBucketOps()
	fake.buckets["a"] = true
	fake.buckets["b"] = true
	iamFake := &fakeIAMUpstream{
		upstreams: []iam.BucketUpstreamItem{{Bucket: "a"}},
	}
	d := &admin.Deps{Buckets: fake, IAM: iamFake}

	resp, err := admin.AdminListBuckets(context.Background(), d)
	require.NoError(t, err)
	require.Len(t, resp.Buckets, 2)
	// sorted: a, b
	assert.True(t, resp.Buckets[0].HasUpstream)  // "a" has upstream
	assert.False(t, resp.Buckets[1].HasUpstream) // "b" does not
}

func TestAdminListBuckets_NilIAM(t *testing.T) {
	fake := newFakeBucketOps()
	fake.buckets["a"] = true
	d := &admin.Deps{Buckets: fake} // IAM is nil

	resp, err := admin.AdminListBuckets(context.Background(), d)
	require.NoError(t, err)
	require.Len(t, resp.Buckets, 1)
	assert.False(t, resp.Buckets[0].HasUpstream)
}

type fakeBucketOpsWithPolicy struct {
	*fakeBucketOps
	policy map[string][]byte
}

func newFakeBucketOpsWithPolicy() *fakeBucketOpsWithPolicy {
	return &fakeBucketOpsWithPolicy{
		fakeBucketOps: newFakeBucketOps(),
		policy:        map[string][]byte{},
	}
}

func (f *fakeBucketOpsWithPolicy) GetBucketPolicy(bucket string) ([]byte, error) {
	p, ok := f.policy[bucket]
	if !ok {
		return nil, nil
	}
	return p, nil
}
func (f *fakeBucketOpsWithPolicy) SetBucketPolicy(bucket string, policyJSON []byte) error {
	f.policy[bucket] = policyJSON
	return nil
}
func (f *fakeBucketOpsWithPolicy) DeleteBucketPolicy(bucket string) error {
	delete(f.policy, bucket)
	return nil
}

func TestAdminGetBucketPolicy(t *testing.T) {
	fake := newFakeBucketOpsWithPolicy()
	fake.buckets["my-bucket"] = true
	fake.policy["my-bucket"] = []byte(`{"Version":"2012-10-17","Statement":[]}`)
	d := &admin.Deps{Buckets: fake}

	resp, err := admin.AdminGetBucketPolicy(context.Background(), d, "my-bucket")
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, string(resp.Policy))
}

func TestAdminGetBucketPolicy_Unsupported(t *testing.T) {
	d := &admin.Deps{Buckets: newFakeBucketOps()}
	_, err := admin.AdminGetBucketPolicy(context.Background(), d, "my-bucket")
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "unsupported", ae.Code)
}

func TestAdminSetBucketPolicy(t *testing.T) {
	fake := newFakeBucketOpsWithPolicy()
	fake.buckets["my-bucket"] = true
	d := &admin.Deps{Buckets: fake}

	policy := json.RawMessage(`{"Version":"2012-10-17","Statement":[]}`)
	err := admin.AdminSetBucketPolicy(context.Background(), d, "my-bucket", admin.BucketPolicySetReq{Policy: policy})
	require.NoError(t, err)
	assert.Equal(t, []byte(policy), fake.policy["my-bucket"])
}

func TestAdminDeleteBucketPolicy(t *testing.T) {
	fake := newFakeBucketOpsWithPolicy()
	fake.buckets["my-bucket"] = true
	fake.policy["my-bucket"] = []byte(`{}`)
	d := &admin.Deps{Buckets: fake}

	err := admin.AdminDeleteBucketPolicy(context.Background(), d, "my-bucket")
	require.NoError(t, err)
	assert.Nil(t, fake.policy["my-bucket"])
}

func TestAdminGetBucketPolicy_NilPolicy(t *testing.T) {
	fake := newFakeBucketOpsWithPolicy()
	fake.buckets["my-bucket"] = true
	d := &admin.Deps{Buckets: fake}

	_, err := admin.AdminGetBucketPolicy(context.Background(), d, "my-bucket")
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "not_found", ae.Code)
}

func TestAdminGetBucketPolicy_InternalForbidden(t *testing.T) {
	d := &admin.Deps{Buckets: newFakeBucketOps()}
	_, err := admin.AdminGetBucketPolicy(context.Background(), d, "__grainfs_internal")
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "forbidden", ae.Code)
}

func TestAdminSetBucketPolicy_Unsupported(t *testing.T) {
	d := &admin.Deps{Buckets: newFakeBucketOps()}
	err := admin.AdminSetBucketPolicy(context.Background(), d, "my-bucket",
		admin.BucketPolicySetReq{Policy: json.RawMessage(`{}`)})
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "unsupported", ae.Code)
}

func TestAdminSetBucketPolicy_InternalForbidden(t *testing.T) {
	d := &admin.Deps{Buckets: newFakeBucketOps()}
	err := admin.AdminSetBucketPolicy(context.Background(), d, "__grainfs_internal",
		admin.BucketPolicySetReq{Policy: json.RawMessage(`{}`)})
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "forbidden", ae.Code)
}

func TestAdminDeleteBucketPolicy_Unsupported(t *testing.T) {
	d := &admin.Deps{Buckets: newFakeBucketOps()}
	err := admin.AdminDeleteBucketPolicy(context.Background(), d, "my-bucket")
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "unsupported", ae.Code)
}

func TestAdminDeleteBucketPolicy_InternalForbidden(t *testing.T) {
	d := &admin.Deps{Buckets: newFakeBucketOps()}
	err := admin.AdminDeleteBucketPolicy(context.Background(), d, "__grainfs_internal")
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	assert.Equal(t, "forbidden", ae.Code)
}
