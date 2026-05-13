package admin_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/adminapi"
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
