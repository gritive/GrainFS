package admin

import (
	"context"
	"errors"
	"sort"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/storage"
)

type CreateBucketAdminReq = adminapi.CreateBucketAdminReq
type ListBucketsAdminResp = adminapi.ListBucketsAdminResp
type BucketInfo = adminapi.BucketInfo

func AdminCreateBucket(ctx context.Context, d *Deps, req CreateBucketAdminReq) (BucketInfo, error) {
	if req.Name == "" {
		return BucketInfo{}, NewInvalid("name required")
	}
	if err := d.Buckets.CreateBucket(ctx, req.Name); err != nil {
		if errors.Is(err, storage.ErrBucketAlreadyExists) {
			return BucketInfo{}, NewConflict("bucket already exists", nil)
		}
		return BucketInfo{}, NewInternal("create bucket: " + err.Error())
	}
	return BucketInfo{Name: req.Name}, nil
}

// AdminListBuckets lists user-facing buckets. Internal __grainfs_* buckets
// are always excluded.
func AdminListBuckets(ctx context.Context, d *Deps) (ListBucketsAdminResp, error) {
	names, err := d.Buckets.ListBuckets(ctx)
	if err != nil {
		return ListBucketsAdminResp{}, NewInternal("list buckets: " + err.Error())
	}
	filtered := make([]string, 0, len(names))
	for _, n := range names {
		if !storage.IsInternalBucket(n) {
			filtered = append(filtered, n)
		}
	}
	sort.Strings(filtered)
	out := make([]BucketInfo, len(filtered))
	for i, n := range filtered {
		out[i] = BucketInfo{Name: n}
	}
	return ListBucketsAdminResp{Buckets: out}, nil
}

// AdminDeleteBucket deletes a bucket. If force is true, all objects are
// removed first; otherwise the bucket must be empty.
func AdminDeleteBucket(ctx context.Context, d *Deps, name string, force bool) error {
	var err error
	if force {
		err = d.Buckets.ForceDeleteBucket(ctx, name)
	} else {
		err = d.Buckets.DeleteBucket(ctx, name)
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrBucketNotFound) {
		return NewNotFound("bucket not found")
	}
	if errors.Is(err, storage.ErrBucketNotEmpty) {
		return NewConflict("bucket not empty; use --force to delete all objects", nil)
	}
	return NewInternal("delete bucket: " + err.Error())
}
