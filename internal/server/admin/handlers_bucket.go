package admin

import (
	"context"
	"errors"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/storage"
)

type CreateBucketAdminReq = adminapi.CreateBucketAdminReq
type ListBucketsAdminResp = adminapi.ListBucketsAdminResp
type BucketInfo = adminapi.BucketInfo

func AdminCreateBucket(ctx context.Context, d *Deps, req CreateBucketAdminReq) (BucketInfo, error) {
	if !storage.ValidBucketName(req.Name) {
		return BucketInfo{}, NewInvalid("invalid bucket name: 3–63 lowercase alphanumeric/dot/hyphen, start and end with alnum")
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
	filtered := userBucketNames(names)
	out := bucketListInfo(filtered, bucketUpstreamSet(ctx, d))
	return ListBucketsAdminResp{Buckets: out}, nil
}

// AdminGetBucket returns BucketInfo (name + object count + upstream + versioning) for a single bucket.
// CountObjects is O(N objects) — only for interactive use.
func AdminGetBucket(ctx context.Context, d *Deps, name string) (BucketInfo, error) {
	if storage.IsInternalBucket(name) {
		return BucketInfo{}, NewForbidden("cannot access internal bucket")
	}
	if err := d.Buckets.HeadBucket(ctx, name); err != nil {
		if errors.Is(err, storage.ErrBucketNotFound) {
			return BucketInfo{}, NewNotFound("bucket not found")
		}
		return BucketInfo{}, NewInternal("head bucket: " + err.Error())
	}
	count, err := d.Buckets.CountObjects(ctx, name)
	if err != nil {
		return BucketInfo{}, NewInternal("count objects: " + err.Error())
	}
	info := BucketInfo{Name: name, ObjectCount: &count}

	if err := annotateBucketUpstream(ctx, d, &info); err != nil {
		return BucketInfo{}, err
	}
	if err := annotateBucketVersioning(d, &info); err != nil {
		return BucketInfo{}, err
	}

	return info, nil
}

// checkBucketExists는 bucket이 존재하지 않으면 not_found, 그 외 에러는 internal을 반환한다.
// IsInternalBucket 체크 이후에 호출해야 한다.
func checkBucketExists(ctx context.Context, d *Deps, name string) error {
	if err := d.Buckets.HeadBucket(ctx, name); err != nil {
		if errors.Is(err, storage.ErrBucketNotFound) {
			return NewNotFound("bucket not found")
		}
		return NewInternal("head bucket: " + err.Error())
	}
	return nil
}

// AdminDeleteBucket deletes a bucket. If force is true, all objects are
// removed first; otherwise the bucket must be empty.
func AdminDeleteBucket(ctx context.Context, d *Deps, name string, force bool) error {
	if storage.IsInternalBucket(name) {
		return NewForbidden("cannot delete internal bucket")
	}
	var hadNfsExport bool
	if d.NfsExports != nil {
		_, hadNfsExport = d.NfsExports.Get(name)
	}
	if d.NfsExports != nil && hadNfsExport {
		if err := d.NfsExports.MarkBucketDeleteCleanup(name); err != nil {
			return NewInternal("mark NFS export bucket-delete cleanup: " + err.Error())
		}
	}
	var err error
	if force {
		err = d.Buckets.ForceDeleteBucket(ctx, name)
	} else {
		err = d.Buckets.DeleteBucket(ctx, name)
	}
	if err == nil {
		return cascadeNfsExportAfterBucketDelete(ctx, d, name, force, hadNfsExport)
	}
	if errors.Is(err, storage.ErrBucketNotFound) {
		if err := cascadeNfsExportAfterMissingBucket(ctx, d, name, force, hadNfsExport); err != nil {
			return err
		}
		return NewNotFound("bucket not found")
	}
	if err := clearNfsExportBucketDeleteCleanupAfterError(d, name, hadNfsExport, err); err != nil {
		return err
	}
	if errors.Is(err, storage.ErrBucketNotEmpty) {
		if force {
			return NewRetry("concurrent write during force-delete; retry the request")
		}
		return NewConflict("bucket not empty; use --force to delete all objects", nil)
	}
	return NewInternal("delete bucket: " + err.Error())
}
