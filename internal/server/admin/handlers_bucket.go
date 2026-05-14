package admin

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

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
	filtered := make([]string, 0, len(names))
	for _, n := range names {
		if !storage.IsInternalBucket(n) {
			filtered = append(filtered, n)
		}
	}
	sort.Strings(filtered)

	hasUpstream := map[string]bool{}
	if d.IAM != nil {
		if upstreams, err := d.IAM.ListBucketUpstreams(ctx); err == nil {
			for _, u := range upstreams {
				hasUpstream[u.Bucket] = true
			}
		}
	}

	out := make([]BucketInfo, len(filtered))
	for i, n := range filtered {
		out[i] = BucketInfo{Name: n, HasUpstream: hasUpstream[n]}
	}
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

	if d.IAM != nil {
		if _, err := d.IAM.GetBucketUpstream(ctx, name); err == nil {
			info.HasUpstream = true
		} else if !adminapi.IsCode(err, "not_found") {
			return BucketInfo{}, NewInternal("get bucket upstream: " + err.Error())
		}
	}

	if versioning, err := d.Buckets.GetBucketVersioning(name); err == nil {
		info.Versioning = versioning
	} else if !errors.Is(err, storage.ErrUnsupportedOperation) {
		return BucketInfo{}, NewInternal("get bucket versioning: " + err.Error())
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
		if d.NfsExports != nil {
			_, hasNfsExportAfterDelete := d.NfsExports.Get(name)
			shouldCascade := hadNfsExport || hasNfsExportAfterDelete
			if shouldCascade && !hadNfsExport {
				if err := d.NfsExports.MarkBucketDeleteCleanup(name); err != nil {
					return NewInternal("mark NFS export bucket-delete cleanup after bucket delete: " + err.Error())
				}
			}
			if !shouldCascade {
				return nil
			}
			if err := d.NfsExports.DeleteForBucketDelete(ctx, name, force); err != nil {
				return NewInternal("cascade delete NFS export after bucket delete: " + err.Error())
			}
			if err := d.NfsExports.ClearBucketDeleteCleanup(name); err != nil {
				return NewInternal("clear NFS export bucket-delete cleanup: " + err.Error())
			}
		}
		return nil
	}
	if errors.Is(err, storage.ErrBucketNotFound) {
		if d.NfsExports != nil && hadNfsExport {
			if cascadeErr := d.NfsExports.DeleteForBucketDelete(ctx, name, force); cascadeErr != nil {
				return NewInternal("bucket not found; cascade delete NFS export: " + cascadeErr.Error())
			}
			if clearErr := d.NfsExports.ClearBucketDeleteCleanup(name); clearErr != nil {
				return NewInternal("bucket not found; clear NFS export bucket-delete cleanup: " + clearErr.Error())
			}
		}
		return NewNotFound("bucket not found")
	}
	if d.NfsExports != nil && hadNfsExport {
		if clearErr := d.NfsExports.ClearBucketDeleteCleanup(name); clearErr != nil {
			return NewInternal("delete bucket: " + err.Error() + "; clear NFS export bucket-delete cleanup: " + clearErr.Error())
		}
	}
	if errors.Is(err, storage.ErrBucketNotEmpty) {
		if force {
			return NewRetry("concurrent write during force-delete; retry the request")
		}
		return NewConflict("bucket not empty; use --force to delete all objects", nil)
	}
	return NewInternal("delete bucket: " + err.Error())
}

func AdminGetBucketPolicy(ctx context.Context, d *Deps, name string) (BucketPolicyResp, error) {
	if storage.IsInternalBucket(name) {
		return BucketPolicyResp{}, NewForbidden("cannot access internal bucket")
	}
	if err := checkBucketExists(ctx, d, name); err != nil {
		return BucketPolicyResp{}, err
	}
	data, err := d.Buckets.GetBucketPolicy(name)
	if err != nil {
		if errors.Is(err, storage.ErrUnsupportedOperation) {
			return BucketPolicyResp{}, NewUnsupported("bucket policy not supported in this configuration", nil)
		}
		if errors.Is(err, storage.ErrBucketNotFound) {
			return BucketPolicyResp{}, NewNotFound("no bucket policy set")
		}
		return BucketPolicyResp{}, NewInternal("get bucket policy: " + err.Error())
	}
	if data == nil {
		return BucketPolicyResp{}, NewNotFound("no bucket policy set")
	}
	return BucketPolicyResp{Policy: json.RawMessage(data)}, nil
}

func AdminSetBucketPolicy(ctx context.Context, d *Deps, name string, req BucketPolicySetReq) error {
	if storage.IsInternalBucket(name) {
		return NewForbidden("cannot access internal bucket")
	}
	if err := checkBucketExists(ctx, d, name); err != nil {
		return err
	}
	if err := d.Buckets.SetBucketPolicy(name, []byte(req.Policy)); err != nil {
		if errors.Is(err, storage.ErrUnsupportedOperation) {
			return NewUnsupported("bucket policy not supported in this configuration", nil)
		}
		return NewInternal("set bucket policy: " + err.Error())
	}
	return nil
}

func AdminDeleteBucketPolicy(ctx context.Context, d *Deps, name string) error {
	if storage.IsInternalBucket(name) {
		return NewForbidden("cannot access internal bucket")
	}
	if err := checkBucketExists(ctx, d, name); err != nil {
		return err
	}
	if err := d.Buckets.DeleteBucketPolicy(name); err != nil {
		if errors.Is(err, storage.ErrUnsupportedOperation) {
			return NewUnsupported("bucket policy not supported in this configuration", nil)
		}
		return NewInternal("delete bucket policy: " + err.Error())
	}
	return nil
}

func AdminGetBucketVersioning(ctx context.Context, d *Deps, name string) (BucketVersioningResp, error) {
	if storage.IsInternalBucket(name) {
		return BucketVersioningResp{}, NewForbidden("cannot access internal bucket")
	}
	if err := checkBucketExists(ctx, d, name); err != nil {
		return BucketVersioningResp{}, err
	}
	status, err := d.Buckets.GetBucketVersioning(name)
	if err != nil {
		if errors.Is(err, storage.ErrUnsupportedOperation) {
			return BucketVersioningResp{}, NewUnsupported("bucket versioning not supported in this configuration", nil)
		}
		return BucketVersioningResp{}, NewInternal("get bucket versioning: " + err.Error())
	}
	return BucketVersioningResp{Status: status}, nil
}

func AdminSetBucketVersioning(ctx context.Context, d *Deps, name string, req BucketVersioningSetReq) error {
	if storage.IsInternalBucket(name) {
		return NewForbidden("cannot access internal bucket")
	}
	if err := checkBucketExists(ctx, d, name); err != nil {
		return err
	}
	if req.Status != "Enabled" && req.Status != "Suspended" {
		return NewInvalid(`status must be "Enabled" or "Suspended"`)
	}
	if err := d.Buckets.SetBucketVersioning(name, req.Status); err != nil {
		if errors.Is(err, storage.ErrUnsupportedOperation) {
			return NewUnsupported("bucket versioning not supported in this configuration", nil)
		}
		return NewInternal("set bucket versioning: " + err.Error())
	}
	return nil
}
