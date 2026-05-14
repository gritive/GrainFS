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

// AdminDeleteBucket deletes a bucket. If force is true, all objects are
// removed first; otherwise the bucket must be empty.
func AdminDeleteBucket(ctx context.Context, d *Deps, name string, force bool) error {
	if storage.IsInternalBucket(name) {
		return NewForbidden("cannot delete internal bucket")
	}
	if d.NfsExports != nil {
		if _, ok := d.NfsExports.Get(name); ok {
			return NewConflict("bucket has an NFS export; remove the export first", map[string]any{
				"bucket": name,
				"hint":   "grainfs nfs export remove " + name,
			})
		}
	}
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
	data, err := d.Buckets.GetBucketPolicy(name)
	if err != nil {
		if errors.Is(err, storage.ErrUnsupportedOperation) {
			return BucketPolicyResp{}, NewUnsupported("bucket policy not supported in this configuration", nil)
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
