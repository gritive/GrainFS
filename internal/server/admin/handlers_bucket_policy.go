package admin

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/gritive/GrainFS/internal/policy"
	"github.com/gritive/GrainFS/internal/storage"
)

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
	if _, err := policy.ParsePolicy(req.Policy); err != nil {
		return NewInvalid("invalid bucket policy: " + err.Error())
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
