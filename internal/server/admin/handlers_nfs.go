package admin

import (
	"context"
	"errors"
	"strings"

	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/storage"
)

func AdminNfsExportUpsert(ctx context.Context, d *Deps, req NfsExportUpsertReq) (NfsExportInfo, error) {
	if d.NfsExports == nil {
		return NfsExportInfo{}, NewUnsupported("NFS export admin not configured on this node", nil)
	}
	if req.Bucket == "" {
		return NfsExportInfo{}, NewInvalid("bucket is required").WithParam("bucket")
	}
	if storage.IsInternalBucket(req.Bucket) {
		return NfsExportInfo{}, NewForbidden("cannot export internal bucket")
	}
	if d.Buckets == nil {
		return NfsExportInfo{}, NewInternal("bucket service not wired")
	}
	if err := d.Buckets.HeadBucket(ctx, req.Bucket); err != nil {
		if errors.Is(err, storage.ErrBucketNotFound) {
			return NfsExportInfo{}, NewBucketNotFound(req.Bucket)
		}
		return NfsExportInfo{}, NewInternal("head bucket: " + err.Error())
	}
	if err := d.NfsExports.Create(ctx, req.Bucket, NfsExportUpsertParams{ReadOnly: req.ReadOnly}); err != nil {
		if errors.Is(err, nfsexport.ErrPropagationBarrierRequired) {
			return NfsExportInfo{}, NewUnsupported("NFS export changes require propagation support in multi-node clusters", nil)
		}
		if errors.Is(err, nfsexport.ErrPropagationTimeout) {
			return NfsExportInfo{}, NewExportPropagationTimeout(req.Bucket)
		}
		if errors.Is(err, nfsexport.ErrExportExists) || strings.Contains(err.Error(), nfsexport.ErrExportExists.Error()) {
			return NfsExportInfo{}, NewConflict("NFS export already registered; use update to change mode", nil)
		}
		var gateErr *compat.GateRejectError
		if errors.As(err, &gateErr) {
			return NfsExportInfo{}, NewUnsupported(gateErr.PublicMessage(), nil)
		}
		if errors.Is(err, compat.ErrCapabilityRejected) {
			return NfsExportInfo{}, NewUnsupported("NFS export create requires every meta-raft member to advertise nfs_export_create_v1; finish the rolling upgrade before retrying", nil)
		}
		return NfsExportInfo{}, NewInternal("upsert NFS export: " + err.Error())
	}
	info, _ := d.NfsExports.Get(req.Bucket)
	return info, nil
}

func AdminNfsExportUpdate(ctx context.Context, d *Deps, bucket string, req NfsExportUpsertReq) (NfsExportInfo, error) {
	if d.NfsExports == nil {
		return NfsExportInfo{}, NewUnsupported("NFS export admin not configured on this node", nil)
	}
	if _, ok := d.NfsExports.Get(bucket); !ok {
		return NfsExportInfo{}, NewExportNotFound(bucket)
	}
	if err := d.NfsExports.Upsert(ctx, bucket, NfsExportUpsertParams{ReadOnly: req.ReadOnly}); err != nil {
		if errors.Is(err, nfsexport.ErrPropagationBarrierRequired) {
			return NfsExportInfo{}, NewUnsupported("NFS export changes require propagation support in multi-node clusters", nil)
		}
		if errors.Is(err, nfsexport.ErrPropagationTimeout) {
			return NfsExportInfo{}, NewExportPropagationTimeout(bucket)
		}
		return NfsExportInfo{}, NewInternal("update NFS export: " + err.Error())
	}
	info, _ := d.NfsExports.Get(bucket)
	return info, nil
}

func AdminNfsExportGet(_ context.Context, d *Deps, bucket string) (NfsExportInfo, error) {
	if d.NfsExports == nil {
		return NfsExportInfo{}, NewUnsupported("NFS export admin not configured on this node", nil)
	}
	info, ok := d.NfsExports.Get(bucket)
	if !ok {
		return NfsExportInfo{}, NewExportNotFound(bucket)
	}
	return info, nil
}

func AdminNfsExportList(_ context.Context, d *Deps) (ListNfsExportsResp, error) {
	if d.NfsExports == nil {
		return ListNfsExportsResp{}, NewUnsupported("NFS export admin not configured on this node", nil)
	}
	return ListNfsExportsResp{Exports: d.NfsExports.List()}, nil
}

func AdminNfsExportDelete(ctx context.Context, d *Deps, bucket string) error {
	if d.NfsExports == nil {
		return NewUnsupported("NFS export admin not configured on this node", nil)
	}
	if err := d.NfsExports.Delete(ctx, bucket); err != nil {
		if errors.Is(err, nfsexport.ErrPropagationBarrierRequired) {
			return NewUnsupported("NFS export changes require propagation support in multi-node clusters", nil)
		}
		if errors.Is(err, nfsexport.ErrPropagationTimeout) {
			return NewExportPropagationTimeout(bucket)
		}
		return NewInternal("delete NFS export: " + err.Error())
	}
	return nil
}
