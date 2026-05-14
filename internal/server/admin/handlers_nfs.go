package admin

import (
	"context"
	"errors"

	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/storage"
)

type NfsExportServiceAdapter struct {
	Svc *nfsexport.ExportService
}

func (a *NfsExportServiceAdapter) Upsert(ctx context.Context, bucket string, p NfsExportUpsertParams) error {
	return a.Svc.Upsert(ctx, bucket, nfsexport.UpsertParams{ReadOnly: p.ReadOnly})
}

func (a *NfsExportServiceAdapter) Delete(ctx context.Context, bucket string) error {
	return a.Svc.Delete(ctx, bucket)
}

func (a *NfsExportServiceAdapter) Get(bucket string) (NfsExportInfo, bool) {
	cfg, ok := a.Svc.Get(bucket)
	if !ok {
		return NfsExportInfo{}, false
	}
	return NfsExportInfo{
		Bucket:     bucket,
		ReadOnly:   cfg.ReadOnly,
		FsidMajor:  cfg.FsidMajor,
		FsidMinor:  cfg.FsidMinor,
		Generation: cfg.Generation,
	}, true
}

func (a *NfsExportServiceAdapter) List() []NfsExportInfo {
	names := a.Svc.List()
	out := make([]NfsExportInfo, 0, len(names))
	for _, name := range names {
		if info, ok := a.Get(name); ok {
			out = append(out, info)
		}
	}
	return out
}

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
	if err := d.NfsExports.Upsert(ctx, req.Bucket, NfsExportUpsertParams{ReadOnly: req.ReadOnly}); err != nil {
		if errors.Is(err, nfsexport.ErrPropagationBarrierRequired) {
			return NfsExportInfo{}, NewUnsupported("NFS export changes require propagation support in multi-node clusters", nil)
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
		return NewInternal("delete NFS export: " + err.Error())
	}
	return nil
}
