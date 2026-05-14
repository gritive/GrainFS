package admin

import (
	"context"
	"errors"
	"time"

	"github.com/gritive/GrainFS/internal/nfs4server"
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

func (a *NfsExportServiceAdapter) DeleteForBucketDelete(ctx context.Context, bucket string, force bool) error {
	return a.Svc.DeleteForBucketDelete(ctx, bucket, force)
}

func (a *NfsExportServiceAdapter) RestoreForBucketDelete(ctx context.Context, info NfsExportInfo) error {
	return a.Svc.RestoreForBucketDelete(ctx, info.Bucket, nfsexport.Config{
		ReadOnly:   info.ReadOnly,
		FsidMajor:  info.FsidMajor,
		FsidMinor:  info.FsidMinor,
		Generation: info.Generation,
	})
}

func (a *NfsExportServiceAdapter) MarkBucketDeleteCleanup(bucket string) error {
	return a.Svc.MarkBucketDeleteCleanup(bucket)
}

func (a *NfsExportServiceAdapter) ClearBucketDeleteCleanup(bucket string) error {
	return a.Svc.ClearBucketDeleteCleanup(bucket)
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
		if errors.Is(err, nfsexport.ErrPropagationTimeout) {
			return NfsExportInfo{}, NewExportPropagationTimeout(req.Bucket)
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

func AdminNfsExportDebug(ctx context.Context, d *Deps, bucket string) (ExportDebugResp, error) {
	if d.NfsExports == nil {
		return ExportDebugResp{}, NewUnsupported("NFS export admin not configured on this node", nil)
	}
	if storage.IsInternalBucket(bucket) {
		return ExportDebugResp{}, NewForbidden("cannot inspect internal bucket")
	}
	resp := ExportDebugResp{Bucket: bucket}
	if info, ok := d.NfsExports.Get(bucket); ok {
		resp.Registered = true
		resp.ReadOnly = info.ReadOnly
		resp.FsidMajor = info.FsidMajor
		resp.FsidMinor = info.FsidMinor
		resp.Generation = info.Generation
	}
	if d.Buckets != nil {
		if err := d.Buckets.HeadBucket(ctx, bucket); err == nil {
			resp.BackendBucket.Exists = true
			count, err := d.Buckets.CountObjects(ctx, bucket)
			if err != nil {
				return ExportDebugResp{}, NewInternal("count objects: " + err.Error())
			}
			resp.BackendBucket.ObjectCount = count
		} else if !errors.Is(err, storage.ErrBucketNotFound) {
			return ExportDebugResp{}, NewInternal("head bucket: " + err.Error())
		}
	}
	if d.NFSDiag != nil {
		resp.RecentLookups = exportDebugLookups(d.NFSDiag.RecentLookups(bucket, time.Minute))
		resp.ActiveMountClients = d.NFSDiag.ActiveMountClients(bucket)
	}
	return resp, nil
}

func exportDebugLookups(records []nfs4server.LookupRecord) []ExportDebugLookup {
	out := make([]ExportDebugLookup, 0, len(records))
	for _, rec := range records {
		out = append(out, ExportDebugLookup{
			Client: rec.Client,
			Bucket: rec.Bucket,
			Result: rec.Result,
			AtUnix: rec.At.Unix(),
		})
	}
	return out
}
