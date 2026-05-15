package admin

import (
	"context"

	"github.com/gritive/GrainFS/internal/nfsexport"
)

type NfsExportServiceAdapter struct {
	Svc *nfsexport.ExportService
}

func (a *NfsExportServiceAdapter) Create(ctx context.Context, bucket string, p NfsExportUpsertParams) error {
	return a.Svc.Create(ctx, bucket, nfsexport.UpsertParams{ReadOnly: p.ReadOnly})
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
