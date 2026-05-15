package admin

import (
	"context"
	"errors"
	"time"

	"github.com/gritive/GrainFS/internal/nfs4server"
	"github.com/gritive/GrainFS/internal/storage"
)

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
