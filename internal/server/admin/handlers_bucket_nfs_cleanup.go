package admin

import "context"

func cascadeNfsExportAfterBucketDelete(ctx context.Context, d *Deps, name string, force bool, hadNfsExport bool) error {
	if d.NfsExports == nil {
		return nil
	}
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
	return nil
}

func cascadeNfsExportAfterMissingBucket(ctx context.Context, d *Deps, name string, force bool, hadNfsExport bool) error {
	if d.NfsExports == nil || !hadNfsExport {
		return nil
	}
	if cascadeErr := d.NfsExports.DeleteForBucketDelete(ctx, name, force); cascadeErr != nil {
		return NewInternal("bucket not found; cascade delete NFS export: " + cascadeErr.Error())
	}
	if clearErr := d.NfsExports.ClearBucketDeleteCleanup(name); clearErr != nil {
		return NewInternal("bucket not found; clear NFS export bucket-delete cleanup: " + clearErr.Error())
	}
	return nil
}

func clearNfsExportBucketDeleteCleanupAfterError(d *Deps, name string, hadNfsExport bool, bucketErr error) error {
	if d.NfsExports == nil || !hadNfsExport {
		return nil
	}
	if clearErr := d.NfsExports.ClearBucketDeleteCleanup(name); clearErr != nil {
		return NewInternal("delete bucket: " + bucketErr.Error() + "; clear NFS export bucket-delete cleanup: " + clearErr.Error())
	}
	return nil
}
