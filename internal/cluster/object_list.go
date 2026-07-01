package cluster

import (
	"context"

	"github.com/gritive/GrainFS/internal/storage"
)

// objectFromCmd converts a PutObjectMetaCmd (from scatter-gather LIST) into
// a storage.Object suitable for callers. Tombstones are pre-filtered by
// scatterGatherList; this function is never called with IsDeleteMarker=true.
func objectFromCmd(cmd PutObjectMetaCmd) *storage.Object {
	etag := cmd.ETag
	if cmd.IsDeleteMarker {
		etag = deleteMarkerETag
	}
	return &storage.Object{
		Key:              cmd.Key,
		Size:             cmd.Size,
		ContentType:      cmd.ContentType,
		ETag:             etag,
		LastModified:     cmd.ModTime,
		VersionID:        cmd.VersionID,
		ACL:              cmd.ACL,
		UserMetadata:     cloneStringMap(cmd.UserMetadata),
		SSEAlgorithm:     cmd.SSEAlgorithm,
		PlacementGroupID: cmd.PlacementGroupID,
		ECData:           cmd.ECData,
		ECParity:         cmd.ECParity,
		StripeBytes:      cmd.StripeBytes,
		NodeIDs:          cloneStringSlice(cmd.NodeIDs),
		Parts:            cmd.Parts,
		Tags:             append([]storage.Tag(nil), cmd.Tags...),
	}
}

// listLatestEntries chooses the per-version derive for versioning-enabled
// buckets, else the legacy latest-only scatter-gather. Both return sorted-by-key,
// tombstone-excluded []PutObjectMetaCmd so the three LIST methods are identical
// downstream. Internal buckets and non-versioned/suspended buckets keep legacy.
//
// The derive is gated on the STAMPED ctx flag ONLY (bucketVersioningFromContext),
// not bucketVersioningEnabled's local-read fallback. The S3 LIST edge stamps the
// ctx (PR-A: list_objects_runtime.go) and the forward receiver re-stamps it, so
// the S3 path's ctx is always resolved → derive activates. Internal/non-S3 LIST
// consumers (DeleteBucket empty-check, metrics) call with an
// UNSTAMPED context.Background() → not resolved → legacy scatterGatherList (their
// existing quorum-acked latest-only view). This keeps the derive scoped to the S3
// LIST surface and avoids a DeleteBucket data-loss path where a best-effort
// per-version write failure could make the derive omit a live object.
func (b *DistributedBackend) listLatestEntries(ctx context.Context, bucket, prefix string) ([]PutObjectMetaCmd, error) {
	if enabled, resolved := bucketVersioningFromContext(ctx); resolved && enabled {
		return b.listObjectsPerVersion(ctx, bucket, prefix)
	}
	return b.scatterGatherList(ctx, bucket, prefix)
}

func pageSortedEntries(entries []PutObjectMetaCmd, marker string, maxKeys int) ([]PutObjectMetaCmd, bool) {
	var page []PutObjectMetaCmd
	if maxKeys > 0 {
		page = make([]PutObjectMetaCmd, 0, min(len(entries), maxKeys))
	}
	for _, e := range entries {
		if marker != "" && e.Key <= marker {
			continue
		}
		if len(page) >= maxKeys {
			return page, true
		}
		page = append(page, e)
	}
	return page, false
}

func (b *DistributedBackend) listLatestEntriesPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]PutObjectMetaCmd, bool, error) {
	if enabled, resolved := bucketVersioningFromContext(ctx); resolved && enabled {
		entries, err := b.listObjectsPerVersion(ctx, bucket, prefix)
		if err != nil {
			return nil, false, err
		}
		page, truncated := pageSortedEntries(entries, marker, maxKeys)
		return page, truncated, nil
	}
	return b.qmsOrBuild().scatterGatherListPage(ctx, bucket, prefix, marker, maxKeys)
}

func (b *DistributedBackend) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, err
	}
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	entries, err := b.listLatestEntries(ctx, bucket, prefix)
	if err != nil {
		return nil, err
	}
	objects := make([]*storage.Object, 0, min(len(entries), maxKeys))
	for _, e := range entries {
		if len(objects) >= maxKeys {
			break
		}
		objects = append(objects, objectFromCmd(e))
	}
	return objects, nil
}

// ListObjectsPage returns one S3 ListObjects page, honoring marker-based
// pagination. truncated is true when more matching entries remain after the
// returned page.
func (b *DistributedBackend) ListObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) ([]*storage.Object, bool, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, false, err
	}
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, false, err
	}
	entries, truncated, err := b.listLatestEntriesPage(ctx, bucket, prefix, marker, maxKeys)
	if err != nil {
		return nil, false, err
	}
	objects := make([]*storage.Object, 0, len(entries))
	for _, e := range entries {
		objects = append(objects, objectFromCmd(e))
	}
	return objects, truncated, nil
}

func (b *DistributedBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return err
	}
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	entries, err := b.listLatestEntries(ctx, bucket, prefix)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if ferr := fn(objectFromCmd(e)); ferr != nil {
			return ferr
		}
	}
	return nil
}

// --- Multipart operations ---
