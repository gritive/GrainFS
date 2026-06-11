package cluster

import (
	"context"

	"github.com/gritive/GrainFS/internal/storage"
)

// objectFromCmd converts a PutObjectMetaCmd (from scatter-gather LIST) into
// a storage.Object suitable for callers. Tombstones are pre-filtered by
// scatterGatherList; this function is never called with IsDeleteMarker=true.
func objectFromCmd(cmd PutObjectMetaCmd) *storage.Object {
	m := buildPutObjectMeta(cmd)
	return &storage.Object{
		Key:              m.Key,
		Size:             m.Size,
		ContentType:      m.ContentType,
		ETag:             m.ETag,
		LastModified:     m.LastModified,
		VersionID:        cmd.VersionID,
		ACL:              m.ACL,
		UserMetadata:     cloneStringMap(m.UserMetadata),
		SSEAlgorithm:     m.SSEAlgorithm,
		PlacementGroupID: m.PlacementGroupID,
		ECData:           m.ECData,
		ECParity:         m.ECParity,
		StripeBytes:      m.StripeBytes,
		NodeIDs:          cloneStringSlice(m.NodeIDs),
		Parts:            m.Parts,
		Tags:             append([]storage.Tag(nil), m.Tags...),
	}
}

func (b *DistributedBackend) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	entries, err := b.scatterGatherList(ctx, bucket, prefix)
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
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, false, err
	}
	entries, err := b.scatterGatherList(ctx, bucket, prefix)
	if err != nil {
		return nil, false, err
	}
	var objects []*storage.Object
	truncated := false
	for _, e := range entries {
		if marker != "" && e.Key <= marker {
			continue
		}
		if len(objects) >= maxKeys {
			truncated = true
			break
		}
		objects = append(objects, objectFromCmd(e))
	}
	return objects, truncated, nil
}

func (b *DistributedBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	entries, err := b.scatterGatherList(ctx, bucket, prefix)
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
