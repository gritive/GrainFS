package cluster

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

func (b *DistributedBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	return b.deleteObjectCtx(ctx, bucket, key)
}

// DeleteObjectReturningMarker satisfies server.VersionedSoftDeleter. Same
// tombstone semantics as DeleteObject but returns the delete marker's
// VersionID so the S3 handler can surface it in the response header.
func (b *DistributedBackend) DeleteObjectReturningMarker(bucket, key string) (string, error) {
	return b.deleteObjectWithMarker(context.Background(), bucket, key)
}

// deleteObjectCtx is the ctx-aware core used by DeleteObject so that HTTP
// request cancellation propagates into the Raft propose call.
func (b *DistributedBackend) deleteObjectCtx(ctx context.Context, bucket, key string) error {
	_, err := b.deleteObjectWithMarker(ctx, bucket, key)
	return err
}

// deleteObjectWithMarker is the single implementation shared by DeleteObject
// and DeleteObjectReturningMarker.
//
// Tombstone semantics: creates a delete marker as a new version. Prior version
// data remains addressable via GetObjectVersion and is NOT physically removed
// here. Hard-delete of a specific version goes through DeleteObjectVersion
// (used by lifecycle/scrubber).
//
// For backward compatibility with the legacy N× on-disk layout, we also
// remove the unversioned local object file if present — it's guaranteed to
// be stale (superseded by a versioned path) and keeping it risks GetObject
// serving it as a fallback read.
func (b *DistributedBackend) deleteObjectWithMarker(ctx context.Context, bucket, key string) (string, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return "", err
	}
	if storage.IsInternalBucket(bucket) {
		return "", b.deleteInternalObject(bucket, key)
	}
	os.Remove(b.objectPath(bucket, key))
	markerID := newVersionID()
	if err := b.propose(ctx, CmdDeleteObject, DeleteObjectCmd{
		Bucket:    bucket,
		Key:       key,
		VersionID: markerID,
	}); err != nil {
		return "", err
	}
	// Write a tombstone to quorum meta so scatter-gather LIST sees the delete.
	// Read the existing placement to determine which nodes to write to.
	// Best-effort: if the object has no quorum meta (pre-Phase-3), skip.
	if b.shardSvc != nil {
		if existing, qerr := b.readQuorumMetaCmd(bucket, key); qerr == nil && len(existing.NodeIDs) > 0 {
			_ = b.writeQuorumMeta(ctx, PutObjectMetaCmd{
				Bucket:         bucket,
				Key:            key,
				VersionID:      markerID,
				ModTime:        time.Now().Unix(),
				IsDeleteMarker: true,
				ECData:         existing.ECData,
				NodeIDs:        existing.NodeIDs,
			})
		} else if !errors.Is(qerr, storage.ErrObjectNotFound) && qerr != nil {
			_ = b.shardSvc.deleteQuorumMetaLocal(bucket, key) // fallback: remove stale file
		}
	}
	return markerID, nil
}

func (b *DistributedBackend) deleteInternalObject(bucket, key string) error {
	objPath := b.internalObjectPath(bucket, key)
	_ = os.Remove(objPath.path)
	b.internalPathCache.Delete(internalObjectCacheKey{bucket: bucket, key: key})
	b.internalSizeCache.Delete(internalObjectCacheKey{bucket: bucket, key: key})
	return b.store.Update(func(txn MetadataTxn) error {
		if item, err := txn.Get(b.ks().LatestKey(bucket, key)); err == nil {
			if err := item.Value(func(v []byte) error {
				versionID := string(v)
				if versionID == "" {
					return nil
				}
				_ = os.Remove(b.objectPathV(bucket, key, versionID))
				if err := txn.Delete(b.ks().ObjectMetaKeyV(bucket, key, versionID)); err != nil && err != ErrMetaKeyNotFound {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		} else if err != ErrMetaKeyNotFound {
			return err
		}
		for _, dbKey := range [][]byte{
			b.ks().LatestKey(bucket, key),
			b.ks().ObjectMetaKey(bucket, key),
		} {
			if err := txn.Delete(dbKey); err != nil && err != ErrMetaKeyNotFound {
				return err
			}
		}
		return nil
	})
}
