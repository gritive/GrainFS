package cluster

import (
	"context"
	"os"

	"github.com/dgraph-io/badger/v4"
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
	// Remove the local quorum meta file so subsequent reads fall through to
	// BadgerDB and find the delete marker.  Best-effort: raft is authoritative.
	if b.shardSvc != nil {
		_ = b.shardSvc.deleteQuorumMetaLocal(bucket, key)
	}
	return markerID, nil
}

func (b *DistributedBackend) deleteInternalObject(bucket, key string) error {
	objPath := b.internalObjectPath(bucket, key)
	_ = os.Remove(objPath.path)
	b.internalPathCache.Delete(internalObjectCacheKey{bucket: bucket, key: key})
	b.internalSizeCache.Delete(internalObjectCacheKey{bucket: bucket, key: key})
	return b.db.Update(func(txn *badger.Txn) error {
		if item, err := txn.Get(b.ks().LatestKey(bucket, key)); err == nil {
			if err := item.Value(func(v []byte) error {
				versionID := string(v)
				if versionID == "" {
					return nil
				}
				_ = os.Remove(b.objectPathV(bucket, key, versionID))
				if err := txn.Delete(b.ks().ObjectMetaKeyV(bucket, key, versionID)); err != nil && err != badger.ErrKeyNotFound {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		for _, dbKey := range [][]byte{
			b.ks().LatestKey(bucket, key),
			b.ks().ObjectMetaKey(bucket, key),
		} {
			if err := txn.Delete(dbKey); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		return nil
	})
}
