package cluster

import (
	"context"
	"fmt"
	"os"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/storage"
)

// ListAllObjects implements storage.Snapshotable by enumerating every live
// (non-tombstone) object across all buckets via the lat: latest-pointer index.
func (b *DistributedBackend) ListAllObjects() ([]storage.SnapshotObject, error) {
	buckets, err := b.ListBuckets()
	if err != nil {
		return nil, err
	}
	var result []storage.SnapshotObject
	for _, bucket := range buckets {
		if err := b.db.View(func(txn *badger.Txn) error {
			latPrefix := []byte("lat:" + bucket + "/")
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(latPrefix); it.ValidForPrefix(latPrefix); it.Next() {
				key := string(it.Item().Key()[len(latPrefix):])
				var versionID string
				if err := it.Item().Value(func(v []byte) error {
					versionID = string(v)
					return nil
				}); err != nil || versionID == "" {
					continue
				}
				metaItem, err := txn.Get(objectMetaKeyV(bucket, key, versionID))
				if err != nil {
					continue
				}
				var meta objectMeta
				if err := metaItem.Value(func(v []byte) error {
					var derr error
					meta, derr = unmarshalObjectMeta(v)
					return derr
				}); err != nil {
					continue
				}
				if meta.ETag == deleteMarkerETag {
					continue
				}
				result = append(result, storage.SnapshotObject{
					Bucket:      bucket,
					Key:         key,
					ETag:        meta.ETag,
					Size:        meta.Size,
					ContentType: meta.ContentType,
					Modified:    meta.LastModified,
					VersionID:   versionID,
					IsLatest:    true,
				})
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("list objects in bucket %s: %w", bucket, err)
		}
	}
	return result, nil
}

// RestoreObjects implements storage.Snapshotable.
// It hard-deletes (metadata only) any versioned object whose bucket+key is
// absent from objects, then re-proposes CmdPutObjectMeta for each snapshot
// object to reset the latest pointer. Objects whose blob is missing on disk
// are returned as StaleBlob entries without being restored.
func (b *DistributedBackend) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	// Index snapshot objects by bucket+key.
	want := make(map[string]storage.SnapshotObject, len(objects))
	for _, o := range objects {
		want[o.Bucket+"\x00"+o.Key] = o
	}

	// Collect current latest-pointer entries that are absent from the snapshot.
	type latEntry struct{ bucket, key, versionID string }
	var toDelete []latEntry

	buckets, err := b.ListBuckets()
	if err != nil {
		return 0, nil, err
	}
	for _, bucket := range buckets {
		if err := b.db.View(func(txn *badger.Txn) error {
			latPrefix := []byte("lat:" + bucket + "/")
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(latPrefix); it.ValidForPrefix(latPrefix); it.Next() {
				key := string(it.Item().Key()[len(latPrefix):])
				if _, wanted := want[bucket+"\x00"+key]; wanted {
					continue
				}
				var versionID string
				_ = it.Item().Value(func(v []byte) error { versionID = string(v); return nil })
				toDelete = append(toDelete, latEntry{bucket, key, versionID})
			}
			return nil
		}); err != nil {
			return 0, nil, fmt.Errorf("scan bucket %s: %w", bucket, err)
		}
	}

	// Hard-delete metadata for objects absent from the snapshot.
	ctx := context.Background()
	for _, d := range toDelete {
		if err := b.propose(ctx, CmdDeleteObjectVersion, DeleteObjectVersionCmd{
			Bucket:    d.bucket,
			Key:       d.key,
			VersionID: d.versionID,
		}); err != nil {
			return 0, nil, fmt.Errorf("delete %s/%s@%s: %w", d.bucket, d.key, d.versionID, err)
		}
	}

	// Verify and restore each snapshot object.
	var stale []storage.StaleBlob
	var count int
	for _, snap := range objects {
		if !b.blobExists(snap.Bucket, snap.Key, snap.VersionID) {
			stale = append(stale, storage.StaleBlob{
				Bucket:       snap.Bucket,
				Key:          snap.Key,
				ExpectedETag: snap.ETag,
			})
			continue
		}
		if err := b.propose(ctx, CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket:      snap.Bucket,
			Key:         snap.Key,
			Size:        snap.Size,
			ContentType: snap.ContentType,
			ETag:        snap.ETag,
			ModTime:     snap.Modified,
			VersionID:   snap.VersionID,
		}); err != nil {
			return count, stale, fmt.Errorf("restore meta %s/%s: %w", snap.Bucket, snap.Key, err)
		}
		count++
	}
	return count, stale, nil
}

// blobExists checks whether the blob for the given object version exists on
// this node's local storage (N× versioned path, legacy unversioned path, or
// first EC shard). When versionID is empty (e.g. from WAL replay that doesn't
// record versionIDs), resolve the current latest pointer from the FSM.
func (b *DistributedBackend) blobExists(bucket, key, versionID string) bool {
	if versionID == "" {
		// Resolve versionID from the lat: pointer so WAL-replayed objects
		// (which carry no versionID) can still be located on disk.
		_ = b.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte("lat:" + bucket + "/" + key))
			if err != nil {
				return err
			}
			return item.Value(func(v []byte) error {
				versionID = string(v)
				return nil
			})
		})
	}
	// Versioned N× path.
	if versionID != "" {
		if _, err := os.Stat(b.objectPathV(bucket, key, versionID)); err == nil {
			return true
		}
	}
	// EC shard path: presence of shard_0 is sufficient evidence.
	if b.ecConfig.IsActive(len(b.allNodes)) && versionID != "" {
		paths := b.ShardPaths(bucket, key, versionID, b.ecConfig.NumShards())
		if len(paths) > 0 {
			if _, err := os.Stat(paths[0]); err == nil {
				return true
			}
		}
	}
	// Legacy unversioned path (read-fallback for pre-versioning data).
	if _, err := os.Stat(b.objectPath(bucket, key)); err == nil {
		return true
	}
	return false
}
