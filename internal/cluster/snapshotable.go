package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/dgraph-io/badger/v4"

	"github.com/gritive/GrainFS/internal/storage"
)

// ListAllObjects implements storage.Snapshotable by enumerating every
// versioned object record, including non-latest versions and delete markers.
func (b *DistributedBackend) ListAllObjects() ([]storage.SnapshotObject, error) {
	ctx := context.Background()
	buckets, err := b.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	var result []storage.SnapshotObject
	for _, bucket := range buckets {
		if err := b.db.View(func(txn *badger.Txn) error {
			latest := make(map[string]string)
			rawLatPrefix := []byte("lat:" + bucket + "/")
			if err := b.ks().scanGroupPrefix(txn, rawLatPrefix, func(raw []byte, item *badger.Item) error {
				key := string(raw[len(rawLatPrefix):])
				_ = item.Value(func(v []byte) error {
					latest[key] = string(v)
					return nil
				})
				return nil
			}); err != nil {
				return err
			}

			rawObjPrefix := []byte("obj:" + bucket + "/")
			return b.ks().scanGroupPrefix(txn, rawObjPrefix, func(raw []byte, item *badger.Item) error {
				rest := string(raw[len(rawObjPrefix):])
				slash := strings.LastIndex(rest, "/")
				if slash < 0 {
					return nil
				}
				key := rest[:slash]
				versionID := rest[slash+1:]
				if key == "" || versionID == "" {
					return nil
				}
				var meta objectMeta
				v, err := b.itemValueCopy(item)
				if err != nil {
					return nil
				}
				meta, err = unmarshalObjectMeta(v)
				if err != nil {
					return nil
				}
				result = append(result, storage.SnapshotObject{
					Bucket:         bucket,
					Key:            key,
					ETag:           meta.ETag,
					Size:           meta.Size,
					ContentType:    meta.ContentType,
					Modified:       meta.LastModified,
					VersionID:      versionID,
					IsDeleteMarker: meta.ETag == deleteMarkerETag,
					IsLatest:       latest[key] == versionID,
					ACL:            meta.ACL,
					SSEAlgorithm:   meta.SSEAlgorithm,
					// Tags copied (not aliased) — meta's backing bytes are reused
					// by badger once the View tx returns. Mirror of LocalBackend
					// fix in b64521bf so snapshot Tags survive ListAllObjects.
					Tags:      append([]storage.Tag(nil), meta.Tags...),
					Segments:  append([]storage.SegmentRef(nil), meta.Segments...),
					Coalesced: coalescedRefsFromMeta(meta.Coalesced),
				})
				return nil
			})
		}); err != nil {
			return nil, fmt.Errorf("list objects in bucket %s: %w", bucket, err)
		}
	}
	return result, nil
}

// ListAllBuckets implements storage.BucketSnapshotable by capturing bucket
// metadata persisted in the cluster FSM.
func (b *DistributedBackend) ListAllBuckets() ([]storage.SnapshotBucket, error) {
	buckets, err := b.ListBuckets(context.Background())
	if err != nil {
		return nil, err
	}
	out := make([]storage.SnapshotBucket, 0, len(buckets))
	for _, bucket := range buckets {
		state, err := b.GetBucketVersioning(bucket)
		if err != nil {
			return nil, fmt.Errorf("get bucket versioning %s: %w", bucket, err)
		}
		out = append(out, storage.SnapshotBucket{
			Name:            bucket,
			VersioningState: state,
		})
	}
	return out, nil
}

// RestoreBuckets implements storage.BucketSnapshotable by recreating buckets
// and restoring their versioning metadata through Raft proposals.
func (b *DistributedBackend) RestoreBuckets(buckets []storage.SnapshotBucket) error {
	ctx := context.Background()
	for _, bucket := range buckets {
		if bucket.Name == "" {
			continue
		}
		if err := b.HeadBucket(ctx, bucket.Name); err != nil {
			if !errors.Is(err, storage.ErrBucketNotFound) {
				return err
			}
			if err := b.propose(ctx, CmdCreateBucket, CreateBucketCmd{Bucket: bucket.Name}); err != nil {
				return fmt.Errorf("restore bucket %s: %w", bucket.Name, err)
			}
		}
		state := bucket.VersioningState
		if state == "" {
			state = "Unversioned"
		}
		if err := b.propose(ctx, CmdSetBucketVersioning, SetBucketVersioningCmd{
			Bucket: bucket.Name,
			State:  state,
		}); err != nil {
			return fmt.Errorf("restore bucket versioning %s: %w", bucket.Name, err)
		}
	}
	return nil
}

// RestoreObjects implements storage.Snapshotable.
// It hard-deletes metadata versions absent from the snapshot, then reproposes
// metadata for every snapshot version. Delete markers do not require blobs.
func (b *DistributedBackend) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	ctx := context.Background()
	objects = b.resolveRestoreObjectVersionIDs(objects)
	// Index snapshot objects by bucket+key+version.
	want := make(map[string]storage.SnapshotObject, len(objects))
	for _, o := range objects {
		want[o.Bucket+"\x00"+o.Key+"\x00"+o.VersionID] = o
	}

	type latEntry struct{ bucket, key, versionID string }
	var toDelete []latEntry

	buckets, err := b.ListBuckets(ctx)
	if err != nil {
		return 0, nil, err
	}
	for _, bucket := range buckets {
		if err := b.db.View(func(txn *badger.Txn) error {
			rawObjPrefix := []byte("obj:" + bucket + "/")
			return b.ks().scanGroupPrefix(txn, rawObjPrefix, func(raw []byte, _ *badger.Item) error {
				rest := string(raw[len(rawObjPrefix):])
				slash := strings.LastIndex(rest, "/")
				if slash < 0 {
					return nil
				}
				key := rest[:slash]
				versionID := rest[slash+1:]
				if _, wanted := want[bucket+"\x00"+key+"\x00"+versionID]; wanted {
					return nil
				}
				toDelete = append(toDelete, latEntry{bucket, key, versionID})
				return nil
			})
		}); err != nil {
			return 0, nil, fmt.Errorf("scan bucket %s: %w", bucket, err)
		}
	}

	// Hard-delete metadata for objects absent from the snapshot.
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
		if !snap.IsDeleteMarker && snap.VersionID != "" && !b.blobExistsForRestore(snap) {
			stale = append(stale, storage.StaleBlob{
				Bucket:       snap.Bucket,
				Key:          snap.Key,
				ExpectedETag: snap.ETag,
			})
			continue
		}
		placement := b.restorePlacementMeta(snap)
		preserveLatest := snap.VersionID != "" && !snap.IsLatest
		if err := b.propose(ctx, CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket:           snap.Bucket,
			Key:              snap.Key,
			Size:             snap.Size,
			ContentType:      snap.ContentType,
			ETag:             snap.ETag,
			ModTime:          snap.Modified,
			VersionID:        snap.VersionID,
			ECData:           placement.ECData,
			ECParity:         placement.ECParity,
			NodeIDs:          placement.NodeIDs,
			PlacementGroupID: placement.PlacementGroupID,
			SSEAlgorithm:     snap.SSEAlgorithm,
			PreserveLatest:   preserveLatest,
			IsDeleteMarker:   snap.IsDeleteMarker,
			Tags:             snap.Tags,
		}); err != nil {
			return count, stale, fmt.Errorf("restore meta %s/%s: %w", snap.Bucket, snap.Key, err)
		}
		count++
	}
	return count, stale, nil
}

func (b *DistributedBackend) resolveRestoreObjectVersionIDs(objects []storage.SnapshotObject) []storage.SnapshotObject {
	out := make([]storage.SnapshotObject, len(objects))
	copy(out, objects)
	for i, obj := range out {
		if obj.IsDeleteMarker {
			continue
		}
		if current, err := b.HeadObject(context.Background(), obj.Bucket, obj.Key); err == nil && current != nil {
			if current.ETag == obj.ETag && current.Size == obj.Size && current.VersionID != "" {
				out[i].VersionID = current.VersionID
				continue
			}
		}
		if versionID := b.latestMatchingObjectVersionID(obj); versionID != "" {
			out[i].VersionID = versionID
			continue
		}
		if obj.VersionID != "" {
			continue
		}
	}
	return out
}

func (b *DistributedBackend) restorePlacementMeta(snap storage.SnapshotObject) PlacementMeta {
	if snap.IsDeleteMarker || snap.VersionID == "" {
		return PlacementMeta{}
	}
	obj, placement, err := b.headObjectMetaV(snap.Bucket, snap.Key, snap.VersionID)
	if err != nil || obj == nil {
		return PlacementMeta{}
	}
	if obj.ETag != snap.ETag || obj.Size != snap.Size {
		return PlacementMeta{}
	}
	return placement
}

func (b *DistributedBackend) latestMatchingObjectVersionID(obj storage.SnapshotObject) string {
	versionID := ""
	_ = b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(b.ks().LatestKey(obj.Bucket, obj.Key))
		if err != nil {
			return nil
		}
		if err := item.Value(func(v []byte) error {
			versionID = string(v)
			return nil
		}); err != nil {
			versionID = ""
			return nil
		}
		if versionID == "" {
			return nil
		}
		item, err = txn.Get(b.ks().ObjectMetaKeyV(obj.Bucket, obj.Key, versionID))
		if err != nil {
			versionID = ""
			return nil
		}
		v, err := b.itemValueCopy(item)
		if err != nil {
			versionID = ""
			return nil
		}
		meta, err := unmarshalObjectMeta(v)
		if err != nil {
			versionID = ""
			return nil
		}
		if meta.ETag != obj.ETag || meta.Size != obj.Size {
			versionID = ""
		}
		return nil
	})
	return versionID
}

func (b *DistributedBackend) blobExistsForRestore(snap storage.SnapshotObject) bool {
	if obj, err := b.HeadObject(context.Background(), snap.Bucket, snap.Key); err == nil && obj != nil {
		if obj.ETag == snap.ETag && obj.Size == snap.Size {
			return true
		}
	}
	return b.blobExists(snap.Bucket, snap.Key, snap.VersionID)
}

// blobExists checks whether the blob for the given object version exists on
// this node's local storage (N× versioned path, legacy unversioned path, or
// first EC shard). When versionID is empty (e.g. from WAL replay that doesn't
// record versionIDs), resolve the current latest pointer from the FSM.
// coalescedRefsFromMeta maps cluster CoalescedShardRef → storage.CoalescedRef,
// carrying the coalesced-blob chunk identifier (the field ChunkLocators reads).
func coalescedRefsFromMeta(in []CoalescedShardRef) []storage.CoalescedRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]storage.CoalescedRef, len(in))
	for i, c := range in {
		out[i] = storage.CoalescedRef{CoalescedID: c.CoalescedID}
	}
	return out
}

func (b *DistributedBackend) blobExists(bucket, key, versionID string) bool {
	if versionID == "" {
		// Resolve versionID from the lat: pointer so WAL-replayed objects
		// (which carry no versionID) can still be located on disk.
		_ = b.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(b.ks().LatestKey(bucket, key))
			if err != nil {
				return err
			}
			return item.Value(func(v []byte) error {
				versionID = string(v)
				return nil
			})
		})
	}
	if versionID != "" {
		if rc, _, err := b.GetObjectVersion(bucket, key, versionID); err == nil {
			_ = rc.Close()
			return true
		}
	}
	// Versioned N× path.
	if versionID != "" {
		if _, err := os.Stat(b.objectPathV(bucket, key, versionID)); err == nil {
			return true
		}
	}
	// EC shard path: presence of shard_0 is sufficient evidence.
	if b.currentECConfig().IsActive(len(b.configuredNodeList())) && versionID != "" {
		paths := b.ShardPaths(bucket, key, versionID, b.currentECConfig().NumShards())
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
