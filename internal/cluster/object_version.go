package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"
)

// HeadObjectVersion returns metadata for a specific version. Returns
// storage.ErrObjectNotFound if the version doesn't exist or is a delete marker.
func (b *DistributedBackend) HeadObjectVersion(bucket, key, versionID string) (*storage.Object, error) {
	obj, _, err := b.headObjectMetaV(bucket, key, versionID)
	return obj, err
}

// headObjectMetaV reads a specific version's metadata and its EC placement
// fields in one transaction. Returns storage.ErrObjectNotFound for a missing
// version and storage.ErrMethodNotAllowed for a delete-marker version (S3
// semantics — the server handler maps that to a 405 with x-amz-delete-marker).
// Parallels headObjectMeta, which does the same for the latest version.
func (b *DistributedBackend) headObjectMetaV(bucket, key, versionID string) (*storage.Object, PlacementMeta, error) {
	ctx := context.Background()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, PlacementMeta{}, err
	}
	// Phase 3: quorum meta is the primary source for non-internal user objects.
	if !storage.IsInternalBucket(bucket) {
		if obj, pm, err := b.readQuorumMeta(bucket, key); err == nil && obj.VersionID == versionID {
			return obj, pm, nil
		}
	}
	var obj storage.Object
	var placement PlacementMeta
	err := b.store.View(func(txn MetadataTxn) error {
		item, err := txn.Get(b.ks().ObjectMetaKeyV(bucket, key, versionID))
		if err == ErrMetaKeyNotFound {
			return storage.ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		val, err := b.itemValueCopy(item)
		if err != nil {
			return err
		}
		m, err := unmarshalObjectMeta(val)
		if err != nil {
			return err
		}
		if m.ETag == deleteMarkerETag {
			return storage.ErrMethodNotAllowed
		}
		obj = storage.Object{
			Key:              m.Key,
			Size:             m.Size,
			ContentType:      m.ContentType,
			ETag:             m.ETag,
			LastModified:     m.LastModified,
			VersionID:        versionID,
			ACL:              m.ACL,
			UserMetadata:     cloneStringMap(m.UserMetadata),
			SSEAlgorithm:     m.SSEAlgorithm,
			PlacementGroupID: m.PlacementGroupID,
			ECData:           m.ECData,
			ECParity:         m.ECParity,
			StripeBytes:      m.StripeBytes,
			NodeIDs:          cloneStringSlice(m.NodeIDs),
			Segments:         m.Segments,
			Parts:            m.Parts,
			Coalesced:        coalescedRefsToStorage(m.Coalesced),
			IsAppendable:     m.IsAppendable,
			// Tags copied (not aliased) — m's backing bytes are reused by
			// badger once the View tx returns. Mirror of headObjectMeta.
			Tags: append([]storage.Tag(nil), m.Tags...),
		}
		placement = PlacementMeta{
			VersionID:        versionID,
			ECData:           m.ECData,
			ECParity:         m.ECParity,
			StripeBytes:      m.StripeBytes,
			NodeIDs:          m.NodeIDs,
			PlacementGroupID: m.PlacementGroupID,
		}
		return nil
	})
	if err != nil {
		return nil, PlacementMeta{}, err
	}
	return &obj, placement, nil
}

// GetObjectVersion reads a specific version's data. Returns
// storage.ErrObjectNotFound if the version doesn't exist. For delete markers,
// returns ErrMethodNotAllowed to mirror the erasure backend's behavior.
func (b *DistributedBackend) GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, *storage.Object, error) {
	ctx := context.Background()
	obj, meta, err := b.headObjectMetaV(bucket, key, versionID)
	if err != nil {
		return nil, nil, err
	}
	if obj.IsDeleteMarker {
		return nil, nil, storage.ErrMethodNotAllowed
	}
	if blocked, q, qerr := b.isObjectQuarantined(bucket, key, versionID); qerr != nil {
		return nil, nil, fmt.Errorf("check quarantine: %w", qerr)
	} else if blocked {
		return nil, nil, objectQuarantinedError(bucket, key, q)
	}
	if obj.IsAppendable && (len(obj.Segments) > 0 || len(obj.Coalesced) > 0) && obj.Size > 0 {
		return b.openAppendableSegments(bucket, key, obj), obj, nil
	}
	if !obj.IsAppendable && len(obj.Segments) > 0 {
		store := &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}
		return storage.NewSegmentReaderCtx(ctx, store, obj.Segments), obj, nil
	}
	// EC path: reconstruct from shards when the bucket is erasure-coded.
	// Mirrors GetObject — versioned objects use shardKey = key+"/"+versionID,
	// which ResolvePlacement derives from PlacementMeta.VersionID. Non-EC and
	// legacy objects fall through to the plain-file path (ResolvePlacement → ErrNotEC).
	if b.shardSvc != nil {
		resolved, rerr := b.ResolvePlacement(ctx, bucket, key, meta)
		if rerr == nil {
			rc, ecErr := b.getObjectECReaderAtShardKey(ctx, bucket, resolved.ShardKey, resolved.Record, obj.Size)
			if ecErr != nil {
				return nil, nil, fmt.Errorf("ec reconstruct %s/%s@%s: %w", bucket, key, versionID, ecErr)
			}
			return rc, obj, nil
		}
		if !errors.Is(rerr, ErrNotEC) {
			return nil, nil, fmt.Errorf("resolve placement for %s/%s@%s: %w", bucket, key, versionID, rerr)
		}
	}
	// Prefer the versioned local file; fall back to legacy unversioned path if
	// the version happens to be the legacy latest (uncommon mid-transition case).
	if f, oerr := os.Open(b.objectPathV(bucket, key, versionID)); oerr == nil {
		return f, obj, nil
	}
	f, err := os.Open(b.objectPath(bucket, key))
	if err != nil {
		return nil, nil, fmt.Errorf("open versioned object: %w", err)
	}
	return f, obj, nil
}

// DeleteObjectVersion hard-deletes a specific version (no tombstone).
// Used by lifecycle/scrubber to reclaim expired versions.
func (b *DistributedBackend) DeleteObjectVersion(bucket, key, versionID string) error {
	ctx := context.Background()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	// Local data cleanup: best-effort (ENOENT is fine — FSM apply is the source of truth).
	_ = os.Remove(b.objectPathV(bucket, key, versionID))
	return b.propose(ctx, CmdDeleteObjectVersion, DeleteObjectVersionCmd{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
	})
}

// ListObjectVersions returns every version (including delete markers) under
// the given prefix, sorted newest-first. When maxKeys > 0 the result is
// truncated. VersionIDs are UUIDv7 (k-sortable ASC by ms timestamp), so we
// sort DESC to get newest-first. Matches server.ObjectVersionLister.
func (b *DistributedBackend) ListObjectVersions(bucket, prefix string, maxKeys int) ([]*storage.ObjectVersion, error) {
	ctx := context.Background()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	var versions []*storage.ObjectVersion
	latestMap := map[string]string{} // key → latestVID
	err := b.store.View(func(txn MetadataTxn) error {
		// Pre-scan latest pointers for the prefix so each version can tag IsLatest.
		rawLatSemanticPfx := []byte("lat:" + bucket + "/" + prefix)
		latPrefix := b.ks().Prefix(rawLatSemanticPfx)
		latIt := txn.NewIterator(MetaIteratorOptions{PrefetchValues: true})
		for latIt.Seek(latPrefix); latIt.ValidForPrefix(latPrefix); latIt.Next() {
			rawKey := b.ks().MustStrip(latIt.Item().Key())
			key := strings.TrimPrefix(string(rawKey), "lat:"+bucket+"/")
			_ = latIt.Item().Value(func(v []byte) error { latestMap[key] = string(v); return nil })
		}
		latIt.Close()

		// Match any object key starting with `prefix` — iterate the per-bucket
		// versioned store and filter in-memory. The version ID is the last
		// path segment after the final `/`; everything before is the S3 key.
		rawObjBucketPfx := []byte("obj:" + bucket + "/")
		objPrefix := b.ks().Prefix(rawObjBucketPfx)
		it := txn.NewIterator(MetaIteratorOptions{PrefetchValues: true})
		defer it.Close()
		for it.Seek(objPrefix); it.ValidForPrefix(objPrefix); it.Next() {
			rawKey := b.ks().MustStrip(it.Item().Key())
			rest := strings.TrimPrefix(string(rawKey), "obj:"+bucket+"/")
			// Versioned format: {key}/{versionID}. Unversioned legacy: {key}.
			slash := strings.LastIndex(rest, "/")
			if slash < 0 {
				if _, hasVersionedRecord := latestMap[rest]; hasVersionedRecord {
					continue
				}
				val, err := b.itemValueCopy(it.Item())
				if err != nil {
					return err
				}
				m, err := unmarshalObjectMeta(val)
				if err != nil {
					return err
				}
				v := storage.ObjectVersion{
					Key:            rest,
					VersionID:      "",
					IsLatest:       true,
					IsDeleteMarker: m.ETag == deleteMarkerETag,
					LastModified:   m.LastModified,
					ETag:           m.ETag,
					Size:           m.Size,
					Tags:           append([]storage.Tag(nil), m.Tags...),
				}
				versions = append(versions, &v)
				continue
			}
			key := rest[:slash]
			vid := rest[slash+1:]
			latestVID, hasVersionedRecord := latestMap[key]
			if !hasVersionedRecord {
				if !strings.HasPrefix(rest, prefix) {
					continue
				}
				val, err := b.itemValueCopy(it.Item())
				if err != nil {
					return err
				}
				m, err := unmarshalObjectMeta(val)
				if err != nil {
					return err
				}
				v := storage.ObjectVersion{
					Key:            rest,
					VersionID:      "",
					IsLatest:       true,
					IsDeleteMarker: m.ETag == deleteMarkerETag,
					LastModified:   m.LastModified,
					ETag:           m.ETag,
					Size:           m.Size,
					Tags:           append([]storage.Tag(nil), m.Tags...),
				}
				versions = append(versions, &v)
				continue
			}
			if vid == "" || !strings.HasPrefix(key, prefix) {
				continue
			}
			val, err := b.itemValueCopy(it.Item())
			if err != nil {
				return err
			}
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			v := storage.ObjectVersion{
				Key:            key,
				VersionID:      vid,
				IsLatest:       vid == latestVID,
				IsDeleteMarker: m.ETag == deleteMarkerETag,
				LastModified:   m.LastModified,
				ETag:           m.ETag,
				Size:           m.Size,
				Tags:           append([]storage.Tag(nil), m.Tags...), // Task 7 carry-over
			}
			versions = append(versions, &v)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Sort DESC by VersionID (UUIDv7 is lex-ASC-by-time, so reverse = newest-first).
	sort.Slice(versions, func(i, j int) bool {
		if versions[i].Key != versions[j].Key {
			return versions[i].Key < versions[j].Key
		}
		return versions[i].VersionID > versions[j].VersionID
	})
	if maxKeys > 0 && len(versions) > maxKeys {
		versions = versions[:maxKeys]
	}
	return versions, nil
}

// --- Path helpers ---
