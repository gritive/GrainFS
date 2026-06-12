package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/gritive/GrainFS/internal/storage"
)

// ReadAt implements partial object reads. Internal buckets use the historical
// direct pread path. EC user buckets read only the data shard segments that
// overlap the requested byte range when those data shards are available.
func (b *DistributedBackend) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("ReadAt negative offset %d", offset)
	}
	if len(buf) == 0 {
		return 0, nil
	}

	obj, placementMeta, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return 0, err
	}
	if blocked, q, qerr := b.isObjectQuarantined(bucket, key, obj.VersionID); qerr != nil {
		return 0, fmt.Errorf("check quarantine: %w", qerr)
	} else if blocked {
		return 0, objectQuarantinedError(bucket, key, q)
	}
	return b.readAtPreparedObject(ctx, bucket, key, obj, placementMeta, offset, buf)
}

func (b *DistributedBackend) ReadAtObject(ctx context.Context, bucket, key string, obj *storage.Object, offset int64, buf []byte) (int, error) {
	if obj == nil {
		return b.ReadAt(ctx, bucket, key, offset, buf)
	}
	if obj.Key != "" && obj.Key != key {
		return 0, fmt.Errorf("ReadAt object key mismatch: got %q, want %q", obj.Key, key)
	}
	if offset < 0 {
		return 0, fmt.Errorf("ReadAt negative offset %d", offset)
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if storage.IsInternalBucket(bucket) {
		return b.ReadAt(ctx, bucket, key, offset, buf)
	}
	placementMeta := PlacementMeta{
		VersionID:        obj.VersionID,
		ECData:           obj.ECData,
		ECParity:         obj.ECParity,
		StripeBytes:      obj.StripeBytes,
		NodeIDs:          obj.NodeIDs,
		PlacementGroupID: obj.PlacementGroupID,
	}
	if !obj.IsAppendable && len(obj.Segments) == 0 && placementMeta.ECData == 0 && len(placementMeta.NodeIDs) == 0 {
		return b.ReadAt(ctx, bucket, key, offset, buf)
	}
	if blocked, q, qerr := b.isObjectQuarantined(bucket, key, obj.VersionID); qerr != nil {
		return 0, fmt.Errorf("check quarantine: %w", qerr)
	} else if blocked {
		return 0, objectQuarantinedError(bucket, key, q)
	}
	return b.readAtPreparedObject(ctx, bucket, key, obj, placementMeta, offset, buf)
}

func (b *DistributedBackend) readAtPreparedObject(ctx context.Context, bucket, key string, obj *storage.Object, placementMeta PlacementMeta, offset int64, buf []byte) (int, error) {
	if offset >= obj.Size {
		return 0, io.EOF
	}
	if max := obj.Size - offset; int64(len(buf)) > max {
		buf = buf[:max]
	}

	// Appendable objects: dispatch via the stitched reader's range path so we
	// only read the chunks that intersect [offset, offset+len(buf)). The
	// generic EC ResolvePlacement returns ErrNotEC for appendables (no
	// placement record); without this fast path ReadAt falls back to a full
	// GET + discard which negates range-read efficiency.
	if obj.IsAppendable && (len(obj.Segments) > 0 || len(obj.Coalesced) > 0) && obj.Size > 0 {
		return b.readAtAppendable(ctx, bucket, key, obj, offset, buf)
	}
	if !obj.IsAppendable && len(obj.Segments) > 0 && obj.Size > 0 {
		store := &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}
		return readAtChunkedSegments(ctx, store, obj.Segments, offset, buf)
	}

	if b.shardSvc != nil {
		resolved, rerr := b.ResolvePlacement(ctx, bucket, key, placementMeta)
		if rerr == nil {
			n, ecErr := b.readObjectECAtShardKey(ctx, bucket, resolved.ShardKey, resolved.Record, obj.Size, offset, buf)
			if ecErr == nil {
				return n, nil
			}
			return b.readAtViaGetObject(ctx, bucket, key, offset, buf)
		}
		if !errors.Is(rerr, ErrNotEC) {
			return 0, fmt.Errorf("resolve placement for %s/%s: %w", bucket, key, rerr)
		}
	}

	if obj.VersionID != "" {
		if f, oerr := b.openObjectIfSizeMatches(b.objectPathV(bucket, key, obj.VersionID), obj); oerr == nil {
			defer f.Close()
			return f.ReadAt(buf, offset)
		}
	}
	if f, oerr := b.openObjectIfSizeMatches(b.objectPath(bucket, key), obj); oerr == nil {
		defer f.Close()
		return f.ReadAt(buf, offset)
	}
	return b.readAtViaGetObject(ctx, bucket, key, offset, buf)
}

func (b *DistributedBackend) PreferReadAt(bucket string) bool {
	return true
}

func (b *DistributedBackend) encryptedShardStorage() bool {
	return b.shardSvc != nil && b.shardSvc.segEnc != nil
}

func (b *DistributedBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	obj, placementMeta, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, nil, err
	}
	if blocked, q, qerr := b.isObjectQuarantined(bucket, key, obj.VersionID); qerr != nil {
		return nil, nil, fmt.Errorf("check quarantine: %w", qerr)
	} else if blocked {
		return nil, nil, objectQuarantinedError(bucket, key, q)
	}
	// HeadObject already rejects tombstones with ErrObjectNotFound, so obj here
	// is a real version. VersionID is non-empty for versioned writes and empty
	// for legacy log replay.

	// Appendable objects store bytes across per-segment blobs under
	// <objectPath>_segments/<blobID> (see writeSegmentBlobForAppend). Stitch
	// them with a multi-segment reader instead of trying to open a single
	// objectPath file (which never exists for appendables).
	if obj.IsAppendable && (len(obj.Segments) > 0 || len(obj.Coalesced) > 0) && obj.Size > 0 {
		return b.openAppendableSegments(bucket, key, obj), obj, nil
	}
	if !obj.IsAppendable && len(obj.Segments) > 0 && obj.Size > 0 {
		store := &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}
		return storage.NewSegmentReaderCtx(ctx, store, obj.Segments), obj, nil
	}

	// EC path: shardKey = key+"/"+versionID for versioned objects.
	shardKey := key
	if obj.VersionID != "" {
		shardKey = key + "/" + obj.VersionID
	}

	if b.shardSvc != nil {
		resolved, rerr := b.ResolvePlacement(ctx, bucket, key, placementMeta)
		if rerr == nil {
			rc, ecErr := b.getObjectECReaderAtShardKey(ctx, bucket, resolved.ShardKey, resolved.Record, obj.Size)
			if ecErr != nil {
				return nil, nil, fmt.Errorf("ec reconstruct %s/%s: %w", bucket, key, ecErr)
			}
			return rc, obj, nil
		}
		if !errors.Is(rerr, ErrNotEC) {
			return nil, nil, fmt.Errorf("resolve placement for %s/%s: %w", bucket, key, rerr)
		}
	}

	// Try the version-addressable local path first (new writers), then the
	// legacy unversioned path (pre-versioning replay). A stale or partial
	// local file must not satisfy metadata that names a larger object; under
	// MultiRaft follower reads that would otherwise surface as a successful
	// GET with an empty body.
	var localErr error
	if obj.VersionID != "" {
		if f, oerr := b.openObjectIfSizeMatches(b.objectPathV(bucket, key, obj.VersionID), obj); oerr == nil {
			return f, obj, nil
		} else if !os.IsNotExist(oerr) {
			localErr = oerr
		}
	}
	f, err := b.openObjectIfSizeMatches(b.objectPath(bucket, key), obj)
	if err == nil {
		return f, obj, nil
	}
	if !os.IsNotExist(err) {
		localErr = err
	}

	// Local file not found — try fetching from peer nodes (healthy first, then all).
	// Peers store under shardKey (key+"/"+versionID) when the write was versioned.
	if b.shardSvc != nil {

		// Try healthy peers first
		for _, peer := range b.liveNodes() {
			if peer == b.currentSelfAddr() {
				continue
			}
			if b.currentPeerHealth() != nil && !b.currentPeerHealth().IsHealthy(peer) {
				continue
			}
			data, fetchErr := b.shardSvc.ReadShard(ctx, peer, bucket, shardKey, 0)
			if fetchErr == nil && data != nil {
				if b.currentPeerHealth() != nil {
					b.currentPeerHealth().MarkHealthy(peer)
				}
				return io.NopCloser(bytes.NewReader(data)), obj, nil
			}
			if fetchErr != nil && b.currentPeerHealth() != nil {
				b.currentPeerHealth().MarkUnhealthy(peer)
			}
		}
		// Fallback: try unhealthy peers (they may have recovered)
		if b.currentPeerHealth() != nil {
			for _, peer := range b.clusterNodes() {
				if peer == b.currentSelfAddr() {
					continue
				}
				if b.currentPeerHealth().IsHealthy(peer) {
					continue // already tried
				}
				data, fetchErr := b.shardSvc.ReadShard(ctx, peer, bucket, shardKey, 0)
				if fetchErr == nil && data != nil {
					b.currentPeerHealth().MarkHealthy(peer)
					return io.NopCloser(bytes.NewReader(data)), obj, nil
				}
			}
		}
	}

	if localErr != nil {
		return nil, nil, fmt.Errorf("open object: %w", localErr)
	}
	return nil, nil, fmt.Errorf("open object: %w", err)
}

func (b *DistributedBackend) openObjectIfSizeMatches(path string, obj *storage.Object) (*os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if st.Size() != obj.Size {
		_ = f.Close()
		return nil, fmt.Errorf("local object size mismatch for %s: metadata=%d file=%d", path, obj.Size, st.Size())
	}
	return f, nil
}

func (b *DistributedBackend) newECObjectReader() ecObjectReader {
	r := ecObjectReader{selfID: b.currentSelfAddr(), shards: b.shardSvc, ecConfig: b.currentECConfig()}
	if b.shardCache != nil {
		r.cache = b.shardCache
	}
	if b.currentPeerHealth() != nil {
		r.peerHealth = b.currentPeerHealth()
	}
	if b.bl != nil && b.clusterCfg.BoundedLoadsEnabled() {
		r.bl = b.bl
	}
	return r
}

func (b *DistributedBackend) getObjectECReaderAtShardKey(ctx context.Context, bucket, shardKey string, rec PlacementRecord, objectSize int64) (io.ReadCloser, error) {
	return b.newECObjectReader().OpenObject(ctx, bucket, shardKey, rec, objectSize)
}

func (b *DistributedBackend) readObjectECAtShardKey(ctx context.Context, bucket, shardKey string, rec PlacementRecord, objectSize int64, offset int64, buf []byte) (int, error) {
	if b.shardSvc == nil {
		return 0, fmt.Errorf("shard service unavailable")
	}
	return b.newECObjectReader().ReadAt(ctx, bucket, shardKey, rec, objectSize, offset, buf)
}

func (b *DistributedBackend) readAtViaGetObject(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	rc, _, err := b.GetObject(ctx, bucket, key)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	if _, err := io.CopyN(io.Discard, rc, offset); err != nil {
		return 0, err
	}
	return io.ReadFull(rc, buf)
}

func (b *DistributedBackend) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	obj, _, err := b.headObjectMeta(ctx, bucket, key)
	return obj, err
}

func (b *DistributedBackend) headObjectMeta(ctx context.Context, bucket, key string) (*storage.Object, PlacementMeta, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, PlacementMeta{}, err
	}

	// Phase 3: non-internal user objects are stored in the quorum meta store.
	// Internal buckets (bucket routing via raft) still use BadgerDB.
	if !storage.IsInternalBucket(bucket) {
		if obj, pm, err := b.readQuorumMeta(bucket, key); err == nil {
			if obj.ETag == deleteMarkerETag {
				return nil, PlacementMeta{}, storage.ErrObjectNotFound
			}
			return obj, pm, nil
		}
		// Fall through to BadgerDB for: multipart-completed objects (their meta
		// lives on raft — see commitCompleteMultipartObjectWriteResult), objects
		// committed before Phase 3 upgrade, and repair/scrubber-written entries.
	}

	var obj storage.Object
	var placement PlacementMeta
	err := b.store.View(func(txn MetadataTxn) error {
		decodeMeta := func(item MetaItem, versionID string) error {
			val, err := b.itemValueCopy(item)
			if err != nil {
				return err
			}
			m, err := unmarshalObjectMeta(val)
			if err != nil {
				return err
			}
			// Tombstone markers aren't observable via HeadObject — callers use
			// HeadObjectVersion / ListObjectVersions to see them explicitly.
			if m.ETag == deleteMarkerETag {
				return storage.ErrObjectNotFound
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
				// badger once the View tx returns.
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
		}

		// Resolve via latest-version pointer when present so callers see the
		// most recent version. Falls back to the legacy single-key read when
		// no lat: pointer exists (e.g., legacy replay).
		if storage.IsInternalBucket(bucket) {
			versionID := ""
			metaKeyBytes := b.internalObjectPath(bucket, key).metaKey
			if latItem, lerr := txn.Get(b.ks().LatestKey(bucket, key)); lerr == nil {
				_ = latItem.Value(func(v []byte) error {
					versionID = string(v)
					return nil
				})
				if versionID != "" {
					metaKeyBytes = b.ks().ObjectMetaKeyV(bucket, key, versionID)
				}
			} else if lerr != ErrMetaKeyNotFound {
				return lerr
			}
			item, err := txn.Get(metaKeyBytes)
			if err == ErrMetaKeyNotFound {
				return storage.ErrObjectNotFound
			}
			if err != nil {
				return err
			}
			if versionID == "" {
				versionID = "current"
			}
			return decodeMeta(item, versionID)
		}

		metaKeyBytes := b.ks().ObjectMetaKey(bucket, key)
		versionID := ""
		if latItem, lerr := txn.Get(b.ks().LatestKey(bucket, key)); lerr == nil {
			_ = latItem.Value(func(v []byte) error {
				versionID = string(v)
				return nil
			})
			if versionID != "" {
				metaKeyBytes = b.ks().ObjectMetaKeyV(bucket, key, versionID)
			}
		} else if lerr != ErrMetaKeyNotFound {
			return lerr
		}

		item, err := txn.Get(metaKeyBytes)
		if err == ErrMetaKeyNotFound {
			return storage.ErrObjectNotFound
		}
		if err != nil {
			return err
		}
		return decodeMeta(item, versionID)
	})
	if err != nil {
		return nil, PlacementMeta{}, err
	}
	return &obj, placement, nil
}

func (b *DistributedBackend) readPlacementMeta(bucket, key, versionID string) PlacementMeta {
	// Phase 3: quorum meta is the primary source for non-internal user objects.
	if !storage.IsInternalBucket(bucket) {
		if _, pm, err := b.readQuorumMeta(bucket, key); err == nil {
			return pm
		}
	}
	meta := PlacementMeta{VersionID: versionID}
	_ = b.store.View(func(txn MetadataTxn) error {
		dbKey := b.ks().ObjectMetaKey(bucket, key)
		if versionID != "" {
			dbKey = b.ks().ObjectMetaKeyV(bucket, key, versionID)
		}
		item, err := txn.Get(dbKey)
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
		meta.ECData = m.ECData
		meta.ECParity = m.ECParity
		meta.StripeBytes = m.StripeBytes
		meta.NodeIDs = m.NodeIDs
		return nil
	})
	return meta
}
