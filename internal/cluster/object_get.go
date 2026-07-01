package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/gritive/GrainFS/internal/storage"
)

// ReadAt implements partial object reads. Internal buckets use the historical
// direct pread path. EC user buckets read only the data shard segments that
// overlap the requested byte range when those data shards are available.
func (b *DistributedBackend) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return 0, err
	}
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
	if err := b.quarantineGate(bucket, key, obj.VersionID); err != nil {
		return 0, err
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
	if err := b.quarantineGate(bucket, key, obj.VersionID); err != nil {
		return 0, err
	}
	return b.readAtPreparedObject(ctx, bucket, key, obj, placementMeta, offset, buf)
}

func (b *DistributedBackend) readAtPreparedObject(ctx context.Context, bucket, key string, obj *storage.Object, placementMeta PlacementMeta, offset int64, buf []byte) (int, error) {
	return b.readAtPreparedObjectStore(ctx, nil, bucket, key, obj, placementMeta, offset, buf)
}

// readAtPreparedObjectStore is readAtPreparedObject with an optional reusable
// clusterSegmentStore for the multi-segment branch. A nil store yields the
// stateless behavior (a fresh store per call). A caller-owned store (see
// PreparedObjectReaderAt) survives across a ranged GET's refills so its
// single-decompressed-segment cache actually hits.
func (b *DistributedBackend) readAtPreparedObjectStore(ctx context.Context, store *clusterSegmentStore, bucket, key string, obj *storage.Object, placementMeta PlacementMeta, offset int64, buf []byte) (int, error) {
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
	if !obj.IsAppendable && len(obj.Segments) > 0 {
		if store == nil {
			store = &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}
		}
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

	return b.readAtViaGetObject(ctx, bucket, key, offset, buf)
}

// distributedObjectRangeReader is a stateful, single-GET-scoped ReaderAt. It
// pins the placement metadata and (for chunked objects) a reusable segment store
// whose single-decompressed-segment cache survives across the GET's refills.
type distributedObjectRangeReader struct {
	b             *DistributedBackend
	ctx           context.Context
	bucket, key   string
	obj           *storage.Object
	placementMeta PlacementMeta
	store         *clusterSegmentStore // reused across refills; nil for non-chunked layouts
}

func (r *distributedObjectRangeReader) ReadAt(offset int64, buf []byte) (int, error) {
	// Re-gate quarantine on every refill, exactly as the stateless ReadAtObject
	// path does: an object quarantined mid-stream must still interrupt an
	// in-flight ranged GET (the fast path only caches the decompressed segment;
	// it must not narrow the data-integrity gate to GET-start).
	if err := r.b.quarantineGate(r.bucket, r.key, r.obj.VersionID); err != nil {
		return 0, err
	}
	return r.b.readAtPreparedObjectStore(r.ctx, r.store, r.bucket, r.key, r.obj, r.placementMeta, offset, buf)
}

// PreparedObjectReaderAt returns a stateful reader scoped to one ranged GET.
// For a compressed chunked object it caches the last decompressed segment across
// refills, so a range GET decompresses each touched segment once instead of once
// per 5 MiB refill. A nil reader (quarantined / nil obj) tells the caller to use
// the stateless dispatch instead.
func (b *DistributedBackend) PreparedObjectReaderAt(ctx context.Context, bucket, key string, obj *storage.Object) (storage.ObjectRangeReaderAt, func()) {
	if obj == nil {
		return nil, func() {}
	}
	if err := b.quarantineGate(bucket, key, obj.VersionID); err != nil {
		// Let the stateless path surface the quarantine error on first read.
		return nil, func() {}
	}
	r := &distributedObjectRangeReader{
		b:      b,
		ctx:    ctx,
		bucket: bucket,
		key:    key,
		obj:    obj,
		placementMeta: PlacementMeta{
			VersionID:        obj.VersionID,
			ECData:           obj.ECData,
			ECParity:         obj.ECParity,
			StripeBytes:      obj.StripeBytes,
			NodeIDs:          obj.NodeIDs,
			PlacementGroupID: obj.PlacementGroupID,
		},
	}
	if !obj.IsAppendable && len(obj.Segments) > 0 {
		r.store = &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}
	}
	cleanup := func() {
		if r.store != nil {
			r.store.cachedSegPlaintext = nil
			r.store.cachedSegKey = ""
		}
	}
	return r, cleanup
}

func (b *DistributedBackend) PreferReadAt(bucket string) bool {
	return true
}

func (b *DistributedBackend) encryptedShardStorage() bool {
	return b.shardSvc != nil && b.shardSvc.segEnc() != nil
}

func (b *DistributedBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, nil, err
	}
	obj, placementMeta, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, nil, err
	}
	if err := b.quarantineGate(bucket, key, obj.VersionID); err != nil {
		return nil, nil, err
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
	if !obj.IsAppendable && len(obj.Segments) > 0 {
		store := &clusterSegmentStore{b: b, bucket: bucket, key: key, obj: obj}
		return storage.NewStreamingSegmentReaderCtx(ctx, store, obj.Segments), obj, nil
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

	return nil, nil, fmt.Errorf("get object %s/%s: object has no readable layout (not appendable, no segments, not EC)", bucket, key)
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

// quarantineGate returns objectQuarantinedError when (bucket,key,versionID) is
// quarantined, a wrapped error if the quarantine lookup itself fails, or nil.
// Callers pass their own versionID source (obj.VersionID for latest, the
// requested versionID for versioned reads, "" for the latest-only write guard)
// and keep their own ordering relative to other gates.
func (b *DistributedBackend) quarantineGate(bucket, key, versionID string) error {
	blocked, cause, err := b.isObjectQuarantined(bucket, key, versionID)
	if err != nil {
		return fmt.Errorf("check quarantine: %w", err)
	}
	if blocked {
		return objectQuarantinedError(bucket, key, cause)
	}
	return nil
}

func (b *DistributedBackend) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, err
	}
	obj, _, err := b.headObjectMeta(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	if err := b.quarantineGate(bucket, key, obj.VersionID); err != nil {
		return nil, err
	}
	return obj, nil
}

func (b *DistributedBackend) headObjectMeta(ctx context.Context, bucket, key string) (*storage.Object, PlacementMeta, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, PlacementMeta{}, err
	}

	// Under blob-authoritative the per-version blob tree is the SOLE AUTHORITY for
	// vid-bearing versioned objects. Unlike the availability-first path below, a
	// blob MISS here never falls through to readQuorumMeta (latest-only): blob
	// absence for a versioned object is a 404.
	if on, err := b.blobAuthReadOn(bucket); err != nil {
		return nil, PlacementMeta{}, err // fail closed
	} else if on {
		// DECODE-STRICT: a corrupt/undecodable per-version blob must NOT be silently
		// dropped (which would let deriveLatestVersion resurrect an older live version
		// past a corrupt delete-marker-latest). On the reader error, fail closed — do
		// NOT fall through to an older-live version.
		cmds, verr := b.readQuorumMetaVersionsDecodeStrict(bucket, key)
		if verr != nil {
			return nil, PlacementMeta{}, verr // fail closed
		}
		if len(cmds) > 0 {
			latest, live := deriveLatestVersion(cmds)
			if live {
				obj, pm := objectAndPlacementFromCmd(latest)
				return obj, pm, nil
			}
			// derive found only delete-markers / not-live → object is gone (404).
			return nil, PlacementMeta{}, storage.ErrObjectNotFound
		}
		// per-version MISS under blob authority → object is gone (404).
		return nil, PlacementMeta{}, storage.ErrObjectNotFound
	}

	// S2a: per-version-authoritative latest derive. On a versioning-enabled
	// bucket, derive latest by scanning the per-version blobs (all-groups
	// fan-out, spanning generations). Zero blobs → latest-only fallback below.
	if b.bucketVersioningEnabled(ctx, bucket) {
		if cmds, verr := b.readQuorumMetaVersions(bucket, key); verr == nil && len(cmds) > 0 {
			latest, live := deriveLatestVersion(cmds)
			if !live {
				return nil, PlacementMeta{}, storage.ErrObjectNotFound
			}
			obj, pm := objectAndPlacementFromCmd(latest)
			return obj, pm, nil
		}
		// zero per-version blobs → latest-only fallback (readQuorumMeta below)
	}

	// Non-versioned objects are served from the latest-only quorum-meta blob.
	// Object metadata is written ONLY to the quorum-meta blob store (off-raft);
	// the FSM keyspace holds no per-object records, so there is no further read
	// fallback — a quorum-meta miss is a 404.
	if obj, pm, err := b.readQuorumMeta(bucket, key); err == nil {
		if obj.ETag == deleteMarkerETag {
			return nil, PlacementMeta{}, storage.ErrObjectNotFound
		}
		if err := b.hydrateClusterAppendSideSegments(ctx, bucket, key, obj); err != nil {
			return nil, PlacementMeta{}, err
		}
		return obj, pm, nil
	}
	return nil, PlacementMeta{}, storage.ErrObjectNotFound
}

func (b *DistributedBackend) readPlacementMeta(bucket, key, versionID string) PlacementMeta {
	// The quorum-meta blob store is the sole source for user object placement.
	if _, pm, err := b.readQuorumMeta(bucket, key); err == nil {
		return pm
	}
	return PlacementMeta{VersionID: versionID}
}
