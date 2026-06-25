package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
)

// HeadObjectVersion returns metadata for a specific version. Returns
// storage.ErrObjectNotFound if the version doesn't exist or is a delete marker.
//
// This satisfies storage.VersionedHeader. The server edge stamps the
// authoritative bucket-versioning decision into ctx (mirroring PUT) so the
// per-version gate inside headObjectMetaV can resolve it; an in-process call
// with an unstamped ctx falls back to a local versioning read.
func (b *DistributedBackend) HeadObjectVersion(ctx context.Context, bucket, key, versionID string) (*storage.Object, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, err
	}
	return b.headObjectVersionCtx(ctx, bucket, key, versionID)
}

// headObjectVersionCtx is the ctx-threaded HeadObjectVersion used by callers
// that stamp the bucket-versioning decision into ctx (coordinator + forward
// receiver). The stamp must reach bucketVersioningEnabled inside headObjectMetaV
// or the per-version read path stays inactive on a non-meta-group / forwarded
// read (the latent S2a multi-group bug this PR fixes).
//
// Guard is present here (in addition to HeadObjectVersion) so that coordinator
// and forward-receiver callers that call this core directly are also blocked from
// targeting internal buckets.
func (b *DistributedBackend) headObjectVersionCtx(ctx context.Context, bucket, key, versionID string) (*storage.Object, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, err
	}
	obj, _, err := b.headObjectMetaV(ctx, bucket, key, versionID)
	if err != nil {
		return nil, err
	}
	if err := b.quarantineGate(bucket, key, versionID); err != nil {
		return nil, err
	}
	return obj, nil
}

// headObjectMetaV reads a specific version's metadata and its EC placement
// fields in one transaction. Returns storage.ErrObjectNotFound for a missing
// version and storage.ErrMethodNotAllowed for a delete-marker version (S3
// semantics — the server handler maps that to a 405 with x-amz-delete-marker).
// Parallels headObjectMeta, which does the same for the latest version.
//
// ctx carries the authoritative bucket-versioning stamp (when the caller is the
// coordinator / forward receiver); bucketVersioningEnabled resolves it before
// falling back to a local store read.
func (b *DistributedBackend) headObjectMetaV(ctx context.Context, bucket, key, versionID string) (*storage.Object, PlacementMeta, error) {
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, PlacementMeta{}, err
	}
	// Under blob authority the per-version blob is authoritative for the exact
	// requested versionID of a vid-bearing versioned object. Unlike the
	// availability-first path below, a blob MISS here is a 404 — blob absence for
	// a versioned object means the version is gone.
	if on, err := b.blobAuthReadOn(bucket); err != nil {
		return nil, PlacementMeta{}, err // fail closed
	} else if on {
		// DECODE-STRICT: an undecodable blob anywhere under this key (incl. a corrupt
		// sibling version) makes the version set untrustworthy → fail closed.
		cmd, ok, verr := b.readQuorumMetaVersionDecodeStrict(bucket, key, versionID)
		if verr != nil {
			return nil, PlacementMeta{}, verr // fail closed
		}
		if ok {
			if cmd.IsHardDeleted {
				// Hard-delete tombstone: the version is permanently gone → 404
				// (NoSuchVersion), distinct from a delete-marker version (405).
				return nil, PlacementMeta{}, storage.ErrObjectNotFound
			}
			if cmd.IsDeleteMarker {
				return nil, PlacementMeta{}, storage.ErrMethodNotAllowed
			}
			obj, pm := objectAndPlacementFromCmd(cmd)
			return obj, pm, nil
		}
		// per-version MISS under blob authority → version is gone (404).
		return nil, PlacementMeta{}, storage.ErrObjectNotFound
	}
	// S2a: per-version-authoritative specific-version read. On a versioning-enabled
	// bucket the per-version store is the primary source: a hit returns/folds.
	// On a MISS the version is gone (404) — object metadata is written ONLY to the
	// quorum-meta blob store, so there is no FSM read fallback; skipping the stale
	// latest-only readQuorumMeta block also prevents resurrecting a hard-deleted
	// version from the latest-only blob (not maintained on hard-delete-of-latest).
	versioningEnabled := b.bucketVersioningEnabled(ctx, bucket)
	if versioningEnabled {
		if cmds, verr := b.readQuorumMetaVersions(bucket, key); verr == nil {
			for _, cmd := range cmds {
				if cmd.VersionID != versionID {
					continue
				}
				if cmd.IsHardDeleted {
					return nil, PlacementMeta{}, storage.ErrObjectNotFound
				}
				if cmd.IsDeleteMarker {
					return nil, PlacementMeta{}, storage.ErrMethodNotAllowed
				}
				obj, pm := objectAndPlacementFromCmd(cmd)
				return obj, pm, nil
			}
		}
		// per-version MISS → version is gone (404), skipping the latest-only block.
		return nil, PlacementMeta{}, storage.ErrObjectNotFound
	}
	// Non-versioned: the latest-only quorum-meta blob is the sole source. A miss
	// (or a version mismatch) is a 404 — there is no FSM read fallback.
	if obj, pm, err := b.readQuorumMeta(bucket, key); err == nil && obj.VersionID == versionID {
		// Fold delete markers to 405, matching this method's contract. After a
		// soft-delete the latest quorum-meta record IS the marker tombstone, so
		// without this a HEAD/GET of the marker version would return 200 instead
		// of MethodNotAllowed.
		if obj.IsDeleteMarker {
			return nil, PlacementMeta{}, storage.ErrMethodNotAllowed
		}
		return obj, pm, nil
	}
	return nil, PlacementMeta{}, storage.ErrObjectNotFound
}

// GetObjectVersion reads a specific version's data. Returns
// storage.ErrObjectNotFound if the version doesn't exist. For delete markers,
// returns ErrMethodNotAllowed to mirror the erasure backend's behavior.
//
// Satisfies storage.VersionedGetter. See HeadObjectVersion — ctx carries the
// authoritative bucket-versioning stamp set at the server edge.
func (b *DistributedBackend) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (io.ReadCloser, *storage.Object, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, nil, err
	}
	return b.getObjectVersionCtx(ctx, bucket, key, versionID)
}

// getObjectVersionCtx is the ctx-threaded GetObjectVersion used by callers that
// stamp the bucket-versioning decision into ctx (coordinator + forward receiver).
// Guard is present here so that forwarded-path callers that call this core
// directly are blocked from targeting internal buckets.
func (b *DistributedBackend) getObjectVersionCtx(ctx context.Context, bucket, key, versionID string) (io.ReadCloser, *storage.Object, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, nil, err
	}
	obj, meta, err := b.headObjectMetaV(ctx, bucket, key, versionID)
	if err != nil {
		return nil, nil, err
	}
	if obj.IsDeleteMarker {
		return nil, nil, storage.ErrMethodNotAllowed
	}
	if err := b.quarantineGate(bucket, key, versionID); err != nil {
		return nil, nil, err
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
	return nil, nil, fmt.Errorf("get object version %s/%s@%s: object has no readable layout (not appendable, no segments, not EC)", bucket, key, versionID)
}

// DeleteObjectVersion hard-deletes a specific version (no tombstone).
// Used by lifecycle/scrubber to reclaim expired versions.
func (b *DistributedBackend) DeleteObjectVersion(bucket, key, versionID string) error {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return err
	}
	ctx := context.Background()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	// Local data cleanup: best-effort (legacy N× on-disk file; ENOENT is fine).
	_ = os.Remove(b.objectPathV(bucket, key, versionID))

	// Blob-primary hard delete: REPLACE the version's per-version blob with a
	// tombstone (IsHardDeleted) durably, fail-closed, with NO raft propose. The
	// tombstone wins the LWW (quorumMetaCmdWins / MetaSeq+1) over any data-blob copy
	// lingering on a node that missed this delete, so the version can never resurrect;
	// the orphan walker reconciles stale data blobs and GCs the tombstone once the
	// delete has propagated cluster-wide. The FSM obj: record (if any) is left
	// untouched — reads are blob-authoritative.
	//
	// The tombstone path is gated on the version having a per-version blob (a
	// cluster-wide read), NOT on the bucket's versioning meta-state: the delete leaf
	// may not be the meta authority and would misread versioning, falling to the
	// legacy purge (losing the tombstone's resurrection guard). A version with no
	// per-version blob (carve-out appendable/coalesced/legacy-bare, non-versioned, or
	// absent) falls through to the legacy FSM-delete path below.
	if b.shardSvc != nil {
		cmd, ok, rerr := b.readQuorumMetaVersionDecodeStrict(bucket, key, versionID)
		if rerr != nil {
			return fmt.Errorf("resolve per-version blob for delete %s/%s@%s: %w", bucket, key, versionID, rerr)
		}
		if ok {
			if cmd.IsHardDeleted {
				return nil // already a tombstone → idempotent
			}
			tomb := cmd
			tomb.IsHardDeleted = true
			tomb.ModTime = time.Now().Unix()
			tomb.MetaSeq = cmd.MetaSeq + 1
			blob, eerr := encodeQuorumMetaBlob(tomb)
			if eerr != nil {
				return fmt.Errorf("encode hard-delete tombstone %s/%s@%s: %w", bucket, key, versionID, eerr)
			}
			if werr := b.fanOutPerVersionBlob(ctx, tomb, blob); werr != nil {
				return fmt.Errorf("write hard-delete tombstone %s/%s@%s: %w", bucket, key, versionID, werr)
			}
			return nil
		}
		// blob-absent: the version has no per-version blob (e.g. an appendable /
		// coalesced object on a non-versioned bucket, served from the latest-only
		// blob). There is no FSM object record to delete; the scrubber / orphan
		// walker handles eventual on-disk cleanup.
	}
	return nil
}

// ListObjectVersions returns every version (including delete markers) under
// the given prefix, sorted newest-first. When maxKeys > 0 the result is
// truncated. VersionIDs are UUIDv7 (k-sortable ASC by ms timestamp), so we
// sort DESC to get newest-first. Matches server.ObjectVersionLister.
func (b *DistributedBackend) ListObjectVersions(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.ObjectVersion, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, err
	}
	if b.testOnListObjectVersionsCtx != nil {
		b.testOnListObjectVersionsCtx(ctx)
	}
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	// S4c-c T2: under blob-authoritative the per-version blob tree (cluster-wide) is the
	// BLOB AUTHORITY for versioned objects, merged with the FSM carve-out classes
	// (appendable/coalesced/legacy-bare) on THIS NODE's local group. Fail closed.
	if on, serr := b.blobAuthReadOn(bucket); serr != nil {
		return nil, serr
	} else if on {
		return b.listObjectVersionsBlobAuth(bucket, prefix, maxKeys)
	}
	var versions []*storage.ObjectVersion
	// Non-versioned/Suspended regular objects are blob-only (latest-only blob),
	// so enumerate them from the latest-only blobs and present each as a single
	// latest version — otherwise the S3 ?versions API and the lifecycle worker
	// (ScanObjectsGrouped) would miss every object. scatterGatherList returns the
	// cluster-wide, tombstone-filtered live view (so a deleted object is absent);
	// each leaf returns the SAME view, deduped at the coordinator
	// (dedupVersionsKeepFirst).
	if b.shardSvc != nil {
		cmds, lerr := b.scatterGatherList(ctx, bucket, prefix)
		if lerr != nil {
			return nil, lerr
		}
		for _, cmd := range cmds {
			versions = append(versions, &storage.ObjectVersion{
				Key:            cmd.Key,
				VersionID:      cmd.VersionID,
				IsLatest:       true,
				IsDeleteMarker: false,
				LastModified:   cmd.ModTime,
				ETag:           cmd.ETag,
				Size:           cmd.Size,
				Tags:           append([]storage.Tag(nil), cmd.Tags...),
			})
		}
	}
	// Defensive (Key,VID) dedup: each leaf returns the same cluster-wide view, so
	// the single-group path and the lifecycle worker (which consume the leaf
	// directly, bypassing the coordinator's dedup) still see a deduped set.
	versions = dedupVersionsKeepFirst(versions)
	// Sort newest-first within each key by ModTime-primary (latestWins), so the
	// IsLatest version sorts first — matching deriveLatestVersion / sortObjectVersions.
	sort.Slice(versions, func(i, j int) bool {
		if versions[i].Key != versions[j].Key {
			return versions[i].Key < versions[j].Key
		}
		return latestWins(versions[i].LastModified, versions[i].VersionID,
			versions[j].LastModified, versions[j].VersionID)
	})
	if maxKeys > 0 && len(versions) > maxKeys {
		versions = versions[:maxKeys]
	}
	return versions, nil
}

// listObjectVersionsBlobAuth builds the on-branch (blob-authoritative) version
// list at the leaf from the cluster-wide all-version blob enumerator
// (scanQuorumMetaVersionsClusterAll) — already (Key,VID)+MetaSeq deduped (T1).
// One ObjectVersion per blob INCLUDING delete markers; the per-key ModTime-primary
// winner is IsLatest EVEN when it is a delete marker (matching the off-path which
// lists+flags a delete-marker-latest).
//
// The result is sorted (sortObjectVersions) and truncated to maxKeys as passed
// (maxKeys<=0 == no limit). The coordinator passes maxKeys=0 under on; a
// single-node/direct caller passes the real maxKeys.
func (b *DistributedBackend) listObjectVersionsBlobAuth(bucket, prefix string, maxKeys int) ([]*storage.ObjectVersion, error) {
	cmds, err := b.scanQuorumMetaVersionsClusterAll(bucket, prefix)
	if err != nil {
		return nil, err
	}
	// Hard-delete tombstones are not versions: drop them before deriving IsLatest /
	// emitting (a delete MARKER is kept — it IS a version in S3 ListObjectVersions).
	cmds = dropHardDeletedVersions(cmds)
	// ModTime-primary winner per key (quorumMetaCmdWins) so the IsLatest flag
	// matches HeadObject's deriveLatestVersion — both ModTime-primary, not max-VID.
	winnerVID := map[string]string{}
	winner := map[string]PutObjectMetaCmd{}
	for _, c := range cmds {
		if ex, ok := winner[c.Key]; !ok || quorumMetaCmdWins(c, ex) {
			winner[c.Key] = c
			winnerVID[c.Key] = c.VersionID
		}
	}
	var versions []*storage.ObjectVersion
	for _, c := range cmds {
		versions = append(versions, &storage.ObjectVersion{
			Key:            c.Key,
			VersionID:      c.VersionID,
			IsLatest:       c.VersionID == winnerVID[c.Key],
			IsDeleteMarker: c.IsDeleteMarker,
			LastModified:   c.ModTime,
			ETag:           c.ETag,
			Size:           c.Size,
			Tags:           append([]storage.Tag(nil), c.Tags...),
		})
	}
	sortObjectVersions(versions)
	if maxKeys > 0 && len(versions) > maxKeys {
		versions = versions[:maxKeys]
	}
	return versions, nil
}

// --- Path helpers ---
