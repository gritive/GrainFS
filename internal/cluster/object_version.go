package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
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
	return obj, err
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
	// S4c-c-read1 T2: under soleauth=on the per-version blob is the SOLE AUTHORITY
	// for the exact requested versionID of a vid-bearing versioned object. Unlike
	// the availability-first path below, a blob MISS here never falls through to a
	// stale vid-bearing FSM record — blob absence for a versioned object is a 404.
	// Only carve-out classes (appendable/coalesced/legacy bare-unversioned) stay
	// FSM-authoritative.
	if on, err := b.soleAuthReadOn(bucket); err != nil {
		return nil, PlacementMeta{}, err // fail closed
	} else if on {
		// DECODE-STRICT: an undecodable blob anywhere under this key (incl. a corrupt
		// sibling version) makes the version set untrustworthy → fail closed, do NOT
		// fall through to the carve-out / a stale FSM record.
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
		// per-version MISS under on → carve-out classes ONLY.
		obj, pm, carve, cerr := b.fsmCarveoutObject(bucket, key, versionID)
		if cerr != nil {
			return nil, PlacementMeta{}, cerr
		}
		if carve {
			return obj, pm, nil
		}
		// No vid-bearing-versioned FSM resurrection under sole authority.
		return nil, PlacementMeta{}, storage.ErrObjectNotFound
	}
	// S2a: per-version-authoritative specific-version read. On a versioning-enabled
	// bucket the per-version store is the primary source: a hit returns/folds.
	// On a MISS we fall through to the BadgerDB ObjectMetaKeyV FSM read ONLY,
	// SKIPPING the stale latest-only readQuorumMeta block below — that step would
	// resurrect a hard-deleted version from the latest-only blob (not maintained
	// on hard-delete-of-latest). The FSM read is safe: applyDeleteObjectVersion
	// deletes the ObjectMetaKeyV record, so a hard-deleted version still 404s,
	// while a mixed-era pre-S1 version (FSM record only, no per-version blob —
	// S1's blob write is versioning+post-S1 gated) correctly resolves.
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
		// per-version MISS → fall through to the FSM ObjectMetaKeyV read below,
		// skipping the latest-only readQuorumMeta block.
	}
	// Phase 3: quorum meta is the primary source for non-internal user objects.
	// Skipped for versioning-enabled buckets (handled above) so a per-version
	// miss never resurrects a stale latest-only blob.
	if !versioningEnabled {
		if obj, pm, err := b.readQuorumMeta(bucket, key); err == nil && obj.VersionID == versionID {
			// Fold delete markers to 405, mirroring the BadgerDB fallback below
			// (deleteMarkerETag → ErrMethodNotAllowed) and this method's contract.
			// After a soft-delete the latest quorum-meta record IS the marker
			// tombstone, so without this a HEAD/GET of the marker version would
			// return 200 instead of MethodNotAllowed.
			if obj.IsDeleteMarker {
				return nil, PlacementMeta{}, storage.ErrMethodNotAllowed
			}
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
			blob, eerr := EncodeCommand(CmdPutObjectMeta, tomb)
			if eerr != nil {
				return fmt.Errorf("encode hard-delete tombstone %s/%s@%s: %w", bucket, key, versionID, eerr)
			}
			if werr := b.fanOutPerVersionBlob(ctx, tomb, blob); werr != nil {
				return fmt.Errorf("write hard-delete tombstone %s/%s@%s: %w", bucket, key, versionID, werr)
			}
			return nil
		}
		// blob-absent: this is a carve-out (appendable/coalesced/legacy-bare) that
		// stays FSM-authoritative, or a truly-absent version. Fall through to the
		// legacy FSM-delete path so a carve-out's FSM record IS removed (a tombstone
		// would be meaningless for an object with no per-version blob).
	}

	// Legacy path (non-versioned / internal buckets, and versioning-enabled carve-outs
	// with no per-version blob): raft propose + cluster-wide per-version blob purge.
	if err := b.propose(ctx, CmdDeleteObjectVersion, DeleteObjectVersionCmd{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
	}); err != nil {
		return err
	}
	if b.shardSvc != nil {
		cmd, ok, rerr := b.readQuorumMetaVersion(bucket, key, versionID)
		if rerr != nil {
			return fmt.Errorf("resolve per-version blob for delete %s/%s@%s: %w", bucket, key, versionID, rerr)
		}
		if ok {
			if derr := b.deleteQuorumMetaVersionQuorum(ctx, bucket, key, versionID, cmd.NodeIDs); derr != nil {
				return fmt.Errorf("delete per-version blob %s/%s@%s: %w", bucket, key, versionID, derr)
			}
		}
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
	// S4c-c T2: under soleauth=on the per-version blob tree (cluster-wide) is the
	// SOLE AUTHORITY for versioned objects, merged with the FSM carve-out classes
	// (appendable/coalesced/legacy-bare) on THIS NODE's local group. Fail closed.
	if on, serr := b.soleAuthReadOn(bucket); serr != nil {
		return nil, serr
	} else if on {
		return b.listObjectVersionsSoleAuth(bucket, prefix, maxKeys)
	}
	var versions []*storage.ObjectVersion
	// Non-versioned/Suspended regular objects are blob-only (latest-only blob, no
	// FSM obj: record), so enumerate them from the latest-only blobs and present
	// each as a single latest version — otherwise the S3 ?versions API and the
	// lifecycle worker (ScanObjectsGrouped) would miss every blob-only object.
	// scatterGatherList returns the cluster-wide, tombstone-filtered live view (so
	// a deleted object is absent); each leaf returns the SAME view, deduped at the
	// coordinator (dedupVersionsKeepFirst). Disjoint from the FSM carve-out scan
	// below: an appendable/coalesced object has its latest-only blob deleted (it is
	// FSM-authoritative), so no key appears in both.
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
				// Split-key disambiguation: a versioned record
				// obj:{bucket}/{key}/{vid} whose lat: pointer lives in ANOTHER
				// group (cross-group key split, or a PreserveLatest write with
				// no lat: at all) lands here with no local latestMap entry. The
				// stored meta.Key holds the real S3 key (no version suffix), so
				// meta.Key == the parsed key means this is a genuine versioned
				// record, not a legacy slash-bearing key. Emit it as a non-latest
				// version — the owning group flags the real latest, and the
				// coordinator's reconcileVersionIsLatest picks the global winner.
				if m.Key == key && vid != "" {
					if !strings.HasPrefix(key, prefix) {
						continue
					}
					v := storage.ObjectVersion{
						Key:            key,
						VersionID:      vid,
						IsLatest:       false,
						IsDeleteMarker: m.ETag == deleteMarkerETag,
						LastModified:   m.LastModified,
						ETag:           m.ETag,
						Size:           m.Size,
						Tags:           append([]storage.Tag(nil), m.Tags...),
					}
					versions = append(versions, &v)
					continue
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
	// Dedup by (Key,VID): defensive dedup for any source overlap. After M3 the
	// production path no longer writes FSM obj:/lat: records for multipart-complete
	// (blob-only path), so the set is non-overlapping in practice. Keep-first
	// prefers the blob entry (appended first) in the rare case duplicates appear
	// (e.g. a crash-recovery replica of both sources, or a future code path that
	// re-introduces overlap). Done at the leaf because the single-group path and the
	// lifecycle worker (ScanObjectsGrouped) consume the leaf directly, bypassing the
	// coordinator's dedup.
	versions = dedupVersionsKeepFirst(versions)
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

// listObjectVersionsSoleAuth builds the on-branch (soleauth=on) version list at
// the leaf from TWO sources merged blob-wins:
//
//  1. Versioned objects from the cluster-wide all-version blob enumerator
//     (scanQuorumMetaVersionsClusterAll) — already (Key,VID)+MetaSeq deduped
//     (T1). One ObjectVersion per blob INCLUDING delete markers; the per-key
//     max VID is IsLatest EVEN when it is a delete marker (matching the off-path
//     which lists+flags a delete-marker-latest).
//  2. FSM carve-out records on THIS NODE's LOCAL group only (the coordinator's
//     fan-out unions each group's locals): appendable / coalesced / legacy-bare.
//     Plain vid-bearing versioned FSM records are DROPPED (blob-authoritative).
//     A (Key,VID) collision with a blob is resolved blob-wins (carve-out skipped).
//
// The result is authority-resolved by construction; it is then sorted
// (sortObjectVersions) and truncated to maxKeys as passed (maxKeys<=0 == no
// limit) — identical to the off-path's tail handling. The coordinator passes
// maxKeys=0 under on; a single-node/direct caller passes the real maxKeys.
func (b *DistributedBackend) listObjectVersionsSoleAuth(bucket, prefix string, maxKeys int) ([]*storage.ObjectVersion, error) {
	cmds, err := b.scanQuorumMetaVersionsClusterAll(bucket, prefix)
	if err != nil {
		return nil, err
	}
	// Hard-delete tombstones are not versions: drop them before deriving IsLatest /
	// emitting (a delete MARKER is kept — it IS a version in S3 ListObjectVersions).
	cmds = dropHardDeletedVersions(cmds)
	// Per-key max VID (UUIDv7 string max) so the max-VID blob is flagged IsLatest.
	maxVID := map[string]string{}
	for _, c := range cmds {
		if c.VersionID > maxVID[c.Key] {
			maxVID[c.Key] = c.VersionID
		}
	}
	var versions []*storage.ObjectVersion
	seen := map[[2]string]bool{} // (Key,VID) emitted from blobs — blob wins collisions
	for _, c := range cmds {
		seen[[2]string{c.Key, c.VersionID}] = true
		versions = append(versions, &storage.ObjectVersion{
			Key:            c.Key,
			VersionID:      c.VersionID,
			IsLatest:       c.VersionID == maxVID[c.Key],
			IsDeleteMarker: c.IsDeleteMarker,
			LastModified:   c.ModTime,
			ETag:           c.ETag,
			Size:           c.Size,
			Tags:           append([]storage.Tag(nil), c.Tags...),
		})
	}
	carve, err := b.scanFsmCarveoutVersions(bucket, prefix, seen)
	if err != nil {
		return nil, err
	}
	versions = append(versions, carve...)
	sortObjectVersions(versions)
	if maxKeys > 0 && len(versions) > maxKeys {
		versions = versions[:maxKeys]
	}
	return versions, nil
}

// scanFsmCarveoutVersions scans THIS NODE's LOCAL FSM obj:/lat: records under
// prefix and returns ObjectVersions ONLY for carve-out classes
// (appendable/coalesced/legacy-bare), per the shared isFsmCarveoutClass
// predicate. Plain vid-bearing versioned records are dropped (blob-authoritative
// under sole authority). A (Key,VID) already present in blobSeen is skipped
// (blob wins). Legacy-bare records emit VersionID="" / IsLatest=true, matching
// the off-path leaf and fsmCarveoutObject.
func (b *DistributedBackend) scanFsmCarveoutVersions(bucket, prefix string, blobSeen map[[2]string]bool) ([]*storage.ObjectVersion, error) {
	var out []*storage.ObjectVersion
	err := b.forEachLocalCarveout(bucket, prefix, func(key, vid string, bareLegacy, hasLat bool, m objectMeta) error {
		if blobSeen[[2]string{key, vid}] {
			return nil // blob wins the (Key,VID) collision
		}
		// A carve-out is its key's latest: bare-legacy has no version identity
		// (IsLatest), and an appendable/coalesced record is stored as its key's
		// single latest (lat: points at it). Mirrors the off-path leaf.
		out = append(out, &storage.ObjectVersion{
			Key:            key,
			VersionID:      vid,
			IsLatest:       bareLegacy || hasLat,
			IsDeleteMarker: m.ETag == deleteMarkerETag,
			LastModified:   m.LastModified,
			ETag:           m.ETag,
			Size:           m.Size,
			Tags:           append([]storage.Tag(nil), m.Tags...),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// forEachLocalCarveout iterates THIS node's local FSM obj:/lat: records under
// prefix and invokes visit for each CARVE-OUT-class record
// (appendable/coalesced/legacy-bare per isFsmCarveoutClass). It resolves
// (key, vid, bareLegacy) and whether the key has a lat: pointer the SAME way for
// every consumer — the ListObjectVersions on-branch (scanFsmCarveoutVersions) and
// the soleauth=on scrubber scan — so the carve-out classification cannot drift.
// Plain vid-bearing versioned records and the slashless MIRROR of a versioned key
// are skipped: a versioned appendable/coalesced object persists both obj:{b}/{k}
// (mirror, vid="") and obj:{b}/{k}/{vid} with lat:{b}/{k}→vid; the mirror resolves
// to (vid="", bareLegacy=false) because a lat: pointer exists, and the
// authoritative carve-out is the versioned record. A genuine legacy-bare record
// (bareLegacy=true, no lat:) is kept.
func (b *DistributedBackend) forEachLocalCarveout(bucket, prefix string, visit func(key, vid string, bareLegacy, hasLat bool, m objectMeta) error) error {
	return b.store.View(func(txn MetadataTxn) error {
		hasLat := map[string]bool{}
		rawLatPfx := []byte("lat:" + bucket + "/" + prefix)
		latPrefix := b.ks().Prefix(rawLatPfx)
		latIt := txn.NewIterator(MetaIteratorOptions{PrefetchValues: false})
		for latIt.Seek(latPrefix); latIt.ValidForPrefix(latPrefix); latIt.Next() {
			rawKey := b.ks().MustStrip(latIt.Item().Key())
			key := strings.TrimPrefix(string(rawKey), "lat:"+bucket+"/")
			hasLat[key] = true
		}
		latIt.Close()

		rawObjPfx := []byte("obj:" + bucket + "/")
		objPrefix := b.ks().Prefix(rawObjPfx)
		it := txn.NewIterator(MetaIteratorOptions{PrefetchValues: true})
		defer it.Close()
		for it.Seek(objPrefix); it.ValidForPrefix(objPrefix); it.Next() {
			rawKey := b.ks().MustStrip(it.Item().Key())
			rest := strings.TrimPrefix(string(rawKey), "obj:"+bucket+"/")
			val, verr := b.itemValueCopy(it.Item())
			if verr != nil {
				return verr
			}
			m, merr := unmarshalObjectMeta(val)
			if merr != nil {
				return merr
			}
			key, vid, bareLegacy := b.resolveFsmRecordIdentity(rest, m, hasLat)
			if vid == "" && !bareLegacy {
				continue // slashless mirror of a versioned key
			}
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			if !isFsmCarveoutClass(m, bareLegacy) {
				continue
			}
			if verr := visit(key, vid, bareLegacy, hasLat[key], m); verr != nil {
				return verr
			}
		}
		return nil
	})
}

// resolveFsmRecordIdentity parses an FSM obj: record's rest-of-key into its S3
// key, version ID, and whether it is a legacy-bare (unversioned, no lat:)
// record — mirroring the off-path leaf's split-key disambiguation and
// fsmCarveoutObject's bare-vs-versioned rule.
//
//   - slash-less rest: a bare obj:{bucket}/{key}. bare-legacy iff no lat: pointer.
//   - {key}/{vid}: a versioned record. meta.Key == key confirms a genuine
//     versioned record (vs. a legacy key that literally contains a slash);
//     versioned records are never bare-legacy.
func (b *DistributedBackend) resolveFsmRecordIdentity(rest string, m objectMeta, hasLat map[string]bool) (key, vid string, bareLegacy bool) {
	slash := strings.LastIndex(rest, "/")
	if slash < 0 {
		return rest, "", !hasLat[rest]
	}
	k := rest[:slash]
	v := rest[slash+1:]
	if m.Key == k && v != "" {
		return k, v, false
	}
	// Legacy slash-bearing key with no version identity.
	return rest, "", !hasLat[rest]
}

// --- Path helpers ---
