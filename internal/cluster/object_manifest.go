package cluster

import (
	"context"
	"fmt"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"
)

// ListAllObjectsStrict enumerates every live object version (with Segments) for
// the segment-GC known-set. It FAILS CLOSED on any unreadable/undecodable object
// metadata (returns an error) so the scrubber skips its sweep rather than
// deleting a segment whose object record it could not read.
//
// Multi-group: unions every locally-hosted group's live objects (each group
// enumerates its OWN buckets via its own keyspace), so the segment-GC known-set
// covers all hosted groups — a sibling group's live segment is never
// false-orphaned. Fail-closed: any group's error fails the whole hoist. The sole
// caller is the scrubber's hoistSegmentSources. Default (un-wired
// hostedGroupBackendsSrc => []{b}) == this backend only (single-group).
func (b *DistributedBackend) ListAllObjectsStrict() ([]storage.SnapshotObject, error) {
	var out []storage.SnapshotObject
	for _, gb := range b.hostedGroupBackends() {
		if gb == nil {
			return nil, fmt.Errorf("list all objects strict: hosted group backend is nil")
		}
		objs, err := gb.listAllObjectsForGC()
		if err != nil {
			return nil, err
		}
		out = append(out, objs...)
	}
	return out, nil
}

// listAllObjectsForGC enumerates every versioned object record (non-latest
// versions and delete markers included) for this backend's groups. It is
// fail-closed: any unreadable/undecodable record returns an error so the
// segment-GC known-set is never silently incomplete (which would let the sweep
// orphan-delete a live segment).
func (b *DistributedBackend) listAllObjectsForGC() ([]storage.SnapshotObject, error) {
	ctx := context.Background()
	buckets, err := b.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	var result []storage.SnapshotObject
	for _, bucket := range buckets {
		// Blob-primary GC known-set authority by bucket class:
		//   - versioning-Enabled user bucket → the per-version blob tree
		//     (listBlobAuthBucketObjectsForGC). blobAuthReadOn == versioning Enabled.
		//   - non-versioned/Suspended user bucket → the latest-only blob tree
		//     (listNonVersionedBucketObjectsForGC) for regular objects, UNION the FSM
		//     obj: scan below for appendable/coalesced carve-outs (FSM-authoritative,
		//     no blob) + any legacy records. A non-versioned regular PUT writes ONLY
		//     the latest-only blob (writeQuorumMeta, no FSM obj: propose), so without
		//     the blob scan the sweep would orphan its live segments.
		//   - internal bucket → guarded: ErrInternalBucketNotObjectStore is returned
		//     before any write reaches this path. The FSM obj: scan below retains
		//     pre-existing internal-bucket objects for best-effort GC cleanup.
		on, saErr := b.blobAuthReadOn(bucket)
		if saErr != nil {
			return nil, fmt.Errorf("list objects blob-authority %s: %w", bucket, saErr)
		}
		if on {
			objs, oerr := b.listBlobAuthBucketObjectsForGC(bucket)
			if oerr != nil {
				return nil, oerr
			}
			result = append(result, objs...)
			continue
		}
		if b.shardSvc != nil {
			// Non-versioned/Suspended user bucket: regular objects on the latest-only
			// blob tree. Falls through to the FSM scan for carve-outs/legacy (additive
			// — a regular non-versioned object has no FSM obj: record, so no dup).
			// shardSvc==nil (FSM-only test backends) → blobs are impossible, FSM only.
			objs, oerr := b.listNonVersionedBucketObjectsForGC(bucket)
			if oerr != nil {
				return nil, oerr
			}
			result = append(result, objs...)
		}
		// FSM obj: scan — internal buckets, plus non-versioned appendable/coalesced
		// carve-outs (FSM-authoritative) and any legacy FSM records.
		if err := b.store.View(func(txn MetadataTxn) error {
			latest := make(map[string]string)
			rawLatPrefix := []byte("lat:" + bucket + "/")
			if err := b.ks().scanGroupPrefix(txn, rawLatPrefix, func(raw []byte, item MetaItem) error {
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
			return b.ks().scanGroupPrefix(txn, rawObjPrefix, func(raw []byte, item MetaItem) error {
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
					return fmt.Errorf("gc known-set: read object meta %s/%s@%s: %w", bucket, key, versionID, err)
				}
				meta, err = unmarshalObjectMeta(v)
				if err != nil {
					return fmt.Errorf("gc known-set: decode object meta %s/%s@%s: %w", bucket, key, versionID, err)
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
					// fix in b64521bf so Tags survive the manifest enumeration.
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

// listBlobAuthBucketObjectsForGC enumerates every per-version blob for a
// blob-authoritative bucket via the FAIL-CLOSED strict enumerator, mapping each
// PutObjectMetaCmd to a storage.SnapshotObject with full fidelity (placement,
// Segments, Tags, ACL). IsLatest is max-VersionID per key over all blobs
// (markers included), matching the LWW semantics of readQuorumMetaVersions. It
// is the blob-authoritative branch of the segment-GC known-set: without it the sweep
// would orphan-delete an on-bucket's live segments.
func (b *DistributedBackend) listBlobAuthBucketObjectsForGC(bucket string) ([]storage.SnapshotObject, error) {
	if b.shardSvc == nil {
		return nil, fmt.Errorf("list blob-authority bucket %s: no shard service", bucket)
	}
	cmds, err := b.shardSvc.scanQuorumMetaVersionsBucketAllStrict(bucket, "")
	if err != nil {
		return nil, fmt.Errorf("list blob-authority bucket %s: %w", bucket, err)
	}
	// Hard-delete tombstones are not live objects: exclude them from the segment-GC
	// known-set so their now-dead segments become orphan-eligible (a live sibling
	// version that shares a coalesced segment still protects it — the known-set
	// unions all remaining live versions).
	cmds = dropHardDeletedVersions(cmds)

	// First pass: find the max VersionID per key (markers included).
	maxVID := make(map[string]string, len(cmds))
	for _, cmd := range cmds {
		if cur, ok := maxVID[cmd.Key]; !ok || cmd.VersionID > cur {
			maxVID[cmd.Key] = cmd.VersionID
		}
	}

	// Second pass: map each PutObjectMetaCmd to a SnapshotObject.
	out := make([]storage.SnapshotObject, 0, len(cmds))
	for _, cmd := range cmds {
		out = append(out, snapshotObjectFromQuorumCmd(bucket, cmd, maxVID[cmd.Key] == cmd.VersionID))
	}
	return out, nil
}

// snapshotObjectFromQuorumCmd maps a decoded quorum-meta PutObjectMetaCmd to a
// storage.SnapshotObject for the segment-GC known-set, copying (not aliasing)
// every slice/map because the cmd's backing bytes are reused by the scanner. The
// caller supplies isLatest (per-version: max-VID-per-key; non-versioned: always
// true). Shared by the versioning-Enabled (per-version) and non-versioned
// (latest-only) GC known-set builders.
func snapshotObjectFromQuorumCmd(bucket string, cmd PutObjectMetaCmd, isLatest bool) storage.SnapshotObject {
	var userMeta map[string]string
	if len(cmd.UserMetadata) > 0 {
		userMeta = make(map[string]string, len(cmd.UserMetadata))
		for k, v := range cmd.UserMetadata {
			userMeta[k] = v
		}
	}
	var parts []storage.MultipartPartEntry
	if len(cmd.Parts) > 0 {
		parts = append([]storage.MultipartPartEntry(nil), cmd.Parts...)
	}
	var tags []storage.Tag
	if len(cmd.Tags) > 0 {
		tags = append([]storage.Tag(nil), cmd.Tags...)
	}
	var nodeIDs []string
	if len(cmd.NodeIDs) > 0 {
		nodeIDs = append([]string(nil), cmd.NodeIDs...)
	}
	etag := cmd.ETag
	if cmd.IsDeleteMarker {
		etag = deleteMarkerETag
	}
	return storage.SnapshotObject{
		Bucket:           bucket,
		Key:              cmd.Key,
		ETag:             etag,
		Size:             cmd.Size,
		ContentType:      cmd.ContentType,
		Modified:         cmd.ModTime,
		VersionID:        cmd.VersionID,
		IsDeleteMarker:   cmd.IsDeleteMarker,
		IsLatest:         isLatest,
		ACL:              cmd.ACL,
		SSEAlgorithm:     cmd.SSEAlgorithm,
		NodeIDs:          nodeIDs,
		ECData:           cmd.ECData,
		ECParity:         cmd.ECParity,
		StripeBytes:      cmd.StripeBytes,
		PlacementGroupID: cmd.PlacementGroupID,
		MetaSeq:          cmd.MetaSeq,
		UserMetadata:     userMeta,
		Parts:            parts,
		Tags:             tags,
		Segments:         segmentMetaEntriesToRefs(cmd.Segments),
		// F5: carry Coalesced so a blob-resident coalesced object's merged blob is
		// in the segment-GC known-set (ChunkLocators pins it) — without it the sweep
		// would orphan-delete a live coalesced blob.
		Coalesced: coalescedRefsFromMeta(cmd.Coalesced),
	}
}

// listNonVersionedBucketObjectsForGC enumerates the latest-only quorum-meta blobs
// of a NON-VERSIONED (or Suspended) user bucket via a FAIL-CLOSED strict scan,
// mapping each to a storage.SnapshotObject (always IsLatest — non-versioned has a
// single latest per key). Hard-delete tombstones are excluded (dead segments).
// Symmetric counterpart of listBlobAuthBucketObjectsForGC for the latest-only tree:
// without it the segment GC would orphan-delete a live non-versioned object's
// segments (a non-versioned PUT writes ONLY the latest-only blob — no FSM record).
func (b *DistributedBackend) listNonVersionedBucketObjectsForGC(bucket string) ([]storage.SnapshotObject, error) {
	if b.shardSvc == nil {
		return nil, fmt.Errorf("list non-versioned bucket %s: no shard service", bucket)
	}
	cmds, err := b.shardSvc.scanQuorumMetaBucketStrict(bucket)
	if err != nil {
		return nil, fmt.Errorf("list non-versioned bucket %s: %w", bucket, err)
	}
	cmds = dropHardDeletedVersions(cmds)
	out := make([]storage.SnapshotObject, 0, len(cmds))
	for _, cmd := range cmds {
		out = append(out, snapshotObjectFromQuorumCmd(bucket, cmd, true))
	}
	return out, nil
}

// coalescedRefsFromMeta maps cluster CoalescedShardRef → storage.CoalescedRef for
// the GC known-set / reachability paths. It preserves EVERY field (size, ETag,
// shardKey, EC placement, nodeIDs) — not just CoalescedID — so a blob-resident
// coalesced object's EC shards stay reachable in every scan path (F5): the shard
// placement (ShardKey + NodeIDs + EC params) is what the EC reader, the rewrap
// lane, and the shard-key resolver need to locate the merged blob's shards. It
// delegates to coalescedRefsToStorage (the single field-preserving projector) so
// the two never drift.
func coalescedRefsFromMeta(in []CoalescedShardRef) []storage.CoalescedRef {
	return coalescedRefsToStorage(in)
}
