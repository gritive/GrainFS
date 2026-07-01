package cluster

import (
	"context"
	"fmt"

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
		// Object metadata lives only in the off-raft quorum-meta blob store, so the
		// GC known-set enumerates from there by bucket class:
		//   - versioning-Enabled user bucket → the per-version blob tree
		//     (listBlobAuthBucketObjectsForGC). blobAuthReadOn == versioning Enabled.
		//   - non-versioned/Suspended user bucket → the latest-only blob tree
		//     (listNonVersionedBucketObjectsForGC). This covers regular PUTs as well
		//     as appendable/coalesced objects (all written via writeQuorumMeta with
		//     their Segments), so the sweep never orphans a live segment.
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
			objs, oerr := b.listNonVersionedBucketObjectsForGC(bucket)
			if oerr != nil {
				return nil, oerr
			}
			result = append(result, objs...)
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

	// First pass: find the ModTime-primary latest version per key (markers
	// included) via quorumMetaCmdWins, matching deriveLatestVersion / HEAD.
	latestVID := make(map[string]string, len(cmds))
	latest := make(map[string]PutObjectMetaCmd, len(cmds))
	for _, cmd := range cmds {
		if ex, ok := latest[cmd.Key]; !ok || quorumMetaCmdWins(cmd, ex) {
			latest[cmd.Key] = cmd
			latestVID[cmd.Key] = cmd.VersionID
		}
	}

	// Second pass: map each PutObjectMetaCmd to a SnapshotObject.
	out := make([]storage.SnapshotObject, 0, len(cmds))
	for _, cmd := range cmds {
		out = append(out, snapshotObjectFromQuorumCmd(bucket, cmd, latestVID[cmd.Key] == cmd.VersionID))
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
