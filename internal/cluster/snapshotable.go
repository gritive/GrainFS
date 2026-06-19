package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/gritive/GrainFS/internal/storage"
)

// ListAllObjects implements storage.Snapshotable by enumerating every
// versioned object record, including non-latest versions and delete markers.
// It is tolerant: it skips unreadable/undecodable object metadata so a single
// corrupt record can't block snapshot creation.
func (b *DistributedBackend) ListAllObjects() ([]storage.SnapshotObject, error) {
	return b.listAllObjects(false)
}

// ListAllObjectsStrict is ListAllObjects for the GC known-set: it FAILS CLOSED
// on any unreadable/undecodable object metadata (returns an error) so the
// scrubber skips its sweep rather than deleting a segment whose object record
// it could not read. snapshot Create deliberately uses the tolerant
// ListAllObjects instead.
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
		objs, err := gb.listAllObjects(true)
		if err != nil {
			return nil, err
		}
		out = append(out, objs...)
	}
	return out, nil
}

func (b *DistributedBackend) listAllObjects(strict bool) ([]storage.SnapshotObject, error) {
	ctx := context.Background()
	buckets, err := b.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	var result []storage.SnapshotObject
	for _, bucket := range buckets {
		saState, saErr := b.GetBucketSoleAuthority(bucket)
		if saErr != nil {
			return nil, fmt.Errorf("list objects soleauth %s: %w", bucket, saErr)
		}
		if saState == soleAuthOn {
			// INVARIANT (v9 scope): only Enabled buckets are ever soleauth=on, so the
			// per-version tree is this bucket's sole authority. S4c-d enforces Enabled-only.
			objs, oerr := b.captureSoleAuthBucketObjects(bucket) // strict; errors fail capture even in tolerant mode
			if oerr != nil {
				return nil, oerr
			}
			result = append(result, objs...)
			continue
		}
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
					if strict {
						return fmt.Errorf("gc known-set: read object meta %s/%s@%s: %w", bucket, key, versionID, err)
					}
					return nil
				}
				meta, err = unmarshalObjectMeta(v)
				if err != nil {
					if strict {
						return fmt.Errorf("gc known-set: decode object meta %s/%s@%s: %w", bucket, key, versionID, err)
					}
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
		saState, err := b.GetBucketSoleAuthority(bucket)
		if err != nil {
			return nil, fmt.Errorf("get bucket soleauth %s: %w", bucket, err)
		}
		// Store "off" as empty so the JSON field is omitted (omitempty) for the
		// common case where soleauth was never set.
		if saState == soleAuthOff {
			saState = ""
		}
		saEpoch, err := b.GetBucketSoleAuthEpoch(bucket)
		if err != nil {
			return nil, fmt.Errorf("get bucket soleauth epoch %s: %w", bucket, err)
		}
		// Capture epoch only when > 0 (omitempty in JSON). Zero means the bucket
		// was never flipped; no floor is needed on restore.
		snap := storage.SnapshotBucket{
			Name:            bucket,
			VersioningState: state,
			SoleAuthState:   saState,
		}
		if saEpoch > 0 {
			snap.SoleAuthEpoch = saEpoch
		}
		out = append(out, snap)
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
		// Compute the soleauth reconciliation plan BEFORE mutating versioning, so
		// an impossible downgrade (stale pre-cutover snapshot over a terminal 'on'
		// bucket) or a corrupted snapshot value fails loudly without leaving the
		// bucket's versioning state half-restored.
		curSA, err := b.GetBucketSoleAuthority(bucket.Name)
		if err != nil {
			return fmt.Errorf("read bucket soleauth %s: %w", bucket.Name, err)
		}
		saPlan, err := planRestoreBucketSoleAuth(curSA, bucket.SoleAuthState)
		if err != nil {
			return fmt.Errorf("restore bucket soleauth %s: %w", bucket.Name, err)
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
		// Apply the pre-validated soleauth plan (guard-permitted transitions only).
		for _, st := range saPlan {
			if err := b.propose(ctx, CmdSetBucketSoleAuthority, SetBucketSoleAuthorityCmd{
				Bucket: bucket.Name,
				State:  st,
			}); err != nil {
				return fmt.Errorf("restore bucket soleauth %s %s: %w", st, bucket.Name, err)
			}
		}
		// Restore the soleauth epoch as a monotonic floor to repair the
		// snapshot/restore fidelity gap: a pending↔off cycle accumulates epoch bumps
		// the transition-replay above cannot reproduce, so the restored epoch could
		// be lower than the original — admitting stale wire epochs. The floor closes
		// that gap by raising max(restored, snapshot.SoleAuthEpoch) without altering
		// the committed state. Safe because a non-zero epoch only exists after a flip,
		// which requires the whole cluster to be upgraded first (spec §Rolling-upgrade).
		if bucket.SoleAuthEpoch > 0 {
			finalState := bucket.SoleAuthState
			if finalState == "" {
				finalState = soleAuthOff
			}
			if err := b.propose(ctx, CmdSetBucketSoleAuthority, SetBucketSoleAuthorityCmd{
				Bucket:     bucket.Name,
				State:      finalState,
				EpochFloor: bucket.SoleAuthEpoch,
			}); err != nil {
				return fmt.Errorf("restore bucket soleauth epoch floor %s: %w", bucket.Name, err)
			}
		}
	}
	return nil
}

// planRestoreBucketSoleAuth returns the ordered soleauth states to propose to
// reconcile a bucket's live state (curSA) toward the snapshot's recorded target,
// using only guard-permitted transitions. It is a pure function (no proposals)
// so the caller can detect an impossible downgrade or corrupted value before
// mutating any other bucket state. target "" means the default off. soleauth is
// one-way only at the COMMITTED end: 'on' is terminal, but 'pending' may abort
// back to 'off' (soleauth.go). A snapshot that demands a downgrade OUT of the
// terminal 'on' state cannot be honored and returns an error.
func planRestoreBucketSoleAuth(curSA, target string) ([]string, error) {
	if target == "" {
		target = soleAuthOff
	}
	terminalConflict := fmt.Errorf("snapshot state %q cannot downgrade terminal current state %q", target, curSA)
	switch target {
	case soleAuthOff:
		switch curSA {
		case soleAuthOff: // idempotent
			return nil, nil
		case soleAuthPending: // abort: pending -> off (guard-permitted)
			return []string{soleAuthOff}, nil
		default: // soleAuthOn — terminal, cannot downgrade
			return nil, terminalConflict
		}
	case soleAuthPending:
		switch curSA {
		case soleAuthOff: // forward: off -> pending
			return []string{soleAuthPending}, nil
		case soleAuthPending: // idempotent
			return nil, nil
		default: // soleAuthOn — terminal, cannot downgrade
			return nil, terminalConflict
		}
	case soleAuthOn:
		switch curSA {
		case soleAuthOff: // forward walk off -> pending -> on
			return []string{soleAuthPending, soleAuthOn}, nil
		case soleAuthPending: // forward: pending -> on
			return []string{soleAuthOn}, nil
		case soleAuthOn: // idempotent
			return nil, nil
		default:
			return nil, fmt.Errorf("invalid current soleauth state %q", curSA)
		}
	default:
		// A snapshot blob is read directly from disk and never passes the
		// apply-time state validator, so a corrupted/manual value must fail
		// loudly here rather than silently leave the bucket unchanged.
		return nil, fmt.Errorf("invalid snapshot state %q", target)
	}
}

// RestoreObjects implements storage.Snapshotable.
// It hard-deletes metadata versions absent from the snapshot, then restores
// each snapshot version. For soleauth-on buckets the per-version blob is
// force-written directly under the quiesce lock (raw snapshot VIDs, no
// resolveRestoreObjectVersionIDs). For off/pending buckets the existing
// resolve+prune+CmdPutObjectMeta path is preserved byte-identical.
func (b *DistributedBackend) RestoreObjects(objects []storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	ctx := context.Background()

	// [P1] Partition on/off BEFORE resolveRestoreObjectVersionIDs.
	// Computing onBuckets from live GetBucketSoleAuthority ensures we never
	// run the VID-rewriting resolver on blob-authoritative on-buckets.
	onBuckets := make(map[string]bool)
	buckets, err := b.ListBuckets(ctx)
	if err != nil {
		return 0, nil, err
	}
	for _, bucket := range buckets {
		saState, saErr := b.GetBucketSoleAuthority(bucket)
		if saErr != nil {
			return 0, nil, fmt.Errorf("restore soleauth %s: %w", bucket, saErr)
		}
		if saState == soleAuthOn {
			onBuckets[bucket] = true
		}
	}

	// Partition objects into on (soleauth-on) and off (everything else).
	var onObjects, offObjects []storage.SnapshotObject
	for _, o := range objects {
		if onBuckets[o.Bucket] {
			onObjects = append(onObjects, o)
		} else {
			offObjects = append(offObjects, o)
		}
	}

	// OFF PATH: resolve VIDs from live metadata (existing behavior, byte-identical).
	offObjects = b.resolveRestoreObjectVersionIDs(offObjects)
	// Build the off-want map from RESOLVED off objects.
	offWant := make(map[string]storage.SnapshotObject, len(offObjects))
	for _, o := range offObjects {
		offWant[o.Bucket+"\x00"+o.Key+"\x00"+o.VersionID] = o
	}

	// ON PATH: build want map from RAW snapshot VIDs (no resolve).
	onWant := make(map[string]storage.SnapshotObject, len(onObjects))
	for _, o := range onObjects {
		onWant[o.Bucket+"\x00"+o.Key+"\x00"+o.VersionID] = o
	}

	// Prune stale FSM metadata for off-bucket objects absent from the snapshot.
	// On-bucket objects have no FSM metadata (the per-version tree is authority).
	type latEntry struct{ bucket, key, versionID string }
	var toDelete []latEntry
	for _, bucket := range buckets {
		if onBuckets[bucket] {
			continue // on-bucket: no FSM obj: records to prune
		}
		if err := b.store.View(func(txn MetadataTxn) error {
			rawObjPrefix := []byte("obj:" + bucket + "/")
			return b.ks().scanGroupPrefix(txn, rawObjPrefix, func(raw []byte, _ MetaItem) error {
				rest := string(raw[len(rawObjPrefix):])
				slash := strings.LastIndex(rest, "/")
				if slash < 0 {
					return nil
				}
				key := rest[:slash]
				versionID := rest[slash+1:]
				if _, wanted := offWant[bucket+"\x00"+key+"\x00"+versionID]; wanted {
					return nil
				}
				toDelete = append(toDelete, latEntry{bucket, key, versionID})
				return nil
			})
		}); err != nil {
			return 0, nil, fmt.Errorf("scan bucket %s: %w", bucket, err)
		}
	}
	for _, d := range toDelete {
		if err := b.propose(ctx, CmdDeleteObjectVersion, DeleteObjectVersionCmd{
			Bucket:    d.bucket,
			Key:       d.key,
			VersionID: d.versionID,
		}); err != nil {
			return 0, nil, fmt.Errorf("delete %s/%s@%s: %w", d.bucket, d.key, d.versionID, err)
		}
	}

	// Restore off-bucket objects via the existing FSM re-propose path.
	var stale []storage.StaleBlob
	var count int
	for _, snap := range offObjects {
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

	// Restore on-bucket objects via the per-version force-write path.
	// Group by bucket so the quiesce lock is held per-bucket (one at a time).
	onByBucket := make(map[string][]storage.SnapshotObject)
	for _, o := range onObjects {
		onByBucket[o.Bucket] = append(onByBucket[o.Bucket], o)
	}
	for bucket, bktObjects := range onByBucket {
		n, bktErr := b.restoreSoleAuthBucketObjects(bucket, bktObjects, onWant, &stale)
		count += n
		if bktErr != nil {
			return count, stale, bktErr
		}
	}

	return count, stale, nil
}

// restoreSoleAuthBucketObjects restores snapshot objects for a single soleauth-on
// bucket by force-writing each per-version blob under the quiesce lock. The caller
// passes the raw-VID onWant map (from snapshot, no VID-rewriting). Task 6 will
// insert the absent-blob purge inside this helper before the restore loop.
func (b *DistributedBackend) restoreSoleAuthBucketObjects(bucket string, objects []storage.SnapshotObject, want map[string]storage.SnapshotObject, stale *[]storage.StaleBlob) (int, error) {
	if b.shardSvc == nil {
		return 0, fmt.Errorf("restore soleauth %s: no shard service", bucket)
	}
	// INVARIANT (v9 scope): only Enabled buckets are ever soleauth=on, so the
	// per-version tree is this bucket's sole authority. S4c-d's flip command
	// enforces Enabled-only; dormant today (no bucket on in prod).
	mu := b.shardSvc.bucketSoleAuthLock(bucket)
	mu.Lock()
	defer mu.Unlock()

	// Purge every on-disk per-version blob absent from want before writing the
	// restore entries. Fail-closed: any scan or delete error aborts the restore so
	// a transiently-unreadable wanted blob is never mis-purged (strict enumerator).
	existing, scanErr := b.shardSvc.scanQuorumMetaVersionsBucketAllStrict(bucket, "")
	if scanErr != nil {
		return 0, fmt.Errorf("restore purge scan %s: %w", bucket, scanErr)
	}
	for _, c := range existing {
		if _, keep := want[bucket+"\x00"+c.Key+"\x00"+c.VersionID]; keep {
			continue
		}
		if err := b.shardSvc.deleteQuorumMetaVersionLocalForceLocked(bucket, c.Key, c.VersionID); err != nil {
			return 0, fmt.Errorf("restore purge %s/%s@%s: %w", bucket, c.Key, c.VersionID, err)
		}
	}

	var count int
	for _, snap := range objects {
		if snap.Bucket != bucket || snap.VersionID == "" {
			continue
		}
		// Stale-blob verification (codex plan-gate [P1]): mirror the off path
		// (snapshotable.go:315) — do NOT publish authoritative per-version metadata
		// for a version whose data blob is missing on this node; record it stale and skip.
		if !snap.IsDeleteMarker && !b.blobExistsForRestore(snap) {
			*stale = append(*stale, storage.StaleBlob{
				Bucket:       snap.Bucket,
				Key:          snap.Key,
				ExpectedETag: snap.ETag,
			})
			continue
		}
		blob, err := EncodeCommand(CmdPutObjectMeta, putObjectMetaCmdFromSnapshot(snap))
		if err != nil {
			return count, fmt.Errorf("restore encode %s/%s: %w", bucket, snap.Key, err)
		}
		if err := b.shardSvc.writeQuorumMetaVersionLocalForceLocked(bucket, path.Join(snap.Key, snap.VersionID), blob); err != nil {
			return count, fmt.Errorf("restore blob %s/%s@%s: %w", bucket, snap.Key, snap.VersionID, err)
		}
		count++
	}
	return count, nil
}

// putObjectMetaCmdFromSnapshot builds a full-fidelity PutObjectMetaCmd from a
// SnapshotObject captured by captureSoleAuthBucketObjects. All placement fields,
// UserMetadata, Parts, Tags, and Segments are copied (not aliased). Segments are
// converted from []storage.SegmentRef → []SegmentMetaEntry via the inverse
// converter segmentRefsToMetaEntries (per_version_backfill_walker.go). SegmentIdx
// is reconstructed as int32(i) by slice order, matching the forward conversion.
func putObjectMetaCmdFromSnapshot(snap storage.SnapshotObject) PutObjectMetaCmd {
	etag := snap.ETag
	if snap.IsDeleteMarker {
		etag = deleteMarkerETag
	}
	var userMeta map[string]string
	if len(snap.UserMetadata) > 0 {
		userMeta = make(map[string]string, len(snap.UserMetadata))
		for k, v := range snap.UserMetadata {
			userMeta[k] = v
		}
	}
	return PutObjectMetaCmd{
		Bucket:           snap.Bucket,
		Key:              snap.Key,
		VersionID:        snap.VersionID,
		ETag:             etag,
		Size:             snap.Size,
		ContentType:      snap.ContentType,
		ModTime:          snap.Modified,
		ECData:           snap.ECData,
		ECParity:         snap.ECParity,
		StripeBytes:      snap.StripeBytes,
		NodeIDs:          cloneStringSlice(snap.NodeIDs),
		PlacementGroupID: snap.PlacementGroupID,
		SSEAlgorithm:     snap.SSEAlgorithm,
		ACL:              snap.ACL,
		MetaSeq:          snap.MetaSeq,
		IsDeleteMarker:   snap.IsDeleteMarker,
		UserMetadata:     userMeta,
		Parts:            append([]storage.MultipartPartEntry(nil), snap.Parts...),
		Tags:             append([]storage.Tag(nil), snap.Tags...),
		Segments:         segmentRefsToMetaEntries(snap.Segments),
		// ExpectedETag and PreserveLatest are write-time-only fields absent from
		// SnapshotObject; they are intentionally left at their zero values.
	}
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
	obj, placement, err := b.headObjectMetaV(context.Background(), snap.Bucket, snap.Key, snap.VersionID)
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
	_ = b.store.View(func(txn MetadataTxn) error {
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

// captureSoleAuthBucketObjects captures every per-version blob for a soleauth-on
// bucket via the FAIL-CLOSED strict enumerator. Each PutObjectMetaCmd is mapped
// to a storage.SnapshotObject with full fidelity: placement (NodeIDs, ECData,
// ECParity, StripeBytes, PlacementGroupID), UserMetadata, Parts, Segments, Tags,
// ACL, MetaSeq — all copied (not aliased). IsLatest is max-VersionID per key
// over all blobs (markers included), matching the LWW semantics of
// readQuorumMetaVersions.
func (b *DistributedBackend) captureSoleAuthBucketObjects(bucket string) ([]storage.SnapshotObject, error) {
	if b.shardSvc == nil {
		return nil, fmt.Errorf("capture soleauth bucket %s: no shard service", bucket)
	}
	cmds, err := b.shardSvc.scanQuorumMetaVersionsBucketAllStrict(bucket, "")
	if err != nil {
		return nil, fmt.Errorf("capture soleauth bucket %s: %w", bucket, err)
	}

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
		// Copy UserMetadata (do not alias).
		var userMeta map[string]string
		if len(cmd.UserMetadata) > 0 {
			userMeta = make(map[string]string, len(cmd.UserMetadata))
			for k, v := range cmd.UserMetadata {
				userMeta[k] = v
			}
		}
		// Copy Parts slice (do not alias).
		var parts []storage.MultipartPartEntry
		if len(cmd.Parts) > 0 {
			parts = append([]storage.MultipartPartEntry(nil), cmd.Parts...)
		}
		// Copy Tags slice (do not alias).
		var tags []storage.Tag
		if len(cmd.Tags) > 0 {
			tags = append([]storage.Tag(nil), cmd.Tags...)
		}
		// Copy NodeIDs slice (do not alias).
		var nodeIDs []string
		if len(cmd.NodeIDs) > 0 {
			nodeIDs = append([]string(nil), cmd.NodeIDs...)
		}
		etag := cmd.ETag
		if cmd.IsDeleteMarker {
			etag = deleteMarkerETag
		}
		out = append(out, storage.SnapshotObject{
			Bucket:           bucket,
			Key:              cmd.Key,
			ETag:             etag,
			Size:             cmd.Size,
			ContentType:      cmd.ContentType,
			Modified:         cmd.ModTime,
			VersionID:        cmd.VersionID,
			IsDeleteMarker:   cmd.IsDeleteMarker,
			IsLatest:         maxVID[cmd.Key] == cmd.VersionID,
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
		})
	}
	return out, nil
}

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

// blobExists checks whether the blob for the given object version exists on
// this node's local storage (N× versioned path, legacy unversioned path, or
// first EC shard). When versionID is empty (e.g. from WAL replay that doesn't
// record versionIDs), resolve the current latest pointer from the FSM.
func (b *DistributedBackend) blobExists(bucket, key, versionID string) bool {
	if versionID == "" {
		// Resolve versionID from the lat: pointer so WAL-replayed objects
		// (which carry no versionID) can still be located on disk.
		_ = b.store.View(func(txn MetadataTxn) error {
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
		if rc, _, err := b.GetObjectVersion(context.Background(), bucket, key, versionID); err == nil {
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
