package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// perVersionBackfillCandidate is an FSM obj: record that needs its per-version
// quorum-meta blob written (the blob is absent from disk). Tasks 2-3 consume
// this to fan out the write and drive the sweep.
type perVersionBackfillCandidate struct {
	Bucket, Key, VersionID string
	Meta                   objectMeta
}

// uuidv7TimeUnix extracts the 48-bit Unix millisecond timestamp embedded in a
// UUIDv7 string and returns it as Unix seconds. Returns 0 if the string is not
// a valid UUID (fail-open: the caller's age gate treats age 0 as epoch → old).
func uuidv7TimeUnix(vid string) int64 {
	id, err := uuid.Parse(vid)
	if err != nil {
		return 0
	}
	// UUIDv7 layout: bytes[0..5] hold the 48-bit unix_ts_ms field (big-endian).
	msBytes := [8]byte{}
	copy(msBytes[2:], id[:6]) // pad to 8 bytes for uint64 decode
	ms := int64(binary.BigEndian.Uint64(msBytes[:]))
	return ms / 1000
}

// lookupSiblingNodeIDs scans the per-version FSM records for bucket/key within
// the supplied transaction and returns the NodeIDs from the first version whose
// VID differs from skipVID and whose NodeIDs are non-empty. Returns nil if no
// such sibling exists.
//
// Rationale: RDH placement is keyed by bucket/key, so every version of the same
// object is placed on the same nodes. A degraded delete marker (empty NodeIDs)
// can therefore borrow placement from any live sibling version.
func lookupSiblingNodeIDs(txn MetadataTxn, gb *DistributedBackend, bucket, key, skipVID string) []string {
	siblingPrefix := []byte("obj:" + bucket + "/" + key + "/")
	var found []string
	_ = gb.ks().scanGroupPrefix(txn, siblingPrefix, func(rawKey []byte, item MetaItem) error {
		rest := strings.TrimPrefix(string(rawKey), "obj:"+bucket+"/"+key+"/")
		vid := rest
		if vid == skipVID {
			return nil
		}
		if _, err := uuid.Parse(vid); err != nil {
			return nil
		}
		raw, err := gb.itemValueCopy(item)
		if err != nil {
			return nil
		}
		m, err := unmarshalObjectMeta(raw)
		if err != nil {
			return nil
		}
		if len(m.NodeIDs) > 0 {
			found = cloneStringSlice(m.NodeIDs)
			return errStopScan
		}
		return nil
	})
	return found
}

// walkPerVersionBackfillCandidates iterates every FSM obj:{bucket}/{key}/{vid}
// record across ALL locally-hosted generation-group stores, and calls fn for
// each version whose per-version quorum-meta blob is ABSENT from disk and which
// passes the placement and age gates. It is read-only (no writes or deletes).
//
// Gates (early return nil):
//   - bucket versioning not enabled
//   - bucket's owning group not locally hosted (cannot judge)
//   - shardSvc is nil (no dataDir to check)
//
// Per-record skips (silent):
//   - Meta.NodeIDs is empty AND record is not a delete marker (no placement → cannot fan out)
//   - blob already present on disk (already backfilled — idempotency check)
//   - VID UUIDv7 timestamp age < minAge seconds (too fresh)
//
// Delete-marker records (ETag == deleteMarkerETag) ARE yielded — they need a
// per-version blob too. Degraded delete markers (NodeIDs == nil, which happens
// when deleteObjectWithMarker skipped writeQuorumMeta because the prior version
// had no quorum meta) have their NodeIDs derived from a sibling version of the
// same key (lookupSiblingNodeIDs). If no sibling with placement exists, the
// degraded marker is skipped.
func (b *DistributedBackend) walkPerVersionBackfillCandidates(
	bucket string,
	now, minAge int64,
	fn func(perVersionBackfillCandidate) error,
) error {
	ctx := context.Background()
	if !b.bucketVersioningEnabled(ctx, bucket) {
		return nil
	}
	if !b.owningGroupHosted(bucket) {
		return nil
	}
	if b.shardSvc == nil {
		return nil
	}
	dataDirs := b.shardSvc.DataDirs()
	if len(dataDirs) == 0 {
		return nil
	}
	versionRoot := filepath.Join(dataDirs[0], quorumMetaVersionsSubDir)

	rawObjPrefix := []byte("obj:" + bucket + "/")

	for _, gb := range b.hostedGroupBackends() {
		if gb == nil {
			continue
		}
		var scanErr error
		_ = gb.store.View(func(txn MetadataTxn) error {
			return gb.ks().scanGroupPrefix(txn, rawObjPrefix, func(rawKey []byte, item MetaItem) error {
				rest := strings.TrimPrefix(string(rawKey), "obj:"+bucket+"/")
				slash := strings.LastIndex(rest, "/")
				if slash < 0 {
					// Legacy unversioned record — not a versioned candidate.
					return nil
				}
				key := rest[:slash]
				vid := rest[slash+1:]

				// UUID guard: VIDs are always UUIDv7. If the vid segment does not
				// parse as a UUID, this is a pre-versioning (unversioned) FSM record
				// whose object key contains a slash (e.g. obj:bkt/a/b → mis-parsed
				// key="a", vid="b"). Skip it to avoid fanning out a garbage-keyed blob.
				if _, uuidErr := uuid.Parse(vid); uuidErr != nil {
					return nil
				}

				// Age gate: derive from UUIDv7 timestamp (LastModified may be 0 for markers).
				vidAge := now - uuidv7TimeUnix(vid)
				if vidAge < minAge {
					return nil
				}

				// Decode FSM record.
				raw, err := gb.itemValueCopy(item)
				if err != nil {
					return nil // transient; skip
				}
				meta, err := unmarshalObjectMeta(raw)
				if err != nil {
					return nil // undecodable; skip
				}

				// Authoritative key-type discriminator: for a genuine versioned
				// record (obj:bkt/key/vid) the decoded meta.Key equals the parsed
				// key segment. For a mis-parsed unversioned record whose object key
				// ends in a UUID (e.g. obj:bkt/a/<uuid>) the decoded meta.Key is
				// the FULL real key ("a/<uuid>"), which differs from the parsed key
				// segment ("a"). Skip the latter to avoid writing phantom blobs.
				if meta.Key != key {
					return nil
				}

				// Appendable/coalesced carve-out (foundation spec): AppendObject
				// objects are FSM-authoritative and must not receive a per-version
				// quorum-meta blob from S3. Backfilling them would reconstruct a
				// blob with IsAppendable=false (PutObjectMetaCmd has no such field),
				// causing GetObjectVersion to see "no readable layout" and fail.
				if meta.IsAppendable || len(meta.Coalesced) > 0 {
					return nil
				}

				// Skip versions with no placement — nothing to fan out to.
				// Exception: degraded delete markers (ETag == deleteMarkerETag) may
				// have empty NodeIDs when deleteObjectWithMarker skipped writeQuorumMeta
				// because the prior version had no quorum meta. For those, derive
				// placement from a sibling version of the same key.
				if len(meta.NodeIDs) == 0 {
					if meta.ETag != deleteMarkerETag {
						return nil
					}
					// Try to derive NodeIDs from a sibling versioned record.
					siblingIDs := lookupSiblingNodeIDs(txn, gb, bucket, key, vid)
					if len(siblingIDs) == 0 {
						return nil // no sibling placement found; cannot fan out
					}
					meta.NodeIDs = siblingIDs
				}

				// Idempotency: check whether the per-version blob exists on disk.
				// Use os.Stat per VID rather than ScanQuorumMetaVersionsBucket
				// (which returns only the max-VID per key and would hide present
				// non-latest blobs).
				blobPath := filepath.Join(versionRoot, bucket, key, vid)
				if _, statErr := os.Stat(blobPath); statErr == nil {
					// Blob already present — skip.
					return nil
				}

				// Yield the candidate.
				if ferr := fn(perVersionBackfillCandidate{
					Bucket:    bucket,
					Key:       key,
					VersionID: vid,
					Meta:      meta,
				}); ferr != nil {
					scanErr = ferr
					return errStopScan
				}
				return nil
			})
		})
		if scanErr != nil {
			return fmt.Errorf("walkPerVersionBackfillCandidates bucket %s: %w", bucket, scanErr)
		}
	}
	return nil
}

// segmentRefsToMetaEntries converts []storage.SegmentRef back to
// []SegmentMetaEntry for use in a reconstructed PutObjectMetaCmd. The
// SegmentIdx field is set from the slice position (matches the original
// ordering established at write time by buildSegmentMetaEntries).
func segmentRefsToMetaEntries(refs []storage.SegmentRef) []SegmentMetaEntry {
	if len(refs) == 0 {
		return nil
	}
	out := make([]SegmentMetaEntry, len(refs))
	for i, r := range refs {
		out[i] = SegmentMetaEntry{
			BlobID:           r.BlobID,
			Size:             r.Size,
			Checksum:         append([]byte(nil), r.Checksum...),
			PlacementGroupID: r.PlacementGroupID,
			ShardSize:        r.ShardSize,
			SegmentIdx:       int32(i),
			NodeIDs:          cloneStringSlice(r.NodeIDs),
			ECData:           r.ECData,
			ECParity:         r.ECParity,
			StripeBytes:      r.StripeBytes,
		}
	}
	return out
}

// backfillPerVersionBlob reconstructs a PutObjectMetaCmd from the candidate's
// FSM objectMeta and fans the encoded blob out to the per-version quorum-meta
// tree on every placement node. It mirrors S1's per-version fan-out block in
// writeQuorumMeta (quorum_meta.go:104-122), reusing the exact same helpers.
//
// Correctness invariants:
//   - IsDeleteMarker is reconstructed from the ETag sentinel (objectMeta has no
//     dedicated flag; c.Meta.ETag == deleteMarkerETag is the only signal).
//   - writeQuorumMetaVersionLocal writes UNCONDITIONALLY via os.Rename (it has
//     no existence check). Idempotency / no-overwrite on the LOCAL node is
//     enforced solely by the walker's os.Stat gate in
//     walkPerVersionBackfillCandidates, which skips candidates whose blob is
//     already present before yielding them here.
//   - The existence gate is LOCAL-NODE-ONLY. fanOutQuorumMeta writes to ALL
//     placement nodes, and both the local write and the remote
//     WriteQuorumMetaVersion handler overwrite unconditionally. A peer that
//     already holds the blob may therefore have it overwritten with this
//     field-equivalent reconstruction. This is safe today because per-version
//     blobs are immutable and the reconstruction is read-field-equivalent (the
//     only diverging fields — ExpectedETag and PreserveLatest — are
//     write-time-only and read-irrelevant). S4 cutover must confirm no read
//     path consumes those dropped write-time fields before relying on this
//     assumption.
//   - ExpectedETag and PreserveLatest are write-time-only fields absent from
//     objectMeta; they are intentionally left at their zero values and are
//     read-irrelevant.
func (b *DistributedBackend) backfillPerVersionBlob(ctx context.Context, c perVersionBackfillCandidate) error {
	cmd := PutObjectMetaCmd{
		Bucket:           c.Bucket,
		Key:              c.Key,
		VersionID:        c.VersionID,
		Size:             c.Meta.Size,
		ContentType:      c.Meta.ContentType,
		ETag:             c.Meta.ETag,
		ModTime:          c.Meta.LastModified,
		ECData:           c.Meta.ECData,
		ECParity:         c.Meta.ECParity,
		StripeBytes:      c.Meta.StripeBytes,
		NodeIDs:          cloneStringSlice(c.Meta.NodeIDs),
		PlacementGroupID: c.Meta.PlacementGroupID,
		UserMetadata:     c.Meta.UserMetadata,
		SSEAlgorithm:     c.Meta.SSEAlgorithm,
		ACL:              c.Meta.ACL,
		Tags:             append([]storage.Tag(nil), c.Meta.Tags...),
		Parts:            append([]storage.MultipartPartEntry(nil), c.Meta.Parts...),
		Segments:         segmentRefsToMetaEntries(c.Meta.Segments),
		MetaSeq:          c.Meta.MetaSeq,
		// Reconstruct the delete-marker flag from the ETag sentinel.
		// objectMeta has no dedicated IsDeleteMarker field; deleteMarkerETag
		// is the sole signal. Without this, a backfilled marker decodes as a
		// live object and corrupts deriveLatestVersion.
		IsDeleteMarker: c.Meta.ETag == deleteMarkerETag,
		// ExpectedETag and PreserveLatest are write-time-only; absent from
		// objectMeta and read-irrelevant — left at zero values.
		//
		// NOT carried: IsAppendable. PutObjectMetaCmd has no IsAppendable field
		// (it lives only on objectMeta), so S1's blob ALSO decodes with
		// IsAppendable=false — the backfill is field-equivalent to S1 here.
		// Appendable-versioned objects are a documented foundation carve-out
		// (future concern); see Task 3's caller.
	}

	blob, err := EncodeCommand(CmdPutObjectMeta, cmd)
	if err != nil {
		return fmt.Errorf("backfillPerVersionBlob encode %s/%s@%s: %w", c.Bucket, c.Key, c.VersionID, err)
	}

	self := b.currentSelfAddr()
	k := max(1, int(c.Meta.ECData))
	verSubpath := path.Join(c.Key, c.VersionID)

	wctx, cancel := context.WithTimeout(ctx, quorumMetaWriteTimeout)
	defer cancel()
	if err := fanOutQuorumMeta(wctx, c.Meta.NodeIDs, k, func(fctx context.Context, node string) error {
		if node == self {
			return b.shardSvc.writeQuorumMetaVersionLocal(c.Bucket, verSubpath, blob)
		}
		addr, rerr := b.shardSvc.resolvePeerAddress(node)
		if rerr != nil {
			return rerr
		}
		return b.shardSvc.WriteQuorumMetaVersion(fctx, addr, c.Bucket, verSubpath, blob)
	}); err != nil {
		return fmt.Errorf("backfillPerVersionBlob fan-out %s/%s@%s: %w", c.Bucket, c.Key, c.VersionID, err)
	}
	return nil
}

// defaultBackfillMinAge is the minimum version age before the backfill sweep
// considers it a candidate. Mirrors the orphan/redundancy sweep age gates.
const defaultBackfillMinAge = 5 * time.Minute

// ListBackfillBuckets returns the union of all locally-hosted generation
// groups' buckets, mirroring SegmentSweepBuckets. Implements scrubber.PerVersionBackfillable.
func (b *DistributedBackend) ListBackfillBuckets(ctx context.Context) ([]string, error) {
	return b.SegmentSweepBuckets(ctx)
}

// WalkPerVersionBackfillCandidates calls fn for each versioned object record
// whose per-version quorum-meta blob is absent. Implements scrubber.PerVersionBackfillable.
// The Opaque field of each BackfillCandidate carries the perVersionBackfillCandidate
// so BackfillPerVersionBlob avoids a second metadata lookup.
func (b *DistributedBackend) WalkPerVersionBackfillCandidates(ctx context.Context, bucket string, fn func(scrubber.BackfillCandidate) error) error {
	now := time.Now().Unix()
	minAge := int64(defaultBackfillMinAge / time.Second)
	return b.walkPerVersionBackfillCandidates(bucket, now, minAge, func(c perVersionBackfillCandidate) error {
		return fn(scrubber.BackfillCandidate{
			Bucket:    c.Bucket,
			Key:       c.Key,
			VersionID: c.VersionID,
			Opaque:    c,
		})
	})
}

// BackfillPerVersionBlob fans out the missing per-version blob to the
// object's placement nodes. Uses the Opaque field (set by WalkPerVersionBackfillCandidates)
// to avoid a second metadata lookup. Implements scrubber.PerVersionBackfillable.
func (b *DistributedBackend) BackfillPerVersionBlob(ctx context.Context, c scrubber.BackfillCandidate) error {
	cand, ok := c.Opaque.(perVersionBackfillCandidate)
	if !ok {
		return fmt.Errorf("BackfillPerVersionBlob %s/%s@%s: missing opaque candidate", c.Bucket, c.Key, c.VersionID)
	}
	return b.backfillPerVersionBlob(ctx, cand)
}
