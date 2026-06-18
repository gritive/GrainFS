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
//   - Meta.NodeIDs is empty (no placement → cannot fan out)
//   - blob already present on disk (already backfilled — idempotency check)
//   - VID UUIDv7 timestamp age < minAge seconds (too fresh)
//
// Delete-marker records (ETag == deleteMarkerETag) ARE yielded — they need a
// per-version blob too.
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

				// Skip versions with no placement — nothing to fan out to.
				if len(meta.NodeIDs) == 0 {
					return nil
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
//   - The blob is NOT written if it already exists on disk (idempotent;
//     writeQuorumMetaVersionLocal uses os.Rename only when the target is
//     absent — the walker's skip-if-exists gate also prevents redundant calls).
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
