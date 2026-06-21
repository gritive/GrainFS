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

// deriveMarkerPlacement computes NodeIDs + ECConfig for a delete marker whose
// FSM record carries no placement (the degraded case where deleteObjectWithMarker
// could not read the prior version's quorum-meta at delete time, so NodeIDs was
// left empty). It places the marker's per-version metadata blob on the owning
// group's current peer set.
//
// Why this placement is correct for a TOMBSTONE (it deliberately does NOT
// reproduce a data write's exact shard placement, and need not): a delete marker
// has no data shards, so the only thing its per-version blob must satisfy is
// DISCOVERABILITY by the derive-by-scan read. readQuorumMetaVersions
// (quorum_meta.go:517) fans out to EVERY ShardGroups() peer and unions whatever
// .quorum_meta_versions/{bucket}/{key}/ blobs each holds — it is NOT keyed to the
// object's specific NodeIDs. The owning group's peers are ShardGroups() members,
// so a marker blob written here is found by that readback and correctly resolves
// as the latest delete-marker (hiding any older live version). A real data write
// keys placement by ecObjectShardKey(key, versionID) / segment keys and a real
// marker copies existing.NodeIDs; matching those byte-for-byte is unnecessary for
// a tombstone's metadata blob (and impossible when the placement was never
// recorded — that is precisely why NodeIDs is empty here).
//
// ECData = the owning group's current EC config: a marker carries no data, so
// ECData only sets the K-of-N write-quorum strength (k = max(1, ECData)) for the
// metadata blob, not any data-shard count. Current-group config is the right
// durability target for a freshly written blob.
//
// Returns (nil, ECConfig{}) if unresolvable (DataShards == 0 or empty node list);
// the caller skips the candidate (the writer cannot fan out to zero nodes).
func deriveMarkerPlacement(gb *DistributedBackend, key string) ([]string, ECConfig) {
	ecCfg := gb.currentECConfig()
	if ecCfg.DataShards == 0 {
		return nil, ECConfig{}
	}
	nodes := gb.configuredNodeList()
	if len(nodes) == 0 {
		return nil, ECConfig{}
	}
	shardKey := ecObjectShardKey(key, "")
	return PlacementForNodes(ecCfg, nodes, shardKey), ecCfg
}

// forEachHostedObjVersion iterates every FSM obj:{bucket}/{key}/{vid} record
// across all locally-hosted generation-group stores, calling fn for each.
//
// The fn receives a non-nil decodeErr when the raw FSM value cannot be read or
// decoded; meta is zero-valued in that case. The fn receives a nil decodeErr
// and a populated meta when the record is a confirmed versioned entry whose
// decoded meta.Key matches the parsed key segment (the unversioned-slash-key
// guard). Returning errStopScan from fn halts the current group's scan cleanly.
//
// Guards applied by this enumerator (silent skip, NOT propagated to fn):
//   - slash < 0 (legacy unversioned record, no version suffix)
//   - uuid.Parse(vid) fails (pre-versioning unversioned key with slash)
//   - decode succeeds but meta.Key != key (unversioned key whose last segment is a valid UUID)
func (b *DistributedBackend) forEachHostedObjVersion(
	bucket string,
	fn func(gb *DistributedBackend, key, versionID string, meta objectMeta, decodeErr error) error,
) error {
	rawObjPrefix := []byte("obj:" + bucket + "/")
	for _, gb := range b.hostedGroupBackends() {
		if gb == nil {
			continue
		}
		var scanErr error
		viewErr := gb.store.View(func(txn MetadataTxn) error {
			return gb.ks().scanGroupPrefix(txn, rawObjPrefix, func(rawKey []byte, item MetaItem) error {
				rest := strings.TrimPrefix(string(rawKey), "obj:"+bucket+"/")
				slash := strings.LastIndex(rest, "/")
				if slash < 0 {
					// Legacy unversioned record — not a versioned entry.
					return nil
				}
				key := rest[:slash]
				vid := rest[slash+1:]

				// UUID guard: VIDs are always UUIDv7. If the vid segment does not
				// parse as a UUID, this is a pre-versioning (unversioned) FSM record
				// whose object key contains a slash (e.g. obj:bkt/a/b → mis-parsed
				// key="a", vid="b"). Skip it silently.
				if _, uuidErr := uuid.Parse(vid); uuidErr != nil {
					return nil
				}

				// Decode FSM record; pass decode errors to fn (fail-closed).
				raw, err := gb.itemValueCopy(item)
				if err != nil {
					if ferr := fn(gb, key, vid, objectMeta{}, err); ferr != nil {
						scanErr = ferr
						return errStopScan
					}
					return nil
				}
				// unmarshalObjectMeta's fbSafe recover covers only the root parse
				// (GetRootAsObjectMeta). FlatBuffers FIELD accessors (NodeIdsLength,
				// Segments, Coalesced, …) panic OUTSIDE that recover on a record that
				// parses as a root but carries a bad vtable/vector offset. Wrap the
				// whole decode so such a panic becomes a decodeErr — fn receives it and
				// fail-closes (the S3 backfill fn skips the record), rather than
				// crashing the sweep.
				meta, err := func() (m objectMeta, e error) {
					defer func() {
						if r := recover(); r != nil {
							e = fmt.Errorf("unmarshalObjectMeta panic: %v", r)
						}
					}()
					return unmarshalObjectMeta(raw)
				}()
				if err != nil {
					if ferr := fn(gb, key, vid, objectMeta{}, err); ferr != nil {
						scanErr = ferr
						return errStopScan
					}
					return nil
				}

				// Authoritative key-type discriminator: for a genuine versioned
				// record (obj:bkt/key/vid) the decoded meta.Key equals the parsed
				// key segment. For a mis-parsed unversioned record whose object key
				// ends in a UUID (e.g. obj:bkt/a/<uuid>) the decoded meta.Key is
				// the FULL real key ("a/<uuid>"), which differs from the parsed key
				// segment ("a"). Skip the latter silently.
				if meta.Key != key {
					return nil
				}

				if ferr := fn(gb, key, vid, meta, nil); ferr != nil {
					scanErr = ferr
					return errStopScan
				}
				return nil
			})
		})
		if scanErr != nil {
			return fmt.Errorf("forEachHostedObjVersion bucket %s: %w", bucket, scanErr)
		}
		if viewErr != nil {
			return fmt.Errorf("forEachHostedObjVersion bucket %s scan: %w", bucket, viewErr)
		}
	}
	return nil
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
// had no readable quorum meta at delete time) have their placement + ECConfig
// derived RDH-direct from the owning group (deriveMarkerPlacement) — sufficient
// because the tombstone blob need only be DISCOVERABLE by the all-groups
// readQuorumMetaVersions readback, not byte-match a data write's shard placement
// (see deriveMarkerPlacement). If placement is unresolvable, the marker is skipped.
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
	// Soleauth pending/on: the bucket is mid/post-cutover; leaderless backfill must
	// not write per-version blobs under the fence (the cutover relies on sole-
	// authority writes only). FAIL-CLOSED: a soleauth READ error must ALSO skip —
	// GetBucketSoleAuthority maps only an ABSENT key to "off"; a real error returns
	// ("", err), so swallowing it would let a pending/on bucket backfill under the
	// fence. Skipping on error is safe: backfill is periodic + idempotent (retried
	// next cycle). Dormant: every prod bucket is off (no skip).
	if sa, saErr := b.GetBucketSoleAuthority(bucket); backfillSkipForSoleAuth(sa, saErr) {
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

	return b.forEachHostedObjVersion(bucket, func(gb *DistributedBackend, key, vid string, meta objectMeta, decodeErr error) error {
		if decodeErr != nil {
			return nil // transient or undecodable; skip silently
		}

		// Age gate: derive from UUIDv7 timestamp (LastModified may be 0 for markers).
		vidAge := now - uuidv7TimeUnix(vid)
		if vidAge < minAge {
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
		// placement via RDH-direct: the same (group peer set, EC config, key)
		// triple a WRITE uses, so the marker lands on the same nodes as its
		// sibling live versions.
		if len(meta.NodeIDs) == 0 {
			if meta.ETag != deleteMarkerETag {
				return nil
			}
			// Derive NodeIDs and ECConfig from the owning group's topology.
			nodeIDs, ecCfg := deriveMarkerPlacement(gb, key)
			if len(nodeIDs) == 0 {
				return nil // unresolvable placement; skip
			}
			meta.NodeIDs = nodeIDs
			meta.ECData = uint8(ecCfg.DataShards)
			meta.ECParity = uint8(ecCfg.ParityShards)
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
		return fn(perVersionBackfillCandidate{
			Bucket:    bucket,
			Key:       key,
			VersionID: vid,
			Meta:      meta,
		})
	})
}

// backfillSkipForSoleAuth decides whether the per-version backfill walker must
// skip a bucket given its soleauth state and the error from reading it.
// FAIL-CLOSED: a non-nil read error skips (a pending/on bucket must never
// backfill under the fence; GetBucketSoleAuthority maps only an absent key to
// "off", so a real error means the true state is unknown). A pending or on state
// skips. Only an off state with no error proceeds.
func backfillSkipForSoleAuth(state string, err error) bool {
	return err != nil || state == soleAuthPending || state == soleAuthOn
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
//   - writeQuorumMetaVersionLocal applies a write-time LWW guard (S4c-0 PR1):
//     before renaming it decodes the existing on-disk blob and SKIPS the write
//     when the existing wins quorumMetaBlobWins(ModTime,VersionID,MetaSeq). So a
//     stale backfill reconstruction (which carries the FSM-derived MetaSeq) is
//     skipped whenever a newer RMW/relocation blob already exists — this closes
//     the leaderless-backfill-vs-RMW lost-update. The walker's os.Stat gate in
//     walkPerVersionBackfillCandidates still skips already-present candidates
//     before yielding them here, so the guard's decode is a no-op in the common
//     (absent-blob) case.
//   - The guard runs on BOTH the local write and the remote
//     WriteQuorumMetaVersion handler (both funnel through the same leaf), so a
//     peer that already holds a NEWER blob is no longer clobbered by this
//     field-equivalent reconstruction. A peer holding an OLDER or tied blob is
//     overwritten, which is safe: per-version blobs are read-field-equivalent
//     here (the only diverging fields — ExpectedETag and PreserveLatest — are
//     write-time-only and read-irrelevant).
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
	epoch, _ := b.GetBucketSoleAuthEpoch(c.Bucket)

	wctx, cancel := context.WithTimeout(ctx, quorumMetaWriteTimeout)
	defer cancel()
	if err := fanOutQuorumMeta(wctx, c.Meta.NodeIDs, k, func(fctx context.Context, node string) error {
		if node == self {
			return b.shardSvc.writeQuorumMetaVersionLocal(c.Bucket, verSubpath, blob, epoch)
		}
		addr, rerr := b.shardSvc.resolvePeerAddress(node)
		if rerr != nil {
			return rerr
		}
		return b.shardSvc.WriteQuorumMetaVersion(fctx, addr, c.Bucket, verSubpath, blob, epoch)
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
