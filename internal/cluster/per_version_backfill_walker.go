package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

// perVersionBackfillCandidate is an FSM obj: record that needs its per-version
// quorum-meta blob written (the blob is absent from disk). Tasks 2-3 consume
// this to fan out the write and drive the sweep.
//
//nolint:unused // consumed by walkPerVersionBackfillCandidates and per_version_backfill_walker_test.go.
type perVersionBackfillCandidate struct {
	Bucket, Key, VersionID string
	Meta                   objectMeta
}

// uuidv7TimeUnix extracts the 48-bit Unix millisecond timestamp embedded in a
// UUIDv7 string and returns it as Unix seconds. Returns 0 if the string is not
// a valid UUID (fail-open: the caller's age gate treats age 0 as epoch → old).
//
//nolint:unused // called by walkPerVersionBackfillCandidates.
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
//
//nolint:unused // invoked by per_version_backfill_walker_test.go; Tasks 2-3 will add the production caller.
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
