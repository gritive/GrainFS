package cluster

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/storage"
)

// CoalesceSegmentsCmd is the Raft payload that records a single coalesce
// operation: take a prefix of objectMeta.Segments (identified by blobIDs in
// ConsumedSegmentIDs) and replace them with one CoalescedShardRef.
//
// Apply MUST be idempotent: replay after partial application is safe because
// the apply path only removes segments whose BlobID still appears in
// objectMeta.Segments. See design 2026-05-18-append-segment-coalesce-ec-design.md
// § "Race handling".
type CoalesceSegmentsCmd struct {
	Bucket             string
	Key                string
	CoalescedID        string   // UUIDv7
	ShardKey           string   // EC shardKey = "<key>/coalesced/<coalescedID>"
	Size               int64    // coalesced data total bytes
	ETag               string   // coalesced body MD5
	ConsumedSegmentIDs []string // segment blob IDs consumed by this operation
}

// MaxCoalescedEntries caps how many CoalescedShardRef entries a single
// appendable object may accumulate. Prevents unbounded chain when raw
// segments keep arriving after each coalesce. Reaching this cap stalls
// coalesce until the object is rotated/closed.
const MaxCoalescedEntries = 1024

// coalescedRefsToStorage projects cluster-level CoalescedShardRef entries
// onto the storage-package mirror type so that storage.Object stays
// independent of cluster wire types.
func coalescedRefsToStorage(in []CoalescedShardRef) []storage.CoalescedRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]storage.CoalescedRef, len(in))
	for i, c := range in {
		out[i] = storage.CoalescedRef{
			CoalescedID: c.CoalescedID,
			Size:        c.Size,
			ETag:        c.ETag,
			ShardKey:    c.ShardKey,
		}
	}
	return out
}

// coalescedBlobPath returns the on-disk path for one coalesced blob.
// Mirrors segmentBlobPath but under "_coalesced" suffix.
func (b *DistributedBackend) coalescedBlobPath(bucket, key, coalescedID string) string {
	return filepath.Join(b.objectPath(bucket, key)+"_coalesced", coalescedID)
}

// coalesceMergeResult is the owner-local merge output before EC distribution.
type coalesceMergeResult struct {
	Path string
	Size int64
	ETag string
}

// mergeSegmentsOwnerLocal reads the given segments owner-locally (no
// forward-on-read) and writes them concatenated to coalescedBlobPath.
// Returns size + MD5 of the concatenated body.
//
// Caller MUST be the owner node — non-owner cannot satisfy local Open.
func (b *DistributedBackend) mergeSegmentsOwnerLocal(bucket, key, coalescedID string, segs []storage.SegmentRef) (coalesceMergeResult, error) {
	out := b.coalescedBlobPath(bucket, key, coalescedID)
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return coalesceMergeResult{}, fmt.Errorf("mkdir: %w", err)
	}
	f, err := os.Create(out)
	if err != nil {
		return coalesceMergeResult{}, fmt.Errorf("create: %w", err)
	}
	h := md5.New()
	var total int64
	for _, s := range segs {
		in, oerr := os.Open(b.segmentBlobPath(bucket, key, s.BlobID))
		if oerr != nil {
			_ = f.Close()
			_ = os.Remove(out)
			return coalesceMergeResult{}, fmt.Errorf("open segment %s: %w", s.BlobID, oerr)
		}
		n, cerr := io.Copy(io.MultiWriter(f, h), in)
		_ = in.Close()
		if cerr != nil {
			_ = f.Close()
			_ = os.Remove(out)
			return coalesceMergeResult{}, fmt.Errorf("copy segment %s: %w", s.BlobID, cerr)
		}
		total += n
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(out)
		return coalesceMergeResult{}, fmt.Errorf("close: %w", err)
	}
	return coalesceMergeResult{
		Path: out,
		Size: total,
		ETag: hex.EncodeToString(h.Sum(nil)),
	}, nil
}

// processCoalesceJobB2 is the Phase B2 implementation (no EC). Phase B3
// will replace the owner-local merge with an EC encode + WriteShard
// distribution.
//
// Steps:
//  1. HeadObject → snapshot S = current segments (must be owner).
//  2. mergeSegmentsOwnerLocal(S) → single coalesced blob on owner disk.
//  3. propose CmdCoalesceSegments.
//  4. Unlink raw segment files for blobs in S (owner-local only).
//
// Best-effort cleanup: an unlink failure leaves orphan raw segments which the
// scrubber sweeps. Idempotent on retry because apply skips already-applied
// CoalescedIDs.
func (b *DistributedBackend) processCoalesceJobB2(ctx context.Context, job coalesceJob) error {
	obj, err := b.HeadObject(ctx, job.Bucket, job.Key)
	if err != nil || obj == nil {
		return nil // object gone — drop
	}
	if !obj.IsAppendable || len(obj.Segments) == 0 {
		return nil
	}
	// Snapshot segments — concurrent appends after this point are preserved
	// by applyCoalesceSegments (consumed-set match is exact BlobID).
	snapshot := make([]storage.SegmentRef, len(obj.Segments))
	copy(snapshot, obj.Segments)
	coalescedID := uuid.Must(uuid.NewV7()).String()
	merged, mErr := b.mergeSegmentsOwnerLocal(job.Bucket, job.Key, coalescedID, snapshot)
	if mErr != nil {
		return fmt.Errorf("merge: %w", mErr)
	}
	consumedIDs := make([]string, len(snapshot))
	for i, s := range snapshot {
		consumedIDs[i] = s.BlobID
	}
	cmd := CoalesceSegmentsCmd{
		Bucket:             job.Bucket,
		Key:                job.Key,
		CoalescedID:        coalescedID,
		ShardKey:           job.Key + "/coalesced/" + coalescedID,
		Size:               merged.Size,
		ETag:               merged.ETag,
		ConsumedSegmentIDs: consumedIDs,
	}
	if err := b.propose(ctx, CmdCoalesceSegments, cmd); err != nil {
		// Cleanup intermediate coalesced blob; raw segments remain intact
		// so a future retry can succeed.
		_ = os.Remove(merged.Path)
		return fmt.Errorf("propose coalesce: %w", err)
	}
	// Unlink raw segment files for blobs we just absorbed. Failure → scrubber
	// sweeps orphans later; apply already removed them from metadata.
	for _, s := range snapshot {
		_ = os.Remove(b.segmentBlobPath(job.Bucket, job.Key, s.BlobID))
	}
	return nil
}

// evaluateCoalesceTrigger returns (trigger, reason) for the given segment
// snapshot. Pure function — no side effects.
//
// firstCreatedAt is the timestamp of segments[0] (or the first observed
// time). Caller passes the wall clock for idle comparison (testable).
//
// Precedence: count → size → idle. The first satisfied condition wins.
func evaluateCoalesceTrigger(segs []storage.SegmentRef, firstCreatedAt, now time.Time, cfg CoalesceConfig) (bool, string) {
	if len(segs) == 0 {
		return false, ""
	}
	if cfg.SegmentCount > 0 && len(segs) >= cfg.SegmentCount {
		return true, "count"
	}
	var total int64
	for _, s := range segs {
		total += s.Size
	}
	if cfg.SizeBytes > 0 && total >= cfg.SizeBytes {
		return true, "size"
	}
	if cfg.IdleTimeout > 0 && now.Sub(firstCreatedAt) >= cfg.IdleTimeout {
		return true, "idle"
	}
	return false, ""
}
