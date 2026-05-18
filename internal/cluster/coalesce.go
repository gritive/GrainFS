package cluster

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

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
