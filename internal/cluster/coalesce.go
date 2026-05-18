package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"
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
	// Phase B3 EC placement params. Order in Placement matches shard index
	// 0..k+m-1. Zero-valued for B2 (owner-local only).
	Placement   []string
	ECData      uint8
	ECParity    uint8
	RingVersion uint64
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
			RingVersion: c.RingVersion,
			ECData:      c.ECData,
			ECParity:    c.ECParity,
			NodeIDs:     append([]string(nil), c.NodeIDs...),
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

// coalescedSpoolDir is the on-disk staging area for EC shard spooling
// during coalesce. Mirrors ecSpoolDir() but isolated so concurrent PutObject
// + coalesce don't clobber each other's tmp files.
func (b *DistributedBackend) coalescedSpoolDir() string {
	return filepath.Join(b.root, "tmp", "coalesced-spool")
}

// processCoalesceJobB3 is the Phase B3 implementation. Same lifecycle as B2
// (snapshot → owner-local merge → propose → cleanup) but adds the EC
// distribute step between merge and propose:
//
//  1. HeadObject → snapshot S = current segments.
//  2. mergeSegmentsOwnerLocal(S) → single coalesced blob on owner disk.
//  3. selectECPlacement + writeSpooledShards distribute the coalesced blob
//     across k+m peers. shardKey = "<key>/coalesced/<coalescedID>".
//  4. propose CmdCoalesceSegments with EC params; FSM apply stores them on
//     the resulting CoalescedShardRef.
//  5. Remove owner-local coalesced blob + raw segment files. EC shards are
//     the source of truth from this point.
//
// Failure recovery: any error after EC write but before propose leaves
// orphan EC shards which the scrubber sweeps. Idempotent on retry — apply
// no-ops if CoalescedID already present.
func (b *DistributedBackend) processCoalesceJobB3(ctx context.Context, job coalesceJob) error {
	obj, err := b.HeadObject(ctx, job.Bucket, job.Key)
	if err != nil || obj == nil {
		return nil
	}
	if !obj.IsAppendable || len(obj.Segments) == 0 {
		return nil
	}
	snapshot := make([]storage.SegmentRef, len(obj.Segments))
	copy(snapshot, obj.Segments)
	coalescedID := uuid.Must(uuid.NewV7()).String()
	merged, mErr := b.mergeSegmentsOwnerLocal(job.Bucket, job.Key, coalescedID, snapshot)
	if mErr != nil {
		return fmt.Errorf("merge: %w", mErr)
	}
	// Cleanup intermediate file on every exit path; success path removes it
	// once EC shards land + propose returns.
	cleanupMerged := func() { _ = os.Remove(merged.Path) }

	shardKey := job.Key + "/coalesced/" + coalescedID
	liveNodes := b.ecWriteNodes()
	cfg := EffectiveConfig(len(liveNodes), b.currentECConfig())
	if cfg.NumShards() == 0 {
		cleanupMerged()
		return fmt.Errorf("coalesce: no effective EC config for %d live nodes", len(liveNodes))
	}
	currentRing, ringErr := b.fsm.GetRingStore().GetCurrentRing()
	placement, ringVer := selectECPlacement(currentRing, ringErr, cfg, liveNodes, shardKey)
	if len(placement) != cfg.NumShards() {
		cleanupMerged()
		return fmt.Errorf("coalesce: placement has %d nodes, need %d (k=%d m=%d)",
			len(placement), cfg.NumShards(), cfg.DataShards, cfg.ParityShards)
	}

	plan := ecObjectWritePlan{
		Bucket:      job.Bucket,
		Key:         job.Key,
		VersionID:   "coalesced/" + coalescedID,
		Config:      cfg,
		Placement:   placement,
		RingVersion: ringVer,
	}
	sp := &spooledObject{Path: merged.Path, Size: merged.Size, ETag: merged.ETag}
	writer := newECObjectWriter(b.currentSelfAddr(), b.shardSvc, b.currentPeerHealth())
	if _, err := writer.writeSpooledShards(ctx, plan, b.coalescedSpoolDir(), sp); err != nil {
		cleanupMerged()
		return fmt.Errorf("ec write: %w", err)
	}

	// Test-only fault hook: simulate a crash after EC shards land but before
	// the FSM commit. The next worker iteration must re-coalesce and reach a
	// consistent final state.
	if hook := b.coalesceFaultAfterECWrite; hook != nil {
		if err := hook(); err != nil {
			cleanupMerged()
			return fmt.Errorf("fault: %w", err)
		}
	}

	consumedIDs := make([]string, len(snapshot))
	for i, s := range snapshot {
		consumedIDs[i] = s.BlobID
	}
	cmd := CoalesceSegmentsCmd{
		Bucket:             job.Bucket,
		Key:                job.Key,
		CoalescedID:        coalescedID,
		ShardKey:           shardKey,
		Size:               merged.Size,
		ETag:               merged.ETag,
		ConsumedSegmentIDs: consumedIDs,
		Placement:          placement,
		ECData:             uint8(cfg.DataShards),
		ECParity:           uint8(cfg.ParityShards),
		RingVersion:        uint64(ringVer),
	}
	if err := b.propose(ctx, CmdCoalesceSegments, cmd); err != nil {
		// EC shards committed but never referenced — scrubber sweeps. Keep raw
		// segments so a future retry can succeed.
		cleanupMerged()
		return fmt.Errorf("propose coalesce: %w", err)
	}
	// Source of truth is now the EC shards. Remove owner-local intermediate +
	// raw segments. Best-effort: scrubber sweeps orphans.
	cleanupMerged()
	for _, s := range snapshot {
		_ = os.Remove(b.segmentBlobPath(job.Bucket, job.Key, s.BlobID))
	}
	return nil
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

// maybeTriggerCoalesce evaluates the trigger thresholds against the supplied
// segment slice and enqueues a coalesce job if any threshold is met. The
// caller passes the live segments (e.g. post-AppendObject HeadObject result)
// so we never re-read BadgerDB on the hot path.
//
// first-seen tracking: a sync.Map keyed by "<bucket>\x00<key>" records the
// timestamp of the first segment observed in the current batch so that the
// idle-timeout branch fires for low-volume objects.
func (b *DistributedBackend) maybeTriggerCoalesce(bucket, key string, segs []storage.SegmentRef) {
	if b.coalesce == nil || len(segs) == 0 {
		return
	}
	cacheKey := bucket + "\x00" + key
	nowT := time.Now()
	firstT := nowT
	if v, ok := b.coalesceFirstSeen.LoadOrStore(cacheKey, nowT); ok {
		firstT = v.(time.Time)
	}
	trig, _ := evaluateCoalesceTrigger(segs, firstT, nowT, b.coalesceCfg)
	if !trig {
		return
	}
	b.coalesce.Enqueue(coalesceJob{Bucket: bucket, Key: key})
	// Reset the first-seen marker so the next batch starts a fresh window.
	b.coalesceFirstSeen.Delete(cacheKey)
}

// coalesceBackstopScan periodically scans appendable objects and re-enqueues
// any whose in-process trigger was missed (process restart, dropped enqueue
// when the worker buffer was full). Cleanup interval is set from
// coalesceCfg.CleanupInterval; zero/negative values disable the scanner.
func (b *DistributedBackend) coalesceBackstopScan(ctx context.Context) {
	if b.coalesceCfg.CleanupInterval <= 0 {
		return
	}
	ticker := time.NewTicker(b.coalesceCfg.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.scanAppendableAndTrigger(ctx)
		}
	}
}

// scanAppendableAndTrigger iterates ObjectMetaKey prefix entries and enqueues
// any appendable object with raw segments whose threshold is satisfied. Each
// object is enqueued at most once per batch thanks to coalesceFirstSeen +
// worker dedup.
//
// Best-effort: decode errors / corrupt entries are skipped silently. The
// scanner is the safety-net for missed in-process triggers (process restart,
// dropped enqueue when buffer is full) — not a strong consistency primitive.
func (b *DistributedBackend) scanAppendableAndTrigger(ctx context.Context) {
	if b.db == nil || b.fsm == nil {
		return
	}
	rawPrefix := []byte("obj:")
	_ = b.db.View(func(txn *badger.Txn) error {
		return b.ks().scanGroupPrefix(txn, rawPrefix, func(rawKey []byte, item *badger.Item) error {
			select {
			case <-ctx.Done():
				return errStopScan
			default:
			}
			// rawKey format: "obj:<bucket>/<key>" or "obj:<bucket>/<key>/<ver>".
			// Skip versioned entries (legacy + latest share the same body via
			// dual-write; iterating both is redundant work).
			rest := rawKey[len("obj:"):]
			slash := bytes.IndexByte(rest, '/')
			if slash <= 0 {
				return nil
			}
			bucket := string(rest[:slash])
			afterBucket := rest[slash+1:]
			// Versioned key has a second '/'; skip those — we already see the
			// canonical legacy mirror via the bucket/key entry.
			if bytes.IndexByte(afterBucket, '/') >= 0 {
				return nil
			}
			key := string(afterBucket)
			var meta objectMeta
			if err := item.Value(func(raw []byte) error {
				v, oerr := b.fsm.openValue(item.Key(), raw)
				if oerr != nil {
					return oerr
				}
				m, derr := unmarshalObjectMeta(v)
				if derr != nil {
					return derr
				}
				meta = m
				return nil
			}); err != nil {
				return nil // skip undecodable
			}
			if !meta.IsAppendable || len(meta.Segments) == 0 {
				return nil
			}
			b.maybeTriggerCoalesce(bucket, key, meta.Segments)
			return nil
		})
	})
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
