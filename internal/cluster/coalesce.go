package cluster

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/uuidutil"
)

// CoalesceSegmentsPlan describes a single coalesce publish: take a prefix of
// objectMeta.Segments (identified by blobIDs in ConsumedSegmentIDs) and replace
// them with one CoalescedShardRef.
//
// Publish MUST be idempotent: retry after partial application is safe because
// the manifest RMW only removes segments whose BlobID still appears in
// objectMeta.Segments. See design 2026-05-18-append-segment-coalesce-ec-design.md
// section "Race handling".
type CoalesceSegmentsPlan struct {
	Bucket             string
	Key                string
	CoalescedID        string   // UUIDv7
	ShardKey           string   // EC shardKey = "<key>/coalesced/<coalescedID>"
	Size               int64    // coalesced data total bytes
	ETag               string   // coalesced body MD5
	ConsumedSegmentIDs []string // segment blob IDs consumed by this operation
	// Phase B3 EC placement params. Order in Placement matches shard index
	// 0..k+m-1. Zero-valued for B2 (owner-local only).
	Placement []string
	ECData    uint8
	ECParity  uint8
}

// MaxCoalescedEntries caps how many CoalescedShardRef entries a single
// appendable object may accumulate. Prevents unbounded chain when raw
// segments keep arriving after each coalesce. Reaching this cap stalls
// coalesce until the object is rotated/closed.
const MaxCoalescedEntries = 1024

// maxCoalesceCASRetries bounds the in-RMW retry on a quorum-meta CAS base
// mismatch during the coalesce publish. Mirrors maxAppendCASRetries: a transient
// concurrent-writer (append) race is absorbed before the best-effort coalesce
// aborts (leaving the raw segments intact — safe).
const maxCoalesceCASRetries = 2

// coalesceOwnerGate reports whether this backend may PUBLISH the coalesce
// manifest for an object (F2). The coalesce worker/backstop start on EVERY
// backend, but only the routed owner — the leader of the object's data group —
// may run the CAS publish, so it serializes against AppendObject on the same node
// via the shared objectMetaRMWLock. A non-leader skips/requeues (the leader's own
// worker/backstop covers the object). A nil node (FSM-only test backend that has
// no leadership concept) is treated as owner so single-node/in-process paths run.
func (b *DistributedBackend) coalesceOwnerGate() bool {
	if b.node == nil {
		return true
	}
	return b.node.IsLeader()
}

// publishCoalesceBlob performs the owner-side CAS publish of one coalesce
// operation against the latest-only quorum-meta manifest blob (off-raft). It
// replaces the old propose(CmdCoalesceSegments). The coalesced DATA blob is
// already durable (B2 owner-local / B3 EC) before this is called (the
// durable→publish→GC order); this step only advances the manifest.
//
// RMW under objectMetaRMWLock (the SAME lock AppendObject takes, F2): read the
// CURRENT base, idempotency (CoalescedID already present → no-op), cap, F8
// consumed-id removal against the CURRENT segments, build the +1 CAS candidate,
// writeQuorumMeta. On a CAS reject (a concurrent append advanced the blob) re-read
// and retry (bounded). Coalesce is best-effort background work: a persistent
// reject or a missing base aborts safely (the raw segments stay intact and a
// future trigger/backstop re-coalesces).
func (b *DistributedBackend) publishCoalesceBlob(ctx context.Context, cmd CoalesceSegmentsPlan) error {
	if b.shardSvc == nil {
		return fmt.Errorf("coalesce publish: quorum-meta store unavailable")
	}
	unlock := b.objectMetaRMWLock(cmd.Bucket, cmd.Key)
	defer unlock()

	// F2/F3 execute-time owner-gate: the top-of-job gate is the cheap route-time
	// skip; leadership can flip between then and here. Re-check AFTER acquiring the
	// lock and before any read/write so the CAS publish only runs on the node that
	// also serializes AppendObject on this same lock (mirrors LocalExecution's
	// IsLeader re-check at write-execute time). A non-owner aborts safely — the new
	// leader's worker/backstop re-coalesces.
	if !b.coalesceOwnerGate() {
		return nil
	}

	for attempt := 0; ; attempt++ {
		base, err := b.readQuorumMetaCmd(cmd.Bucket, cmd.Key)
		if err != nil {
			// Object gone (deleted between merge and publish) → drop the coalesce.
			// The merged blob becomes an orphan reclaimed by the sweep.
			if errors.Is(err, storage.ErrObjectNotFound) {
				return nil
			}
			return fmt.Errorf("coalesce publish: read base: %w", err)
		}
		summary, hasSummary, tailSize, err := b.readCoalesceAppendSideSummary(ctx, base)
		if err != nil {
			return fmt.Errorf("coalesce publish: read append side summary: %w", err)
		}
		alreadyPublished := coalescedRefPublished(base.Coalesced, cmd.CoalescedID)
		if hasSummary && summary.Size != tailSize && !alreadyPublished {
			return fmt.Errorf("coalesce publish: append side summary size %d does not match object tail size %d", summary.Size, tailSize)
		}
		next, nextSummary, res, perr := planCoalesceBlobRMWWithSideSummary(base, summary, hasSummary, cmd)
		if perr != nil {
			// Cap reached: stall coalesce for this object (best-effort) rather than
			// fail the worker loudly. Segments stay intact.
			if res.CoalescedEntriesAtCap {
				return nil
			}
			return fmt.Errorf("coalesce publish: plan rmw: %w", perr)
		}
		if res.Noop {
			if hasSummary && summary.Size != tailSize {
				repairedSummary, err := advanceAppendSummaryForCoalesce(summary, cmd)
				if err != nil {
					return fmt.Errorf("coalesce publish append side summary repair: %w", err)
				}
				if repairedSummary.Size != tailSize {
					return fmt.Errorf("coalesce publish append side summary repair size %d does not match object tail size %d", repairedSummary.Size, tailSize)
				}
				if err := b.writeClusterAppendSideRecords(ctx, cmd.Bucket, cmd.Key, base.VersionID, base.NodeIDs, int(base.ECData), repairedSummary, nil); err != nil {
					return fmt.Errorf("coalesce publish append side summary repair commit: %w", err)
				}
			}
			// CoalescedID already published (idempotent retry) → success no-op.
			return nil
		}
		werr := b.writeQuorumMeta(ctx, next)
		if werr == nil {
			if hasSummary {
				if err := b.writeClusterAppendSideRecords(ctx, cmd.Bucket, cmd.Key, next.VersionID, next.NodeIDs, int(next.ECData), nextSummary, nil); err != nil {
					return fmt.Errorf("coalesce publish append side summary commit: %w", err)
				}
			}
			return nil
		}
		if !errors.Is(werr, errQuorumMetaCASReject) || attempt >= maxCoalesceCASRetries {
			if errors.Is(werr, errQuorumMetaCASReject) {
				// Persistent CAS loss to a concurrent append: abort the best-effort
				// coalesce; the raw segments are intact and a later trigger retries.
				return nil
			}
			return fmt.Errorf("coalesce publish commit: %w", werr)
		}
		// CAS reject: loop, re-read the current base, recompute consumed-id removal.
	}
}

func coalescedRefPublished(refs []CoalescedShardRef, coalescedID string) bool {
	for _, c := range refs {
		if c.CoalescedID == coalescedID {
			return true
		}
	}
	return false
}

func (b *DistributedBackend) readCoalesceAppendSideSummary(ctx context.Context, base PutObjectMetaCmd) (storage.AppendSummary, bool, int64, error) {
	if !base.IsAppendable || len(base.Segments) > 0 || base.Size == 0 {
		return storage.AppendSummary{}, false, 0, nil
	}
	coalescedSize := int64(0)
	for _, c := range base.Coalesced {
		coalescedSize += c.Size
	}
	tailSize := base.Size - coalescedSize
	if tailSize < 0 {
		return storage.AppendSummary{}, false, 0, fmt.Errorf("invalid coalesced size %d exceeds object size %d", coalescedSize, base.Size)
	}
	if tailSize == 0 {
		return storage.AppendSummary{}, false, tailSize, nil
	}
	summary, err := b.readClusterAppendSummary(ctx, base.Bucket, base.Key, base.VersionID, base.NodeIDs)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return storage.AppendSummary{}, false, tailSize, fmt.Errorf("missing append side summary for %s/%s", base.Bucket, base.Key)
		}
		return storage.AppendSummary{}, false, tailSize, err
	}
	return summary, true, tailSize, nil
}

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
			ECData:      c.ECData,
			ECParity:    c.ECParity,
			StripeBytes: c.StripeBytes,
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
//  3. planObjectWritePlacement + writeSpooledShards distribute the coalesced
//     blob across k+m peers. shardKey = "<key>/coalesced/<coalescedID>".
//  4. publishCoalesceBlob CAS-publishes the CoalescedShardRef onto the
//     quorum-meta manifest blob (owner-gated, off-raft — no FSM propose).
//  5. Remove owner-local coalesced blob. Raw segment metadata is dropped by the
//     manifest publish; raw segment files are left for the orphan scrubber so
//     stale readers that observed pre-coalesce metadata can finish safely.
//
// Owner-gate (F2): only the routed owner (this group's leader) publishes the
// manifest. A non-owner skips at the top (no wasted merge / EC distribute).
// Failure recovery: any error after EC write but before publish leaves orphan EC
// shards + the raw segments intact, so a future trigger/backstop re-coalesces.
// Idempotent on retry — publish no-ops if CoalescedID already present.
func (b *DistributedBackend) processCoalesceJobB3(ctx context.Context, job coalesceJob) error {
	start := time.Now()
	var resultLabel = "abort"
	var coalescedBytes int64
	defer func() {
		metrics.AppendCoalesceTotal.WithLabelValues(resultLabel).Inc()
		metrics.AppendCoalesceLatencySeconds.Observe(time.Since(start).Seconds())
		if coalescedBytes > 0 {
			metrics.AppendCoalesceBytes.Add(float64(coalescedBytes))
		}
	}()
	// F2 owner-gate: only the routed owner (group leader) coalesces; a non-owner
	// would race the owner's append RMW on a different node. Skip → requeue covered
	// by the owner's own worker/backstop.
	if !b.coalesceOwnerGate() {
		return nil
	}
	obj, err := b.HeadObject(ctx, job.Bucket, job.Key)
	if err != nil || obj == nil {
		return nil
	}
	if !obj.IsAppendable || len(obj.Segments) == 0 {
		return nil
	}
	coalescedID := uuidutil.MustNewV7()
	coalescePlan := planCoalesceSnapshot(job.Bucket, job.Key, obj.Segments, coalescedID)
	merged, mErr := b.mergeSegmentsOwnerLocal(job.Bucket, job.Key, coalescedID, coalescePlan.Segments)
	if mErr != nil {
		return fmt.Errorf("merge: %w", mErr)
	}
	mergedPath := merged.Path
	mergedCleaned := false
	cleanupMerged := func() {
		if mergedCleaned {
			return
		}
		mergedCleaned = true
		_ = os.Remove(mergedPath)
	}

	placementGroupID := obj.PlacementGroupID
	if placementGroupID == "" {
		placementGroupID = b.lookupPlacementGroupForAppend(ctx, obj)
	}
	placementPlan, err := b.planObjectWritePlacement(ctx, ObjectWritePlacementInput{
		Operation:        "coalesce_object",
		PlacementGroupID: placementGroupID,
		LiveNodes:        b.ecWriteNodes(),
		ShardKey:         coalescePlan.ShardKey,
	})
	if err != nil {
		cleanupMerged()
		return fmt.Errorf("coalesce: plan placement: %w", err)
	}
	cfg := placementPlan.Config
	placement := placementPlan.NodeIDs

	plan := ecObjectWritePlan{
		Bucket:           job.Bucket,
		Key:              job.Key,
		VersionID:        "coalesced/" + coalescedID,
		PlacementGroupID: placementPlan.PlacementGroupID,
		Config:           cfg,
		Placement:        placement,
	}
	// Stream the owner-local merged blob straight into the EC stream encoder; its
	// MD5 ETag was already computed during the merge, so no md5 tee is needed here.
	mergedSrc, err := os.Open(merged.Path)
	if err != nil {
		cleanupMerged()
		return fmt.Errorf("open merged blob: %w", err)
	}
	writer := newECObjectWriter(b.currentSelfAddr(), b.shardSvc, b.currentPeerHealth())
	_, ecErr := writer.writeStreamShards(ctx, plan, b.coalescedSpoolDir(), mergedSrc, merged.Size, func() string {
		return merged.ETag
	})
	_ = mergedSrc.Close()
	if ecErr != nil {
		cleanupMerged()
		if placementPlan.TopologyWrite {
			return topologyShardWriteError(placementPlan.TopologyGroup, placementPlan.Config, ecErr)
		}
		return fmt.Errorf("ec write: %w", ecErr)
	}
	// EC shards now contain the merged body while raw segments remain the
	// metadata source of truth. Drop the owner-local intermediate before
	// publishing CoalescedShardRef metadata so observers cannot see completed
	// EC metadata while this temporary blob still exists.
	cleanupMerged()

	// Test-only fault hook: simulate a crash after EC shards land but before
	// the FSM commit. The next worker iteration must re-coalesce and reach a
	// consistent final state.
	if hook := b.coalesceFaultAfterECWrite; hook != nil {
		if err := hook(); err != nil {
			cleanupMerged()
			return fmt.Errorf("fault: %w", err)
		}
	}

	cmd := CoalesceSegmentsPlan{
		Bucket:             job.Bucket,
		Key:                job.Key,
		CoalescedID:        coalescePlan.CoalescedID,
		ShardKey:           coalescePlan.ShardKey,
		Size:               merged.Size,
		ETag:               merged.ETag,
		ConsumedSegmentIDs: coalescePlan.ConsumedSegmentIDs,
		Placement:          placement,
		ECData:             uint8(cfg.DataShards),
		ECParity:           uint8(cfg.ParityShards),
	}
	if err := b.publishCoalesceBlob(ctx, cmd); err != nil {
		// EC shards distributed but never referenced. Best-effort orphan cleanup
		// (full sweep deferred to TODOS.md). Keep raw segments so a future retry
		// can succeed.
		cleanupMerged()
		return fmt.Errorf("publish coalesce: %w", err)
	}
	// Source of truth is now the EC shards. Do not unlink raw segment files
	// here: a concurrent GET may have observed pre-coalesce metadata and still
	// needs those blobs. The orphan segment scrubber owns physical cleanup
	// (GC-after-publish-and-grace, F-step 4).
	resultLabel = "success"
	coalescedBytes = merged.Size
	return nil
}

// processCoalesceJobB2 is the Phase B2 implementation (no EC), kept for the
// owner-local merge path and tests.
//
// Steps (durable→publish→GC):
//  1. Owner-gate (F2): only the routed owner (group leader) publishes.
//  2. HeadObject → snapshot S = current segments (must be owner).
//  3. mergeSegmentsOwnerLocal(S) → single coalesced blob on owner disk (durable).
//  4. publishCoalesceBlob CAS-publishes the CoalescedShardRef onto the
//     quorum-meta manifest blob (off-raft — no FSM propose).
//
// GC of the consumed raw segment files is NOT done inline (F-step 4): a reader
// that observed the pre-coalesce manifest may still be streaming those blobs, so
// physical cleanup is deferred to the orphan segment scrubber (reachability sweep
// + grace). Idempotent on retry: publish no-ops if CoalescedID already present.
//
//nolint:unused // referenced from coalesce_process_test.go / coalesce_concurrent_test.go
func (b *DistributedBackend) processCoalesceJobB2(ctx context.Context, job coalesceJob) error {
	if !b.coalesceOwnerGate() {
		return nil
	}
	obj, err := b.HeadObject(ctx, job.Bucket, job.Key)
	if err != nil || obj == nil {
		return nil // object gone — drop
	}
	if !obj.IsAppendable || len(obj.Segments) == 0 {
		return nil
	}
	// Snapshot segments — concurrent appends after this point are preserved
	// by planCoalesceBlobRMW (consumed-set match is exact BlobID, F8).
	coalescedID := uuidutil.MustNewV7()
	coalescePlan := planCoalesceSnapshot(job.Bucket, job.Key, obj.Segments, coalescedID)
	merged, mErr := b.mergeSegmentsOwnerLocal(job.Bucket, job.Key, coalescedID, coalescePlan.Segments)
	if mErr != nil {
		return fmt.Errorf("merge: %w", mErr)
	}
	cmd := CoalesceSegmentsPlan{
		Bucket:             coalescePlan.Bucket,
		Key:                coalescePlan.Key,
		CoalescedID:        coalescePlan.CoalescedID,
		ShardKey:           coalescePlan.ShardKey,
		Size:               merged.Size,
		ETag:               merged.ETag,
		ConsumedSegmentIDs: coalescePlan.ConsumedSegmentIDs,
	}
	if err := b.publishCoalesceBlob(ctx, cmd); err != nil {
		// Cleanup intermediate coalesced blob; raw segments remain intact
		// so a future retry can succeed.
		_ = os.Remove(merged.Path)
		return fmt.Errorf("publish coalesce: %w", err)
	}
	// Raw segment files are NOT unlinked here (F-step 4): a concurrent GET may
	// have observed the pre-coalesce manifest and still needs them. The orphan
	// segment scrubber owns physical cleanup after a reachability sweep + grace.
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
	plan := planCoalesceTrigger(segs, firstT, nowT, *b.coalesceCfg.Load())
	if !plan.ShouldEnqueue {
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
	cfg := b.coalesceCfg.Load()
	if cfg.CleanupInterval <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.CleanupInterval)
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

// scanAppendableAndTrigger scans the latest-only quorum-meta blob tree (where
// off-raft appendable objects live — Task 4: appendable metadata is blob-resident,
// not BadgerDB) and enqueues any appendable object with raw segments whose
// threshold is satisfied. Each object is enqueued at most once per batch thanks to
// coalesceFirstSeen + worker dedup.
//
// Owner-gated: only the routed owner (group leader) coalesces, so a non-owner's
// backstop skips entirely (the owner's own scan covers its objects). Best-effort:
// decode/scan errors are skipped silently — the scanner is the safety-net for
// missed in-process triggers (process restart, dropped enqueue when buffer is
// full), not a strong consistency primitive.
func (b *DistributedBackend) scanAppendableAndTrigger(ctx context.Context) {
	if b.shardSvc == nil {
		return
	}
	if !b.coalesceOwnerGate() {
		return
	}
	buckets, err := b.ListBuckets(ctx)
	if err != nil {
		return
	}
	for _, bucket := range buckets {
		select {
		case <-ctx.Done():
			return
		default:
		}
		cmds, serr := b.shardSvc.ScanQuorumMetaBucket(bucket, "")
		if serr != nil {
			continue // best-effort: skip an unreadable bucket
		}
		for _, cmd := range cmds {
			if !cmd.IsAppendable || len(cmd.Segments) == 0 {
				continue
			}
			b.maybeTriggerCoalesce(bucket, cmd.Key, segmentMetaEntriesToRefs(cmd.Segments))
		}
	}
}
