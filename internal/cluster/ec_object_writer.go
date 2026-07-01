package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/rs/zerolog/log"
)

type ecObjectSizedShardStore interface {
	WriteLocalShardStreamSizedContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader, streamSize int64) error
}

type ecObjectStagedSizedShardStore interface {
	WriteLocalShardStreamStagedSizedContext(ctx context.Context, bucket, stagingKey, finalKey string, shardIdx int, body io.Reader, streamSize, logicalSize int64) error
}

type ecObjectRemoteStagedSizedShardStore interface {
	WriteShardStreamStagedSized(ctx context.Context, peer, bucket, stagingKey, finalKey string, shardIdx int, body io.Reader, streamSize int64) error
}

type ecObjectRemoteSizedShardStore interface {
	WriteShardStreamSized(ctx context.Context, peer, bucket, key string, shardIdx int, body io.Reader, streamSize int64) error
}

type ecObjectPeerHealth interface {
	MarkHealthy(peer string) bool
	MarkUnhealthy(peer string) bool
}

type ecObjectWriter struct {
	selfID        string
	shards        ecShardStore
	peerHealth    ecObjectPeerHealth
	writeAttempts int
	writeBackoff  time.Duration
}

type ecObjectWritePlan struct {
	Bucket           string
	Key              string
	VersionID        string
	PlacementGroupID string
	ModTime          int64
	PreserveModTime  bool
	ExpectedETag     string
	Config           ECConfig
	Placement        []string
	ContentType      string
	UserMetadata     map[string]string
	SSEAlgorithm     string
	// ACL is the s3auth.ACLGrant bitmask to persist on the object meta. 0 = private.
	ACL uint8
	// SegmentBlobID is empty for legacy single-blob writes; when non-empty
	// the plan refers to a chunked-PUT segment and shardKey is scoped by
	// SegmentBlobID via ecObjectSegmentShardKey.
	SegmentBlobID string
	// SegmentIdx is the 0-based chunk index. Meaningful only when
	// SegmentBlobID != "".
	SegmentIdx int
	// StagingTxnID, when non-empty, activates PR1 segment staging for this
	// segment write: each shard's bytes land in the per-node staging dir
	// (.segstaging/<StagingTxnID>/<SegmentBlobID>) while the AAD stays the FINAL
	// shardKey, so a post-promote read decrypts correctly. Meaningful only when
	// SegmentBlobID != "". Empty ⇒ legacy direct-to-final write.
	StagingTxnID string
}

type ecObjectWriteResult struct {
	Size      int64
	ETag      string
	ModTime   int64
	ShardKey  string
	Placement []string
	ECData    uint8
	ECParity  uint8
	Parts     []storage.MultipartPartEntry // populated by CompleteMultipartUpload
	Tags      []storage.Tag                // populated by CompleteMultipartUpload (carried from multipartMeta.Tags)

	cancelBackgroundWrites func()
	waitBackgroundWrites   func()
}

func (r ecObjectWriteResult) abortBackgroundWrites() {
	if r.cancelBackgroundWrites != nil {
		r.cancelBackgroundWrites()
	}
	if r.waitBackgroundWrites != nil {
		r.waitBackgroundWrites()
	}
}

type ecObjectShardWriteError struct {
	shardIdx int
	node     string
	err      error
}

func (e *ecObjectShardWriteError) Error() string {
	return fmt.Sprintf("ec write shard %d to %s: %v", e.shardIdx, e.node, e.err)
}

func (e *ecObjectShardWriteError) Unwrap() error {
	return e.err
}

func newECObjectWriter(selfID string, shards ecShardStore, peerHealth ecObjectPeerHealth) ecObjectWriter {
	return ecObjectWriter{
		selfID:        selfID,
		shards:        shards,
		peerHealth:    peerHealth,
		writeAttempts: ecShardWriteAttempts,
		writeBackoff:  ecShardWriteBackoff,
	}
}

func ecObjectShardKey(key, versionID string) string {
	if versionID == "" {
		return key
	}
	return key + "/" + versionID
}

// ecObjectSegmentShardKey is the post-Phase-2 unified shardKey helper. It
// preserves legacy behaviour when plan.SegmentBlobID == "" (returns
// ecObjectShardKey(Key, VersionID) verbatim) and switches to a
// segment-scoped key when SegmentBlobID != "". Because the existing
// ShardService inline AAD is bucket+"/"+shardKey+"/"+shardIdx, swapping the
// shardKey automatically scopes AAD to the segment.
func ecObjectSegmentShardKey(plan ecObjectWritePlan) string {
	if plan.SegmentBlobID == "" {
		return ecObjectShardKey(plan.Key, plan.VersionID)
	}
	return plan.Key + "/segments/" + plan.SegmentBlobID
}

// stagingShardKey is the per-node STAGING physical shard key for this plan's
// segment when PR1 staging is active, else "". Only segment writes
// (SegmentBlobID set) with a StagingTxnID stage; whole-object EC writes never do.
func (plan ecObjectWritePlan) stagingShardKey() string {
	if plan.StagingTxnID == "" || plan.SegmentBlobID == "" {
		return ""
	}
	return segmentStagingShardKey(plan.StagingTxnID, plan.SegmentBlobID)
}

// writeSegmentInput bundles the per-segment write parameters consumed by
// writeOneSegment. Caller (clusterSegmentBackend.WriteSegment) buffers the
// SegmentWriter chunk into Data; segments are ≤ DefaultChunkSize.
//
//nolint:unused // wired up by clusterSegmentBackend.WriteSegment in Task 2.3.
type writeSegmentInput struct {
	Bucket, Key, VersionID string
	SegmentBlobID          string
	SegmentIdx             int
	Group                  ShardGroupEntry
	Cfg                    ECConfig
	Data                   []byte
	// LogicalSize is the segment's PRE-compression plaintext size. Data may be
	// the zstd-compressed bytes (#986); LogicalSize is threaded so the shard
	// fsync class keys off logical size, not the compressed on-disk size. -1 ⇒
	// unknown (treat on-disk size as logical).
	LogicalSize int64
	// Weights is the per-peer disk-capacity weight aligned 1:1 with
	// Group.PeerIDs (Weights[i] is the weight of Group.PeerIDs[i]). nil ⇒
	// unweighted HRW. Computed once by the writer (clusterSegmentBackend) so the
	// recorded placement is deterministic; readers never recompute.
	Weights []float64
	// WeightedEnabled mirrors ClusterConfig.WeightedHRWEnabled(). When false the
	// segment placement uses plain (unweighted) HRW regardless of Weights.
	WeightedEnabled bool
	// StagingTxnID propagates clusterSegmentBackend.stagingTxnID into the per-
	// segment write plan (PR1 segment staging). Empty ⇒ legacy direct-to-final.
	StagingTxnID string
}

// writeOneSegment is a thin wrapper around writeDataShards that fills in the
// segment-scoped fields on ecObjectWritePlan. Returns the PlacementRecord
// (synthesized from the chosen K+M peers), the data-shard etag, and the same
// blobID echoed back for caller convenience.
//
//nolint:unused // wired up by clusterSegmentBackend.WriteSegment in Task 2.3.
func (w ecObjectWriter) writeOneSegment(ctx context.Context, in writeSegmentInput) (PlacementRecord, string, string, error) {
	nShards := in.Cfg.DataShards + in.Cfg.ParityShards
	if len(in.Group.PeerIDs) < nShards {
		return PlacementRecord{}, "", "", fmt.Errorf("group %s has %d peers, need %d", in.Group.ID, len(in.Group.PeerIDs), nShards)
	}
	placementKey := ecObjectSegmentShardKey(ecObjectWritePlan{
		Key:           in.Key,
		VersionID:     in.VersionID,
		SegmentBlobID: in.SegmentBlobID,
	})
	placement := selectShardPlacement(in.Cfg, in.Group.PeerIDs, placementKey, in.Weights, in.WeightedEnabled)
	if len(placement) != nShards {
		return PlacementRecord{}, "", "", fmt.Errorf("group %s: placement has %d nodes, need %d", in.Group.ID, len(placement), nShards)
	}
	plan := ecObjectWritePlan{
		Bucket:           in.Bucket,
		Key:              in.Key,
		VersionID:        in.VersionID,
		PlacementGroupID: in.Group.ID,
		Config:           in.Cfg,
		Placement:        placement,
		SegmentBlobID:    in.SegmentBlobID,
		SegmentIdx:       in.SegmentIdx,
		StagingTxnID:     in.StagingTxnID,
	}
	res, err := w.writeDataShards(ctx, plan, in.Data, in.LogicalSize)
	if err != nil {
		return PlacementRecord{}, "", "", fmt.Errorf("write segment %d (blob %s): %w", in.SegmentIdx, in.SegmentBlobID, err)
	}
	// Segment writes feed the chunked PUT staging/promote pipeline, which promotes
	// every planned shard before metadata commit. Let background parity writes
	// finish here so promote never races a still-writing staging shard.
	res.waitBackgroundWrites()
	rec := PlacementRecord{Nodes: placement, K: in.Cfg.DataShards, M: in.Cfg.ParityShards}
	return rec, res.ETag, in.SegmentBlobID, nil
}

// writeDataShards EC-encodes `data` (post-compression on the chunked segment
// path) and writes each shard. logicalSize is the segment's PRE-compression
// plaintext size (-1 when unknown/uncompressed); it is split per-data-shard and
// threaded to the shard writers purely for the fsync-class decision, so a
// logically-large but well-compressed redundant shard still skips the
// commit-tail fsync (EC owns its durability).
func (w ecObjectWriter) writeDataShards(ctx context.Context, plan ecObjectWritePlan, data []byte, logicalSize int64) (ecObjectWriteResult, error) {
	perShardLogical := int64(-1)
	if logicalSize >= 0 && plan.Config.DataShards > 0 {
		perShardLogical = (logicalSize + int64(plan.Config.DataShards) - 1) / int64(plan.Config.DataShards)
	}
	splitStart := time.Now()
	shards, padding, err := ecSplitBodiesPooled(plan.Config, data)
	if err != nil {
		ObservePutTraceStage(ctx, PutTraceStageECSplit, splitStart, PutTraceStageFields{
			Bytes:      int64(len(data)),
			ShardIndex: plan.SegmentIdx,
			Error:      err.Error(),
		})
		return ecObjectWriteResult{}, fmt.Errorf("ec split: %w", err)
	}
	ObservePutTraceStage(ctx, PutTraceStageECSplit, splitStart, PutTraceStageFields{
		Bytes:      int64(len(data)),
		ShardIndex: plan.SegmentIdx,
	})
	header := encodeShardHeader(int64(len(data)))
	// ETag intentionally empty: caller (WriteSegmentBytes) discards this ETag;
	// segment checksum is xxhash3 via storage.ChecksumOf.
	result, err := w.writeShardReadersWithSize(ctx, plan, int64(len(data)), perShardLogical, "", func(idx int) (io.Reader, error) {
		if idx < 0 || idx >= len(shards) {
			return nil, fmt.Errorf("ec data shard %d out of range", idx)
		}
		return io.MultiReader(bytes.NewReader(header[:]), bytes.NewReader(shards[idx])), nil
	}, func(idx int) (int64, error) {
		if idx < 0 || idx >= len(shards) {
			return 0, fmt.Errorf("ec data shard %d out of range", idx)
		}
		return int64(shardHeaderSize + len(shards[idx])), nil
	}, "ec")
	if err != nil {
		putECSplitPaddingShards(padding)
		return ecObjectWriteResult{}, err
	}
	if len(padding) == 0 {
		return result, nil
	}
	var releasePadding sync.Once
	waitBackgroundWrites := result.waitBackgroundWrites
	result.waitBackgroundWrites = func() {
		if waitBackgroundWrites != nil {
			waitBackgroundWrites()
		}
		releasePadding.Do(func() {
			putECSplitPaddingShards(padding)
		})
	}
	return result, nil
}

// writeStreamShards feeds a plain one-pass reader + known size straight into the
// whole-object EC stream encoder (spoolECShardsStream); it never stages the body
// to a disk temp file. `etagFn` is invoked AFTER the source has
// been fully consumed by the encoder, so callers can compute the object ETag via
// an inline md5 tee over `src` and read it at EOF; a nil etagFn yields an empty
// ETag. The caller owns `src`'s lifecycle (close after this returns).
func (w ecObjectWriter) writeStreamShards(ctx context.Context, plan ecObjectWritePlan, spoolDir string, src io.Reader, size int64, etagFn func() string) (ecObjectWriteResult, error) {
	stageStart := time.Now()
	shards, err := spoolECShardsStream(ctx, plan.Config, spoolDir, src, size)
	if err != nil {
		return ecObjectWriteResult{}, err
	}
	observePutStage("ec", "spool_shards", stageStart)

	etag := ""
	if etagFn != nil {
		etag = etagFn()
	}
	// Whole-object stream path is never compressed, so the on-disk shard size IS
	// the logical size for the fsync class: pass -1 (use on-disk).
	return w.writeShardReadersWithSizeCleanup(ctx, plan, size, -1, etag, func(idx int) (io.Reader, error) {
		return shards.OpenShard(idx)
	}, shards.ShardSize, "ec", shards.Cleanup)
}

// perShardLogicalSize is the segment's PRE-compression per-data-shard size used
// ONLY for the fsync-class decision (-1 ⇒ unknown/uncompressed ⇒ on-disk size).
func (w ecObjectWriter) writeShardReadersWithSize(
	ctx context.Context,
	plan ecObjectWritePlan,
	size int64,
	perShardLogicalSize int64,
	etag string,
	openShard func(idx int) (io.Reader, error),
	shardSize func(idx int) (int64, error),
	metricPath string,
) (ecObjectWriteResult, error) {
	return w.writeShardReadersWithSizeCleanup(ctx, plan, size, perShardLogicalSize, etag, openShard, shardSize, metricPath, nil)
}

func (w ecObjectWriter) writeShardReadersWithSizeCleanup(
	ctx context.Context,
	plan ecObjectWritePlan,
	size int64,
	perShardLogicalSize int64,
	etag string,
	openShard func(idx int) (io.Reader, error),
	shardSize func(idx int) (int64, error),
	metricPath string,
	cleanup func(),
) (ecObjectWriteResult, error) {
	shardKey := ecObjectSegmentShardKey(plan)
	// PR1 segment staging: when active, shards are written to the per-node staging
	// path (stagingShardKey) while keeping shardKey (the FINAL path) as AAD. The
	// write-failure cleanup must therefore target the path the bytes actually
	// landed on — staging when active, final otherwise.
	stagingShardKey := plan.stagingShardKey()
	cleanupKey := shardKey
	if stagingShardKey != "" {
		cleanupKey = stagingShardKey
	}
	quorum := plan.Config.DataShards
	if quorum <= 0 || quorum > len(plan.Placement) {
		if cleanup != nil {
			cleanup()
		}
		return ecObjectWriteResult{}, fmt.Errorf("ec write quorum %d invalid for %d placements", quorum, len(plan.Placement))
	}

	stageStart := time.Now()
	type shardWriteResult struct {
		shardIdx int
		node     string
		err      error
	}
	writeCtx, cancelWrites := context.WithCancel(context.WithoutCancel(ctx))
	// Respect caller cancellation until quorum; after K successes, remaining
	// shard writes run detached unless explicitly aborted by the commit path.
	stopParentCancel := context.AfterFunc(ctx, cancelWrites)
	cancelAllWrites := func() {
		stopParentCancel()
		cancelWrites()
	}
	resultCh := make(chan shardWriteResult, len(plan.Placement))
	var wg sync.WaitGroup
	for i, node := range plan.Placement {
		i, node := i, node
		wg.Add(1)
		go func() {
			defer wg.Done()
			ep := w.endpointFor(node)
			if werr := ep.WriteShardReader(writeCtx, plan.Bucket, shardKey, stagingShardKey, i, perShardLogicalSize, openShard, shardSize); werr != nil {
				resultCh <- shardWriteResult{shardIdx: i, node: node, err: &ecObjectShardWriteError{shardIdx: i, node: node, err: werr}}
				return
			}
			resultCh <- shardWriteResult{shardIdx: i, node: node}
		}()
	}

	deleteLanded := func(nodes []string) {
		cleanupCtx := context.WithoutCancel(ctx)
		for _, node := range nodes {
			_ = w.endpointFor(node).DeleteShards(cleanupCtx, plan.Bucket, cleanupKey)
		}
	}
	var rootErr error
	waitCleanupDrainAndDelete := func(completed int, landed []string) error {
		cancelAllWrites()
		wg.Wait()
		if cleanup != nil {
			cleanup()
		}
		for ; completed < len(plan.Placement); completed++ {
			res := <-resultCh
			if res.err != nil {
				if rootErr == nil {
					rootErr = res.err
				}
				continue
			}
			landed = append(landed, res.node)
		}
		deleteLanded(landed)
		return rootErr
	}
	successResult := func(done <-chan struct{}) ecObjectWriteResult {
		return ecObjectWriteResult{
			Size:      size,
			ETag:      etag,
			ModTime:   time.Now().Unix(),
			ShardKey:  shardKey,
			Placement: cloneStringSlice(plan.Placement),
			ECData:    uint8(plan.Config.DataShards),
			ECParity:  uint8(plan.Config.ParityShards),
			cancelBackgroundWrites: func() {
				cancelAllWrites()
			},
			waitBackgroundWrites: func() {
				<-done
			},
		}
	}

	successes := 0
	completed := 0
	landed := make([]string, 0, len(plan.Placement))
	for completed < len(plan.Placement) {
		select {
		case res := <-resultCh:
			completed++
			if res.err != nil {
				if rootErr == nil {
					rootErr = res.err
				}
				if successes < quorum || successes+len(plan.Placement)-completed < quorum {
					rootErr = waitCleanupDrainAndDelete(completed, landed)
					return ecObjectWriteResult{}, rootErr
				}
				continue
			}
			successes++
			landed = append(landed, res.node)
			if successes < quorum {
				continue
			}

			observePutStage(metricPath, "write_shards", stageStart)
			stopParentCancel()
			done := make(chan struct{})
			go func(completed int) {
				defer cancelAllWrites()
				defer close(done)
				for ; completed < len(plan.Placement); completed++ {
					res := <-resultCh
					if res.err != nil {
						if errors.Is(res.err, context.Canceled) {
							continue
						}
						log.Warn().
							Err(res.err).
							Str("bucket", plan.Bucket).
							Str("shard_key", shardKey).
							Int("shard_idx", res.shardIdx).
							Str("node", res.node).
							Msg("ec object writer: background shard write failed after quorum")
					}
				}
				wg.Wait()
				if cleanup != nil {
					cleanup()
				}
			}(completed)
			return successResult(done), nil
		case <-ctx.Done():
			if err := waitCleanupDrainAndDelete(completed, landed); err != nil {
				return ecObjectWriteResult{}, err
			}
			return ecObjectWriteResult{}, ctx.Err()
		}
	}
	if rootErr == nil {
		rootErr = fmt.Errorf("ec write quorum %d not reached with %d placements", quorum, len(plan.Placement))
	}
	rootErr = waitCleanupDrainAndDelete(completed, landed)
	return ecObjectWriteResult{}, rootErr
}
