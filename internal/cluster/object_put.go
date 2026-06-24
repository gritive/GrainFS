package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gritive/GrainFS/internal/gossip"
	"github.com/gritive/GrainFS/internal/hrw"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage"
)

// validateContentMD5 rejects a PUT whose computed body MD5 (hex) does not match
// the client-supplied Content-MD5. No-op when the client sent none or the
// digest is unavailable. Returns storage.ErrContentMD5Mismatch (mapped to 400
// BadDigest) on mismatch.
func validateContentMD5(computedHex, clientHex string) error {
	if clientHex == "" || computedHex == "" || computedHex == clientHex {
		return nil
	}
	return fmt.Errorf("%w: client %s, body %s", storage.ErrContentMD5Mismatch, clientHex, computedHex)
}

// derefACL returns the request ACL bitmask, or 0 (private — the default) when
// unset. nil and &0 are equivalent: both store the private default.
func derefACL(acl *uint8) uint8 {
	if acl == nil {
		return 0
	}
	return *acl
}

func (b *DistributedBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	return b.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, nil)
}

func (b *DistributedBackend) PutObjectWithUserMetadata(ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string) (*storage.Object, error) {
	return b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:       bucket,
		Key:          key,
		Body:         r,
		ContentType:  contentType,
		UserMetadata: userMetadata,
	})
}

func (b *DistributedBackend) PutObjectWithRequest(ctx context.Context, req storage.PutObjectRequest) (*storage.Object, error) {
	if err := guardInternalBucketObjectOp(req.Bucket); err != nil {
		return nil, err
	}
	bucket, key, r, contentType := req.Bucket, req.Key, req.Body, req.ContentType
	userMetadata := req.UserMetadata
	sseAlgorithm := req.SystemMetadata.SSEAlgorithm
	acl := derefACL(req.ACL)
	stageStart := time.Now()
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, err
	}
	observePutStage("distributed", "head_bucket", stageStart)

	stageStart = time.Now()
	if err := b.quarantineGate(bucket, key, ""); err != nil {
		return nil, err
	}
	observePutStage("distributed", "quarantine_check", stageStart)

	versionID := newVersionID()
	// Single write path: spool the body, then route by size/topology in
	// putObjectECSpooled (chunked / single-local-spooled / EC-memory /
	// EC-spooled). The pre-spool single-local / EC in-memory / known-size fast
	// paths were removed — they duplicated placement + size routing and let a
	// large known-size object slip through as one over-cap whole-object shard.
	stageStart = time.Now()
	sp, err := b.spoolPutObject(ctx, bucket, r)
	if err != nil {
		return nil, err
	}
	observePutStage("distributed", "spool_object", stageStart)
	defer sp.Cleanup()
	// Single write-path Content-MD5 validation: sp.ETag is the body's md5,
	// computed during spooling before any shard/metadata is written. Covers
	// both spool sub-cases (single-local and multi-shard EC). No-op when the
	// client sent no Content-MD5.
	if err := validateContentMD5(sp.ETag, req.ContentMD5Hex); err != nil {
		return nil, err
	}

	if b.currentECConfig().NumShards() == 0 || b.shardSvc == nil {
		return nil, fmt.Errorf("put object: EC storage is required")
	}
	return b.putObjectECSpooled(ctx, bucket, key, versionID, sp, contentType, userMetadata, sseAlgorithm, acl)
}

// PutObjectAsync is the write-back variant of PutObject.
// It delegates to putObjectECSpooled and returns a no-op commitFn for API
// compatibility with callers that batch commitFns (e.g., block_io_executor).
func (b *DistributedBackend) PutObjectAsync(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, func() error, error) {
	if err := guardInternalBucketObjectOp(bucket); err != nil {
		return nil, nil, err
	}
	if err := b.HeadBucket(ctx, bucket); err != nil {
		return nil, nil, err
	}
	sp, err := b.spoolPutObject(ctx, bucket, r)
	if err != nil {
		return nil, nil, err
	}
	defer sp.Cleanup()
	versionID := newVersionID()
	if b.currentECConfig().NumShards() == 0 || b.shardSvc == nil {
		return nil, nil, fmt.Errorf("put object async: EC storage is required")
	}
	obj, err := b.putObjectECSpooled(ctx, bucket, key, versionID, sp, contentType, nil, "", 0)
	return obj, func() error { return nil }, err
}

func objectWritePlacementNodeStatesFromRuntime(liveNodes []string, store *gossip.NodeStatsStore) []ObjectWritePlacementNodeState {
	if store == nil {
		return nil
	}
	states := make([]ObjectWritePlacementNodeState, 0, len(liveNodes))
	for _, nodeID := range liveNodes {
		state := ObjectWritePlacementNodeState{NodeID: nodeID}
		if ns, ok := store.Get(nodeID); ok {
			state.DiskAvailBytes = ns.DiskAvailBytes
		}
		states = append(states, state)
	}
	return states
}

func selectECPlacementFromNodeStates(
	cfg ECConfig,
	liveNodes []string,
	shardKey string,
	nodeStates []ObjectWritePlacementNodeState,
	weightedEnabled bool,
) []string {
	if !weightedEnabled || len(nodeStates) == 0 {
		return selectShardPlacement(cfg, liveNodes, shardKey, nil, weightedEnabled)
	}
	stateByNode := make(map[string]ObjectWritePlacementNodeState, len(nodeStates))
	for _, state := range nodeStates {
		stateByNode[state.NodeID] = state
	}
	weights := make([]float64, len(liveNodes))
	for i, nodeID := range liveNodes {
		state, ok := stateByNode[nodeID]
		if !ok || state.DiskAvailBytes == 0 {
			weights[i] = 0
			continue
		}
		weights[i] = float64(state.DiskAvailBytes)
	}
	return selectShardPlacement(cfg, liveNodes, shardKey, weights, weightedEnabled)
}

// selectShardPlacement is the single shared placement helper used by BOTH the
// non-chunked fallback (selectECPlacementFromNodeStates) and the chunked
// segment hot path (ecObjectWriter.writeOneSegment). It returns the ordered
// per-shard NodeIDs for shardKey across peers via weighted Rendezvous Hashing.
//
// weights, when non-nil, is aligned 1:1 with peers (weights[i] is peers[i]'s
// disk-capacity weight; 0 ⇒ drain). It honors weightedEnabled and falls back to
// unweighted HRW when weighting is disabled, no weights are supplied, or every
// peer's weight is stale/zero (all-stale safeguard so a boot-before-first-gossip
// write does not fail). It also degrades to unweighted HRW when weighting drops
// the placement below cfg.NumShards() (weight-0 peers excluded by PlaceShards).
//
// The caller computes weights once (single coordinator) and records the result;
// readers never recompute placement, so per-node/per-time weight divergence is
// irrelevant (see generation_placement.go).
func selectShardPlacement(cfg ECConfig, peers []string, shardKey string, weights []float64, weightedEnabled bool) []string {
	nShards := cfg.NumShards()
	if !weightedEnabled || len(weights) == 0 {
		return hrw.PlaceShards(shardKey, peers, nil, nShards)
	}

	active := 0
	for i, w := range weights {
		if w <= 0 {
			if i < len(peers) {
				metrics.ClusterPlacementSkipped.WithLabelValues(peers[i], "stale").Inc()
			}
			continue
		}
		active++
	}

	// All-stale safeguard: no live weight data (e.g. boot before first gossip).
	// Fall back to unweighted placement so writes don't fail.
	if active == 0 {
		metrics.ClusterPlacementSkipped.WithLabelValues("*", "all_stale_fallback").Inc()
		return hrw.PlaceShards(shardKey, peers, nil, nShards)
	}

	placement := hrw.PlaceShards(shardKey, peers, weights, nShards)
	// Weight-0 peers are excluded by PlaceShards. If that drops the stripe below
	// the required width, fall back to unweighted HRW (which considers every
	// peer) so the len(placement) == NumShards guard downstream still holds. A
	// weight-0 peer here means "no current capacity stats" (stale gossip), not an
	// operator drain — there is no drain concept — so re-including it is correct:
	// availability over capacity-optimization while stats are incomplete. Emit a
	// distinct metric so this degradation is visible (the all-stale path above
	// has its own marker).
	if len(placement) < nShards {
		metrics.ClusterPlacementSkipped.WithLabelValues("*", "weight_shortfall_fallback").Inc()
		return hrw.PlaceShards(shardKey, peers, nil, nShards)
	}
	return placement
}

func (b *DistributedBackend) putObjectECSpooled(ctx context.Context, bucket, key, versionID string, sp *spooledObject, contentType string, userMetadata map[string]string, sseAlgorithm string, acl uint8) (*storage.Object, error) {
	return b.putObjectECSpooledWithOptionalModTime(ctx, bucket, key, versionID, sp, contentType, userMetadata, sseAlgorithm, acl, 0, false, "", nil, nil, nil, "")
}

func (b *DistributedBackend) putObjectECSpooledWithOptionalModTime(ctx context.Context, bucket, key, versionID string, sp *spooledObject, contentType string, userMetadata map[string]string, sseAlgorithm string, acl uint8, modTime int64, preserveModTime bool, expectedETag string, beforeCommit func() error, parts []storage.MultipartPartEntry, tags []storage.Tag, multipartUploadID string) (*storage.Object, error) {
	stageStart := time.Now()
	shardKey := ecObjectShardKey(key, versionID)
	placementPlan, err := b.planObjectWritePlacement(ctx, ObjectWritePlacementInput{
		Operation: "put_object",
		ShardKey:  shardKey,
	})
	if err != nil {
		return nil, err
	}
	placementGroupID := placementPlan.PlacementGroupID
	effectiveCfg := placementPlan.Config
	placement := placementPlan.NodeIDs

	// Single write path: every non-empty simple PUT (any size, internal or
	// S3-exposed bucket) takes the segmented/chunked path so the route does not
	// branch on object size — N×16 MiB segments (1 segment for <=16 MiB), single
	// atomic metadata commit. The chunked SegmentWriter computes a bucket-aware
	// ETag (xxhash3 for internal buckets) so EC-rewrap verification still holds.
	// Requires a ShardGroupSource (always wired in production by bootOwnedGroupsAndEC).
	// Exceptions that keep the existing paths below: multipart-complete
	// (multipartUploadID / parts) and internal rewrap (beforeCommit), which are
	// distinct operations; and test backends that never wire a ShardGroupSource.
	if beforeCommit == nil && multipartUploadID == "" && len(parts) == 0 && b.shardGroup != nil {
		return b.putObjectChunked(
			ctx, bucket, key, versionID, sp,
			contentType, userMetadata, sseAlgorithm, acl,
			modTime, preserveModTime, expectedETag, beforeCommit, parts, tags,
		)
	}

	observePutStage("ec", "placement", stageStart)

	plan := ecObjectWritePlan{
		Bucket:           bucket,
		Key:              key,
		VersionID:        versionID,
		PlacementGroupID: placementGroupID,
		ModTime:          modTime,
		PreserveModTime:  preserveModTime,
		ExpectedETag:     expectedETag,
		Config:           effectiveCfg,
		Placement:        placement,
		ContentType:      contentType,
		UserMetadata:     cloneStringMap(userMetadata),
		SSEAlgorithm:     sseAlgorithm,
		ACL:              acl,
	}
	writer := newECObjectWriter(b.currentSelfAddr(), b.shardSvc, b.currentPeerHealth())
	result, err := writer.writeSpooledShards(ctx, plan, b.ecSpoolDir(), sp)
	if err != nil {
		if placementPlan.TopologyWrite {
			return nil, topologyShardWriteError(placementPlan.TopologyGroup, effectiveCfg, err)
		}
		return nil, err
	}
	if beforeCommit != nil {
		if err := beforeCommit(); err != nil {
			b.deleteShardsAsync(plan.Bucket, result.Placement, result.ShardKey)
			return nil, err
		}
	}

	result.Parts = parts
	result.Tags = tags
	if multipartUploadID != "" {
		return b.commitCompleteMultipartObjectWriteResult(ctx, multipartUploadID, plan, result, "ec")
	}
	return b.commitECObjectWriteResult(ctx, plan, result, "ec")
}

func topologyShardWriteError(group ShardGroupEntry, cfg ECConfig, err error) error {
	var shardErr *ecObjectShardWriteError
	if !errors.As(err, &shardErr) {
		return err
	}
	return &ErrInsufficientPlacementTargets{
		Operation:     "put_object",
		GroupID:       group.ID,
		Desired:       cfg,
		Configured:    cloneStringSlice(group.PeerIDs),
		Unavailable:   []string{shardErr.node},
		FailureReason: fmt.Sprintf("ec write shard %d failed: %v", shardErr.shardIdx, shardErr.err),
	}
}

func (b *DistributedBackend) commitECObjectWriteResult(
	ctx context.Context,
	plan ecObjectWritePlan,
	result ecObjectWriteResult,
	metricPath string,
) (*storage.Object, error) {
	stageStart := time.Now()
	modTime := result.ModTime
	if plan.PreserveModTime {
		modTime = plan.ModTime
	}
	metaCmd := PutObjectMetaCmd{
		Bucket:           plan.Bucket,
		Key:              plan.Key,
		Size:             result.Size,
		ContentType:      plan.ContentType,
		ETag:             result.ETag,
		ModTime:          modTime,
		VersionID:        plan.VersionID,
		PlacementGroupID: plan.PlacementGroupID,
		ECData:           result.ECData,
		ECParity:         result.ECParity,
		NodeIDs:          result.Placement,
		UserMetadata:     cloneStringMap(plan.UserMetadata),
		SSEAlgorithm:     plan.SSEAlgorithm,
		ACL:              plan.ACL,
		ExpectedETag:     plan.ExpectedETag,
		Parts:            result.Parts,
		Tags:             result.Tags,
	}
	// Phase 3: quorum meta write replaces data_raft propose.
	if merr := b.writeQuorumMeta(ctx, metaCmd); merr != nil {
		ObservePutTraceStage(ctx, PutTraceStageQuorumMetaWrite, stageStart, PutTraceStageFields{Error: merr.Error()})
		go b.deleteShardsAsync(plan.Bucket, result.Placement, result.ShardKey)
		return nil, merr
	}
	ObservePutTraceStage(ctx, PutTraceStageQuorumMetaWrite, stageStart, PutTraceStageFields{})
	observePutStage(metricPath, "quorum_meta", stageStart)

	// result.Tags aliases the caller's slice; do not introduce concurrent
	// readers/writers on result after this point.
	return &storage.Object{
		Key:              plan.Key,
		Size:             result.Size,
		ContentType:      plan.ContentType,
		ETag:             result.ETag,
		LastModified:     modTime,
		VersionID:        plan.VersionID,
		UserMetadata:     cloneStringMap(plan.UserMetadata),
		SSEAlgorithm:     plan.SSEAlgorithm,
		PlacementGroupID: plan.PlacementGroupID,
		ECData:           result.ECData,
		ECParity:         result.ECParity,
		NodeIDs:          cloneStringSlice(result.Placement),
		Parts:            result.Parts,
		Tags:             result.Tags,
	}, nil
}

// commitCompleteMultipartObjectWriteResult commits the completed object's
// quorum-meta blob, FAIL-CLOSED — M3: the multipart complete is raft-free (no
// CmdCompleteMultipart propose). For a versioning-enabled bucket the per-version
// blob is the blob authority; non-versioned / Suspended commits the latest-only
// quorum-meta blob (also fail-closed, mirroring the regular non-versioned PUT). On
// any commit error nothing is durable, so the EC shards are eager-deleted and the
// client can safely retry CompleteMultipartUpload (its manifest blob is intact). The
// idempotency anchor is the deterministic det-vid blob + the existence short-circuit
// in CompleteMultipartUpload, NOT a done-marker. This is off the PUT/GET/HEAD hot
// path (CompleteMultipartUpload is its own S3 API).
//
// Used only by the legacy spool path (backends without a ShardGroupSource — never
// production); the production chunked path is runChunkedPutWithParts.
func (b *DistributedBackend) commitCompleteMultipartObjectWriteResult(
	ctx context.Context,
	uploadID string,
	plan ecObjectWritePlan,
	result ecObjectWriteResult,
	metricPath string,
) (*storage.Object, error) {
	_ = uploadID
	stageStart := time.Now()
	modTime := result.ModTime
	if plan.PreserveModTime {
		modTime = plan.ModTime
	}
	metaCmd := PutObjectMetaCmd{
		Bucket:           plan.Bucket,
		Key:              plan.Key,
		Size:             result.Size,
		ContentType:      plan.ContentType,
		ETag:             result.ETag,
		ModTime:          modTime,
		VersionID:        plan.VersionID,
		PlacementGroupID: plan.PlacementGroupID,
		ECData:           result.ECData,
		ECParity:         result.ECParity,
		NodeIDs:          result.Placement,
		UserMetadata:     cloneStringMap(plan.UserMetadata),
		SSEAlgorithm:     plan.SSEAlgorithm,
		ACL:              plan.ACL,
		Parts:            result.Parts,
		Tags:             result.Tags,
	}
	// Versioning-enabled buckets get an encoded per-version blob; non-versioned /
	// Suspended get nil and commit via the latest-only quorum-meta write below.
	metaBlob, mberr := b.buildMultipartMetaBlob(ctx, metaCmd)
	if mberr != nil {
		return nil, mberr
	}
	if len(metaBlob) > 0 {
		// VERSIONED: the per-version blob is the blob authority, FAIL-CLOSED. On
		// failure nothing is durable — eager-delete the shards and let the client retry.
		winCmd, werr := b.writeCompletedMultipartBlob(ctx, metaBlob)
		if werr != nil {
			go b.deleteShardsAsync(plan.Bucket, result.Placement, result.ShardKey)
			return nil, werr
		}
		ObservePutTraceStage(ctx, PutTraceStageDataRaftProposeMeta, stageStart, PutTraceStageFields{})
		observePutStage(metricPath, "propose_meta", stageStart)
		winObj, _ := objectAndPlacementFromCmd(winCmd)
		return winObj, nil
	}

	// NON-VERSIONED / Suspended: the latest-only quorum-meta blob is the sole
	// authority, written FAIL-CLOSED (M3 F7) — mirrors the regular non-versioned PUT
	// (commitECObjectWriteResult). On failure nothing is durable; eager-delete shards.
	if merr := b.writeQuorumMeta(ctx, metaCmd); merr != nil {
		ObservePutTraceStage(ctx, PutTraceStageDataRaftProposeMeta, stageStart, PutTraceStageFields{Error: merr.Error()})
		go b.deleteShardsAsync(plan.Bucket, result.Placement, result.ShardKey)
		return nil, merr
	}
	ObservePutTraceStage(ctx, PutTraceStageDataRaftProposeMeta, stageStart, PutTraceStageFields{})
	observePutStage(metricPath, "propose_meta", stageStart)

	return &storage.Object{
		Key:              plan.Key,
		Size:             result.Size,
		ContentType:      plan.ContentType,
		ETag:             result.ETag,
		LastModified:     modTime,
		VersionID:        plan.VersionID,
		UserMetadata:     cloneStringMap(plan.UserMetadata),
		SSEAlgorithm:     plan.SSEAlgorithm,
		PlacementGroupID: plan.PlacementGroupID,
		ECData:           result.ECData,
		ECParity:         result.ECParity,
		NodeIDs:          cloneStringSlice(result.Placement),
		Parts:            result.Parts,
		Tags:             result.Tags,
	}, nil
}

// deleteShardsAsync는 쓰기 commit 실패 시 고아 샤드를 백그라운드에서 삭제한다.
// best-effort: 실패는 무시한다. 현재 EC 샤드용 orphan scrubber가 없으므로
// (OrphanWalkable 미구현) 이 호출이 유일한 회수 경로다. M3 이후 multipart-complete의
// quorum-meta blob 쓰기는 동기적(fail-closed)이므로 commit 실패 = 확정 미커밋이고,
// 항상 즉시 회수해도 안전하다(과거의 propose phantom-commit window는 없다).
func (b *DistributedBackend) deleteShardsAsync(bucket string, placement []string, shardKey string) {
	// Drop any cached entries for this shardKey before/after the disk
	// delete. Reads after this point must miss the cache so they can
	// learn the object is gone (or at least re-fetch fresh placement).
	b.invalidateShardCache(bucket, shardKey, len(placement))
	for _, node := range placement {
		if node == b.currentSelfAddr() {
			_ = b.shardSvc.DeleteLocalShards(bucket, shardKey)
		} else {
			_ = b.shardSvc.DeleteShards(context.Background(), node, bucket, shardKey)
		}
	}
}
