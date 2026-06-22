package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog/log"

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
	if blocked, q, qerr := b.isObjectQuarantined(bucket, key, ""); qerr != nil {
		return nil, fmt.Errorf("check quarantine: %w", qerr)
	} else if blocked {
		return nil, objectQuarantinedError(bucket, key, q)
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
		return hrw.PlaceShards(shardKey, liveNodes, nil, cfg.NumShards())
	}
	stateByNode := make(map[string]ObjectWritePlacementNodeState, len(nodeStates))
	for _, state := range nodeStates {
		stateByNode[state.NodeID] = state
	}
	weights := make([]float64, len(liveNodes))
	skipReason := make([]string, len(liveNodes))
	active := 0
	for i, nodeID := range liveNodes {
		state, ok := stateByNode[nodeID]
		if !ok || state.DiskAvailBytes == 0 {
			weights[i] = 0
			skipReason[i] = "stale"
			continue
		}
		weights[i] = float64(state.DiskAvailBytes)
		active++
	}

	// All-stale safeguard: if no live data (e.g. boot before first gossip),
	// fall back to legacy unweighted placement so writes don't fail.
	if active == 0 && countSkippedFor(skipReason, "stale") == len(liveNodes) {
		metrics.ClusterPlacementSkipped.WithLabelValues("*", "all_stale_fallback").Inc()
		return hrw.PlaceShards(shardKey, liveNodes, nil, cfg.NumShards())
	}

	// Emit per-reason skip metrics.
	for i, r := range skipReason {
		if r != "" {
			metrics.ClusterPlacementSkipped.WithLabelValues(liveNodes[i], r).Inc()
		}
	}

	return hrw.PlaceShards(shardKey, liveNodes, weights, cfg.NumShards())
}

// countSkippedFor returns the number of entries in reasons matching target.
func countSkippedFor(reasons []string, target string) int {
	n := 0
	for _, r := range reasons {
		if r == target {
			n++
		}
	}
	return n
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

// commitCompleteMultipartObjectWriteResult commits the completed object via the
// *group* raft (b.node — data plane) FIRST: applyCompleteMultipart atomically
// writes the object meta to the FSM AND deletes the multipart manifest key in a
// single BadgerDB txn. That atomicity is load-bearing — it prevents a manifest
// leak / upload-ID reuse (re-completing the same upload would mint a duplicate
// version), so the raft propose stays the authoritative durable commit. On
// failure nothing is committed and the manifest is intact, so clean up the
// shards and fail; the client can safely retry CompleteMultipartUpload.
//
// After the durable commit, the object is mirrored into quorum-meta so Phase 4
// index-free LIST can enumerate it (the same source as a regular PUT). A failure
// of that mirror must NOT delete shards or fail the op — the object is already
// committed and served by GET/HEAD via the BadgerDB FSM fallback; only LIST
// visibility lags until a repair re-derives quorum-meta. This is off the
// PUT/GET/HEAD hot path (CompleteMultipartUpload is its own S3 API). Phase 3/4 경계.
func (b *DistributedBackend) commitCompleteMultipartObjectWriteResult(
	ctx context.Context,
	uploadID string,
	plan ecObjectWritePlan,
	result ecObjectWriteResult,
	metricPath string,
) (*storage.Object, error) {
	stageStart := time.Now()
	modTime := result.ModTime
	if plan.PreserveModTime {
		modTime = plan.ModTime
	}
	// Blob-authoritative multipart (versioning-enabled): build the completed object's
	// encoded PutObjectMetaCmd here (the proposer) and pass it as meta_blob; the
	// per-version blob (written after the propose, from the done-marker) is then the
	// sole authority. nil for non-versioned/Suspended (legacy FSM obj:/lat:).
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
	metaBlob, mberr := b.buildMultipartMetaBlob(ctx, metaCmd)
	if mberr != nil {
		return nil, mberr
	}
	// Authoritative commit: atomic {write object meta to FSM, delete manifest}.
	if merr := b.propose(ctx, CmdCompleteMultipart, CompleteMultipartCmd{
		Bucket:           plan.Bucket,
		Key:              plan.Key,
		UploadID:         uploadID,
		Size:             result.Size,
		ContentType:      plan.ContentType,
		ETag:             result.ETag,
		ModTime:          modTime,
		VersionID:        plan.VersionID,
		PlacementGroupID: plan.PlacementGroupID,
		ECData:           result.ECData,
		ECParity:         result.ECParity,
		NodeIDs:          result.Placement,
		Parts:            result.Parts,
		Tags:             result.Tags,
		MetaBlob:         metaBlob,
	}); merr != nil {
		ObservePutTraceStage(ctx, PutTraceStageDataRaftProposeMeta, stageStart, PutTraceStageFields{Error: merr.Error()})
		if shardCleanupSafeOnProposeError(merr) {
			go b.deleteShardsAsync(plan.Bucket, result.Placement, result.ShardKey)
		}
		return nil, merr
	}

	// Blob-authoritative: write the WINNER's per-version blob from the done-marker's
	// meta_blob, FAIL-CLOSED. winner+loser both run this (idempotent same vid+bytes);
	// the loser uses the marker's meta_blob. The object is durably committed via raft,
	// so a blob-write failure returns an error and the client's retry re-writes it.
	if len(metaBlob) > 0 {
		marker, merr := b.readDoneMarker(uploadID)
		if merr != nil {
			return nil, fmt.Errorf("multipart complete: done-marker readback: %w", merr)
		}
		winnerBlob := metaBlob
		if marker != nil && len(marker.MetaBlob) > 0 {
			winnerBlob = marker.MetaBlob
		}
		winCmd, werr := b.writeCompletedMultipartBlob(ctx, winnerBlob)
		if werr != nil {
			return nil, werr
		}
		ObservePutTraceStage(ctx, PutTraceStageDataRaftProposeMeta, stageStart, PutTraceStageFields{})
		observePutStage(metricPath, "propose_meta", stageStart)
		winObj, _ := objectAndPlacementFromCmd(winCmd)
		return winObj, nil
	}

	// Non-versioned/Suspended legacy path (no meta_blob).
	// Phantom-winner guard: after a successful propose, check who actually won
	// this uploadID. If another concurrent completion committed first (its
	// marker holds a different versionID), our FSM apply was a no-op — do NOT
	// mirror our version into quorum-meta (that would publish a duplicate
	// version for versioning-enabled buckets). Return the winner's object;
	// our shards become orphans collected by the segment scrubber.
	var winnerObj *storage.Object
	if marker, merr := b.readDoneMarker(uploadID); merr != nil {
		log.Warn().Err(merr).Str("bucket", plan.Bucket).Str("key", plan.Key).Str("upload_id", uploadID).
			Msg("multipart complete: done-marker readback failed; proceeding with mirror")
	} else if marker != nil && marker.VersionID != plan.VersionID {
		obj, herr := func() (*storage.Object, error) {
			o, _, e := b.headObjectMetaV(ctx, plan.Bucket, plan.Key, marker.VersionID)
			return o, e
		}()
		if herr != nil {
			return nil, fmt.Errorf("multipart complete: winner readback: %w", herr)
		}
		winnerObj = obj
	}
	if winnerObj != nil {
		ObservePutTraceStage(ctx, PutTraceStageDataRaftProposeMeta, stageStart, PutTraceStageFields{})
		observePutStage(metricPath, "propose_meta", stageStart)
		return winnerObj, nil
	}

	// LIST-visibility mirror into quorum-meta (best-effort; the object is already
	// durably committed above). Must NOT delete shards or fail the op on error.
	if merr := b.writeQuorumMeta(ctx, metaCmd); merr != nil {
		log.Warn().Err(merr).Str("bucket", plan.Bucket).Str("key", plan.Key).Str("upload_id", uploadID).
			Msg("multipart complete: quorum-meta mirror failed (object committed via raft; LIST visibility deferred until repair)")
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

// shardCleanupSafeOnProposeError reports whether a failed CmdCompleteMultipart
// propose may safely trigger eager shard deletion. It is false ONLY for the
// phantom-commit window — a server-side propose timeout (ErrProposeTimeout) or
// a client cancellation mid-commit (context.Canceled). In those cases the raft
// entry may still commit later; applyCompleteMultipart would then write object
// meta referencing the now-deleted shards, yielding an unreadable committed
// object with no retry. Every other error (encode failure, FSM apply error,
// terminal non-leader / forward failure where no entry was accepted) means the
// entry did not and will not commit, so the shards are true orphans and eager
// deletion reclaims them immediately.
//
// NOTE: there is currently no orphan-shard scrubber for EC shards in production
// (the OrphanWalkable interface is unimplemented by DistributedBackend), so
// shards retained here on the phantom-commit window are NOT reclaimed if the
// entry ultimately does not commit — a bounded, rare slow-leak. The proper
// foundation (an EC orphan-shard sweep) is tracked in TODOS.md; until then the
// trade is: rare bounded leak on ambiguous failure vs. destroying a committed
// object's data.
func shardCleanupSafeOnProposeError(err error) bool {
	return !errors.Is(err, ErrProposeTimeout) && !errors.Is(err, context.Canceled)
}

// deleteShardsAsync는 propose 실패 시 고아 샤드를 백그라운드에서 삭제한다.
// best-effort: 실패는 무시한다. 현재 EC 샤드용 orphan scrubber가 없으므로
// (OrphanWalkable 미구현) 이 호출이 유일한 회수 경로다 — 따라서 propose가
// 확정적으로 commit되지 않은 경우(shardCleanupSafeOnProposeError)에만 호출한다.
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
