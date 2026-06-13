package cluster

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
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
	if b.currentECConfig().NumShards() != 0 && b.shardSvc != nil {
		obj, handled, err := b.tryPutObjectSingleLocalShardInMemory(ctx, bucket, key, versionID, r, contentType, userMetadata, sseAlgorithm, acl, req.ContentMD5Hex)
		if handled || err != nil {
			return obj, err
		}
		obj, handled, err = b.tryPutObjectSingleLocalShardKnownSize(ctx, bucket, key, versionID, r, req.SizeHint, contentType, userMetadata, sseAlgorithm, acl, req.ContentMD5Hex)
		if handled || err != nil {
			return obj, err
		}
		obj, handled, err = b.tryPutObjectECDataInMemory(ctx, bucket, key, versionID, r, contentType, userMetadata, sseAlgorithm, acl, req.ContentMD5Hex)
		if handled || err != nil {
			return obj, err
		}
	}

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

func (b *DistributedBackend) tryPutObjectSingleLocalShardInMemory(
	ctx context.Context,
	bucket, key, versionID string,
	r io.Reader,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	acl uint8,
	contentMD5Hex string,
) (*storage.Object, bool, error) {
	placementGroupID, ok := PlacementGroupFromContext(ctx)
	if !ok {
		if b.bypassBucketCheck {
			return nil, false, nil
		}
		placementGroupID = "group-0"
	}
	shardKey := ecObjectShardKey(key, versionID)
	placementPlan, err := b.planObjectWritePlacement(ctx, ObjectWritePlacementInput{
		Operation:        "put_object",
		PlacementGroupID: placementGroupID,
		ShardKey:         shardKey,
	})
	if err != nil {
		return nil, false, nil
	}
	if placementPlan.Config.DataShards != 1 || placementPlan.Config.ParityShards != 0 {
		return nil, false, nil
	}
	if len(placementPlan.NodeIDs) != 1 || placementPlan.NodeIDs[0] != b.currentSelfAddr() {
		return nil, false, nil
	}

	stageStart := time.Now()
	body, size, ok := sizedReaderAtSection(r, maxSingleLocalShardMemoryFastPathBytes)
	if !ok {
		return nil, false, nil
	}
	observePutStage("ec_single_memory", "read_body", stageStart)

	sp := &spooledObject{
		Size: size,
	}
	h := md5Pool.Get().(hash.Hash)
	h.Reset()
	defer md5Pool.Put(h)
	obj, err := b.putObjectSingleLocalShardFromReader(
		ctx,
		bucket,
		key,
		versionID,
		placementPlan.PlacementGroupID,
		placementPlan.NodeIDs,
		sp,
		body,
		contentType,
		userMetadata,
		sseAlgorithm,
		acl,
		"ec_single_memory",
		h,
		nil,
		nil,
		"",
		contentMD5Hex,
	)
	return obj, true, err
}

func (b *DistributedBackend) tryPutObjectSingleLocalShardKnownSize(
	ctx context.Context,
	bucket, key, versionID string,
	r io.Reader,
	sizeHint *int64,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	acl uint8,
	contentMD5Hex string,
) (*storage.Object, bool, error) {
	if sizeHint == nil || *sizeHint < 0 {
		return nil, false, nil
	}
	placementGroupID, ok := PlacementGroupFromContext(ctx)
	if !ok {
		if b.bypassBucketCheck {
			return nil, false, nil
		}
		placementGroupID = "group-0"
	}
	shardKey := ecObjectShardKey(key, versionID)
	placementPlan, err := b.planObjectWritePlacement(ctx, ObjectWritePlacementInput{
		Operation:        "put_object",
		PlacementGroupID: placementGroupID,
		ShardKey:         shardKey,
	})
	if err != nil {
		return nil, false, nil
	}
	if placementPlan.Config.DataShards != 1 || placementPlan.Config.ParityShards != 0 {
		return nil, false, nil
	}
	if len(placementPlan.NodeIDs) != 1 || placementPlan.NodeIDs[0] != b.currentSelfAddr() {
		return nil, false, nil
	}

	sp := &spooledObject{
		Size: *sizeHint,
	}
	h := md5Pool.Get().(hash.Hash)
	h.Reset()
	defer md5Pool.Put(h)
	obj, err := b.putObjectSingleLocalShardFromReader(
		ctx,
		bucket,
		key,
		versionID,
		placementPlan.PlacementGroupID,
		placementPlan.NodeIDs,
		sp,
		r,
		contentType,
		userMetadata,
		sseAlgorithm,
		acl,
		"ec_single_stream",
		h,
		nil,
		nil,
		"",
		contentMD5Hex,
	)
	return obj, true, err
}

type sizedReaderAt interface {
	io.ReaderAt
	Len() int
	Size() int64
}

func sizedReaderAtSection(r io.Reader, maxBytes int64) (io.Reader, int64, bool) {
	sr, ok := r.(sizedReaderAt)
	if !ok {
		return nil, 0, false
	}
	remaining := int64(sr.Len())
	if remaining < 0 || remaining > maxBytes {
		return nil, 0, false
	}
	offset := sr.Size() - remaining
	return io.NewSectionReader(sr, offset, remaining), remaining, true
}

func (b *DistributedBackend) tryPutObjectECDataInMemory(
	ctx context.Context,
	bucket, key, versionID string,
	r io.Reader,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	acl uint8,
	contentMD5Hex string,
) (*storage.Object, bool, error) {
	limit, ok := b.ecMemoryShardFastPathLimit(ctx)
	if !ok {
		return nil, false, nil
	}
	body, size, ok := sizedReaderAtSection(r, limit)
	if !ok {
		return nil, false, nil
	}
	// Pre-size the buffer from the known body length: avoids the geometric
	// growth churn of io.ReadAll (top alloc_space contributor for this path
	// in iter1 pprof) and skips zeroing the unused tail.
	data := make([]byte, size)
	if _, err := io.ReadFull(body, data); err != nil {
		return nil, true, fmt.Errorf("read small EC object: %w", err)
	}

	// Single write-path Content-MD5 validation: the full body is in memory, so
	// validate before any shard/metadata write. No-op when no Content-MD5.
	if contentMD5Hex != "" {
		sum := md5.Sum(data)
		if err := validateContentMD5(hex.EncodeToString(sum[:]), contentMD5Hex); err != nil {
			clear(data)
			return nil, true, err
		}
	}

	obj, err := b.putObjectECData(ctx, bucket, key, versionID, data, contentType, userMetadata, sseAlgorithm, acl)
	clear(data)
	return obj, true, err
}

// ecMemoryShardFastPathLimit returns the maximum body size (in bytes) for which
// the in-memory EC fast path may be used in this request, or ok=false when the
// fast path is disabled for this placement.
func (b *DistributedBackend) ecMemoryShardFastPathLimit(ctx context.Context) (int64, bool) {
	plan, err := b.planObjectWritePlacement(ctx, ObjectWritePlacementInput{
		Operation: "put_object",
	})
	if err != nil {
		return 0, false
	}
	cfg := plan.Config
	if !ecMemoryShardFastPathEnabled(cfg) {
		return 0, false
	}
	return maxECMemoryShardFastPathBytesForCfg(cfg), true
}

func ecMemoryShardFastPathEnabled(cfg ECConfig) bool {
	return cfg.NumShards() != 0
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

func PlacementGroupHasFullEntry(ctx context.Context) bool {
	_, ok := PlacementGroupEntryFromContext(ctx)
	return ok
}

// putObjectECData is the Phase 18 Cluster EC path: Reed-Solomon split into
// cfg.NumShards() shards, fan-out each to its placed node (self or peer),
// then commit metadata through Raft.
//
// Consistency: write-all. Any shard write failure → cleanup + error.
// Placement is derived deterministically via PlaceShards (HRW).
// ECData/ECParity + NodeIDs are stored in object metadata so reads can
// reconstruct shards without a separate Raft record.
func (b *DistributedBackend) putObjectECData(ctx context.Context, bucket, key, versionID string, data []byte, contentType string, userMetadata map[string]string, sseAlgorithm string, acl uint8) (*storage.Object, error) {
	// ShardService's key parameter carries the versionID as a suffix so shards
	// for different versions land at different paths without changing the API.
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

	// Commit metadata. ECData/ECParity + NodeIDs stored so reads can
	// reconstruct shards without a separate placement record.
	// On failure, best-effort cleanup of orphaned shards.
	plan := ecObjectWritePlan{
		Bucket:           bucket,
		Key:              key,
		VersionID:        versionID,
		PlacementGroupID: placementGroupID,
		Config:           effectiveCfg,
		Placement:        placement,
		ContentType:      contentType,
		UserMetadata:     cloneStringMap(userMetadata),
		SSEAlgorithm:     sseAlgorithm,
		ACL:              acl,
	}
	writer := newECObjectWriter(b.currentSelfAddr(), b.shardSvc, b.currentPeerHealth())
	result, err := writer.writeDataShards(ctx, plan, data)
	if err != nil {
		if placementPlan.TopologyWrite {
			return nil, topologyShardWriteError(placementPlan.TopologyGroup, effectiveCfg, err)
		}
		return nil, err
	}
	return b.commitECObjectWriteResult(ctx, plan, result, "ec")
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

	// Phase 2 chunked PUT routing: objects larger than DefaultChunkSize
	// (16 MiB) take the segmented path — N×16 MiB segments fanned across
	// placement groups, single atomic metadata commit, best-effort blob
	// fanout with defer cleanup on error. <= 16 MiB stays on the existing
	// single-segment EC path.
	//
	// Chunked PUT requires a ShardGroupSource for SelectSegmentPlacementGroup;
	// legacy single-group setups (b.shardGroup == nil) fall back to the
	// existing single-blob EC path even for large objects. Once a cluster
	// wires SetShardGroupSource, large objects automatically segment.
	if b.chunkedPathThresholdMet(sp.Size) && b.shardGroup != nil {
		return b.putObjectChunked(
			ctx, bucket, key, versionID, sp,
			contentType, userMetadata, sseAlgorithm, acl,
			modTime, preserveModTime, expectedETag, beforeCommit, parts, tags,
		)
	}

	observePutStage("ec", "placement", stageStart)

	selfID := b.currentSelfAddr()
	if beforeCommit == nil && effectiveCfg.DataShards == 1 && effectiveCfg.ParityShards == 0 && len(placement) == 1 && placement[0] == selfID {
		return b.putObjectSingleLocalShardSpooled(ctx, bucket, key, versionID, placementGroupID, placement, sp, contentType, userMetadata, sseAlgorithm, acl, parts, tags, multipartUploadID)
	}

	if beforeCommit == nil && ecMemoryShardFastPathEnabled(effectiveCfg) && sp.Size <= maxECMemoryShardFastPathBytesForCfg(effectiveCfg) {
		obj, handled, err := b.tryPutObjectECMemoryShards(ctx, bucket, key, versionID, placementGroupID, placement, effectiveCfg, sp, contentType, userMetadata, sseAlgorithm, acl, parts, tags, multipartUploadID)
		if err != nil && placementPlan.TopologyWrite {
			return nil, topologyShardWriteError(placementPlan.TopologyGroup, effectiveCfg, err)
		}
		if handled || err != nil {
			return obj, err
		}
	}

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

func (b *DistributedBackend) tryPutObjectECMemoryShards(
	ctx context.Context,
	bucket, key, versionID, placementGroupID string,
	placement []string,
	cfg ECConfig,
	sp *spooledObject,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	acl uint8,
	parts []storage.MultipartPartEntry,
	tags []storage.Tag,
	multipartUploadID string,
) (*storage.Object, bool, error) {
	writer := newECObjectWriter(b.currentSelfAddr(), b.shardSvc, b.currentPeerHealth())
	result, err := writer.writeMemoryShards(ctx, ecObjectWritePlan{
		Bucket:           bucket,
		Key:              key,
		VersionID:        versionID,
		PlacementGroupID: placementGroupID,
		Config:           cfg,
		Placement:        placement,
		ContentType:      contentType,
		UserMetadata:     cloneStringMap(userMetadata),
		SSEAlgorithm:     sseAlgorithm,
		ACL:              acl,
	}, sp)
	if err != nil {
		return nil, true, err
	}

	result.Parts = parts
	result.Tags = tags
	if multipartUploadID != "" {
		obj, err := b.commitCompleteMultipartObjectWriteResult(
			ctx,
			multipartUploadID,
			ecObjectWritePlan{
				Bucket:           bucket,
				Key:              key,
				VersionID:        versionID,
				PlacementGroupID: placementGroupID,
				Config:           cfg,
				Placement:        placement,
				ContentType:      contentType,
				UserMetadata:     cloneStringMap(userMetadata),
				SSEAlgorithm:     sseAlgorithm,
				ACL:              acl,
			},
			result,
			"ec_memory",
		)
		return obj, true, err
	}
	obj, err := b.commitECObjectWriteResult(
		ctx,
		ecObjectWritePlan{
			Bucket:           bucket,
			Key:              key,
			VersionID:        versionID,
			PlacementGroupID: placementGroupID,
			Config:           cfg,
			Placement:        placement,
			ContentType:      contentType,
			UserMetadata:     cloneStringMap(userMetadata),
			SSEAlgorithm:     sseAlgorithm,
			ACL:              acl,
		},
		result,
		"ec_memory",
	)
	return obj, true, err
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
	}); merr != nil {
		ObservePutTraceStage(ctx, PutTraceStageDataRaftProposeMeta, stageStart, PutTraceStageFields{Error: merr.Error()})
		if shardCleanupSafeOnProposeError(merr) {
			go b.deleteShardsAsync(plan.Bucket, result.Placement, result.ShardKey)
		}
		return nil, merr
	}
	// LIST-visibility mirror into quorum-meta (best-effort; the object is already
	// durably committed above). Must NOT delete shards or fail the op on error.
	if merr := b.writeQuorumMeta(ctx, PutObjectMetaCmd{
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
	}); merr != nil {
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

func (b *DistributedBackend) putObjectSingleLocalShardSpooled(
	ctx context.Context,
	bucket, key, versionID, placementGroupID string,
	placement []string,
	sp *spooledObject,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	acl uint8,
	parts []storage.MultipartPartEntry,
	tags []storage.Tag,
	multipartUploadID string,
) (*storage.Object, error) {
	shardKey := ecObjectShardKey(key, versionID)
	stageStart := time.Now()
	body, err := sp.Open()
	if err != nil {
		return nil, fmt.Errorf("open single shard body: %w", err)
	}
	obj, writeErr := b.putObjectSingleLocalShardFromReader(
		ctx,
		bucket,
		key,
		versionID,
		placementGroupID,
		placement,
		sp,
		body,
		contentType,
		userMetadata,
		sseAlgorithm,
		acl,
		"ec_single",
		nil,
		parts,
		tags,
		multipartUploadID,
		"", // Content-MD5 already validated at the spool site (sp.ETag)
	)
	closeErr := body.Close()
	if writeErr != nil {
		return nil, writeErr
	}
	if closeErr != nil {
		_ = b.shardSvc.DeleteLocalShards(bucket, shardKey)
		return nil, fmt.Errorf("close single shard body: %w", closeErr)
	}
	observePutStage("ec_single", "total_with_open_close", stageStart)
	return obj, nil
}

func (b *DistributedBackend) putObjectSingleLocalShardFromReader(
	ctx context.Context,
	bucket, key, versionID, placementGroupID string,
	placement []string,
	sp *spooledObject,
	body io.Reader,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	acl uint8,
	metricPath string,
	bodyHash hash.Hash,
	parts []storage.MultipartPartEntry,
	tags []storage.Tag,
	multipartUploadID string,
	contentMD5Hex string,
) (*storage.Object, error) {
	plan := ecObjectWritePlan{
		Bucket:           bucket,
		Key:              key,
		VersionID:        versionID,
		PlacementGroupID: placementGroupID,
		Config:           ECConfig{DataShards: 1, ParityShards: 0},
		Placement:        placement,
		ContentType:      contentType,
		UserMetadata:     cloneStringMap(userMetadata),
		SSEAlgorithm:     sseAlgorithm,
		ACL:              acl,
	}
	writer := newECObjectWriter(b.currentSelfAddr(), b.shardSvc, b.currentPeerHealth())
	result, err := writer.writeSingleLocalReader(ctx, plan, sp, body, metricPath, bodyHash)
	if err != nil {
		return nil, err
	}
	// Single write-path Content-MD5 validation: the shard is written but
	// metadata is not yet committed; reject before commit and drop the shard.
	// No-op when no Content-MD5 (e.g. multipart-complete, which has none).
	if verr := validateContentMD5(result.ETag, contentMD5Hex); verr != nil {
		_ = b.shardSvc.DeleteLocalShards(bucket, result.ShardKey)
		return nil, verr
	}
	result.Parts = parts
	result.Tags = tags

	if multipartUploadID != "" {
		return b.commitCompleteMultipartObjectWriteResult(ctx, multipartUploadID, plan, result, "ec_single")
	}

	stageStart := time.Now()
	metaCmd := PutObjectMetaCmd{
		Bucket:           bucket,
		Key:              key,
		Size:             result.Size,
		ContentType:      contentType,
		ETag:             result.ETag,
		ModTime:          result.ModTime,
		VersionID:        versionID,
		PlacementGroupID: placementGroupID,
		ECData:           result.ECData,
		ECParity:         result.ECParity,
		NodeIDs:          result.Placement,
		UserMetadata:     cloneStringMap(userMetadata),
		SSEAlgorithm:     sseAlgorithm,
		ACL:              acl,
		Parts:            parts,
		Tags:             tags,
	}
	// Phase 3: quorum meta write replaces data_raft propose.
	if merr := b.writeQuorumMeta(ctx, metaCmd); merr != nil {
		ObservePutTraceStage(ctx, PutTraceStageQuorumMetaWrite, stageStart, PutTraceStageFields{Error: merr.Error()})
		_ = b.shardSvc.DeleteLocalShards(bucket, result.ShardKey)
		return nil, merr
	}
	ObservePutTraceStage(ctx, PutTraceStageQuorumMetaWrite, stageStart, PutTraceStageFields{})
	observePutStage("ec_single", "quorum_meta", stageStart)

	return &storage.Object{
		Key:              key,
		Size:             result.Size,
		ContentType:      contentType,
		ETag:             result.ETag,
		LastModified:     result.ModTime,
		VersionID:        versionID,
		UserMetadata:     cloneStringMap(userMetadata),
		SSEAlgorithm:     sseAlgorithm,
		PlacementGroupID: placementGroupID,
		ECData:           result.ECData,
		ECParity:         result.ECParity,
		NodeIDs:          cloneStringSlice(result.Placement),
		Parts:            parts,
		Tags:             tags,
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
