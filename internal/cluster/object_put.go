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

	if b.putPipeline != nil && b.putPipelineEnabled && b.shardSvc != nil && !storage.IsInternalBucket(bucket) && req.SizeHint != nil {
		selfID := b.currentSelfAddr()
		placementPlan, planErr := b.planObjectWritePlacement(ctx, ObjectWritePlacementInput{
			Operation: "put_object",
			ShardKey:  ecObjectShardKey(key, ""),
		})
		// The put pipeline writes a stripe-INTERLEAVED shard layout (one fragment per
		// stripe per shard); the GET reader de-interleaves when object metadata carries
		// StripeBytes > 0. Both the all-local and multi-node branches below stamp
		// StripeBytes from the same live pipeline config, so any K round-trips.
		//
		// Commit semantics are inherited from the shipped all-local path (same
		// CommitCoord/putWaiter): finalize when all shards are accounted for AND all K
		// data shards are durable (commit.go:132), parity best-effort. De-interleave at
		// read needs only the K data shards (guaranteed present at commit), so any K
		// reads correctly; parity is for erasure-reconstruct, not the happy-path read.
		// The multi-node branch is opt-in via putPipelineMultiNode (env at boot).
		// StripeBytes is stamped on the multi-node branch too (see above), so any K
		// de-interleaves correctly — no DataShards==1 restriction. The branch dispatch
		// still requires a real EC config via NumShards()>0.
		pipelineLayoutSafe := planErr == nil
		allLocal := false
		if planErr == nil {
			// Actor-eligible: all shards local and EC config is non-zero.
			allLocal = placementPlan.Config.NumShards() > 0
			for _, p := range placementPlan.NodeIDs {
				if p != selfID {
					allLocal = false
					break
				}
			}
		}
		if allLocal {
			versionID := newVersionID()
			shardKey := ecObjectShardKey(key, versionID)
			obj, err := b.putPipeline.PutShard(ctx, shardKey, storage.PutObjectRequest{
				Bucket:         bucket,
				Key:            shardKey,
				Body:           r,
				SizeHint:       req.SizeHint,
				ContentType:    contentType,
				UserMetadata:   userMetadata,
				SystemMetadata: req.SystemMetadata,
				ContentMD5Hex:  req.ContentMD5Hex,
			}, placementPlan.Config)
			if err != nil {
				return nil, err
			}
			placementGroupID := placementPlan.PlacementGroupID
			nodeIDs := make([]string, placementPlan.Config.NumShards())
			for i := range nodeIDs {
				nodeIDs[i] = selfID
			}
			stripeBytes := uint32(b.putPipeline.StripeBytes())
			pipelineAllLocalMetaCmd := PutObjectMetaCmd{
				Bucket:           bucket,
				Key:              key,
				Size:             obj.Size,
				ContentType:      contentType,
				ETag:             obj.ETag,
				ModTime:          obj.LastModified,
				VersionID:        versionID,
				PlacementGroupID: placementGroupID,
				ECData:           uint8(placementPlan.Config.DataShards),
				ECParity:         uint8(placementPlan.Config.ParityShards),
				// load-bearing: this marker MUST equal the StripeBytes the pipeline
				// split on; both read the live pipeline config so they cannot drift.
				StripeBytes:  stripeBytes,
				NodeIDs:      nodeIDs,
				UserMetadata: cloneStringMap(userMetadata),
				SSEAlgorithm: sseAlgorithm,
			}
			// Phase 3: quorum meta write replaces data_raft propose.
			if merr := b.writeQuorumMeta(ctx, pipelineAllLocalMetaCmd); merr != nil {
				go b.deleteShardsAsync(bucket, nodeIDs, shardKey)
				return nil, merr
			}
			return &storage.Object{
				Key:              key,
				Size:             obj.Size,
				ContentType:      contentType,
				ETag:             obj.ETag,
				LastModified:     obj.LastModified,
				VersionID:        versionID,
				UserMetadata:     cloneStringMap(userMetadata),
				SSEAlgorithm:     sseAlgorithm,
				PlacementGroupID: placementGroupID,
				ECData:           uint8(placementPlan.Config.DataShards),
				ECParity:         uint8(placementPlan.Config.ParityShards),
				StripeBytes:      stripeBytes,
				NodeIDs:          cloneStringSlice(nodeIDs),
			}, nil
		} else if b.putPipelineMultiNode && pipelineLayoutSafe && placementPlan.Config.NumShards() > 0 {
			// EXPERIMENTAL multi-node streaming EC (opt-in via putPipelineMultiNode):
			// stream remote shards to their peers (verbatim WriteSealedShard) instead
			// of spooling. Mirrors the all-local block but records the real per-shard
			// NodeIDs. Commit semantics inherited from the all-local path: data-shards-
			// required, parity best-effort (commit.go:132); de-interleave needs only
			// the K data shards, so any K reads correctly.
			placement, rerr := resolveShardPlacement(placementPlan.NodeIDs, selfID, b.shardSvc.resolvePeerAddress)
			if rerr != nil {
				return nil, rerr
			}
			versionID := newVersionID()
			shardKey := ecObjectShardKey(key, versionID)
			obj, err := b.putPipeline.PutShardPlaced(ctx, shardKey, storage.PutObjectRequest{
				Bucket:         bucket,
				Key:            shardKey,
				Body:           r,
				SizeHint:       req.SizeHint,
				ContentType:    contentType,
				UserMetadata:   userMetadata,
				SystemMetadata: req.SystemMetadata,
				ContentMD5Hex:  req.ContentMD5Hex,
			}, placement, placementPlan.Config)
			if err != nil {
				return nil, err
			}
			placementGroupID := placementPlan.PlacementGroupID
			nodeIDs := cloneStringSlice(placementPlan.NodeIDs)
			stripeBytes := uint32(b.putPipeline.StripeBytes())
			pipelineMetaCmd := PutObjectMetaCmd{
				Bucket:           bucket,
				Key:              key,
				Size:             obj.Size,
				ContentType:      contentType,
				ETag:             obj.ETag,
				ModTime:          obj.LastModified,
				VersionID:        versionID,
				PlacementGroupID: placementGroupID,
				ECData:           uint8(placementPlan.Config.DataShards),
				ECParity:         uint8(placementPlan.Config.ParityShards),
				// load-bearing: this marker MUST equal the StripeBytes the pipeline
				// split on; both read the live pipeline config so they cannot drift.
				StripeBytes:  stripeBytes,
				NodeIDs:      nodeIDs,
				UserMetadata: cloneStringMap(userMetadata),
				SSEAlgorithm: sseAlgorithm,
			}
			// Phase 3: quorum meta write replaces data_raft propose.
			if merr := b.writeQuorumMeta(ctx, pipelineMetaCmd); merr != nil {
				go b.deleteShardsAsync(bucket, nodeIDs, shardKey)
				return nil, merr
			}
			return &storage.Object{
				Key:              key,
				Size:             obj.Size,
				ContentType:      contentType,
				ETag:             obj.ETag,
				LastModified:     obj.LastModified,
				VersionID:        versionID,
				UserMetadata:     cloneStringMap(userMetadata),
				SSEAlgorithm:     sseAlgorithm,
				PlacementGroupID: placementGroupID,
				ECData:           uint8(placementPlan.Config.DataShards),
				ECParity:         uint8(placementPlan.Config.ParityShards),
				StripeBytes:      stripeBytes,
				NodeIDs:          cloneStringSlice(nodeIDs),
			}, nil
		}
	}

	versionID := newVersionID()
	if b.currentECConfig().NumShards() != 0 && b.shardSvc != nil {
		obj, handled, err := b.tryPutObjectSingleLocalShardInMemory(ctx, bucket, key, versionID, r, contentType, userMetadata, sseAlgorithm, req.ContentMD5Hex)
		if handled || err != nil {
			return obj, err
		}
		obj, handled, err = b.tryPutObjectSingleLocalShardKnownSize(ctx, bucket, key, versionID, r, req.SizeHint, contentType, userMetadata, sseAlgorithm, req.ContentMD5Hex)
		if handled || err != nil {
			return obj, err
		}
		obj, handled, err = b.tryPutObjectECDataInMemory(ctx, bucket, key, versionID, r, contentType, userMetadata, sseAlgorithm, req.ContentMD5Hex)
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
	return b.putObjectECSpooled(ctx, bucket, key, versionID, sp, contentType, userMetadata, sseAlgorithm)
}

// resolveShardPlacement maps each shard's target node to the address the put
// pipeline streams to: "" when the shard is local (node == selfID), otherwise
// the resolved peer address. Used only on the multi-node streaming PUT path.
func resolveShardPlacement(nodeIDs []string, selfID string, resolve func(node string) (string, error)) ([]string, error) {
	placement := make([]string, len(nodeIDs))
	for i, node := range nodeIDs {
		if node == selfID {
			continue
		}
		addr, err := resolve(node)
		if err != nil {
			return nil, fmt.Errorf("put object: resolve shard %d peer %q: %w", i, node, err)
		}
		placement[i] = addr
	}
	return placement, nil
}

func (b *DistributedBackend) tryPutObjectSingleLocalShardInMemory(
	ctx context.Context,
	bucket, key, versionID string,
	r io.Reader,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
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

	obj, err := b.putObjectECData(ctx, bucket, key, versionID, data, contentType, userMetadata, sseAlgorithm)
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
	obj, err := b.putObjectECSpooled(ctx, bucket, key, versionID, sp, contentType, nil, "")
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
func (b *DistributedBackend) putObjectECData(ctx context.Context, bucket, key, versionID string, data []byte, contentType string, userMetadata map[string]string, sseAlgorithm string) (*storage.Object, error) {
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

func (b *DistributedBackend) putObjectECSpooled(ctx context.Context, bucket, key, versionID string, sp *spooledObject, contentType string, userMetadata map[string]string, sseAlgorithm string) (*storage.Object, error) {
	return b.putObjectECSpooledWithOptionalModTime(ctx, bucket, key, versionID, sp, contentType, userMetadata, sseAlgorithm, 0, false, "", nil, nil, nil, "")
}

func (b *DistributedBackend) putObjectECSpooledWithOptionalModTime(ctx context.Context, bucket, key, versionID string, sp *spooledObject, contentType string, userMetadata map[string]string, sseAlgorithm string, modTime int64, preserveModTime bool, expectedETag string, beforeCommit func() error, parts []storage.MultipartPartEntry, tags []storage.Tag, multipartUploadID string) (*storage.Object, error) {
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
			contentType, userMetadata, sseAlgorithm,
			modTime, preserveModTime, expectedETag, beforeCommit, parts, tags,
		)
	}

	observePutStage("ec", "placement", stageStart)

	selfID := b.currentSelfAddr()
	if beforeCommit == nil && effectiveCfg.DataShards == 1 && effectiveCfg.ParityShards == 0 && len(placement) == 1 && placement[0] == selfID {
		return b.putObjectSingleLocalShardSpooled(ctx, bucket, key, versionID, placementGroupID, placement, sp, contentType, userMetadata, sseAlgorithm, parts, tags, multipartUploadID)
	}

	if beforeCommit == nil && ecMemoryShardFastPathEnabled(effectiveCfg) && sp.Size <= maxECMemoryShardFastPathBytesForCfg(effectiveCfg) {
		obj, handled, err := b.tryPutObjectECMemoryShards(ctx, bucket, key, versionID, placementGroupID, placement, effectiveCfg, sp, contentType, userMetadata, sseAlgorithm, parts, tags, multipartUploadID)
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

// commitCompleteMultipartObjectWriteResult intentionally proposes on the *group*
// raft (b.node — data plane), not on quorum meta and not on the meta-raft control
// plane: applyCompleteMultipart atomically writes object meta AND deletes the
// multipart manifest key in a single BadgerDB txn. Splitting that atomicity (quorum
// write + separate manifest delete) would open a window where the manifest leaks or
// the object disappears, so the manifest lives on group-raft rather than quorum
// meta. This is off the PUT/GET/HEAD hot path (CompleteMultipartUpload is its own S3
// API); headObjectMeta falls back to BadgerDB when quorum meta is absent. Phase 3 경계.
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

func (b *DistributedBackend) putObjectSingleLocalShardSpooled(
	ctx context.Context,
	bucket, key, versionID, placementGroupID string,
	placement []string,
	sp *spooledObject,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
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

// deleteShardsAsync는 propose 실패 시 고아 샤드를 백그라운드에서 삭제한다.
// best-effort: 실패는 무시하고 scrubber fallback에 위임한다.
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
