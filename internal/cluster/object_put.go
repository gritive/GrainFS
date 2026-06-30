package cluster

import (
	"context"
	"crypto/md5"
	"encoding/hex"
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
// the client-supplied Content-MD5. No-op when the client sent no Content-MD5.
// Returns storage.ErrContentMD5Mismatch (→ 400 BadDigest) on mismatch.
// Returns a plain error (→ 500 InternalError) if a Content-MD5 was sent but
// the body MD5 was not computed — indicating a broken needsMD5 invariant.
func validateContentMD5(computedHex, clientHex string) error {
	if clientHex == "" {
		return nil // no Content-MD5 header sent; nothing to validate
	}
	if computedHex == "" {
		// Unreachable by construction (needsMD5=true whenever ContentMD5Hex != "").
		// Plain error (not ErrContentMD5Mismatch) so a regression surfaces as 500, not 400.
		return fmt.Errorf("internal: needsMD5 invariant broken — Content-MD5 requested but body MD5 was not computed")
	}
	if computedHex == clientHex {
		return nil
	}
	return fmt.Errorf("%w: client %s, body %s", storage.ErrContentMD5Mismatch, clientHex, computedHex)
}

type exactObjectSizeReader struct {
	r         io.Reader
	remaining int64
	probed    bool
}

func (r *exactObjectSizeReader) Read(p []byte) (int, error) {
	if r.remaining > 0 {
		if int64(len(p)) > r.remaining {
			p = p[:r.remaining]
		}
		n, err := r.r.Read(p)
		r.remaining -= int64(n)
		if err == io.EOF && r.remaining > 0 {
			return n, io.ErrUnexpectedEOF
		}
		return n, err
	}
	if r.probed {
		return 0, io.EOF
	}
	r.probed = true
	if len(p) == 0 {
		return 0, nil
	}
	n, err := r.r.Read(p[:1])
	if n > 0 {
		return n, fmt.Errorf("body exceeds exact size")
	}
	if err != nil && err != io.EOF {
		return 0, err
	}
	return 0, io.EOF
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
	// No-spool sizing for in-process callers: a body that already knows its exact
	// remaining length (bytes.Reader / bytes.Buffer / strings.Reader — e.g. the
	// WriteAt/Truncate RMW writes and forwarded small bodies) is sized from Len()
	// so it takes the streaming path below instead of needing the removed spool.
	// Len() is the exact unread byte count == what we store, so it is authoritative
	// even when the caller supplied only an ADVISORY SizeHint (SizeHint set,
	// SizeHintExact=false): the exact Len() overrides the hint. This fires whenever
	// the size is not already known-exact; the server S3 ingress sets SizeHint +
	// SizeHintExact itself and is untouched.
	if !req.SizeHintExact {
		if sizer, ok := r.(interface{ Len() int }); ok {
			n := int64(sizer.Len())
			req.SizeHint = &n
			req.SizeHintExact = true
		}
	}
	stageStart := time.Now()
	unlockBucketWrite, err := b.enterBucketObjectWrite(ctx, bucket)
	if err != nil {
		return nil, err
	}
	defer unlockBucketWrite()
	observePutStage("distributed", "head_bucket", stageStart)

	stageStart = time.Now()
	if err := b.quarantineGate(bucket, key, ""); err != nil {
		return nil, err
	}
	observePutStage("distributed", "quarantine_check", stageStart)

	versionID := newVersionID()
	// Single streaming write path: every PUT carries an exact size (server S3
	// ingress, copy, pull-through, and forwarded writes all stamp SizeHint +
	// SizeHintExact; the bare PutObject convenience auto-stamps it from a Len()
	// reader above) and a real shard group (always wired in production), so the
	// body streams straight into putObjectChunkedReader with no disk spool. A
	// Content-MD5, if present, is validated by teeing the body's PLAINTEXT
	// through md5 (encryption is downstream in the shard write) and rejecting in
	// a beforeCommit hook — which fires after the body is streamed but before the
	// quorum-meta commit, so a mismatch leaves nothing committed.
	streamable := req.SizeHint != nil && req.SizeHintExact && *req.SizeHint >= 0 && b.shardGroup != nil
	if !streamable {
		return nil, fmt.Errorf("put object: streaming requires an exact size hint and a shard group")
	}
	if b.currentECConfig().NumShards() == 0 || b.shardSvc == nil {
		return nil, fmt.Errorf("put object: EC storage is required")
	}
	if _, err := b.planObjectWritePlacement(ctx, ObjectWritePlacementInput{
		Operation: "put_object",
		ShardKey:  ecObjectShardKey(key, versionID),
	}); err != nil {
		return nil, err
	}
	var body io.Reader = &exactObjectSizeReader{r: r, remaining: *req.SizeHint}
	var beforeCommit func() error
	if req.ContentMD5Hex != "" {
		h := md5.New()
		body = io.TeeReader(body, h)
		beforeCommit = func() error {
			return validateContentMD5(hex.EncodeToString(h.Sum(nil)), req.ContentMD5Hex)
		}
	}
	return b.putObjectChunkedReader(ctx, bucket, key, versionID, body, *req.SizeHint, contentType, userMetadata, sseAlgorithm, acl, 0, false, "", beforeCommit, nil, nil)
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
