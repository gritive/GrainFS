package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"golang.org/x/sync/errgroup"
)

type ecObjectSizedShardStore interface {
	WriteLocalShardStreamSizedContext(ctx context.Context, bucket, key string, shardIdx int, body io.Reader, streamSize int64) error
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
	res, err := w.writeDataShards(ctx, plan, in.Data)
	if err != nil {
		return PlacementRecord{}, "", "", fmt.Errorf("write segment %d (blob %s): %w", in.SegmentIdx, in.SegmentBlobID, err)
	}
	rec := PlacementRecord{Nodes: placement, K: in.Cfg.DataShards, M: in.Cfg.ParityShards}
	return rec, res.ETag, in.SegmentBlobID, nil
}

func (w ecObjectWriter) writeDataShards(ctx context.Context, plan ecObjectWritePlan, data []byte) (ecObjectWriteResult, error) {
	shards, err := ecSplitBodies(plan.Config, data)
	if err != nil {
		return ecObjectWriteResult{}, fmt.Errorf("ec split: %w", err)
	}
	header := encodeShardHeader(int64(len(data)))
	h := md5.Sum(data)
	sp := &spooledObject{
		Size: int64(len(data)),
		ETag: hex.EncodeToString(h[:]),
	}

	return w.writeShardReadersWithSize(ctx, plan, sp, func(idx int) (io.Reader, error) {
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
}

func (w ecObjectWriter) writeSpooledShards(ctx context.Context, plan ecObjectWritePlan, spoolDir string, sp *spooledObject) (ecObjectWriteResult, error) {
	stageStart := time.Now()
	shards, err := spoolECShards(ctx, plan.Config, spoolDir, sp)
	if err != nil {
		return ecObjectWriteResult{}, err
	}
	observePutStage("ec", "spool_shards", stageStart)
	defer shards.Cleanup()

	return w.writeShardReadersWithSize(ctx, plan, sp, func(idx int) (io.Reader, error) {
		return shards.OpenShard(idx)
	}, shards.ShardSize, "ec")
}

func (w ecObjectWriter) writeShardReadersWithSize(
	ctx context.Context,
	plan ecObjectWritePlan,
	sp *spooledObject,
	openShard func(idx int) (io.Reader, error),
	shardSize func(idx int) (int64, error),
	metricPath string,
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
	written := make(chan string, len(plan.Placement))

	cleanup := func() {
		close(written)
		for node := range written {
			_ = w.endpointFor(node).DeleteShards(ctx, plan.Bucket, cleanupKey)
		}
	}

	stageStart := time.Now()
	g, gctx := errgroup.WithContext(ctx)
	for i, node := range plan.Placement {
		i, node := i, node
		g.Go(func() error {
			ep := w.endpointFor(node)
			if werr := ep.WriteShardReader(gctx, plan.Bucket, shardKey, stagingShardKey, i, openShard, shardSize); werr != nil {
				return &ecObjectShardWriteError{shardIdx: i, node: node, err: werr}
			}
			written <- node
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		cleanup()
		return ecObjectWriteResult{}, err
	}
	observePutStage(metricPath, "write_shards", stageStart)
	close(written)

	return ecObjectWriteResult{
		Size:      sp.Size,
		ETag:      sp.ETag,
		ModTime:   time.Now().Unix(),
		ShardKey:  shardKey,
		Placement: cloneStringSlice(plan.Placement),
		ECData:    uint8(plan.Config.DataShards),
		ECParity:  uint8(plan.Config.ParityShards),
	}, nil
}
