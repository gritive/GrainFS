package cluster

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage"
)

const (
	defaultMaxChunkedMultipartCompletes = 24
	defaultChunkedMultipartCompleteSize = 8 << 20
)

var chunkedMultipartCompleteSlots = make(chan struct{}, defaultMaxChunkedMultipartCompletes)

// clusterSegmentBackend implements storage.segmentWriterBackend for the
// chunked-PUT cluster pipeline. One instance is constructed per PUT; the
// SegmentWriter's 8 workers call WriteSegment concurrently with unique
// idx per call, so the pre-allocated placements/blobIDs slots are race-free.
//
// Per-segment placement uses Task 2.1's SelectSegmentPlacementGroup (PG
// fan-out hashed by bucket+key+idx+blobID) and Task 2.2's writeOneSegment
// (segment-scoped shardKey via SegmentBlobID, AAD propagates automatically).
type clusterSegmentBackend struct {
	b            *DistributedBackend
	bucket       string
	key          string
	versionID    string
	blobIDs      []string // pre-allocated UUIDv7 per segment index
	contentType  string
	userMetadata map[string]string
	sseAlgorithm string
	acl          uint8              // s3auth.ACLGrant bitmask; 0 = private (default)
	placements   []segmentPlacement // indexed by segmentIdx; pre-allocated; mutex-free

	// Test seams. Production constructor leaves these nil; defaults route
	// through b.shardGroup / newECObjectWriter / b.shardSvc.DeleteShards /
	// b.writeQuorumMeta. Tests inject these to exercise putObjectChunked without
	// a full RaftNode + ShardService.
	writeSegmentFn    func(ctx context.Context, idx int, in writeSegmentInput) (PlacementRecord, string, string, error)
	groupSelectorFn   func(bucket, key string, idx int, blobID string) (ShardGroupEntry, error)
	deleteShardsFn    func(ctx context.Context, peer, bucket, shardKey string) error
	writeQuorumMetaFn func(ctx context.Context, cmd PutObjectMetaCmd) error
	ecConfigFn        func() ECConfig
	// peerWeightsFn returns the per-peer disk-capacity weight snapshot aligned
	// 1:1 with peers and whether weighting is enabled. Production constructor
	// leaves it nil; the default routes through b.nodeStatsStore + b.clusterCfg.
	peerWeightsFn func(peers []string) (weights []float64, enabled bool)
	chunkSize     int
}

// segmentPlacement captures the post-write placement metadata for one
// segment. SegmentIdx is redundant with the slice index but propagates into
// SegmentMetaEntry.SegmentIdx so apply sees deterministic ordering even if
// the slice were ever reordered.
type segmentPlacement struct {
	BlobID           string
	SegmentIdx       int
	PlacementGroupID string
	NodeIDs          []string
	Config           ECConfig
	ShardSize        int32
}

// WriteSegment buffers the chunk, picks a PG for this (bucket,key,idx,blobID),
// EC-writes via writeOneSegment, and records the placement in the pre-allocated
// slot. Returns a storage.SegmentRef carrying the xxhash3-128 plaintext
// checksum.
func (c *clusterSegmentBackend) WriteSegment(ctx context.Context, bucket, key string, idx int, r io.Reader) (storage.SegmentRef, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return storage.SegmentRef{}, fmt.Errorf("segment %d: read chunk: %w", idx, err)
	}
	return c.WriteSegmentBytes(ctx, bucket, key, idx, data)
}

// WriteSegmentBytes receives an owned chunk from SegmentWriter's chunker. This
// avoids re-reading the bytes.Reader with io.ReadAll on the cluster hot path.
func (c *clusterSegmentBackend) WriteSegmentBytes(ctx context.Context, bucket, key string, idx int, data []byte) (storage.SegmentRef, error) {
	if idx < 0 || idx >= len(c.blobIDs) {
		return storage.SegmentRef{}, fmt.Errorf("segment %d: out of range (allocated %d)", idx, len(c.blobIDs))
	}

	// 1. Pick PG for this segment.
	group, err := c.selectGroup(bucket, key, idx, c.blobIDs[idx])
	if err != nil {
		return storage.SegmentRef{}, fmt.Errorf("segment %d: pick PG: %w", idx, err)
	}

	// 2. Delegate to writeOneSegment.
	cfg := c.currentECConfig()
	// Compute the per-peer weight snapshot for THIS group's peers once, on the
	// coordinating writer. writeOneSegment passes it to weighted HRW; the chosen
	// NodeIDs are recorded and replayed on read, so readers never recompute.
	weights, weightedEnabled := c.peerWeights(group.PeerIDs)
	in := writeSegmentInput{
		Bucket:          bucket,
		Key:             key,
		VersionID:       c.versionID,
		SegmentBlobID:   c.blobIDs[idx],
		SegmentIdx:      idx,
		Group:           group,
		Cfg:             cfg,
		Data:            data,
		Weights:         weights,
		WeightedEnabled: weightedEnabled,
	}
	rec, _, blobID, werr := c.writeOne(ctx, idx, in)
	if werr != nil {
		return storage.SegmentRef{}, fmt.Errorf("segment %d: EC write: %w", idx, werr)
	}

	// 3. Record placement. Workers receive unique idx, so the slot write
	// is race-free without a mutex.
	shardSize := int32(0)
	if cfg.DataShards > 0 {
		shardSize = int32((int64(len(data)) + int64(cfg.DataShards) - 1) / int64(cfg.DataShards))
	}
	c.placements[idx] = segmentPlacement{
		BlobID:           blobID,
		SegmentIdx:       idx,
		PlacementGroupID: group.ID,
		NodeIDs:          rec.Nodes,
		Config:           cfg,
		ShardSize:        shardSize,
	}

	// 4. xxhash3-128 over plaintext.
	sum := storage.ChecksumOf(data)
	return storage.SegmentRef{
		BlobID:           blobID,
		Size:             int64(len(data)),
		Checksum:         sum,
		PlacementGroupID: group.ID,
		ShardSize:        shardSize,
	}, nil
}

// vidDeterministicBlobNodeIDs computes the per-version blob's recorded NodeIDs
// from (bucket, key, vid) alone — independent of any segment's RANDOM blobID — so
// two independent completers of the same upload (which derive the SAME det-vid
// via deriveMultipartVID) record the SAME placement. That recorded set is the
// per-version blob WRITE target AND the hard-delete / tombstone-GC target
// (fanOutPerVersionBlob / deleteQuorumMetaVersionQuorum / tombstoneConverged all
// fan over cmd.NodeIDs), so a divergent placement would land a loser's blob on
// nodes a later hard-delete never visits → orphan/resurrection. Keying on vid
// closes that. The group is selected via the same seam as segment writes
// (selectGroup), passing vid in the blobID slot so it hashes by vid; the
// placement permutation is then keyed on a vid-derived shard key. Segments keep
// their own random-blobID placement; reads are placement-blind (all-peer
// fan-out), so regular PUTs are unaffected.
//
// Placement uses the SAME weighted HRW as segment writes (selectShardPlacement
// with the group's peer weights), consistent with the cluster-wide weighted-HRW
// placement model (FNV modulo was retired in #843). The vid-derived key — not
// the segment's random blobID — makes two completers converge whenever they
// share a peer-weight snapshot (the common case). A rare divergent weight
// snapshot (gossip lag) can place the same vid's blob on different nodes; that
// residual is resolved without a strict placement guarantee: the per-version
// read-merge LWW (quorumMetaCmdWins, F1.1) makes reads deterministic
// (higher-ModTime winner), a hard-delete's tombstone is read-gathered and
// shadows the loser, and the orphan walker reclaims the stale loser blob after
// delete. The divergence is thus a rare transient extra-replica, not a
// correctness or resurrection hazard.
func (c *clusterSegmentBackend) vidDeterministicBlobNodeIDs(bucket, key, vid string, cfg ECConfig) ([]string, error) {
	group, err := c.selectGroup(bucket, key, 0, vid)
	if err != nil {
		return nil, fmt.Errorf("vid-deterministic blob placement: %w", err)
	}
	if len(group.PeerIDs) == 0 {
		return nil, fmt.Errorf("vid-deterministic blob placement: group %q has no peers", group.ID)
	}
	placementKey := key + "/versions/" + vid
	weights, weightedEnabled := c.peerWeights(group.PeerIDs)
	return selectShardPlacement(cfg, group.PeerIDs, placementKey, weights, weightedEnabled), nil
}

func (c *clusterSegmentBackend) selectGroup(bucket, key string, idx int, blobID string) (ShardGroupEntry, error) {
	if c.groupSelectorFn != nil {
		return c.groupSelectorFn(bucket, key, idx, blobID)
	}
	if c.b == nil || c.b.shardGroup == nil {
		return ShardGroupEntry{}, fmt.Errorf("shardGroup source not wired")
	}
	return SelectSegmentPlacementGroup(bucket, key, idx, blobID, c.b.shardGroup.ShardGroups(), c.currentECConfig())
}

func (c *clusterSegmentBackend) currentECConfig() ECConfig {
	if c.ecConfigFn != nil {
		return c.ecConfigFn()
	}
	return c.b.currentECConfig()
}

// peerWeights returns the per-peer disk-capacity weight snapshot for this
// group's peers (aligned 1:1 with peers) and whether weighting is enabled.
// Tests inject peerWeightsFn; production reads b.nodeStatsStore + b.clusterCfg.
// A nil backend (test seam wiring) means unweighted placement.
func (c *clusterSegmentBackend) peerWeights(peers []string) ([]float64, bool) {
	if c.peerWeightsFn != nil {
		return c.peerWeightsFn(peers)
	}
	if c.b == nil {
		return nil, false
	}
	return c.b.peerPlacementWeights(peers)
}

func (c *clusterSegmentBackend) writeOne(ctx context.Context, idx int, in writeSegmentInput) (PlacementRecord, string, string, error) {
	if c.writeSegmentFn != nil {
		return c.writeSegmentFn(ctx, idx, in)
	}
	w := newECObjectWriter(c.b.currentSelfAddr(), c.b.shardSvc, c.b.currentPeerHealth())
	return w.writeOneSegment(ctx, in)
}

func (c *clusterSegmentBackend) deleteShards(ctx context.Context, peer, bucket, shardKey string) error {
	if c.deleteShardsFn != nil {
		return c.deleteShardsFn(ctx, peer, bucket, shardKey)
	}
	if c.b.shardSvc == nil {
		return fmt.Errorf("shard service not wired")
	}
	return c.b.shardSvc.DeleteShards(ctx, peer, bucket, shardKey)
}

func chunkedMultipartCompleteChunkSize(defaultSize int) int {
	if defaultSize <= 0 || defaultSize > defaultChunkedMultipartCompleteSize {
		return defaultChunkedMultipartCompleteSize
	}
	return defaultSize
}

func acquireChunkedMultipartCompleteSlot(ctx context.Context) (func(), error) {
	select {
	case chunkedMultipartCompleteSlots <- struct{}{}:
		return func() { <-chunkedMultipartCompleteSlots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// putObjectChunked is the chunked-PUT cluster pipeline: split the spooled
// body into N segments via storage.SegmentWriter, fan out per-segment EC
// writes across placement groups (best-effort, defer cleanup on any error),
// then commit a single PutObjectMetaCmd carrying all per-segment placements.
// The single raft commit is the atomic point.
func (b *DistributedBackend) putObjectChunked(
	ctx context.Context,
	bucket, key, versionID string,
	sp *spooledObject,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	acl uint8,
	modTime int64,
	preserveModTime bool,
	expectedETag string,
	beforeCommit func() error,
	parts []storage.MultipartPartEntry,
	tags []storage.Tag,
) (*storage.Object, error) {
	// Pre-allocate blobIDs + placements sized to exact segment count. A 0-byte
	// object still gets one (empty) segment so every simple PUT — including
	// empty objects — takes this single chunked path (mirrors the multipart
	// variant + SegmentWriter, which always emits one segment for empty input).
	chunkSize := int64(b.effectiveChunkedPutChunkSize())
	numSegments := int((sp.Size + chunkSize - 1) / chunkSize)
	if numSegments < 1 {
		numSegments = 1
	}
	blobIDs := make([]string, numSegments)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := &clusterSegmentBackend{
		b:            b,
		bucket:       bucket,
		key:          key,
		versionID:    versionID,
		blobIDs:      blobIDs,
		contentType:  contentType,
		userMetadata: userMetadata,
		sseAlgorithm: sseAlgorithm,
		acl:          acl,
		placements:   make([]segmentPlacement, numSegments),
		chunkSize:    int(chunkSize),
	}
	body, err := sp.Open()
	if err != nil {
		return nil, fmt.Errorf("open spool: %w", err)
	}
	defer body.Close()
	return runChunkedPut(ctx, csb, body, bucket, key, versionID, contentType, userMetadata, sseAlgorithm, modTime, preserveModTime, expectedETag, beforeCommit, parts, tags)
}

func (b *DistributedBackend) putMultipartObjectChunked(
	ctx context.Context,
	bucket, key, versionID, uploadID string,
	manifest multipartCompleteManifest,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	modTime int64,
	preserveModTime bool,
	expectedETag string,
	beforeCommit func() error,
	tags []storage.Tag,
) (*storage.Object, error) {
	chunkSize := int64(chunkedMultipartCompleteChunkSize(b.effectiveChunkedPutChunkSize()))
	numSegments := int((manifest.TotalSize + chunkSize - 1) / chunkSize)
	if numSegments < 1 {
		numSegments = 1
	}
	blobIDs := make([]string, numSegments)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := &clusterSegmentBackend{
		b:            b,
		bucket:       bucket,
		key:          key,
		versionID:    versionID,
		blobIDs:      blobIDs,
		contentType:  contentType,
		userMetadata: userMetadata,
		sseAlgorithm: sseAlgorithm,
		placements:   make([]segmentPlacement, numSegments),
		chunkSize:    int(chunkSize),
	}
	body, err := manifest.Open()
	if err != nil {
		return nil, fmt.Errorf("open multipart manifest: %w", err)
	}
	defer body.Close()
	return runChunkedPutWithParts(ctx, csb, body, bucket, key, versionID, contentType,
		userMetadata, sseAlgorithm, modTime, preserveModTime, expectedETag, beforeCommit, manifest.Parts, tags, uploadID)
}

func (b *DistributedBackend) effectiveChunkedPutChunkSize() int {
	if b != nil && b.chunkedPutChunkSize > 0 {
		return b.chunkedPutChunkSize
	}
	return storage.DefaultChunkSize
}

// runChunkedPut is the test-injectable core of putObjectChunked. The caller
// supplies a fully wired clusterSegmentBackend and the already-opened body
// reader (production: from putObjectChunked which opens sp.Open(); tests:
// arbitrary readers — including ones that error mid-stream — wired directly).
func runChunkedPut(
	ctx context.Context,
	csb *clusterSegmentBackend,
	body io.Reader,
	bucket, key, versionID, contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	modTime int64,
	preserveModTime bool,
	expectedETag string,
	beforeCommit func() error,
	parts []storage.MultipartPartEntry,
	tags []storage.Tag,
) (*storage.Object, error) {
	return runChunkedPutWithParts(ctx, csb, body, bucket, key, versionID, contentType,
		userMetadata, sseAlgorithm, modTime, preserveModTime, expectedETag, beforeCommit, parts, tags, "")
}

func runChunkedPutWithParts(
	ctx context.Context,
	csb *clusterSegmentBackend,
	body io.Reader,
	bucket, key, versionID, contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	modTime int64,
	preserveModTime bool,
	expectedETag string,
	beforeCommit func() error,
	parts []storage.MultipartPartEntry,
	tags []storage.Tag,
	completeUploadID string,
) (*storage.Object, error) {

	// Best-effort blob cleanup on any error path before the commit.
	// SegmentWriter.Write joins all workers before returning, so by the time
	// defer runs csb.placements is settled — no race. M3: the multipart-complete
	// commit (per-version blob or latest-only quorum-meta) is synchronous and
	// FAIL-CLOSED, so a commit error means nothing is durable — the segment shards
	// are eager-cleaned here, same as the non-multipart chunked PUT. There is no
	// raft propose and therefore no phantom-commit window to preserve shards for.
	var committed bool
	defer func() {
		if committed {
			return
		}
		for _, p := range csb.placements {
			if p.BlobID == "" {
				continue
			}
			shardKey := key + "/segments/" + p.BlobID
			for _, node := range p.NodeIDs {
				_ = csb.deleteShards(context.Background(), node, bucket, shardKey)
			}
		}
	}()

	// 3. Stream the body through Phase 1 SegmentWriter.
	sw := storage.NewSegmentWriter(csb)
	if csb.chunkSize > 0 {
		sw = storage.NewSegmentWriterWithChunkSize(csb, csb.chunkSize)
	}
	if completeUploadID != "" {
		release, err := acquireChunkedMultipartCompleteSlot(ctx)
		if err != nil {
			return nil, err
		}
		defer release()
		sw = storage.NewSegmentWriterWithChunkSizeAndWorkers(csb, csb.chunkSize, 1)
	}
	obj, err := sw.Write(ctx, bucket, key, contentType, body)
	if err != nil {
		return nil, fmt.Errorf("segment write: %w", err)
	}
	obj.UserMetadata = userMetadata
	obj.SSEAlgorithm = sseAlgorithm
	obj.IsAppendable = false
	partsMeta := cloneMultipartPartEntries(parts)
	obj.Parts = partsMeta

	// 4. beforeCommit hook before raft commit.
	if beforeCommit != nil {
		if cerr := beforeCommit(); cerr != nil {
			return nil, cerr
		}
	}

	// 5. Build PutObjectMetaCmd. csb.placements is already SegmentIdx-indexed.
	if len(csb.placements) == 0 || csb.placements[0].BlobID == "" {
		return nil, fmt.Errorf("putObjectChunked: placement[0] not recorded")
	}
	commitModTime := chunkedChooseModTime(modTime, preserveModTime, time.Now().Unix())
	obj.LastModified = commitModTime
	obj.VersionID = versionID
	segments := buildSegmentMetaEntries(csb.placements, obj.Segments)

	// 6. Commit. M3: the multipart complete is raft-free — there is no
	// CmdCompleteMultipart propose. The completed object's quorum-meta blob IS the
	// durable commit, FAIL-CLOSED: a write failure leaves nothing committed, so the
	// segment shards are eager-cleaned by the defer (committed stays false). Same-vid
	// convergence across concurrent completers / idempotent retry is provided by the
	// deterministic VersionID + the det-vid existence short-circuit in
	// CompleteMultipartUpload — NOT a done-marker.
	var commitErr error
	if completeUploadID != "" {
		// Vid-deterministic recorded blob placement (same-vid convergence): the
		// recorded NodeIDs must be a function of (bucket,key,det-vid), NOT segment-0's
		// random blobID, so concurrent completers of the same upload record the same
		// per-version blob WRITE target and hard-delete / tombstone-GC target. Segment
		// data shards keep their own random-blobID placement (csb.placements).
		blobNodeIDs, bnerr := csb.vidDeterministicBlobNodeIDs(bucket, key, versionID, csb.currentECConfig())
		if bnerr != nil {
			return nil, bnerr
		}
		metaCmd := PutObjectMetaCmd{
			Bucket:           bucket,
			Key:              key,
			Size:             obj.Size,
			ETag:             obj.ETag,
			VersionID:        versionID,
			ContentType:      contentType,
			ModTime:          commitModTime,
			UserMetadata:     userMetadata,
			SSEAlgorithm:     sseAlgorithm,
			ACL:              csb.acl,
			Parts:            partsMeta,
			NodeIDs:          blobNodeIDs,
			ECData:           uint8(csb.placements[0].Config.DataShards),
			ECParity:         uint8(csb.placements[0].Config.ParityShards),
			PlacementGroupID: csb.placements[0].PlacementGroupID,
			Segments:         segments,
			Tags:             tags,
		}
		// Versioning-enabled buckets get an encoded per-version blob; non-versioned /
		// Suspended get nil and commit via the latest-only quorum-meta write below.
		metaBlob, mberr := csb.buildMultipartMetaBlob(ctx, metaCmd)
		if mberr != nil {
			return nil, mberr
		}
		if len(metaBlob) > 0 {
			// VERSIONED: the per-version quorum-meta blob is the sole authority,
			// written FAIL-CLOSED. On failure nothing is committed; the segment shards
			// are cleaned by the defer and the client retries CompleteMultipartUpload.
			winCmd, werr := csb.writeCompletedMultipartBlob(ctx, metaBlob)
			if werr != nil {
				return nil, werr
			}
			committed = true
			metrics.ChunkFanoutBreadth.Observe(float64(countDistinctPlacementGroups(csb.placements)))
			winObj, _ := objectAndPlacementFromCmd(winCmd)
			return winObj, nil
		}
		// NON-VERSIONED / Suspended: the latest-only quorum-meta blob is the sole
		// authority, written FAIL-CLOSED (M3 F7) — mirrors the regular non-versioned
		// PUT. On failure nothing is committed; the cleanup defer reclaims the shards.
		commitErr = csb.writeQuorumMeta(ctx, metaCmd)
	} else {
		// Phase 3: commit via per-node quorum meta write (bypasses data_raft).
		commitErr = csb.writeQuorumMeta(ctx, PutObjectMetaCmd{
			Bucket:           bucket,
			Key:              key,
			Size:             obj.Size,
			ETag:             obj.ETag,
			VersionID:        versionID,
			ContentType:      contentType,
			ModTime:          commitModTime,
			UserMetadata:     userMetadata,
			SSEAlgorithm:     sseAlgorithm,
			ACL:              csb.acl,
			ExpectedETag:     expectedETag,
			IsDeleteMarker:   false,
			Parts:            partsMeta,
			NodeIDs:          csb.placements[0].NodeIDs,
			ECData:           uint8(csb.placements[0].Config.DataShards),
			ECParity:         uint8(csb.placements[0].Config.ParityShards),
			PlacementGroupID: csb.placements[0].PlacementGroupID,
			Segments:         segments,
			Tags:             tags,
		})
	}
	if commitErr != nil {
		return nil, fmt.Errorf("commit meta: %w", commitErr)
	}
	metrics.ChunkFanoutBreadth.Observe(float64(countDistinctPlacementGroups(csb.placements)))
	committed = true
	// Symmetric with commitECObjectWriteResult: the returned *storage.Object
	// must carry Tags even though PutObjectMetaCmd above already persists them.
	// Defensive copy because callers may outlive the cmd's tags slice.
	obj.Tags = append([]storage.Tag(nil), tags...)
	obj.Parts = append([]storage.MultipartPartEntry(nil), parts...)
	return obj, nil
}

func (c *clusterSegmentBackend) writeQuorumMeta(ctx context.Context, cmd PutObjectMetaCmd) error {
	if c.writeQuorumMetaFn != nil {
		return c.writeQuorumMetaFn(ctx, cmd)
	}
	return c.b.writeQuorumMeta(ctx, cmd)
}

// buildMultipartMetaBlob delegates to the DistributedBackend. A nil backend
// (test seam wiring without a real DistributedBackend) means the legacy
// non-versioned path → no meta_blob.
func (c *clusterSegmentBackend) buildMultipartMetaBlob(ctx context.Context, cmd PutObjectMetaCmd) ([]byte, error) {
	if c.b == nil {
		return nil, nil
	}
	return c.b.buildMultipartMetaBlob(ctx, cmd)
}

// writeCompletedMultipartBlob delegates to the DistributedBackend. Only reached on
// the blob-authoritative path (meta_blob present), which a nil backend never takes.
func (c *clusterSegmentBackend) writeCompletedMultipartBlob(ctx context.Context, metaBlob []byte) (PutObjectMetaCmd, error) {
	if c.b == nil {
		return PutObjectMetaCmd{}, fmt.Errorf("multipart complete: no backend for per-version blob write")
	}
	return c.b.writeCompletedMultipartBlob(ctx, metaBlob)
}

// chunkedChooseModTime returns ModTime according to the preserveModTime
// flag. When preserveModTime is true the caller-supplied modTime is kept
// verbatim (used by snapshot restore); otherwise `now` is stamped.
func chunkedChooseModTime(modTime int64, preserveModTime bool, now int64) int64 {
	if preserveModTime {
		return modTime
	}
	return now
}

func countDistinctPlacementGroups(placements []segmentPlacement) int {
	seen := make(map[string]struct{}, len(placements))
	for _, p := range placements {
		if p.PlacementGroupID == "" {
			continue
		}
		seen[p.PlacementGroupID] = struct{}{}
	}
	return len(seen)
}

func cloneMultipartPartEntries(parts []storage.MultipartPartEntry) []storage.MultipartPartEntry {
	if len(parts) == 0 {
		return nil
	}
	out := make([]storage.MultipartPartEntry, len(parts))
	copy(out, parts)
	return out
}

// buildSegmentMetaEntries joins post-write placement metadata with the
// plaintext SegmentRefs produced by SegmentWriter. Both inputs are indexed
// by SegmentIdx, so out[i] holds segment i's full record.
func buildSegmentMetaEntries(placements []segmentPlacement, refs []storage.SegmentRef) []SegmentMetaEntry {
	out := make([]SegmentMetaEntry, len(placements))
	for i, p := range placements {
		var (
			size     int64
			checksum []byte
		)
		if i < len(refs) {
			size = refs[i].Size
			checksum = refs[i].Checksum
		}
		out[i] = SegmentMetaEntry{
			BlobID:           p.BlobID,
			Size:             size,
			Checksum:         checksum,
			PlacementGroupID: p.PlacementGroupID,
			ShardSize:        p.ShardSize,
			SegmentIdx:       int32(p.SegmentIdx),
			NodeIDs:          p.NodeIDs,
			ECData:           uint8(p.Config.DataShards),
			ECParity:         uint8(p.Config.ParityShards),
		}
	}
	return out
}
