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
	placements   []segmentPlacement // indexed by segmentIdx; pre-allocated; mutex-free

	// Test seams. Production constructor leaves these nil; defaults route
	// through b.shardGroup / newECObjectWriter / b.shardSvc.DeleteShards /
	// b.propose. Tests inject all four to exercise putObjectChunked without
	// a full RaftNode + ShardService.
	writeSegmentFn  func(ctx context.Context, idx int, in writeSegmentInput) (PlacementRecord, string, string, error)
	groupSelectorFn func(bucket, key string, idx int, blobID string) (ShardGroupEntry, error)
	deleteShardsFn  func(ctx context.Context, peer, bucket, shardKey string) error
	proposeFn       func(ctx context.Context, cmdType CommandType, payload any) error
	ecConfigFn      func() ECConfig
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
	RingVersion      RingVersion
	ShardSize        int32
}

// WriteSegment buffers the chunk, picks a PG for this (bucket,key,idx,blobID),
// EC-writes via writeOneSegment, and records the placement in the pre-allocated
// slot. Returns a storage.SegmentRef carrying the xxhash3-128 plaintext
// checksum.
func (c *clusterSegmentBackend) WriteSegment(ctx context.Context, bucket, key string, idx int, r io.Reader) (storage.SegmentRef, error) {
	if idx < 0 || idx >= len(c.blobIDs) {
		return storage.SegmentRef{}, fmt.Errorf("segment %d: out of range (allocated %d)", idx, len(c.blobIDs))
	}

	// 1. Buffer chunk to []byte. SegmentWriter caps each chunk at
	// storage.DefaultChunkSize (16 MiB); 8 workers × 16 MiB ≈ 128 MiB peak.
	data, err := io.ReadAll(r)
	if err != nil {
		return storage.SegmentRef{}, fmt.Errorf("segment %d: read chunk: %w", idx, err)
	}

	// 2. Pick PG for this segment.
	group, err := c.selectGroup(bucket, key, idx, c.blobIDs[idx])
	if err != nil {
		return storage.SegmentRef{}, fmt.Errorf("segment %d: pick PG: %w", idx, err)
	}

	// 3. Delegate to writeOneSegment.
	cfg := c.currentECConfig()
	in := writeSegmentInput{
		Bucket:        bucket,
		Key:           key,
		VersionID:     c.versionID,
		SegmentBlobID: c.blobIDs[idx],
		SegmentIdx:    idx,
		Group:         group,
		Cfg:           cfg,
		Data:          data,
	}
	rec, _, blobID, werr := c.writeOne(ctx, idx, in)
	if werr != nil {
		return storage.SegmentRef{}, fmt.Errorf("segment %d: EC write: %w", idx, werr)
	}

	// 4. Record placement. Workers receive unique idx, so the slot write
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

	// 5. xxhash3-128 over plaintext.
	sum := storage.ChecksumOf(data)
	return storage.SegmentRef{
		BlobID:           blobID,
		Size:             int64(len(data)),
		Checksum:         sum,
		PlacementGroupID: group.ID,
		ShardSize:        shardSize,
	}, nil
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

// chunkedPathThresholdMet reports whether a spooled object with the given
// size is large enough to justify the chunked-PUT pipeline. Objects at or
// below the threshold stay on the legacy single-segment EC path; strictly
// larger objects route through putObjectChunked. Extracted from the call
// site so the outer routing decision in putObjectECSpooledWithOptionalModTime
// is unit-testable without standing up a full DistributedBackend fixture.
func chunkedPathThresholdMet(spSize int64) bool {
	return spSize > int64(storage.DefaultChunkSize)
}

// putObjectChunked is the chunked-PUT cluster pipeline: split the spooled
// body into N segments via storage.SegmentWriter, fan out per-segment EC
// writes across placement groups (best-effort, defer cleanup on any error),
// then commit a single PutObjectMetaCmd carrying all per-segment placements.
// The single raft commit is the atomic point.
//
// The old spooled chunked entry point rejects multipart parts; multipart
// callers must use the zero-spool Complete path.
func (b *DistributedBackend) putObjectChunked(
	ctx context.Context,
	bucket, key, versionID string,
	sp *spooledObject,
	contentType string,
	userMetadata map[string]string,
	sseAlgorithm string,
	modTime int64,
	preserveModTime bool,
	expectedETag string,
	beforeCommit func() error,
	parts []storage.MultipartPartEntry,
	tags []storage.Tag,
) (*storage.Object, error) {
	if parts != nil {
		return nil, fmt.Errorf("chunked PUT with multipart parts: deferred to zero-spool Complete path")
	}

	// Pre-allocate blobIDs + placements sized to exact segment count.
	chunkSize := int64(storage.DefaultChunkSize)
	numSegments := int((sp.Size + chunkSize - 1) / chunkSize)
	if numSegments < 1 {
		return nil, fmt.Errorf("putObjectChunked: sp.Size=%d below chunk threshold; caller should not have routed here", sp.Size)
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
	}
	body, err := sp.Open()
	if err != nil {
		return nil, fmt.Errorf("open spool: %w", err)
	}
	defer body.Close()
	return runChunkedPut(ctx, csb, body, bucket, key, versionID, contentType, userMetadata, sseAlgorithm, modTime, preserveModTime, expectedETag, beforeCommit, tags)
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
	chunkSize := int64(storage.DefaultChunkSize)
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
	}
	body, err := manifest.Open()
	if err != nil {
		return nil, fmt.Errorf("open multipart manifest: %w", err)
	}
	defer body.Close()
	return runChunkedPutWithParts(ctx, csb, body, bucket, key, versionID, contentType,
		userMetadata, sseAlgorithm, modTime, preserveModTime, expectedETag, beforeCommit, manifest.Parts, tags, uploadID)
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
	tags []storage.Tag,
) (*storage.Object, error) {
	return runChunkedPutWithParts(ctx, csb, body, bucket, key, versionID, contentType,
		userMetadata, sseAlgorithm, modTime, preserveModTime, expectedETag, beforeCommit, nil, tags, "")
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

	// Best-effort blob cleanup on any error path before raft commit.
	// SegmentWriter.Write joins all workers before returning, so by the time
	// defer runs csb.placements is settled — no race.
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

	// 6. Single atomic raft commit.
	var commitErr error
	if completeUploadID != "" {
		commitErr = csb.propose(ctx, CmdCompleteMultipart, CompleteMultipartCmd{
			Bucket:           bucket,
			Key:              key,
			UploadID:         completeUploadID,
			Size:             obj.Size,
			ETag:             obj.ETag,
			VersionID:        versionID,
			ContentType:      contentType,
			ModTime:          commitModTime,
			Parts:            partsMeta,
			NodeIDs:          csb.placements[0].NodeIDs,
			ECData:           uint8(csb.placements[0].Config.DataShards),
			ECParity:         uint8(csb.placements[0].Config.ParityShards),
			PlacementGroupID: csb.placements[0].PlacementGroupID,
			RingVersion:      csb.placements[0].RingVersion,
			Segments:         segments,
			Tags:             tags,
		})
	} else {
		commitErr = csb.propose(ctx, CmdPutObjectMeta, PutObjectMetaCmd{
			Bucket:           bucket,
			Key:              key,
			Size:             obj.Size,
			ETag:             obj.ETag,
			VersionID:        versionID,
			ContentType:      contentType,
			ModTime:          commitModTime,
			UserMetadata:     userMetadata,
			SSEAlgorithm:     sseAlgorithm,
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
	return obj, nil
}

func (c *clusterSegmentBackend) propose(ctx context.Context, cmdType CommandType, payload any) error {
	if c.proposeFn != nil {
		return c.proposeFn(ctx, cmdType, payload)
	}
	return c.b.propose(ctx, cmdType, payload)
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
			RingVersion:      p.RingVersion,
		}
	}
	return out
}
