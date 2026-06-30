package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/hrw"
	"github.com/gritive/GrainFS/internal/storage"
)

// newTestClusterSegmentBackend builds a minimal clusterSegmentBackend suitable
// for WriteSegmentBytes unit tests: one pre-allocated blob ID, fake seams for
// group selection and shard writes, no real DistributedBackend required.
func newTestClusterSegmentBackend(t *testing.T) *clusterSegmentBackend {
	t.Helper()
	deps := newFakeBackendWithGroups(fourPGFixture())
	blobIDs := []string{uuid.Must(uuid.NewV7()).String()}
	csb := newCSBWithDeps(deps, blobIDs)
	csb.bucket = "bkt"
	csb.key = "key"
	return csb
}

func TestWriteSegmentBytes_CompressibleSetsStoredSize(t *testing.T) {
	c := newTestClusterSegmentBackend(t)
	plaintext := bytes.Repeat([]byte("grainfs zstd segment payload "), 8192) // ~232 KiB, compressible
	ref, err := c.WriteSegmentBytes(context.Background(), "bkt", "key", 0, append([]byte(nil), plaintext...))
	if err != nil {
		t.Fatalf("WriteSegmentBytes: %v", err)
	}
	if ref.Size != int64(len(plaintext)) {
		t.Fatalf("Size must stay plaintext: got %d want %d", ref.Size, int64(len(plaintext)))
	}
	if ref.StoredSize <= 0 || ref.StoredSize >= ref.Size {
		t.Fatalf("StoredSize should be compressed (>0, <Size): got %d", ref.StoredSize)
	}
	if want := storage.ChecksumOf(plaintext); !bytes.Equal(ref.Checksum, want) {
		t.Fatalf("Checksum must be over plaintext")
	}
}

func TestWriteSegmentBytes_IncompressibleStoresRaw(t *testing.T) {
	c := newTestClusterSegmentBackend(t)
	rnd := make([]byte, 256<<10)
	// Fixed-seed pseudo-random bytes approximate maximum entropy: the Go PRNG
	// output has no repeating byte patterns, so zstd finds no back-references
	// and cannot compress it below the input size (i.e. compressing would
	// expand). WriteSegmentBytes must detect this and store raw (StoredSize==0).
	r := rand.New(rand.NewSource(0xdeadbeef)) //nolint:gosec // fixed-seed PRNG, deterministic test data
	_, _ = r.Read(rnd)
	ref, err := c.WriteSegmentBytes(context.Background(), "bkt", "key", 0, append([]byte(nil), rnd...))
	if err != nil {
		t.Fatalf("WriteSegmentBytes: %v", err)
	}
	if ref.StoredSize != 0 {
		t.Fatalf("incompressible segment must store raw (StoredSize==0): got %d", ref.StoredSize)
	}
	if ref.Size != int64(len(rnd)) {
		t.Fatalf("Size mismatch")
	}
}

// TestAdaptiveChunkSize locks the size-aware chunk policy derived from the GCP
// size×chunk sweep: small objects stay single-segment (split overhead dominates,
// e.g. 1MiB PUT dropped 288→157 MB/s when split), mid-size objects split into ~2
// (10MiB: +13.7% at 2 segments), and large objects cap at DefaultChunkSize so
// they split naturally without per-segment overhead blowup.
func TestAdaptiveChunkSize(t *testing.T) {
	cases := []struct {
		name         string
		size         int64
		wantSegments int
	}{
		{"1MiB stays single (split overhead dominates)", 1 << 20, 1},
		{"9MiB single (below split floor)", 9 << 20, 1},
		{"10MiB splits to 2 (sweet spot)", 10 << 20, 2},
		{"20MiB two segments", 20 << 20, 2},
		{"64MiB four segments at 16MiB cap", 64 << 20, 4},
		{"unknown size: single chunk", 0, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			chunk := adaptiveChunkSize(c.size)
			require.Greater(t, chunk, int64(0))
			require.LessOrEqual(t, chunk, int64(storage.DefaultChunkSize))
			segs := 1
			if c.size > 0 {
				segs = int((c.size + chunk - 1) / chunk)
			}
			require.Equal(t, c.wantSegments, segs)
		})
	}
}

// fakeSegmentBackendDeps wires the four test seams on clusterSegmentBackend.
// Each closure records calls so individual tests can assert call counts,
// argument equivalence, and ordering.
type fakeSegmentBackendDeps struct {
	mu sync.Mutex

	// Group selector — defaults to a deterministic round-robin across
	// groups. Override per test for specific assertions.
	groups []ShardGroupEntry

	writeCalls  int32
	writeErrAt  map[int]error          // segment idx → error to return
	writeRecord []ShardGroupEntry      // recorded chosen group per write call
	writePeer   func(idx int) []string // override peer list per call
	writeMu     sync.Mutex

	deleteCalls []deleteCall

	deleteShardErr       error
	writeQuorumMetaCalls []PutObjectMetaCmd
	writeQuorumMetaErr   error
}

type deleteCall struct {
	peer, bucket, shardKey string
}

func (f *fakeSegmentBackendDeps) groupSelector(bucket, key string, idx int, blobID string) (ShardGroupEntry, error) {
	if len(f.groups) == 0 {
		return ShardGroupEntry{}, fmt.Errorf("no groups configured")
	}
	g := f.groups[idx%len(f.groups)]
	return g, nil
}

func (f *fakeSegmentBackendDeps) writeSegment(ctx context.Context, idx int, in writeSegmentInput) (PlacementRecord, string, string, error) {
	atomic.AddInt32(&f.writeCalls, 1)
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	if err, ok := f.writeErrAt[idx]; ok {
		return PlacementRecord{}, "", "", err
	}
	f.writeRecord = append(f.writeRecord, in.Group)
	var peers []string
	if f.writePeer != nil {
		peers = f.writePeer(idx)
	} else {
		// Mirror the production writeOneSegment placement: weighted HRW over the
		// chosen group's peers, keyed by the segment-scoped shardKey. This keeps
		// the recorded NodeIDs HRW-derived so commit-path tests can assert it.
		placementKey := ecObjectSegmentShardKey(ecObjectWritePlan{
			Key:           in.Key,
			VersionID:     in.VersionID,
			SegmentBlobID: in.SegmentBlobID,
		})
		peers = selectShardPlacement(in.Cfg, in.Group.PeerIDs, placementKey, in.Weights, in.WeightedEnabled)
	}
	rec := PlacementRecord{Nodes: peers, K: in.Cfg.DataShards, M: in.Cfg.ParityShards}
	return rec, "etag-seg-" + fmt.Sprint(idx), in.SegmentBlobID, nil
}

func (f *fakeSegmentBackendDeps) deleteShards(ctx context.Context, peer, bucket, shardKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, deleteCall{peer: peer, bucket: bucket, shardKey: shardKey})
	return f.deleteShardErr
}

func (f *fakeSegmentBackendDeps) writeQuorumMeta(ctx context.Context, cmd PutObjectMetaCmd) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeQuorumMetaCalls = append(f.writeQuorumMetaCalls, cmd)
	return f.writeQuorumMetaErr
}

func newFakeBackendWithGroups(groups []ShardGroupEntry) *fakeSegmentBackendDeps {
	return &fakeSegmentBackendDeps{groups: groups, writeErrAt: map[int]error{}}
}

func newCSBWithDeps(deps *fakeSegmentBackendDeps, blobIDs []string) *clusterSegmentBackend {
	return &clusterSegmentBackend{
		bucket:            "test-bucket",
		key:               "large.bin",
		versionID:         "v1",
		blobIDs:           blobIDs,
		placements:        make([]segmentPlacement, len(blobIDs)),
		writeSegmentFn:    deps.writeSegment,
		groupSelectorFn:   deps.groupSelector,
		deleteShardsFn:    deps.deleteShards,
		writeQuorumMetaFn: deps.writeQuorumMeta,
		ecConfigFn:        func() ECConfig { return ECConfig{DataShards: 4, ParityShards: 2} },
		sizeHint:          -1, // no hint by default; tests that exercise hinting set it explicitly
	}
}

func newCSBWithTestChunks(deps *fakeSegmentBackendDeps, blobIDs []string) *clusterSegmentBackend {
	csb := newCSBWithDeps(deps, blobIDs)
	csb.chunkSize = testChunkedPutChunkSize
	return csb
}

// fourPGFixture returns 4 placement groups, each with K+M=6 peers.
func fourPGFixture() []ShardGroupEntry {
	groups := make([]ShardGroupEntry, 4)
	for g := 0; g < 4; g++ {
		peers := make([]string, 6)
		for p := 0; p < 6; p++ {
			peers[p] = fmt.Sprintf("g%d-n%d", g, p)
		}
		groups[g] = ShardGroupEntry{ID: fmt.Sprintf("group-%d", g+1), PeerIDs: peers}
	}
	return groups
}

func TestClusterSegmentBackend_WriteSegment_FanOutAcrossPGs(t *testing.T) {
	deps := newFakeBackendWithGroups(fourPGFixture())
	blobIDs := make([]string, 4)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithDeps(deps, blobIDs)

	for i := 0; i < 4; i++ {
		_, err := csb.WriteSegment(context.Background(), csb.bucket, csb.key, i, bytes.NewReader([]byte("segment data")))
		require.NoError(t, err)
	}

	// With a round-robin fake selector across 4 distinct groups, every
	// segment maps to a unique PG (4 distinct).
	seen := map[string]struct{}{}
	for _, p := range csb.placements {
		require.NotEmpty(t, p.PlacementGroupID)
		seen[p.PlacementGroupID] = struct{}{}
	}
	assert.GreaterOrEqual(t, len(seen), 2, "expected fan-out across at least 2 PGs, got %v", seen)
}

func TestClusterSegmentBackend_WriteSegment_RecordsPlacement(t *testing.T) {
	deps := newFakeBackendWithGroups(fourPGFixture())
	blobIDs := make([]string, 3)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithDeps(deps, blobIDs)

	for i := 0; i < 3; i++ {
		ref, err := csb.WriteSegment(context.Background(), csb.bucket, csb.key, i, bytes.NewReader([]byte(fmt.Sprintf("data-%d", i))))
		require.NoError(t, err)
		assert.Equal(t, blobIDs[i], ref.BlobID, "ref.BlobID mirrors pre-allocated id at idx %d", i)
		assert.NotEmpty(t, ref.Checksum, "xxhash3-128 checksum populated")
		assert.Len(t, ref.Checksum, 16)
	}

	require.Len(t, csb.placements, 3)
	for i, p := range csb.placements {
		assert.Equal(t, blobIDs[i], p.BlobID, "placement[%d].BlobID", i)
		assert.Equal(t, i, p.SegmentIdx, "placement[%d].SegmentIdx", i)
		assert.NotEmpty(t, p.NodeIDs, "placement[%d].NodeIDs", i)
	}
}

const testChunkedPutChunkSize = 64 << 10

// makeSpool writes payload to a temp file and returns a *spooledObject.
func makeSpool(t clusterTestTB, payload []byte) *spooledObject {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "spool")
	require.NoError(t, os.WriteFile(path, payload, 0o600))
	return &spooledObject{Path: path, Size: int64(len(payload))}
}

func chunkFanoutMetricCount(t *testing.T) uint64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "grainfs_chunk_fanout_breadth" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if h := m.GetHistogram(); h != nil {
				return h.GetSampleCount()
			}
		}
	}
	return 0
}

func TestPutObjectChunked_SingleAtomicMetaCommit(t *testing.T) {
	chunk := testChunkedPutChunkSize
	payload := bytes.Repeat([]byte("A"), 4*chunk) // exactly 4 segments
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())

	blobIDs := make([]string, 4)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithTestChunks(deps, blobIDs)

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	obj, err := runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, int64(len(payload)), obj.Size)
	assert.Equal(t, "v1", obj.VersionID)
	assert.False(t, obj.IsAppendable)
	assert.Len(t, obj.Segments, 4)

	require.Len(t, deps.writeQuorumMetaCalls, 1, "exactly one quorum meta write")
	cmd := deps.writeQuorumMetaCalls[0]
	require.Len(t, cmd.Segments, 4)
	for i, seg := range cmd.Segments {
		assert.Equal(t, int32(i), seg.SegmentIdx, "Segments[%d].SegmentIdx deterministic", i)
		assert.NotEmpty(t, seg.NodeIDs, "Segments[%d].NodeIDs", i)
		assert.NotEmpty(t, seg.PlacementGroupID, "Segments[%d].PG", i)
		assert.Len(t, seg.Checksum, 16, "Segments[%d].Checksum", i)
	}
	// Top-level mirrors segment 0 for back-compat readers.
	assert.Equal(t, cmd.Segments[0].PlacementGroupID, cmd.PlacementGroupID)
	assert.Equal(t, cmd.Segments[0].NodeIDs, cmd.NodeIDs)
	assert.Equal(t, cmd.Segments[0].ECData, cmd.ECData)
	assert.Equal(t, cmd.Segments[0].ECParity, cmd.ECParity)

	// No cleanup calls on success path.
	assert.Empty(t, deps.deleteCalls)
}

// TestWriteSegment_RecordsHRWPlacement guards the WriteSegmentBytes → record →
// segmentPlacement plumbing: the recorded NodeIDs must be the HRW-derived order
// the writer chose. The fake writeSegment mirrors the production weighted-HRW
// selection (selectShardPlacement) so this pins the recorded-placement wiring;
// the production writeOneSegment HRW swap itself is pinned by
// TestWriteOneSegment_HRWPlacement.
func TestWriteSegment_RecordsHRWPlacement(t *testing.T) {
	groups := fourPGFixture()
	deps := newFakeBackendWithGroups(groups)
	blobIDs := make([]string, 4)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithDeps(deps, blobIDs)
	cfg := csb.currentECConfig()

	for i := 0; i < 4; i++ {
		_, err := csb.WriteSegment(context.Background(), csb.bucket, csb.key, i, bytes.NewReader([]byte("payload")))
		require.NoError(t, err)
	}

	for i, p := range csb.placements {
		group := groups[i%len(groups)] // matches round-robin groupSelector
		placementKey := ecObjectSegmentShardKey(ecObjectWritePlan{
			Key:           csb.key,
			VersionID:     csb.versionID,
			SegmentBlobID: blobIDs[i],
		})
		want := hrw.PlaceShards(placementKey, group.PeerIDs, nil, cfg.NumShards())
		assert.Equalf(t, want, p.NodeIDs, "placement[%d].NodeIDs must be HRW-derived", i)
	}
}

// TestWriteSegment_WeightedHRWPlacement guards that the per-peer weight snapshot
// (from peerWeightsFn → b.nodeStatsStore in production) is threaded into the
// segment placement so the dominant data path is under disk-capacity weighting.
func TestWriteSegment_WeightedHRWPlacement(t *testing.T) {
	groups := fourPGFixture()
	deps := newFakeBackendWithGroups(groups)
	blobIDs := []string{uuid.Must(uuid.NewV7()).String()}
	csb := newCSBWithDeps(deps, blobIDs)
	cfg := csb.currentECConfig()

	// Skew weights so the weighted order differs from the unweighted one.
	weightByPeer := map[string]float64{}
	for _, g := range groups {
		for j, peer := range g.PeerIDs {
			weightByPeer[peer] = float64((j + 1) * 1000)
		}
	}
	csb.peerWeightsFn = func(peers []string) ([]float64, bool) {
		w := make([]float64, len(peers))
		for i, p := range peers {
			w[i] = weightByPeer[p]
		}
		return w, true
	}

	_, err := csb.WriteSegment(context.Background(), csb.bucket, csb.key, 0, bytes.NewReader([]byte("payload")))
	require.NoError(t, err)

	group := groups[0]
	weights := make([]float64, len(group.PeerIDs))
	for i, p := range group.PeerIDs {
		weights[i] = weightByPeer[p]
	}
	placementKey := ecObjectSegmentShardKey(ecObjectWritePlan{
		Key:           csb.key,
		VersionID:     csb.versionID,
		SegmentBlobID: blobIDs[0],
	})
	want := hrw.PlaceShards(placementKey, group.PeerIDs, weights, cfg.NumShards())
	assert.Equal(t, want, csb.placements[0].NodeIDs, "weighted HRW must thread through WriteSegment")
}

func TestRunChunkedPutWithParts_CommitsPartsAndSegments(t *testing.T) {
	chunk := storage.DefaultChunkSize
	payload := bytes.Repeat([]byte("P"), chunk+1)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())
	blobIDs := []string{uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String()}
	csb := newCSBWithDeps(deps, blobIDs)
	parts := []storage.MultipartPartEntry{
		{PartNumber: 1, Size: int64(chunk), ETag: "etag-1"},
		{PartNumber: 2, Size: 1, ETag: "etag-2"},
	}
	wantParts := append([]storage.MultipartPartEntry(nil), parts...)

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	obj, err := runChunkedPutWithParts(context.Background(), csb, body,
		"bucket", "large-mp.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, parts, nil, "upload-1")
	require.NoError(t, err)
	require.NotNil(t, obj)
	require.Equal(t, parts, obj.Parts)

	// M3: the multipart complete is raft-free. The non-versioned (c.b == nil → no
	// meta_blob) commit is a single FAIL-CLOSED latest-only quorum-meta write — no
	// CmdCompleteMultipart propose (proven at the integration level via
	// recordingMultipartRaftNode in multipart_complete_offraft_test.go).
	require.Len(t, deps.writeQuorumMetaCalls, 1, "non-versioned multipart commits via one quorum-meta write")
	cmd := deps.writeQuorumMetaCalls[0]
	require.Len(t, cmd.Segments, 2)
	require.Equal(t, parts, cmd.Parts)

	parts[0].ETag = "mutated"
	require.Equal(t, wantParts, obj.Parts)
	require.Equal(t, wantParts, cmd.Parts)
}

// TestRunChunkedPutWithParts_CommitError_ShardCleanup guards the M3 fail-closed
// commit: the non-versioned (c.b == nil → no meta_blob) multipart complete commits
// via a single synchronous latest-only quorum-meta write. On any commit error
// nothing is durable, so the segment shards are ALWAYS eager-deleted (there is no
// raft propose and therefore no phantom-commit window) and the error surfaces.
//
// Contrast: TestPutObjectChunked_PartialFailRollsBackBlobs_* prove PRE-commit
// failures (segment write, beforeCommit) clean up eagerly too — nothing committed.
//
// Neuter test: drop the `committed = true` after a successful commit (or skip the
// eager-delete defer) and this is RED.
func TestRunChunkedPutWithParts_CommitError_ShardCleanup(t *testing.T) {
	chunk := storage.DefaultChunkSize
	payload := bytes.Repeat([]byte("Q"), chunk+1)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())
	deps.writeQuorumMetaErr = errors.New("quorum-meta write failed")
	blobIDs := []string{uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String()}
	csb := newCSBWithDeps(deps, blobIDs)
	parts := []storage.MultipartPartEntry{
		{PartNumber: 1, Size: int64(chunk), ETag: "etag-1"},
		{PartNumber: 2, Size: 1, ETag: "etag-2"},
	}

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPutWithParts(context.Background(), csb, body,
		"bucket", "large-mp.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, parts, nil, "upload-1")
	require.Error(t, err, "commit error must surface to the caller")
	require.NotEmpty(t, deps.deleteCalls,
		"a fail-closed commit error must eager-delete the un-committed segment shards")
}

func TestPutObjectChunked_ObservesChunkFanoutBreadth(t *testing.T) {
	chunk := testChunkedPutChunkSize
	payload := bytes.Repeat([]byte("M"), 4*chunk) // exactly 4 segments
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())

	blobIDs := make([]string, 4)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithTestChunks(deps, blobIDs)

	before := chunkFanoutMetricCount(t)
	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil, nil)
	require.NoError(t, err)

	after := chunkFanoutMetricCount(t)
	require.Greater(t, after, before, "chunked PUT must observe fan-out breadth")
}

func TestPutObjectChunked_PartialFailRollsBackBlobs_WorkerError(t *testing.T) {
	chunk := testChunkedPutChunkSize
	payload := bytes.Repeat([]byte("B"), 3*chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())
	deps.writeErrAt[2] = errors.New("EC write failed at segment 2")

	blobIDs := make([]string, 3)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithTestChunks(deps, blobIDs)

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "segment write")

	// No commit (pre-commit failure).
	assert.Empty(t, deps.writeQuorumMetaCalls)

	// Cleanup called for the two segments that DID complete (0 and 1).
	// Each segment has 6 placement peers in the 4+2 fixture, so 2 × 6 = 12
	// delete calls.
	cleanedBlobs := map[string]struct{}{}
	for _, dc := range deps.deleteCalls {
		assert.Equal(t, "bucket", dc.bucket)
		cleanedBlobs[dc.shardKey] = struct{}{}
	}
	// Segment 2 never wrote, so its blob_id should NOT appear in cleanup.
	assert.NotEmpty(t, cleanedBlobs, "at least one segment's shards cleaned up")
	for blob := range cleanedBlobs {
		assert.NotContains(t, blob, blobIDs[2], "segment 2 (failed) should not appear in cleanup keys")
	}
}

func TestPutObjectChunked_PartialFailRollsBackBlobs_BeforeCommitError(t *testing.T) {
	// beforeCommit hook failure exercises the same defer-cleanup path as
	// the chunker-error case: all completed segments must be torn down
	// before returning, and no raft commit must happen.
	chunk := testChunkedPutChunkSize
	payload := bytes.Repeat([]byte("F"), 2*chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())

	blobIDs := make([]string, 2)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithTestChunks(deps, blobIDs)

	hookErr := errors.New("beforeCommit failed (e.g. quota check)")
	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "",
		func() error { return hookErr }, nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, hookErr)

	assert.Empty(t, deps.writeQuorumMetaCalls, "beforeCommit failure must not commit")
	cleanedBlobs := map[string]struct{}{}
	for _, dc := range deps.deleteCalls {
		cleanedBlobs[dc.shardKey] = struct{}{}
	}
	for _, id := range blobIDs {
		assert.Contains(t, cleanedBlobs, "large.bin/segments/"+id, "all completed blobs cleaned up")
	}
}

func TestPutObjectChunked_PartialFailRollsBackBlobs_ProposeError(t *testing.T) {
	chunk := testChunkedPutChunkSize
	payload := bytes.Repeat([]byte("D"), 2*chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())
	deps.writeQuorumMetaErr = errors.New("quorum meta write failed")

	blobIDs := make([]string, 2)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithTestChunks(deps, blobIDs)

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit meta")

	// writeQuorumMeta was attempted exactly once (and failed).
	require.Len(t, deps.writeQuorumMetaCalls, 1)

	// Both segments' blobs cleaned up after propose failure (committed=false).
	cleanedBlobs := map[string]struct{}{}
	for _, dc := range deps.deleteCalls {
		cleanedBlobs[dc.shardKey] = struct{}{}
	}
	for _, id := range blobIDs {
		assert.Contains(t, cleanedBlobs, "large.bin/segments/"+id, "all blobs cleaned up after propose failure")
	}
}

func TestPutObjectChunked_PartialFailRollsBackBlobs_DeleteShardsBestEffort(t *testing.T) {
	chunk := testChunkedPutChunkSize
	payload := bytes.Repeat([]byte("E"), 2*chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())
	deps.writeErrAt[1] = errors.New("EC write failed at segment 1")
	deps.deleteShardErr = errors.New("delete failed too")

	blobIDs := make([]string, 2)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithTestChunks(deps, blobIDs)

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil, nil)
	require.Error(t, err)
	// PUT errors cleanly even though cleanup also failed (no panic).
	assert.Empty(t, deps.writeQuorumMetaCalls)
}

func TestPutObjectChunked_CommitsMultipartParts(t *testing.T) {
	chunk := storage.DefaultChunkSize
	payload := bytes.Repeat([]byte("P"), 2*chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())

	blobIDs := make([]string, 2)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithDeps(deps, blobIDs)
	parts := []storage.MultipartPartEntry{{PartNumber: 1, Size: int64(chunk), ETag: "etag-1"}}

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	obj, err := runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, parts, nil)
	require.NoError(t, err)
	require.Equal(t, parts, obj.Parts)

	require.Len(t, deps.writeQuorumMetaCalls, 1)
	cmd := deps.writeQuorumMetaCalls[0]
	require.Equal(t, parts, cmd.Parts)
}

func TestPutObjectChunked_BatchesStagedPromotesByNode(t *testing.T) {
	chunk := testChunkedPutChunkSize
	payload := bytes.Repeat([]byte("S"), 2*chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())
	deps.writePeer = func(int) []string { return []string{"node-a", "node-b"} }

	blobIDs := []string{
		uuid.Must(uuid.NewV7()).String(),
		uuid.Must(uuid.NewV7()).String(),
	}
	csb := newCSBWithTestChunks(deps, blobIDs)
	csb.stagingTxnID = "txn-promote"

	type batchCall struct {
		node  string
		pairs []stagedPromotePair
	}
	var calls []batchCall
	csb.promoteStagedBatchFn = func(_ context.Context, node, _ string, pairs []stagedPromotePair) error {
		calls = append(calls, batchCall{node: node, pairs: append([]stagedPromotePair(nil), pairs...)})
		return nil
	}

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil, nil)
	require.NoError(t, err)

	require.Len(t, calls, 2, "two placement nodes should produce two promote batches, not segment*node calls")
	callsByNode := make(map[string][]stagedPromotePair, len(calls))
	for _, call := range calls {
		callsByNode[call.node] = call.pairs
	}
	require.ElementsMatch(t, []string{"node-a", "node-b"}, []string{calls[0].node, calls[1].node})
	for _, node := range []string{"node-a", "node-b"} {
		pairs := callsByNode[node]
		require.Len(t, pairs, 2, "each node batch should promote both segment blobs")
		for i, pair := range pairs {
			require.Equal(t, segmentStagingShardKey("txn-promote", blobIDs[i]), pair.stagingKey)
			require.Equal(t, "large.bin/segments/"+blobIDs[i], pair.finalKey)
		}
	}
}

func TestPutObjectChunked_EmitsPromoteAndQuorumMetaTrace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", path)
	t.Setenv("GRAINFS_NODE_ID", "node-trace")
	reloadPutTraceSinkForTest()
	t.Cleanup(reloadPutTraceSinkForTest)

	chunk := testChunkedPutChunkSize
	payload := bytes.Repeat([]byte("T"), chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())
	deps.writePeer = func(int) []string { return []string{"node-a", "node-b"} }

	blobIDs := []string{uuid.Must(uuid.NewV7()).String()}
	csb := newCSBWithTestChunks(deps, blobIDs)
	csb.stagingTxnID = "txn-trace"
	csb.promoteStagedBatchFn = func(_ context.Context, _ string, _ string, _ []stagedPromotePair) error {
		return nil
	}

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	ctx := ContextWithPutTrace(context.Background(), PutTraceRequest{
		Bucket:      "bucket",
		Key:         "large.bin",
		Ingress:     PutTraceIngressLocalLeader,
		SizeClass:   PutTraceSizeSmall,
		ForwardMode: PutTraceForwardNone,
	})
	_, err = runChunkedPut(ctx, csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil, nil)
	require.NoError(t, err)

	events := readPutTraceEvents(t, path)
	requirePutTraceStage(t, events, PutTraceStageSegmentWritePrepare)
	requirePutTraceStage(t, events, PutTraceStagePromoteStagedNodeBatch)
	requirePutTraceStage(t, events, PutTraceStagePromoteStagedShards)
	requirePutTraceStage(t, events, PutTraceStageQuorumMetaWrite)
}

func TestPutObjectChunked_PromotesStagedNodeBatchesConcurrently(t *testing.T) {
	chunk := testChunkedPutChunkSize
	payload := bytes.Repeat([]byte("P"), 2*chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())
	deps.writePeer = func(int) []string { return []string{"node-a", "node-b"} }

	blobIDs := []string{
		uuid.Must(uuid.NewV7()).String(),
		uuid.Must(uuid.NewV7()).String(),
	}
	csb := newCSBWithTestChunks(deps, blobIDs)
	csb.stagingTxnID = "txn-promote-concurrent"

	started := make(chan string, 2)
	release := make(chan struct{})
	csb.promoteStagedBatchFn = func(_ context.Context, node, _ string, _ []stagedPromotePair) error {
		started <- node
		<-release
		return nil
	}

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	done := make(chan error, 1)
	go func() {
		_, runErr := runChunkedPut(context.Background(), csb, body,
			"bucket", "large.bin", "v1", "application/octet-stream",
			nil, "", 0, false, "", nil, nil, nil)
		done <- runErr
	}()

	first := <-started
	var second string
	select {
	case second = <-started:
	case <-time.After(250 * time.Millisecond):
		close(release)
		require.Failf(t, "promote batches did not run concurrently", "only %s started before release", first)
	}
	require.ElementsMatch(t, []string{"node-a", "node-b"}, []string{first, second})
	close(release)
	require.NoError(t, <-done)
}

func TestPutObjectChunked_RejectsBelowChunkThreshold(t *testing.T) {
	// putObjectChunked is internal; callers must route only objects
	// > DefaultChunkSize. Direct call with smaller size returns an error.
	sp := makeSpool(t, []byte("small"))
	b := &DistributedBackend{}
	_, err := b.putObjectChunked(context.Background(),
		"bucket", "k", "v", sp, "ct", nil, "",
		0, 0, false, "", nil, nil, nil)
	require.Error(t, err)
}

// TestChunkedPut_PreservesTags asserts the chunked-PUT cluster pipeline
// threads the tags argument into PutObjectMetaCmd. Pre-fix, putObjectChunked
// did not accept tags and the inner runChunkedPut built PutObjectMetaCmd
// without a Tags field — applyPutObjectMeta then wrote empty Tags,
// clobbering any caller-supplied tags on every large-object PUT.
func TestChunkedPut_PreservesTags(t *testing.T) {
	chunk := testChunkedPutChunkSize
	payload := bytes.Repeat([]byte("T"), 2*chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())

	blobIDs := make([]string, 2)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithTestChunks(deps, blobIDs)

	wantTags := []storage.Tag{
		{Key: "env", Value: "prod"},
		{Key: "owner", Value: "platform"},
	}

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil, wantTags)
	require.NoError(t, err)

	require.Len(t, deps.writeQuorumMetaCalls, 1, "exactly one quorum meta write")
	cmd := deps.writeQuorumMetaCalls[0]
	require.Equal(t, wantTags, cmd.Tags,
		"chunked-PUT must thread tags into PutObjectMetaCmd; clobbering this drops tags on every large object")
}

func TestChunkedChooseModTime(t *testing.T) {
	assert.Equal(t, int64(123), chunkedChooseModTime(123, true, 999), "preserve=true returns caller modTime")
	assert.Equal(t, int64(999), chunkedChooseModTime(123, false, 999), "preserve=false stamps now")
}

func TestAcquireChunkedMultipartCompleteSlotHonorsContext(t *testing.T) {
	oldSlots := chunkedMultipartCompleteSlots
	chunkedMultipartCompleteSlots = make(chan struct{}, 1)
	t.Cleanup(func() { chunkedMultipartCompleteSlots = oldSlots })

	release, err := acquireChunkedMultipartCompleteSlot(context.Background())
	require.NoError(t, err)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = acquireChunkedMultipartCompleteSlot(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestChunkedMultipartCompleteChunkSizeCapsDefault(t *testing.T) {
	require.Equal(t, int64(8<<20), chunkedMultipartCompleteChunkSize(storage.DefaultChunkSize))
	require.Equal(t, int64(4<<20), chunkedMultipartCompleteChunkSize(4<<20))
	require.Equal(t, int64(8<<20), chunkedMultipartCompleteChunkSize(0))
}

// errReaderAfter wraps an underlying io.Reader; once the cumulative bytes
// returned to callers reaches `at`, the next Read returns
// io.ErrUnexpectedEOF. Bytes prior to the threshold are passed through
// verbatim.
type errReaderAfter struct {
	r     io.Reader
	at    int64
	read  int64
	errAt error
}

func (e *errReaderAfter) Read(p []byte) (int, error) {
	if e.read >= e.at {
		return 0, e.errAt
	}
	remaining := e.at - e.read
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := e.r.Read(p)
	e.read += int64(n)
	if e.read >= e.at && err == nil {
		err = e.errAt
	}
	return n, err
}

// TestPutObjectChunked_PartialFailRollsBackBlobs_ChunkerError exercises a
// chunker-mid-stream failure: the body delivers a full first segment
// (16 MiB) cleanly, then surfaces io.ErrUnexpectedEOF partway through the
// second chunk's fillChunk. SegmentWriter propagates the chunker error and
// runChunkedPut's defer cleanup must tear down every blob whose worker
// completed before the failure (and any partial blobs recorded in
// placements), with no PutObjectMetaCmd proposed.
func TestPutObjectChunked_PartialFailRollsBackBlobs_ChunkerError(t *testing.T) {
	chunk := int64(testChunkedPutChunkSize)
	// Three chunks of payload, but the wrapped reader will error halfway
	// through segment 1's chunk read.
	totalPayload := 3 * chunk
	errAt := chunk + (chunk / 2)
	payload := bytes.Repeat([]byte("C"), int(totalPayload))
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())

	// Pre-allocate blobIDs sized for the would-be segment count (3) so any
	// completed worker has a slot to record into.
	blobIDs := make([]string, 3)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithTestChunks(deps, blobIDs)

	spBody, err := sp.Open()
	require.NoError(t, err)
	defer spBody.Close()
	wrapped := &errReaderAfter{r: spBody, at: errAt, errAt: io.ErrUnexpectedEOF}

	_, err = runChunkedPut(context.Background(), csb, wrapped,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil, nil)
	require.Error(t, err)
	// Chunker error must surface through the segment write wrap.
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF,
		"io.ErrUnexpectedEOF from the body must propagate through runChunkedPut")

	// No commit on chunker failure.
	assert.Empty(t, deps.writeQuorumMetaCalls, "chunker error must not commit meta")

	// Every blob that the placement layer recorded MUST appear in the
	// cleanup recorder. We don't pin the exact count (segment 0 always
	// completes; segment 1's worker may or may not receive the partial
	// 8 MiB chunk depending on chunker scheduling), but whatever was
	// recorded must be torn down.
	cleanedBlobs := map[string]struct{}{}
	for _, dc := range deps.deleteCalls {
		assert.Equal(t, "bucket", dc.bucket)
		cleanedBlobs[dc.shardKey] = struct{}{}
	}
	recordedAny := false
	for i, p := range csb.placements {
		if p.BlobID == "" {
			continue
		}
		recordedAny = true
		want := "large.bin/segments/" + p.BlobID
		assert.Contains(t, cleanedBlobs, want,
			"placement[%d] blob %s must be cleaned up on chunker error", i, p.BlobID)
	}
	assert.True(t, recordedAny,
		"chunker error after segment 0 must have allowed at least one placement to be recorded")
}
