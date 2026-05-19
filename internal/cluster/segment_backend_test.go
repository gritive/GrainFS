package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

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
	writtenData map[int][]byte // idx → data bytes echoed back

	deleteCalls []deleteCall

	proposeCalls   []proposeCall
	proposeErr     error
	deleteShardErr error
}

type deleteCall struct {
	peer, bucket, shardKey string
}

type proposeCall struct {
	cmdType CommandType
	cmd     PutObjectMetaCmd
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
	if f.writtenData == nil {
		f.writtenData = make(map[int][]byte)
	}
	f.writtenData[idx] = append([]byte(nil), in.Data...)
	f.writeRecord = append(f.writeRecord, in.Group)
	var peers []string
	if f.writePeer != nil {
		peers = f.writePeer(idx)
	} else {
		peers = append([]string(nil), in.Group.PeerIDs...)
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

func (f *fakeSegmentBackendDeps) propose(ctx context.Context, cmdType CommandType, payload any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd, _ := payload.(PutObjectMetaCmd)
	f.proposeCalls = append(f.proposeCalls, proposeCall{cmdType: cmdType, cmd: cmd})
	return f.proposeErr
}

func newFakeBackendWithGroups(groups []ShardGroupEntry) *fakeSegmentBackendDeps {
	return &fakeSegmentBackendDeps{groups: groups, writeErrAt: map[int]error{}}
}

func newCSBWithDeps(deps *fakeSegmentBackendDeps, blobIDs []string) *clusterSegmentBackend {
	return &clusterSegmentBackend{
		bucket:          "test-bucket",
		key:             "large.bin",
		versionID:       "v1",
		blobIDs:         blobIDs,
		placements:      make([]segmentPlacement, len(blobIDs)),
		writeSegmentFn:  deps.writeSegment,
		groupSelectorFn: deps.groupSelector,
		deleteShardsFn:  deps.deleteShards,
		proposeFn:       deps.propose,
		ecConfigFn:      func() ECConfig { return ECConfig{DataShards: 4, ParityShards: 2} },
	}
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

// makeSpool writes payload to a temp file and returns a *spooledObject.
func makeSpool(t *testing.T, payload []byte) *spooledObject {
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
	chunk := storage.DefaultChunkSize
	payload := bytes.Repeat([]byte("A"), 4*chunk) // exactly 4 segments
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())

	blobIDs := make([]string, 4)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithDeps(deps, blobIDs)

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	obj, err := runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.Equal(t, int64(len(payload)), obj.Size)
	assert.Equal(t, "v1", obj.VersionID)
	assert.False(t, obj.IsAppendable)
	assert.Len(t, obj.Segments, 4)

	require.Len(t, deps.proposeCalls, 1, "exactly one PutObjectMetaCmd proposed")
	cmd := deps.proposeCalls[0].cmd
	assert.Equal(t, CmdPutObjectMeta, deps.proposeCalls[0].cmdType)
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
		nil, "", 0, false, "", nil, parts, nil)
	require.NoError(t, err)
	require.NotNil(t, obj)
	require.Equal(t, parts, obj.Parts)

	require.Len(t, deps.proposeCalls, 1)
	cmd := deps.proposeCalls[0].cmd
	require.Len(t, cmd.Segments, 2)
	require.Equal(t, parts, cmd.Parts)

	parts[0].ETag = "mutated"
	require.Equal(t, wantParts, obj.Parts)
	require.Equal(t, wantParts, cmd.Parts)
}

func TestPutObjectChunked_ObservesChunkFanoutBreadth(t *testing.T) {
	chunk := storage.DefaultChunkSize
	payload := bytes.Repeat([]byte("M"), 4*chunk) // exactly 4 segments
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())

	blobIDs := make([]string, 4)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithDeps(deps, blobIDs)

	before := chunkFanoutMetricCount(t)
	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil)
	require.NoError(t, err)

	after := chunkFanoutMetricCount(t)
	require.Greater(t, after, before, "chunked PUT must observe fan-out breadth")
}

func TestPutObjectChunked_PartialFailRollsBackBlobs_WorkerError(t *testing.T) {
	chunk := storage.DefaultChunkSize
	payload := bytes.Repeat([]byte("B"), 3*chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())
	deps.writeErrAt[2] = errors.New("EC write failed at segment 2")

	blobIDs := make([]string, 3)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithDeps(deps, blobIDs)

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "segment write")

	// No raft commit.
	assert.Empty(t, deps.proposeCalls)

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
	chunk := storage.DefaultChunkSize
	payload := bytes.Repeat([]byte("F"), 2*chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())

	blobIDs := make([]string, 2)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithDeps(deps, blobIDs)

	hookErr := errors.New("beforeCommit failed (e.g. quota check)")
	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "",
		func() error { return hookErr }, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, hookErr)

	assert.Empty(t, deps.proposeCalls, "beforeCommit failure must not commit")
	cleanedBlobs := map[string]struct{}{}
	for _, dc := range deps.deleteCalls {
		cleanedBlobs[dc.shardKey] = struct{}{}
	}
	for _, id := range blobIDs {
		assert.Contains(t, cleanedBlobs, "large.bin/segments/"+id, "all completed blobs cleaned up")
	}
}

func TestPutObjectChunked_PartialFailRollsBackBlobs_ProposeError(t *testing.T) {
	chunk := storage.DefaultChunkSize
	payload := bytes.Repeat([]byte("D"), 2*chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())
	deps.proposeErr = errors.New("raft propose failed")

	blobIDs := make([]string, 2)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithDeps(deps, blobIDs)

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit meta")

	// propose was attempted exactly once (and failed).
	require.Len(t, deps.proposeCalls, 1)

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
	chunk := storage.DefaultChunkSize
	payload := bytes.Repeat([]byte("E"), 2*chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())
	deps.writeErrAt[1] = errors.New("EC write failed at segment 1")
	deps.deleteShardErr = errors.New("delete failed too")

	blobIDs := make([]string, 2)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithDeps(deps, blobIDs)

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil)
	require.Error(t, err)
	// PUT errors cleanly even though cleanup also failed (no panic).
	assert.Empty(t, deps.proposeCalls)
}

func TestPutObjectChunked_RejectsMultipartParts(t *testing.T) {
	sp := makeSpool(t, []byte("any"))
	b := &DistributedBackend{}
	_, err := b.putObjectChunked(context.Background(),
		"bucket", "k", "v", sp, "ct", nil, "",
		0, false, "", nil,
		[]storage.MultipartPartEntry{{PartNumber: 1, Size: 1, ETag: "e"}}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multipart parts: deferred to zero-spool Complete path")
}

func TestPutObjectChunked_RejectsBelowChunkThreshold(t *testing.T) {
	// putObjectChunked is internal; callers must route only objects
	// > DefaultChunkSize. Direct call with smaller size returns an error.
	sp := makeSpool(t, []byte("small"))
	b := &DistributedBackend{}
	_, err := b.putObjectChunked(context.Background(),
		"bucket", "k", "v", sp, "ct", nil, "",
		0, false, "", nil, nil, nil)
	require.Error(t, err)
}

// TestChunkedPut_PreservesTags asserts the chunked-PUT cluster pipeline
// threads the tags argument into PutObjectMetaCmd. Pre-fix, putObjectChunked
// did not accept tags and the inner runChunkedPut built PutObjectMetaCmd
// without a Tags field — applyPutObjectMeta then wrote empty Tags,
// clobbering any caller-supplied tags on every large-object PUT.
func TestChunkedPut_PreservesTags(t *testing.T) {
	chunk := storage.DefaultChunkSize
	payload := bytes.Repeat([]byte("T"), 2*chunk)
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())

	blobIDs := make([]string, 2)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithDeps(deps, blobIDs)

	wantTags := []storage.Tag{
		{Key: "env", Value: "prod"},
		{Key: "owner", Value: "platform"},
	}

	body, err := sp.Open()
	require.NoError(t, err)
	defer body.Close()
	_, err = runChunkedPut(context.Background(), csb, body,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, wantTags)
	require.NoError(t, err)

	require.Len(t, deps.proposeCalls, 1, "exactly one PutObjectMetaCmd proposed")
	cmd := deps.proposeCalls[0].cmd
	assert.Equal(t, CmdPutObjectMeta, deps.proposeCalls[0].cmdType)
	require.Equal(t, wantTags, cmd.Tags,
		"chunked-PUT must thread tags into PutObjectMetaCmd; clobbering this drops tags on every large object")
}

func TestChunkedChooseModTime(t *testing.T) {
	assert.Equal(t, int64(123), chunkedChooseModTime(123, true, 999), "preserve=true returns caller modTime")
	assert.Equal(t, int64(999), chunkedChooseModTime(123, false, 999), "preserve=false stamps now")
}

// TestPutObjectChunked_SizeGuard_EndToEnd locks down the outer routing
// decision in putObjectECSpooledWithOptionalModTime (backend.go): objects at
// or below DefaultChunkSize must NOT take the chunked path; strictly larger
// objects must. The routing predicate is extracted as chunkedPathThresholdMet
// so the threshold can be asserted without a full DistributedBackend fixture
// while still being the exact predicate consulted at the call site.
func TestPutObjectChunked_SizeGuard_EndToEnd(t *testing.T) {
	chunk := int64(storage.DefaultChunkSize)
	cases := []struct {
		name      string
		size      int64
		wantChunk bool
	}{
		{name: "below_threshold_8MiB", size: 8 << 20, wantChunk: false},
		{name: "at_threshold_16MiB", size: chunk, wantChunk: false},
		{name: "just_above_threshold", size: chunk + 1, wantChunk: true},
		{name: "above_threshold_17MiB", size: 17 << 20, wantChunk: true},
		{name: "well_above_threshold_64MiB", size: 64 << 20, wantChunk: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantChunk, chunkedPathThresholdMet(tc.size),
				"size=%d (chunk=%d) routing decision", tc.size, chunk)
		})
	}
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
	chunk := int64(storage.DefaultChunkSize)
	// 48 MiB of payload, but the wrapped reader will error at byte 24 MiB —
	// after segment 0's full 16 MiB has been delivered and 8 MiB into
	// segment 1's chunk read.
	totalPayload := 3 * chunk
	errAt := chunk + (chunk / 2) // 24 MiB
	payload := bytes.Repeat([]byte("C"), int(totalPayload))
	sp := makeSpool(t, payload)
	deps := newFakeBackendWithGroups(fourPGFixture())

	// Pre-allocate blobIDs sized for the would-be segment count (3) so any
	// completed worker has a slot to record into.
	blobIDs := make([]string, 3)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	csb := newCSBWithDeps(deps, blobIDs)

	spBody, err := sp.Open()
	require.NoError(t, err)
	defer spBody.Close()
	wrapped := &errReaderAfter{r: spBody, at: errAt, errAt: io.ErrUnexpectedEOF}

	_, err = runChunkedPut(context.Background(), csb, wrapped,
		"bucket", "large.bin", "v1", "application/octet-stream",
		nil, "", 0, false, "", nil, nil)
	require.Error(t, err)
	// Chunker error must surface through the segment write wrap.
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF,
		"io.ErrUnexpectedEOF from the body must propagate through runChunkedPut")

	// No raft commit on chunker failure.
	assert.Empty(t, deps.proposeCalls, "chunker error must not propose meta")

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
		"chunker error after 24 MiB must have allowed at least segment 0 to record a placement")
}
