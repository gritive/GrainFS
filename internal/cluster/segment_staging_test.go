package cluster

// PR1 (segment staging write): the staged-write primitive must encrypt a shard with the FINAL
// logical shard key as AAD even while writing it to a STAGING physical path, so a post-promote read
// (which decrypts with the final key) succeeds. The promote is a local rename of the staging dir to
// the final path. If the AAD were the staging key, ReadLocalShard(finalKey) would fail to decrypt —
// that is exactly what this locks against (codex plan-gate load-bearing fix #2).

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// newRealStagingCSB builds a clusterSegmentBackend wired to a REAL single-node EC
// backend (real ShardService on disk, self-only placement). Unlike newCSBWithDeps
// it leaves writeSegmentFn nil so the production writeOneSegment runs — exercising
// the real staged shard write + AAD + on-disk layout. stagingTxnID is set so PR1
// segment staging is active. The caller injects promoteStagedFn to choose whether
// the commit-time promote actually runs (real) or is suppressed (RED probe).
func newRealStagingCSB(t *testing.T, b *DistributedBackend, bucket, key, txnID string, numSegments int) *clusterSegmentBackend {
	t.Helper()
	blobIDs := make([]string, numSegments)
	for i := range blobIDs {
		blobIDs[i] = uuid.Must(uuid.NewV7()).String()
	}
	return &clusterSegmentBackend{
		b:            b,
		bucket:       bucket,
		key:          key,
		versionID:    "v1",
		blobIDs:      blobIDs,
		placements:   make([]segmentPlacement, numSegments),
		chunkSize:    testChunkedPutChunkSize,
		stagingTxnID: txnID,
	}
}

func realStagingBackend(t *testing.T) (*DistributedBackend, string, string, []byte) {
	t.Helper()
	b := setupECBackend(t)
	b.chunkedPutChunkSize = testChunkedPutChunkSize
	b.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-a": {ID: "group-a", PeerIDs: []string{"self", "self", "self"}},
	}})
	const bucket, key = "staging-bucket", "staging-object"
	require.NoError(t, b.CreateBucket(context.Background(), bucket))
	body := makeChunkedTestBody(testChunkedPutChunkSize + 4096) // 2 segments
	return b, bucket, key, body
}

// TestRunChunkedPut_StagingNotPromoted_ObjectUnreadable is the dependency-direction
// driver for Task 4 (staging wiring). With staging wired, segment shards land in
// .segstaging and only the commit-time promote moves them to the final path the
// manifest references. Suppressing promote MUST therefore strand the shards at
// staging and leave the committed object unreadable. Pre-wiring (shards written
// directly to final, promote irrelevant) the object reads fine — so this test fails
// (RED) until the staging write path is wired, and passes once it is.
func TestRunChunkedPut_StagingNotPromoted_ObjectUnreadable(t *testing.T) {
	ctx := context.Background()
	b, bucket, key, body := realStagingBackend(t)
	numSegments := int((int64(len(body)) + int64(testChunkedPutChunkSize) - 1) / int64(testChunkedPutChunkSize))

	csb := newRealStagingCSB(t, b, bucket, key, "txn-suppressed", numSegments)
	csb.promoteStagedFn = func(context.Context, string, string, string, string) error {
		return nil // NO-OP: suppress the commit-time promote
	}

	_, err := runChunkedPut(ctx, csb, bytes.NewReader(body),
		bucket, key, "v1", "application/octet-stream", nil, "", 0, false, "", nil, nil, nil)
	require.NoError(t, err, "commit (manifest write) still succeeds — only promote was suppressed")

	// The committed manifest points at the final segment paths, but the shards are
	// stranded at staging, so the object cannot be reconstructed. GetObject resolves
	// the metadata lazily (no eager shard read), so the missing-shard failure
	// surfaces on the body read — assert on the combined outcome.
	rd, _, gerr := b.GetObject(ctx, bucket, key)
	if gerr == nil {
		_, rerr := io.Copy(io.Discard, rd)
		_ = rd.Close()
		gerr = rerr
	}
	require.Error(t, gerr, "suppressed promote must leave shards at staging -> object unreadable")
}

// TestRunChunkedPut_StagedPromote_RoundTripByteIdentical is the happy-path
// counterpart: with the REAL commit-time promote (promoteStagedFn left nil), a
// multi-segment chunked PUT writes its shards to staging, promotes them to their
// final paths at commit, and a subsequent GET reconstructs byte-identical data.
// This locks the staging-write + final-key-AAD + promote roundtrip end-to-end at
// the local level (the e2e covers the cluster wire path).
func TestRunChunkedPut_StagedPromote_RoundTripByteIdentical(t *testing.T) {
	ctx := context.Background()
	b, bucket, key, body := realStagingBackend(t)
	numSegments := int((int64(len(body)) + int64(testChunkedPutChunkSize) - 1) / int64(testChunkedPutChunkSize))
	require.Greater(t, numSegments, 1, "fixture must span multiple segments")

	csb := newRealStagingCSB(t, b, bucket, key, "txn-roundtrip", numSegments)
	// promoteStagedFn left nil -> the real PromoteLocalStagedShards runs.

	_, err := runChunkedPut(ctx, csb, bytes.NewReader(body),
		bucket, key, "v1", "application/octet-stream", nil, "", 0, false, "", nil, nil, nil)
	require.NoError(t, err)

	rd, _, gerr := b.GetObject(ctx, bucket, key)
	require.NoError(t, gerr)
	got, rerr := io.ReadAll(rd)
	_ = rd.Close()
	require.NoError(t, rerr, "promoted shards must be readable at their final path")
	require.Equal(t, body, got, "GET after staged promote must be byte-identical")
}

// TestNativeWriteHandler_StagedRequest_WritesStagingWithFinalAAD locks the
// RECEIVER side of the staging wire: a ShardWriteRequest carrying StagingKey must
// write the bytes to the staging physical path while sealing with Key (the FINAL
// logical key) as AAD. The proof is that a post-promote ReadLocalShard(finalKey) —
// which decrypts with the final-key AAD — returns the bytes byte-identical, and a
// pre-promote read of the final key finds nothing (the bytes were at staging). This
// is the highest-risk path (a receiver deriving the dir from Key instead of
// StagingKey would corrupt silently).
func TestNativeWriteHandler_StagedRequest_WritesStagingWithFinalAAD(t *testing.T) {
	svc, _ := newTestShardService(t)
	handler := svc.NativeWriteHandler()
	const (
		bucket     = "b"
		finalKey   = "obj/segments/blobR"
		stagingKey = ".segstaging/txnR/blobR"
	)
	data := []byte("receiver-staged-shard-payload-9876543210")

	req := transport.ShardWriteRequest{Bucket: bucket, Key: finalKey, StagingKey: stagingKey, ShardIdx: 0, Sealed: false}
	require.NoError(t, handler(req, bytes.NewReader(data)))

	// Before promote, the bytes are at staging, not at the final path.
	_, preErr := svc.ReadLocalShard(bucket, finalKey, 0)
	require.Error(t, preErr, "staged write must not land at the final path before promote")

	// Promote, then the final-key read (final-key AAD) must succeed byte-identical.
	require.NoError(t, svc.PromoteLocalStagedShards(bucket, stagingKey, finalKey, -1))
	got, err := svc.ReadLocalShard(bucket, finalKey, 0)
	require.NoError(t, err)
	require.Equal(t, data, got, "receiver must seal with finalKey AAD so the promoted read decrypts")
}

func TestNativeWriteHandler_StagedSizedRequestRejectsSizeMismatch(t *testing.T) {
	svc, _ := newTestShardService(t)
	handler := svc.NativeWriteHandler()
	const (
		bucket     = "b"
		finalKey   = "obj/segments/blob-sized"
		stagingKey = ".segstaging/txn-sized/blob-sized"
	)
	data := []byte("receiver-staged-shard-payload-9876543210")

	req := transport.ShardWriteRequest{
		Bucket:          bucket,
		Key:             finalKey,
		StagingKey:      stagingKey,
		ShardIdx:        0,
		Sealed:          false,
		StreamSizeKnown: true,
		StreamSize:      int64(len(data) - 1),
	}
	require.Error(t, handler(req, bytes.NewReader(data)))
	_, preErr := svc.ReadLocalShard(bucket, finalKey, 0)
	require.Error(t, preErr, "mismatched staged write must not publish final shard")
}

// TestPromoteLocalStagedShards_NothingStaged_Fails locks the code-gate fix: a
// promote that renames nothing AND finds nothing already at the final path must
// FAIL, not silently report success — otherwise the manifest commit would
// reference shards that are absent on that node (over-eager cleanup race / never
// written).
func TestPromoteLocalStagedShards_NothingStaged_Fails(t *testing.T) {
	svc, _ := newTestShardService(t)
	err := svc.PromoteLocalStagedShards("b", ".segstaging/txnX/blobX", "obj/segments/blobX", -1)
	require.Error(t, err, "promote with no staged or final shards must fail")
}

// TestPromoteLocalStagedShards_DstAlreadyPresent_StillFsyncs locks the second
// code-gate fix: when the staged dir is gone but the final dst already exists (a
// rename race / concurrent completer / idempotent retry), this caller must still
// fsync the final dir chain before treating the promote as done — the earlier
// mover may have created dst but crashed before ITS fsync, so proceeding to the
// manifest commit without a personal fsync would leave a non-durable link.
func TestPromoteLocalStagedShards_DstAlreadyPresent_StillFsyncs(t *testing.T) {
	svc, root := newTestShardService(t)
	var synced []string
	svc.local.syncDirHook = func(dir string) error { synced = append(synced, dir); return nil }

	const bucket = "b"
	const finalKey = "obj/segments/blobP"
	// Pre-create the final dst dir (no staged src) to simulate a prior mover.
	dst, err := svc.getShardDir(bucket, finalKey, 0)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(dst, 0o755))

	require.NoError(t, svc.PromoteLocalStagedShards(bucket, ".segstaging/txnP/blobP", finalKey, -1))

	// The dst chain must have been fsynced (durability before commit) even though
	// nothing was renamed on this call.
	require.NotEmpty(t, synced, "dst-already-present path must still fsync the final chain")
	var sawDst bool
	for _, d := range synced {
		if strings.HasPrefix(d, root) && strings.Contains(d, finalKey) {
			sawDst = true
		}
	}
	require.True(t, sawDst, "fsync must cover the final dst path, got %v", synced)
}

// TestPromoteLocalStagedShards_LargeRedundant_SkipsDirFsync locks the commit-tail
// optimization: a large (>= largeShardFsyncThreshold) shard in a redundant (EC
// parity) deployment skips the promote dir-fsync, mirroring the write path which
// already skips file-fsync for the same class (EC reconstruction + the scrubber
// own durability — see shardWriteRequiresFsync). The rename stays synchronous so
// read-after-write still works; only the durability fsync is deferred to
// EC/scrubber. This is what removes promote from the PUT commit-tail latency.
func TestPromoteLocalStagedShards_LargeRedundant_SkipsDirFsync(t *testing.T) {
	svc, _ := newTestShardService(t) // noRedundancy nil => counts as redundant
	var syncedCount int
	svc.local.syncDirHook = func(string) error { syncedCount++; return nil }

	const bucket = "b"
	const stagingKey = ".segstaging/txnL/blobL"
	const finalKey = "obj/segments/blobL"
	large := bytes.Repeat([]byte("x"), largeShardFsyncThreshold)
	for d := 0; d < len(svc.local.dataDirs); d++ {
		sdir, err := svc.local.getShardDir(bucket, stagingKey, d)
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll(sdir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(sdir, fmt.Sprintf("shard_%d", d)), large, 0o644))
	}
	require.NoError(t, svc.PromoteLocalStagedShards(bucket, stagingKey, finalKey, -1))
	require.Zero(t, syncedCount, "large+redundant promote must skip dir-fsync (EC+scrubber own durability, mirrors write path)")
}

// TestHandlePromoteStagedBatch_Failure_ReturnsErrorEnvelope locks the receiver
// half of the all-or-fail contract for the batch path: when
// PromoteLocalStagedShards fails, handlePromoteStagedBatch must surface it as
// an in-band "Error" reply envelope, not a silent OK.
func TestHandlePromoteStagedBatch_Failure_ReturnsErrorEnvelope(t *testing.T) {
	svc, _ := newTestShardService(t)
	// Encode one pair where the stagingKey doesn't exist -> PromoteLocalStagedShards fails.
	data, err := encodeStagedPromotePairs([]stagedPromotePair{
		{stagingKey: ".segstaging/txnZ/blobZ", finalKey: "obj/segments/blobZ"},
	})
	require.NoError(t, err)
	sr := &shardRequest{Bucket: "b", Data: data}
	resp := svc.handlePromoteStagedBatch(sr)
	rpcType, _, err := unmarshalEnvelope(resp)
	require.NoError(t, err)
	require.Equal(t, "Error", rpcType, "a failed batch promote must reply with an Error envelope")
}

func TestWriteLocalShardStaged_AADIsFinalKey_PromoteReadable(t *testing.T) {
	svc, _ := newTestShardService(t)
	ctx := context.Background()
	const bucket = "b"
	const finalKey = "obj/segments/blob1"       // final logical shard key (drives AAD + final path)
	const stagingKey = ".segstaging/txn1/blob1" // staging physical path (no "/segments/")
	data := []byte("segment-shard-payload-0123456789")

	// Write to the STAGING physical path, but with the FINAL key as the encryption AAD.
	require.NoError(t, svc.local.writeLocalShardStaged(ctx, bucket, stagingKey, finalKey, 0, data))

	// Promote: local rename of the staging shard dir(s) to the final path.
	require.NoError(t, svc.PromoteLocalStagedShards(bucket, stagingKey, finalKey, -1))

	// Reading at the FINAL key (which decrypts with the final-key AAD) must return the bytes —
	// proving the staged write used the final key as AAD, not the staging key.
	got, err := svc.ReadLocalShard(bucket, finalKey, 0)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestStagedPromotePairsCodec(t *testing.T) {
	// logicalShardSize is intentionally NOT carried on the wire (the batch encoding
	// holds only the staging/final key pair), so a real per-shard size on the
	// sender is dropped and decode yields the -1 "unknown" sentinel — the receiver
	// then falls back to the on-disk shard size for the promote fsync class (see
	// decodeStagedPromotePairs / promoteShardRequiresFsync).
	want := []stagedPromotePair{
		{stagingKey: ".segstaging/txn/blob-a", finalKey: "obj/segments/blob-a", logicalShardSize: 1 << 20},
		{stagingKey: ".segstaging/txn/blob-b", finalKey: "obj/segments/blob-b", logicalShardSize: 7},
	}
	data, err := encodeStagedPromotePairs(want)
	require.NoError(t, err)

	got, err := decodeStagedPromotePairs(data)
	require.NoError(t, err)
	require.Equal(t, []stagedPromotePair{
		{stagingKey: ".segstaging/txn/blob-a", finalKey: "obj/segments/blob-a", logicalShardSize: -1},
		{stagingKey: ".segstaging/txn/blob-b", finalKey: "obj/segments/blob-b", logicalShardSize: -1},
	}, got)

	_, err = decodeStagedPromotePairs(data[:len(data)-1])
	require.Error(t, err, "truncated batch payload must fail closed")
}

// PR2 Task (delete-time liveness rework): the orphan-shard walker AGES OUT abandoned
// .segstaging/ staged shard leaves (crash / failed PUT / LWW loser) instead of
// skipping them forever, while NEVER deleting a committed (live) object however it
// was written. The branch is now: anchored match (relParts[1]==SegStagingPrefix) →
// structural /segments/ exclusion → generous staging age gate → full-object liveness
// IDENTICAL to the regular path → direct os.RemoveAll. The proof is on-disk: an OLD
// abandoned staged leaf is gone after the walk; a RECENT one survives; a mid-aged one
// survives (large in-flight PUT); a chunked user object keyed under .segstaging
// (.../segments/...) survives via the /segments/ exclusion; a live non-chunked
// full-object user object keyed under .segstaging survives via the liveness check; a
// "foo/.segstaging/bar" path is handled by the regular path (anchored match excludes
// it). None is ever yielded to fn as a fake full-object orphan.
//
// The staging age gate is the GENEROUS segStagingReclaimAge (~24h), NOT the per-shard
// floor (~466s): all of an object's segments are staged before the commit-time
// promote and each leaf's mtime is fixed at write time, so a large in-flight PUT's
// early staging leaf can be minutes old (past minOrphanShardAge) while the PUT is
// still running. The mid-age case below is exactly that regression.
func TestWalkOrphanShards_SegStagingAgedOut(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	// Old (abandoned) staged leaf: backdated past the generous staging floor. Derive
	// from the constant so it tracks any future tuning (never silently drops under).
	old := writeShardLeaf(t, root, "bkt/.segstaging/txnOld/blobOld", []int{0}, segStagingReclaimAge+time.Hour)
	oldTxn := filepath.Dir(old)
	// Recent staged leaf: just written, could be a live in-flight PUT.
	recent := writeShardLeaf(t, root, "bkt/.segstaging/txnNew/blobNew", []int{0}, 0)
	recentTxn := filepath.Dir(recent)
	// Mid-aged staged leaf: older than the per-shard floor (minOrphanShardAge ~466s,
	// via oldEnough) but well under segStagingReclaimAge (~24h). The exact regression
	// the reviewer flagged: a large in-flight PUT whose early segment was staged minutes
	// ago. Under the per-shard `cutoff` it would be reclaimed mid-write; under the
	// generous `stagingCutoff` it is kept.
	midAge := writeShardLeaf(t, root, "bkt/.segstaging/txnMid/blobMid", []int{0}, oldEnough)

	got := collectOrphans(t, b, map[string]bool{})

	require.NoDirExists(t, old,
		"abandoned (old) .segstaging staged leaf must be reclaimed (age-out)")
	require.NoDirExists(t, oldTxn,
		"empty .segstaging transaction parent must be removed after its last staged leaf is reclaimed")
	require.DirExists(t, recent,
		"recent .segstaging staged leaf must be kept (could be a live in-flight PUT)")
	require.DirExists(t, recentTxn,
		"non-empty .segstaging transaction parent must be kept")
	require.DirExists(t, midAge,
		"staged leaf older than the per-shard floor but younger than segStagingReclaimAge "+
			"must be kept (could be a large in-flight PUT) — the per-shard cutoff would wrongly reclaim it")
	require.NotContains(t, got, old,
		".segstaging dir must never be yielded to fn as a full-object orphan")
	require.NotContains(t, got, recent,
		".segstaging dir must never be yielded to fn as a full-object orphan")
	require.NotContains(t, got, midAge,
		".segstaging dir must never be yielded to fn as a full-object orphan")
}

// TestWalkOrphanShards_SegStaging_ChunkedUserObjectKept proves the structural
// /segments/ exclusion (step 1 of the reworked branch): a CHUNKED user object whose
// KEY lands under ".segstaging" has its committed segments at
// <bucket>/.segstaging/<key>/segments/<blobID>. That path is .segstaging-prefixed AND
// contains "/segments/", so it is a real committed object, NOT a staging leaf, and
// must be kept WHOLESALE — never reclaimed and never parse-routed to the segment
// liveness oracle. Old enough (>24h) that without the /segments/ guard it would be
// reclaimed.
func TestWalkOrphanShards_SegStaging_ChunkedUserObjectKept(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	// A chunked user object keyed "userobj" under .segstaging: its committed segment
	// shard lives at .segstaging/userobj/segments/<blobID>. Backdated past the staging
	// floor so only the /segments/ exclusion can keep it.
	chunked := writeShardLeaf(t, root, "bkt/.segstaging/userobj/segments/blobC", []int{0}, segStagingReclaimAge+time.Hour)

	got := collectOrphans(t, b, map[string]bool{})

	require.DirExists(t, chunked,
		"a chunked user object keyed under .segstaging (.../segments/...) must be kept via the /segments/ exclusion")
	require.NotContains(t, got, chunked,
		".segstaging chunked user object must never be yielded to fn")
}

// TestWalkOrphanShards_SegStaging_LiveFullObjectKept proves the full-object liveness
// check (step 3 of the reworked branch): a NON-chunked full-object user object whose
// KEY is literally ".segstaging/foo" (data at <bucket>/.segstaging/foo/<vid>, NO
// "/segments/") that is COMMITTED (live) must be kept by the SAME hasLiveShardRecord
// the regular path uses. Liveness is established via a per-version blob on a
// versioning-enabled bucket (seedVersionBlob) — NOT an FSM obj: record, which would
// be forward-mapped into the live[] set and short-circuit BEFORE the liveness check,
// making the teeth false-green. With only the blob, the dir is in neither known[] nor
// live[]/frozen[], so the sole thing keeping it is hasLiveShardRecord. Old enough
// (>24h) that without that check it would be reclaimed.
func TestWalkOrphanShards_SegStaging_LiveFullObjectKept(t *testing.T) {
	ctx := context.Background()
	b := orphanWalkerBackend(t)
	const vbkt = "vbkt" // distinct versioning-enabled bucket so it does not bleed into others
	require.NoError(t, b.CreateBucket(ctx, vbkt))
	setVersioningForTest(t, b, vbkt, "Enabled")
	root := b.shardSvc.DataDirs()[0]
	self := b.currentSelfAddr()

	// Live non-chunked full object keyed ".segstaging/foo": per-version blob (the
	// shard-liveness authority for a versioning-enabled bucket) + on-disk shard leaf.
	seedVersionBlob(t, b, vbkt, ".segstaging/foo", "vLive", PutObjectMetaCmd{ETag: "e", ECData: 1, NodeIDs: []string{self}})
	liveObj := writeShardLeaf(t, root, vbkt+"/.segstaging/foo/vLive", []int{0}, segStagingReclaimAge+time.Hour)

	got := collectOrphans(t, b, map[string]bool{})

	require.DirExists(t, liveObj,
		"a live non-chunked full object keyed under .segstaging must be kept via hasLiveShardRecord (delete-time liveness)")
	require.NotContains(t, got, liveObj,
		".segstaging live full object must never be yielded to fn")
}

// TestWalkOrphanShards_SegStaging_AnchoredMatchExcludesNestedKey proves the anchored
// match (relParts[1]==SegStagingPrefix) ROUTES a path whose key merely contains
// ".segstaging" to the REGULAR path, not the staging branch. The path
// "bkt/foo/.segstaging/bar" has relParts[1]=="foo" (bucket=relParts[0]="bkt"; key is
// "foo/.segstaging" with ".segstaging" NOT directly under the bucket), so it is NOT a
// staging leaf.
//
// To DISCRIMINATE the two branches the candidate must be a DEAD object (no record)
// backdated `oldEnough` — squarely BETWEEN the per-shard regular cutoff (~466s) and
// the generous staging cutoff (~24h):
//   - Correct anchoring → regular path → hasLiveShardRecord=(false,certain) → YIELDED.
//   - Broken anchoring (broad strings.Contains) → staging branch → the 24h staging age
//     gate KEEPS it (466s < 24h) → not yielded → this assertion FAILS.
//
// A live object would be kept by BOTH branches (both consult liveness), so "mark live
// → kept" cannot separate them; only a dead object in the inter-cutoff window can.
func TestWalkOrphanShards_SegStaging_AnchoredMatchExcludesNestedKey(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	// Bucket "bkt", object key "foo/.segstaging" — ".segstaging" is relParts[2], not [1].
	// Dead (no obj:/blob record), aged into the inter-cutoff window.
	nested := writeShardLeaf(t, root, "bkt/foo/.segstaging/bar", []int{0}, oldEnough)

	got := collectOrphans(t, b, map[string]bool{})

	require.Equal(t, []string{nested}, got,
		"a dead object whose key contains .segstaging must be reclaimed via the REGULAR path "+
			"(anchored staging match routes it there); the broad-Contains staging branch would wrongly keep it under the 24h gate")
}

// TestWalkOrphanShards_SegStaging_BareUnparseableKept proves the fail-CLOSED guard
// on an UNPARSEABLE staging-prefixed leaf. A bare-legacy (unversioned) full object
// keyed EXACTLY ".segstaging" stores its shards directly at <bucket>/.segstaging/shard_N,
// so rel == "<bucket>/.segstaging" — relParts==[bucket, ".segstaging"], len 2. It
// matches relParts[1]==SegStagingPrefix (enters the staging branch), has no
// "/segments/", and is old (>24h), yet parseFullObjectRel returns okF=false (a len-2
// path can't be split into bucket/key/version). The branch must therefore KEEP it,
// exactly as the regular full-object path treats okF=false ("unversioned / unparseable
// → keep, never delete"). Without the !okF⇒keep guard the okF block is skipped and
// execution falls through to os.RemoveAll → a committed object is wrongly reclaimed
// (data loss). This is the only seg-staging case whose on-disk leaf is len-2, so it
// alone exercises the fail-closed-on-unparseable path.
func TestWalkOrphanShards_SegStaging_BareUnparseableKept(t *testing.T) {
	b := orphanWalkerBackend(t)
	root := b.shardSvc.DataDirs()[0]
	// Leaf is the bucket's ".segstaging" dir itself, holding shard files directly
	// (rel == "bkt/.segstaging", len 2). Old enough that only the okF=false⇒keep
	// guard can save it from the age-out reclaim.
	bare := writeShardLeaf(t, root, "bkt/.segstaging", []int{0}, segStagingReclaimAge+time.Hour)

	got := collectOrphans(t, b, map[string]bool{})

	require.DirExists(t, bare,
		"a bare-legacy full object keyed exactly .segstaging (unparseable len-2 leaf) must be KEPT "+
			"(fail-closed on unparseable, mirroring the regular path) — not reclaimed by the staging age-out")
	require.NotContains(t, got, bare,
		".segstaging bare leaf must never be yielded to fn as a full-object orphan")
}
