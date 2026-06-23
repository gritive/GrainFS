package cluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// newTestBackendWithQuorumMeta builds a single-node EC 1+0 backend with the
// shard service + shard group wired so AppendObject commits its manifest to the
// quorum-meta blob store (the off-raft authority) and reads it back. Mirrors
// newSingleNode1Plus0ChunkCapable (single_put_path_test.go) — the existing
// single-node ctor that exercises the quorum-meta write/read path.
func newTestBackendWithQuorumMeta(t *testing.T) *DistributedBackend {
	t.Helper()
	return newSingleNode1Plus0ChunkCapable(t)
}

// TestAppendObject_BlobRMW_AccumulatesAndRejectsStaleOffset proves AppendObject
// is an owner-locked blob CAS read-modify-write: two appends accumulate Size,
// the object is appendable, the quorum-meta blob advances MetaSeq by exactly 2,
// and a retry issued at the old (stale) offset is rejected with
// ErrAppendOffsetMismatch WITHOUT changing the object's size or segment count
// (at-most-once guarantee).
func TestAppendObject_BlobRMW_AccumulatesAndRejectsStaleOffset(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bk"))

	_, err := b.AppendObject(ctx, "bk", "k", 0, bytes.NewReader([]byte("aaa")))
	require.NoError(t, err)
	obj, err := b.AppendObject(ctx, "bk", "k", 3, bytes.NewReader([]byte("bbbb")))
	require.NoError(t, err)
	require.Equal(t, int64(7), obj.Size)
	require.True(t, obj.IsAppendable)

	// blob is authority: read it back, MetaSeq advanced by exactly 2.
	cmd, err := b.readQuorumMetaCmd("bk", "k")
	require.NoError(t, err)
	require.Equal(t, uint64(2), cmd.MetaSeq)
	require.True(t, cmd.IsAppendable && cmd.MetaSeqCAS)
	require.Equal(t, int64(7), cmd.Size)
	require.Len(t, cmd.Segments, 2)

	// At-most-once: a retried append issued at the stale offset (3, which is now
	// behind the current size of 7) must be rejected and must NOT change the
	// object's size or segment count.
	_, retryErr := b.AppendObject(ctx, "bk", "k", 3, bytes.NewReader([]byte("retry")))
	require.ErrorIs(t, retryErr, storage.ErrAppendOffsetMismatch)

	after, err := b.readQuorumMetaCmd("bk", "k")
	require.NoError(t, err)
	require.Equal(t, int64(7), after.Size,
		"object size must not change after stale-offset rejection")
	require.Len(t, after.Segments, 2,
		"segment count must not change after stale-offset rejection")
}

// TestAppendObject_SkewImmune_CASNotModTime proves that an append whose
// candidate ModTime is OLDER than the existing blob's ModTime still commits,
// because the CAS branch keys on MetaSeq+1 (not on ModTime). This locks the
// invariant: NTP clock skew cannot cause a later acknowledged append to be
// silently dropped.
//
// The test:
//  1. Performs a first append (MetaSeq=1) and captures the resulting ModTime.
//  2. Injects a second append by directly calling planAppendObjectBlobRMW with
//     a ModifiedUnixSec that is one hour IN THE PAST (older than the first
//     append's ModTime).
//  3. Writes the resulting cmd via writeQuorumMeta.
//  4. Asserts the write SUCCEEDS (no CAS reject) and the object reflects the
//     appended data, i.e. MetaSeq==2 and Size==first+second.
//
// If the CAS guard were keyed on ModTime (LWW) instead of MetaSeq, step 3
// would either silently overwrite the first blob or be rejected — the test
// captures both failure modes by asserting the state after the write.
func TestAppendObject_SkewImmune_CASNotModTime(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bk"))

	// First append: establishes MetaSeq=1.
	firstBody := []byte("hello")
	_, err := b.AppendObject(ctx, "bk", "skew", 0, bytes.NewReader(firstBody))
	require.NoError(t, err)

	base, err := b.readQuorumMetaCmd("bk", "skew")
	require.NoError(t, err)
	require.Equal(t, uint64(1), base.MetaSeq)

	// Build a second segment with a ModTime far in the past (clock-skew simulation).
	// planAppendObjectBlobRMW must accept this because the CAS check uses MetaSeq,
	// not ModTime.
	pastUnixSec := time.Now().Add(-time.Hour).Unix()
	secondBody := []byte(" world")
	seg, err := b.writeSegmentBlobForAppend("bk", "skew", bytes.NewReader(secondBody))
	require.NoError(t, err)

	cmd, perr := planAppendObjectBlobRMW(appendBlobRMWInput{
		Bucket:           "bk",
		Key:              "skew",
		ExpectedOffset:   int64(len(firstBody)),
		Segment:          seg,
		PlacementGroupID: base.PlacementGroupID,
		VersionID:        base.VersionID,
		ModifiedUnixSec:  pastUnixSec, // skewed OLDER than base.ModTime
		Base:             base,
		BaseExists:       true,
	})
	require.NoError(t, perr, "planAppendObjectBlobRMW must succeed even when ModTime < base.ModTime")
	require.Equal(t, uint64(2), cmd.MetaSeq, "MetaSeq must advance to base+1 regardless of ModTime")
	require.True(t, cmd.MetaSeqCAS, "MetaSeqCAS flag must be set")
	require.Equal(t, pastUnixSec, cmd.ModTime, "ModTime is stored as-is (caller's clock, not compared)")

	// Write via the real CAS path — must succeed even with a stale ModTime.
	require.NoError(t, b.writeQuorumMeta(ctx, cmd),
		"writeQuorumMeta must commit when MetaSeq==base+1 even if new ModTime < existing ModTime")

	// Verify the committed state: both segments present, total size correct.
	after, err := b.readQuorumMetaCmd("bk", "skew")
	require.NoError(t, err)
	require.Equal(t, uint64(2), after.MetaSeq,
		"blob MetaSeq must be 2 after the skewed-clock append commits")
	require.Equal(t, int64(len(firstBody)+len(secondBody)), after.Size,
		"object size must reflect both appended segments even with a skewed ModTime")
	require.Len(t, after.Segments, 2,
		"both segments must be present in the committed manifest")
}

// TestAppendObject_CASRejectsStaleOwnerWrite proves the failover lost-update
// guard (spec §7-A): a stalled owner that read base=1 and then writes a direct
// CAS blob at base+1=2 is rejected with errQuorumMetaCASReject after a
// concurrent append already advanced the blob to MetaSeq=2.
func TestAppendObject_CASRejectsStaleOwnerWrite(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "bk"))

	_, err := b.AppendObject(ctx, "bk", "k", 0, bytes.NewReader([]byte("aaa"))) // MetaSeq=1
	require.NoError(t, err)
	base, err := b.readQuorumMetaCmd("bk", "k") // old owner reads base=1
	require.NoError(t, err)
	require.Equal(t, uint64(1), base.MetaSeq)

	// A concurrent writer advances the blob to MetaSeq=2.
	_, err = b.AppendObject(ctx, "bk", "k", 3, bytes.NewReader([]byte("bbb"))) // MetaSeq=2
	require.NoError(t, err)

	// Old owner resumes: a direct CAS write at base+1=2 must be rejected because
	// existing is already MetaSeq=2.
	stale := base
	stale.MetaSeqCAS = true
	stale.MetaSeq = base.MetaSeq + 1
	stale.Segments = append(append([]SegmentMetaEntry(nil), base.Segments...), SegmentMetaEntry{BlobID: "late"})
	err = b.writeQuorumMeta(ctx, stale)
	require.ErrorIs(t, err, errQuorumMetaCASReject)
}
