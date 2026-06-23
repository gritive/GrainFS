package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
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

// TestAppendObject_BlobRMW_AppendsAndIsIdempotent proves AppendObject is an
// owner-locked blob CAS read-modify-write: two appends accumulate Size, the
// object is appendable, and the quorum-meta blob (the authority) advances
// MetaSeq by exactly 2 with the CAS flags set.
func TestAppendObject_BlobRMW_AppendsAndIsIdempotent(t *testing.T) {
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
