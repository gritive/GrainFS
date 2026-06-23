package cluster

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
)

func TestQuarantine_FoldedIntoBlob_BlocksGet(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	putTestObjectForRetire(t, b, "b", "k", []byte("data"))
	require.NoError(t, b.QuarantineObject(ctx, "b", "k", "", "corrupt-shard", "scrub"))

	m, err := b.readQuorumMetaCmd("b", "k")
	require.NoError(t, err)
	require.True(t, m.IsQuarantined)
	require.Equal(t, "corrupt-shard", m.QuarantineCause)

	_, _, gerr := b.GetObject(ctx, "b", "k")
	require.ErrorIs(t, gerr, ErrObjectQuarantined)
}

// TestQuarantine_IsMonotonic_PersistsUntilCleared verifies that the quarantine
// flag persists in the quorum-meta blob and is NOT cleared by a read. It can
// only be cleared by a new blob write (QuarantineObject with IsQuarantined=false
// or a fresh PUT that replaces the blob). This test verifies the blob flag
// survives the isObjectQuarantined read path.
func TestQuarantine_IsMonotonic_PersistsUntilCleared(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	putTestObjectForRetire(t, b, "b", "k", []byte("data"))
	require.NoError(t, b.QuarantineObject(ctx, "b", "k", "", "corrupt-shard", "scrub"))

	// First read: quarantined.
	q1, cause1, err := b.isObjectQuarantined("b", "k", "")
	require.NoError(t, err)
	require.True(t, q1)
	require.Equal(t, "corrupt-shard", cause1)

	// Second read: still quarantined (reads do not clear the flag).
	q2, _, err := b.isObjectQuarantined("b", "k", "")
	require.NoError(t, err)
	require.True(t, q2, "quarantine must persist across reads")

	// Clear by writing a new blob via writeQuorumMeta.
	cmd, err := b.readQuorumMetaCmd("b", "k")
	require.NoError(t, err)
	cmd.IsQuarantined = false
	cmd.QuarantineCause = ""
	cmd.MetaSeq++
	require.NoError(t, b.writeQuorumMeta(ctx, cmd))

	q3, _, err := b.isObjectQuarantined("b", "k", "")
	require.NoError(t, err)
	require.False(t, q3, "quarantine must be cleared after writeQuorumMeta with IsQuarantined=false")
}

func TestQuarantine_PutOverwriteGuard_RejectsWhenQuarantined(t *testing.T) {
	b := newTestBackendWithQuorumMeta(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))
	putTestObjectForRetire(t, b, "b", "k", []byte("data"))
	require.NoError(t, b.QuarantineObject(ctx, "b", "k", "", "corrupt-shard", "scrub"))
	sz := int64(8)
	_, putErr := b.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:      "b",
		Key:         "k",
		Body:        strings.NewReader("override!"),
		ContentType: "application/octet-stream",
		SizeHint:    &sz,
	})
	require.ErrorIs(t, putErr, ErrObjectQuarantined)
}

// TestClusterCoordinator_QuarantineObject_Forwarded proves the owner-routing
// forward op end-to-end at the coordinator boundary: when the object's owning
// group is NOT locally resolvable (peer "a", self="self"), the coordinator
// forwards ForwardOpSetObjectQuarantine to the owner with the correct args,
// rather than executing the quarantine SET locally. This is the path the
// scrubber's QuarantineRouter takes from a non-owner node — without it, a
// leaf-local RMW could lose the quarantine flag to a concurrent owner write.
func TestClusterCoordinator_QuarantineObject_Forwarded(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	d.replyByOp[raftpb.ForwardOpSetObjectQuarantine] = buildOKReply()

	err := c.QuarantineObject(context.Background(), "bk", "key.txt", "v9", "corrupt-shard", "scrub-CRC")
	require.NoError(t, err)

	require.Len(t, d.calls, 1, "QuarantineObject must route through forward.Send to the owner")
	require.Equal(t, raftpb.ForwardOpSetObjectQuarantine, d.calls[0].op)
	require.Equal(t, "g1", d.calls[0].gid)

	args := raftpb.GetRootAsSetObjectQuarantineArgs(d.calls[0].args, 0)
	require.Equal(t, "bk", string(args.Bucket()))
	require.Equal(t, "key.txt", string(args.Key()))
	require.Equal(t, "v9", string(args.VersionId()))
	require.Equal(t, "corrupt-shard", string(args.Cause()))
	require.Equal(t, "scrub-CRC", string(args.Reason()))
}
