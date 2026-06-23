package cluster

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

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
