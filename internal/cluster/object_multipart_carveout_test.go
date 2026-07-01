package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestMultipartCompleted_NotFsmCarveout_BlobAuthoritative proves multipart is NO
// LONGER an FSM carve-out: a versioning-enabled multipart complete is now
// blob-authoritative (its per-version blob is written FAIL-CLOSED and is the sole
// authority, identical to a regular versioned PUT). A stale FSM-only multipart
// obj: record with NO per-version blob must therefore NOT resurrect under sole
// authority — it reads 404, exactly like any other non-carve-out versioned record.
// (Greenfield: a live multipart object always has its per-version blob; this guards
// the inverse — an orphaned FSM record can never serve a read.)
func TestMultipartCompleted_NotFsmCarveout_BlobAuthoritative(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "vb"))
	setVersioningForTest(t, b, "vb", "Enabled")

	// A stale multipart FSM obj: record with Parts but NO per-version blob.
	seedFSMObject(t, b, "vb", "mpu", "v1", objectMeta{
		Key:   "mpu",
		ETag:  "mp-etag",
		Size:  100,
		Parts: []storage.MultipartPartEntry{{PartNumber: 1, Size: 100, ETag: "p1"}},
	}, true)

	_, _, err := b.headObjectMeta(ctx, "vb", "mpu")
	require.ErrorIs(t, err, storage.ErrObjectNotFound,
		"multipart is blob-authoritative: an FSM-only record (no per-version blob) must not resurrect under blob authority")
}
