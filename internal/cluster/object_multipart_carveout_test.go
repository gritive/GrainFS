package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

// TestMultipartCompleted_FSMCarveoutReadable proves a multipart-completed object —
// which keeps its FSM obj: record via the retained CmdCompleteMultipart propose —
// stays readable under blob-primary even when its best-effort per-version blob
// mirror is absent: its FSM record is an authoritative carve-out (4th carve-out
// class, alongside appendable/coalesced). Without this a multipart object whose
// mirror lagged/failed would 404.
func TestMultipartCompleted_FSMCarveoutReadable(t *testing.T) {
	ctx := context.Background()
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(ctx, "vb"))
	setVersioningForTest(t, b, "vb", "Enabled")

	// A multipart-completed object: FSM obj: record with Parts, NO per-version blob.
	seedFSMObject(t, b, "vb", "mpu", "v1", objectMeta{
		Key:   "mpu",
		ETag:  "mp-etag",
		Size:  100,
		Parts: []storage.MultipartPartEntry{{PartNumber: 1, Size: 100, ETag: "p1"}},
	}, true)

	obj, _, err := b.headObjectMeta(ctx, "vb", "mpu")
	require.NoError(t, err, "multipart object must be readable via its FSM carve-out when the blob mirror is absent")
	require.NotNil(t, obj)
	require.Equal(t, "mp-etag", obj.ETag)
}
