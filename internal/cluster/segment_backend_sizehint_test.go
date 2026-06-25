package cluster

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// A1 (cluster SizeHint): the cluster chunked-PUT path streams an opaque body
// (a spool/network reader with no Len) through the SegmentWriter, which without
// a hint allocates a full DefaultChunkSize (16 MiB) chunk buffer for any object.
// putObjectChunked/relocate/multipart-complete all know the object size, so the
// clusterSegmentBackend carries it as sizeHint; runChunkedPut must thread it to
// SegmentWriter.WriteSized so a small object sizes its chunk to the object.
func TestRunChunkedPut_SizeHintRightSizesChunks(t *testing.T) {
	const objSize = 256 << 10
	payload := bytes.Repeat([]byte("x"), objSize)
	deps := newFakeBackendWithGroups(fourPGFixture())
	blobIDs := []string{uuid.Must(uuid.NewV7()).String()} // 256 KiB < 16 MiB → 1 segment
	csb := newCSBWithDeps(deps, blobIDs)                  // chunkSize 0 → DefaultChunkSize (16 MiB)
	csb.sizeHint = int64(objSize)

	perOp := allocBytesPerRunForTest(t, 5, func() error {
		_, err := runChunkedPut(context.Background(), csb,
			&noSeekReader{r: bytes.NewReader(payload)},
			"bucket", "k", "v1", "application/octet-stream",
			nil, "", 0, false, "", nil, nil, nil)
		return err
	})
	// Without threading: a 16 MiB chunk buffer (plus EC encode of 16 MiB).
	// With it: ~objSize. 4 MiB leaves wide margin for EC/transport overhead.
	t.Logf("cluster chunked PUT (256 KiB, hinted) per-op alloc: %d bytes", perOp)
	require.Less(t, perOp, uint64(4<<20), "per-op alloc bytes too high: %d", perOp)
}
