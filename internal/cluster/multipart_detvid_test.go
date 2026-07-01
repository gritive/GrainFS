package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func mkV7At(msEpoch int64) string { // build a UUIDv7 with a controlled 48-bit ms timestamp
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(msEpoch)<<16) // top 48 bits = ms
	b[6] = (b[6] & 0x0f) | 0x70                             // version 7
	b[8] = (b[8] & 0x3f) | 0x80                             // RFC4122 variant
	u, _ := uuid.FromBytes(b[:])
	return u.String()
}

func TestDeriveMultipartVID_DeterministicAndMsOrdered(t *testing.T) {
	raw := mkV7At(1_000_000)
	v1, err := deriveMultipartVID(raw)
	require.NoError(t, err)
	v2, err := deriveMultipartVID(raw)
	require.NoError(t, err)
	require.Equal(t, v1, v2, "same uploadID → same vid (manifest-independent)")
	require.NotEqual(t, raw, v1, "vid ≠ rawUploadID")
	_, perr := uuid.Parse(v1)
	require.NoError(t, perr)
	// Create-time ordering is MILLISECOND-granular (sub-ms is hash-arbitrary, by design).
	early, _ := deriveMultipartVID(mkV7At(1_000_000))
	late, _ := deriveMultipartVID(mkV7At(2_000_000))
	require.Less(t, early, late, "earlier-ms uploadID → smaller vid")
	other, _ := deriveMultipartVID(mkV7At(3_000_000))
	require.NotEqual(t, v1, other)
}

// TestCompleteMultipartUsesDeterministicVID proves Create mints a UUIDv7
// uploadID and CompleteMultipartUpload tags the finalised object with the
// deterministic det-vid derived from that (raw) uploadID — not a fresh random
// VersionID. Single-node returns a raw (un-prefixed) uploadID, but parse it
// through the coordinator's prefix splitter defensively to recover the raw.
func TestCompleteMultipartUsesDeterministicVID(t *testing.T) {
	b := newSingleNode1Plus0ChunkCapable(t)
	ctx := context.Background()
	require.NoError(t, b.CreateBucket(ctx, "b"))

	up, err := b.CreateMultipartUpload(ctx, "b", "mp.bin", "application/octet-stream")
	require.NoError(t, err)

	// Recover the raw backend uploadID (single-node returns it un-prefixed; the
	// splitter falls back to the whole id when there is no "mpg:" prefix).
	raw := up.UploadID
	if _, parsed, ok := parseMultipartUploadID(up.UploadID); ok {
		raw = parsed
	}
	_, perr := uuid.Parse(raw)
	require.NoError(t, perr, "Create must mint a parseable UUID(v7) uploadID")

	part, err := b.UploadPart(ctx, "b", "mp.bin", up.UploadID, 1, bytes.NewReader([]byte("det-vid-body")), "")
	require.NoError(t, err)

	obj, err := b.CompleteMultipartUpload(ctx, "b", "mp.bin", up.UploadID, []storage.Part{*part})
	require.NoError(t, err)

	wantVID, err := deriveMultipartVID(raw)
	require.NoError(t, err)
	require.Equal(t, wantVID, obj.VersionID, "completed object VersionID must be the det-vid derived from the uploadID")
}
