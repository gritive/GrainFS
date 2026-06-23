package cluster

import (
	"encoding/binary"
	"testing"

	"github.com/google/uuid"
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
