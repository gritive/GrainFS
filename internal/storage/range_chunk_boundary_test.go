package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRangeGet_ChunkBoundaries locks the segment-aware Range GET contract at
// the storage layer. HTTP Range goes through LocalBackend.ReadAt (see
// internal/server/object_range_reader.go), so this exercises the same code
// path with all 6 boundary patterns: start of chunk, middle of chunk, end of
// chunk, single-chunk span, cross-chunk boundary, and cross-many-chunks.
//
// Each case is run against both the plaintext and encrypted backends because
// the segment-walk in ReadAt has independent code paths for the two.
func TestRangeGet_ChunkBoundaries(t *testing.T) {
	t.Parallel()

	const chunk = 1 << 20
	const total = 4 * chunk
	data := makePattern(total)

	cases := []struct {
		name     string
		from, to int64
	}{
		{"start_of_chunk", 0, chunk - 1},
		{"middle_of_chunk", chunk / 2, (chunk / 2) + 4096},
		{"end_of_chunk", chunk - 16, chunk - 1},
		{"single_chunk_span", 100, 100 + 1024},
		{"cross_chunk_boundary", chunk - 1024, chunk + 1024},
		{"cross_many_chunks", 100, (3 * chunk) + 100},
	}

	backends := []struct {
		name string
		make func(t *testing.T) *LocalBackend
	}{
		{
			name: "plain",
			make: func(t *testing.T) *LocalBackend { return newTestLocalBackend(t) },
		},
		{
			name: "encrypted",
			make: func(t *testing.T) *LocalBackend {
				b := newDEKLocalBackend(t)
				require.NoError(t, b.CreateBucket(context.Background(), "test"), "CreateBucket")
				return b
			},
		},
	}

	for _, be := range backends {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			b := be.make(t)
			w := NewSegmentWriterWithChunkSize(localBackendAdapter{b}, chunk)
			obj, err := w.Write(context.Background(), "test", "k", "application/octet-stream", bytes.NewReader(data))
			require.NoError(t, err, "segment write")
			require.NoError(t, b.PutObjectRecord(context.Background(), "test", "k", obj), "PutObjectRecord")
			// Sanity: the object should have produced multiple segments so
			// the boundary cases below actually cross chunk boundaries.
			head, err := b.HeadObject(context.Background(), "test", "k")
			require.NoError(t, err, "HeadObject")
			require.GreaterOrEqual(t, len(head.Segments), 4)

			for _, tc := range cases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					length := tc.to - tc.from + 1
					got := make([]byte, length)
					n, err := b.ReadAt(context.Background(), "test", "k", tc.from, got)
					require.NoError(t, err, "ReadAt(%d, len=%d): n=%d", tc.from, length, n)
					require.Equal(t, length, int64(n), "ReadAt(%d, len=%d) short read", tc.from, length)
					want := data[tc.from : tc.to+1]
					require.True(t, bytes.Equal(got, want), "range %d-%d differs at offset %d", tc.from, tc.to, firstDiff(got, want))
				})
			}
		})
	}
}
