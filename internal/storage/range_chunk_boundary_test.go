package storage

import (
	"bytes"
	"context"
	"testing"
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

	const total = 64 << 20 // 4 segments × 16 MiB
	data := makePattern(total)

	cases := []struct {
		name     string
		from, to int64
	}{
		{"start_of_chunk", 0, (16 << 20) - 1},
		{"middle_of_chunk", 1 << 20, (1 << 20) + 4096},
		{"end_of_chunk", (16 << 20) - 16, (16 << 20) - 1},
		{"single_chunk_span", 100, 100 + 1024},
		{"cross_chunk_boundary", (16 << 20) - 1024, (16 << 20) + 1024},
		{"cross_many_chunks", 100, (48 << 20) + 100},
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
				enc := testEncryptor(t)
				b, err := NewEncryptedLocalBackend(t.TempDir(), enc)
				if err != nil {
					t.Fatalf("NewEncryptedLocalBackend: %v", err)
				}
				t.Cleanup(func() { _ = b.Close() })
				if err := b.CreateBucket(context.Background(), "test"); err != nil {
					t.Fatalf("CreateBucket: %v", err)
				}
				return b
			},
		},
	}

	for _, be := range backends {
		be := be
		t.Run(be.name, func(t *testing.T) {
			t.Parallel()
			b := be.make(t)
			if _, err := b.PutObject(context.Background(), "test", "k", bytes.NewReader(data), "application/octet-stream"); err != nil {
				t.Fatalf("PutObject: %v", err)
			}
			// Sanity: the object should have produced multiple segments so
			// the boundary cases below actually cross chunk boundaries.
			head, err := b.HeadObject(context.Background(), "test", "k")
			if err != nil {
				t.Fatalf("HeadObject: %v", err)
			}
			if len(head.Segments) < 4 {
				t.Fatalf("expected >=4 segments, got %d", len(head.Segments))
			}

			for _, tc := range cases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					length := tc.to - tc.from + 1
					got := make([]byte, length)
					n, err := b.ReadAt(context.Background(), "test", "k", tc.from, got)
					if err != nil {
						t.Fatalf("ReadAt(%d, len=%d): n=%d err=%v", tc.from, length, n, err)
					}
					if int64(n) != length {
						t.Fatalf("ReadAt(%d, len=%d) short read: n=%d", tc.from, length, n)
					}
					want := data[tc.from : tc.to+1]
					if !bytes.Equal(got, want) {
						t.Fatalf("range %d-%d differs at offset %d", tc.from, tc.to, firstDiff(got, want))
					}
				})
			}
		})
	}
}
