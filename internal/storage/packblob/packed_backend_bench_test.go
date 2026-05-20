package packblob

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

func BenchmarkPackblob_SaveIndex(b *testing.B) {
	for _, size := range []int{100, 10_000, 100_000} {
		b.Run(fmt.Sprintf("entries=%d", size), func(b *testing.B) {
			dir := b.TempDir()
			pb := newBenchPackedBackend(b, dir)
			for i := 0; i < size; i++ {
				pk := packedKey{
					bucket: fmt.Sprintf("bucket-%d", i%16),
					key:    fmt.Sprintf("object-%010d", i),
				}
				e := &indexEntry{
					Location:     BlobLocation{BlobID: uint64(i / 1000), Offset: uint64(i * 4096), Length: 1234},
					OriginalSize: 1234,
					ContentType:  "application/octet-stream",
					ETag:         "0123456789abcdef",
					LastModified: 1700000000,
					UserMetadata: map[string]string{
						"x-amz-meta-k1": "v1",
						"x-amz-meta-k2": "v2",
						"x-amz-meta-k3": "v3",
						"x-amz-meta-k4": "v4",
					},
					SSEAlgorithm: "AES256",
				}
				e.Refcount.Store(1)
				pb.index.Store(pk, e)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := pb.SaveIndex(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPackedBackend_ListObjectsPage_LargePackedIndex(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			pb, _ := setupPackedBackend(b, n)
			b.Cleanup(func() { _ = pb.Close() })

			ctx := context.Background()
			marker := "key-50000"
			if n < 100_000 {
				marker = "key-500"
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				objs, _, err := pb.ListObjectsPage(ctx, "bench", "key-", marker, 1000)
				if err != nil {
					b.Fatal(err)
				}
				if len(objs) == 0 {
					b.Fatal("empty page")
				}
			}
		})
	}
}

func BenchmarkReadPackedCandidateReusable_SizedSmall(b *testing.B) {
	const threshold = 1024 * 1024
	body := bytes.Repeat([]byte("x"), 64*1024)

	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	var r bytes.Reader
	for i := 0; i < b.N; i++ {
		r.Reset(body)
		got, large, pooled, err := readPackedCandidateReusable(&r, threshold)
		if err != nil {
			b.Fatal(err)
		}
		if large {
			b.Fatal("candidate unexpectedly routed as large")
		}
		if len(got) != len(body) {
			b.Fatalf("len=%d want %d", len(got), len(body))
		}
		if pooled {
			releasePackedCandidateBuffer(got)
		}
	}
}

func newBenchPackedBackend(b *testing.B, dir string) *PackedBackend {
	b.Helper()
	// Same pattern as newTestPackedBackend (test file :19), but accepts a
	// b *testing.B and a pre-allocated dir for predictable bench fixtures.
	inner, err := storage.NewLocalBackend(dir + "/local")
	if err != nil {
		b.Fatal(err)
	}
	pb, err := NewPackedBackend(inner, dir+"/blobs", 64*1024)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { pb.Close() })
	return pb
}
