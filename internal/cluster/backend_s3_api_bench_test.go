package cluster

// Cluster-path micro-benchmarks for the S3 APIs that the single-node suite
// (internal/storage/operations_s3_api_micro_bench_test.go) covers only for the
// bare LocalBackend. These drive the EC path: staged parts / append segments ->
// SegmentWriter -> ShardService EC encode + per-shard DEK sealing + quorum-meta,
// all in-process.
//
// HARNESS CAVEAT (all-local): newECBenchmarkBackend points every shard at
// selfAddr, so this measures EC encode + quorum-meta + DEK sealing WITHOUT
// transport/network shard fanout (2 of the 3 cluster-path stages). allocs/op
// and B/op are deterministic and valid; wall-time is NOT a multi-node number.
//
// ENCRYPTION: unlike the single-node LocalBackend (encryptor optional, so that
// suite sweeps plaintext vs encrypted to isolate Seal cost), ShardService
// requires segEnc -- NewShardService panics without it, so there is no plaintext
// EC path to use as a control. The contrast is recovered with an object-size
// sweep: if allocs/op scales ~linearly with object size, the per-1MiB-chunk
// enc.Seal in eccodec.EncodeEncryptedShard (shard_service.go) is un-pooled --
// the same class #893 fixed in writeEncryptedObjectFile via SealTo. Flat
// allocs/op would mean the EC path already pools.

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// BenchmarkClusterMultipart_Complete isolates the EC multipart-complete
// re-encode (runChunkedPutWithParts: read staged parts -> SegmentWriter -> EC
// shards via EncodeEncryptedShard). Create + UploadPart are excluded via
// Stop/StartTimer, matching the single-node BenchmarkS3Multipart_Complete. The
// size sweep is the headline: linear allocs/op growth across 1MiB -> 16MiB =
// un-pooled per-chunk Seal.
func BenchmarkClusterMultipart_Complete(b *testing.B) {
	// S3 requires every part except the last to be >= 5 MiB, so the size sweep
	// stays above 2x5 MiB. 10 MiB -> 32 MiB (3.2x) is enough to read off whether
	// allocs/op scales with object size (un-pooled per-chunk Seal) or stays flat.
	sizes := []struct {
		name  string
		total int
	}{
		{"10MiB", 10 << 20},
		{"32MiB", 32 << 20},
	}
	const numParts = 2
	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			bk := newECBenchmarkBackend(b)
			require.NoError(b, bk.CreateBucket(context.Background(), "bench"))
			partSize := sz.total / numParts
			part := bytes.Repeat([]byte("x"), partSize)
			b.SetBytes(int64(sz.total))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				key := fmt.Sprintf("mpu-%d", i)
				up, err := bk.CreateMultipartUpload(context.Background(), "bench", key, "application/octet-stream")
				require.NoError(b, err)
				parts := make([]storage.Part, 0, numParts)
				for pn := 1; pn <= numParts; pn++ {
					p, err := bk.UploadPart(context.Background(), "bench", key, up.UploadID, pn, bytes.NewReader(part), "")
					require.NoError(b, err)
					parts = append(parts, *p)
				}
				b.StartTimer()
				_, err = bk.CompleteMultipartUpload(context.Background(), "bench", key, up.UploadID, parts)
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkClusterAppend sweeps sequential appends to one EC object, mirroring
// the single-node BenchmarkS3Append. Each op builds a fresh object via N appends
// (readAppendBase decode + segment blob write + manifest re-marshal +
// quorum-meta replication per call). If op/N grows super-linearly in N, cluster
// append carries the same O(N^2) meta re-write the single-node bench found.
func BenchmarkClusterAppend(b *testing.B) {
	const chunk = 64 << 10
	for _, n := range []int{4, 8, 16} {
		b.Run(fmt.Sprintf("appends=%d", n), func(b *testing.B) {
			bk := newECBenchmarkBackend(b)
			// Disable coalesce so the bench isolates the per-append META re-write
			// path (readAppendBase decode + manifest re-marshal + quorum-meta)
			// deterministically. The default SegmentCount=16 trigger fires the
			// background coalesce worker mid-run, adding non-deterministic alloc
			// churn and "context canceled" teardown noise. Production coalesces
			// every 16 segments (resetting N, softening the curve); off, the bench
			// shows the raw per-append meta growth single-node also exhibits.
			bk.SetCoalesceConfig(CoalesceConfig{
				SegmentCount:    1 << 30,
				SizeBytes:       1 << 60,
				IdleTimeout:     1 << 60,
				CleanupInterval: 0,
				SizeCapBytes:    5 << 40,
			})
			require.NoError(b, bk.CreateBucket(context.Background(), "bench"))
			data := bytes.Repeat([]byte("x"), chunk)
			b.SetBytes(int64(chunk * n))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("ap-%d", i)
				var off int64
				for k := 0; k < n; k++ {
					obj, err := bk.AppendObject(context.Background(), "bench", key, off, bytes.NewReader(data))
					require.NoError(b, err)
					off = obj.Size
				}
			}
		})
	}
}
