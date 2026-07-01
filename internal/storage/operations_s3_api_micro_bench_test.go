package storage

// Micro-benchmarks for S3 operation APIs NOT covered by
// operations_s3_micro_bench_test.go (which covers PUT/GET/HEAD/DELETE/COPY).
//
// Every bench here drives the bare single-node path: storage.Operations over a
// LocalBackend. Production-path caveats per op:
//   - Multipart Complete/UploadPart: this is the SINGLE-NODE path — parts are
//     staged as files and Complete re-encodes them into one encrypted object
//     file (128 KiB records) via the pooled writeEncryptedObjectFile. The
//     CLUSTER multipart-complete path is cluster.runChunkedPutWithParts (EC
//     segments) and is NOT exercised here.
//   - Append: LocalBackend segment-append (adds a segment per call up to
//     MaxAppendSegments=10000), single-node. The sequence sweep checks whether
//     per-append cost stays flat (O(1)/append) or degrades.
//   - ListObjectsPage / tags: single-node LocalBackend; versioning is
//     control-plane (not implemented on the bare backend) so it is omitted.
//
// allocs/op (deterministic) is the headline; wall-time on macOS fsync is noisy.
// Reuses newS3BenchOps / s3BenchPayload / s3BenchBucket / cryptoLabel.

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkS3Multipart_Complete isolates CompleteMultipartUpload — the
// whole-object re-encode (read every staged part, re-write as one object).
// Create + UploadPart are excluded via StopTimer. Cost scales with object size.
func BenchmarkS3Multipart_Complete(b *testing.B) {
	const partSize = 5 << 20
	const numParts = 2
	for _, enc := range []bool{false, true} {
		b.Run(cryptoLabel(enc), func(b *testing.B) {
			ops, _ := newS3BenchOps(b, enc)
			ctx := context.Background()
			part := s3BenchPayload(partSize)
			b.SetBytes(int64(partSize * numParts))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				key := fmt.Sprintf("mpu-%d", i)
				mpu, err := ops.CreateMultipartUpload(ctx, s3BenchBucket, key, "application/octet-stream")
				require.NoError(b, err)
				parts := make([]Part, 0, numParts)
				for pn := 1; pn <= numParts; pn++ {
					p, err := ops.UploadPart(ctx, s3BenchBucket, key, mpu.UploadID, pn, bytes.NewReader(part), "")
					require.NoError(b, err)
					parts = append(parts, *p)
				}
				b.StartTimer()
				_, err = ops.CompleteMultipartUpload(ctx, s3BenchBucket, key, mpu.UploadID, parts)
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkS3Multipart_UploadPart measures a single staged-part write (streaming
// into the part file via the pooled writeEncryptedObjectFile). One upload, a new
// part number each iteration.
func BenchmarkS3Multipart_UploadPart(b *testing.B) {
	const partSize = 5 << 20
	for _, enc := range []bool{false, true} {
		b.Run(cryptoLabel(enc), func(b *testing.B) {
			ops, _ := newS3BenchOps(b, enc)
			ctx := context.Background()
			part := s3BenchPayload(partSize)
			mpu, err := ops.CreateMultipartUpload(ctx, s3BenchBucket, "up", "application/octet-stream")
			require.NoError(b, err)
			b.SetBytes(int64(partSize))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ops.UploadPart(ctx, s3BenchBucket, "up", mpu.UploadID, i+1, bytes.NewReader(part), "")
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkS3Append sweeps the number of sequential appends to one object. Each
// op is a fresh object built by N appends (first appendNew, rest appendExisting,
// each adding a segment). If per-op cost grows faster than linearly in N,
// repeated append is super-linear; if op/N is flat, it is O(1) per append.
func BenchmarkS3Append(b *testing.B) {
	const chunk = 64 << 10
	for _, enc := range []bool{false, true} {
		for _, n := range []int{4, 8, 16} {
			b.Run(fmt.Sprintf("%s/appends=%d", cryptoLabel(enc), n), func(b *testing.B) {
				ops, _ := newS3BenchOps(b, enc)
				ctx := context.Background()
				data := s3BenchPayload(chunk)
				b.SetBytes(int64(chunk * n))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					key := fmt.Sprintf("ap-%d", i)
					var off int64
					for k := 0; k < n; k++ {
						obj, err := ops.AppendObject(ctx, s3BenchBucket, key, off, bytes.NewReader(data))
						require.NoError(b, err)
						off = obj.Size
					}
				}
			})
		}
	}
}

// BenchmarkS3ListObjectsPage measures one S3 LIST page (the real paginated
// edge path) over a populated bucket. populate is excluded via StopTimer.
func BenchmarkS3ListObjectsPage(b *testing.B) {
	const total = 1000
	const pageSize = 100
	ops, backend := newS3BenchOps(b, false)
	ctx := context.Background()
	for i := 0; i < total; i++ {
		_, err := backend.PutObject(ctx, s3BenchBucket, fmt.Sprintf("obj/%06d", i), bytes.NewReader([]byte("x")), "application/octet-stream")
		require.NoError(b, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		objs, _, err := ops.ListObjectsPage(ctx, s3BenchBucket, "obj/", "", pageSize)
		require.NoError(b, err)
		require.Len(b, objs, pageSize)
	}
}

// BenchmarkS3PutObjectTags measures the object-tagging write (control-metadata
// op, no object body rewrite).
func BenchmarkS3PutObjectTags(b *testing.B) {
	ops, _ := newS3BenchOps(b, false)
	ctx := context.Background()
	_, err := ops.PutObjectWithResult(ctx, s3BenchBucket, "tagged", bytes.NewReader([]byte("x")), "application/octet-stream")
	require.NoError(b, err)
	tags := []Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "storage"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, ops.SetObjectTags(s3BenchBucket, "tagged", "", tags))
	}
}

// BenchmarkS3GetObjectTags measures the object-tagging read.
func BenchmarkS3GetObjectTags(b *testing.B) {
	ops, _ := newS3BenchOps(b, false)
	ctx := context.Background()
	_, err := ops.PutObjectWithResult(ctx, s3BenchBucket, "tagged", bytes.NewReader([]byte("x")), "application/octet-stream")
	require.NoError(b, err)
	require.NoError(b, ops.SetObjectTags(s3BenchBucket, "tagged", "", []Tag{{Key: "env", Value: "prod"}}))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ops.GetObjectTags(s3BenchBucket, "tagged", "")
		require.NoError(b, err)
	}
}
