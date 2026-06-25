package cluster

// Cluster-path micro-benchmarks for the read/metadata S3 APIs the single-node
// suite (internal/storage/operations_s3_micro_bench_test.go) covers only for the
// bare LocalBackend: HEAD, DELETE, ListObjectsPage, object tagging, and COPY.
// These drive the EC cluster path (DistributedBackend over quorum-meta + EC
// shards) so we can compare allocation against the single-node numbers.
//
// HARNESS CAVEAT (all-local): newECBenchmarkBackend points every shard at
// selfAddr, so HEAD/DELETE/list/tags exercise quorum-meta read/write in-process
// and COPY exercises EC read+re-encode in-process, WITHOUT transport/network
// shard fanout. allocs/op and B/op are deterministic and valid; wall-time is NOT
// a multi-node number.

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// BenchmarkClusterHead measures HeadObject (quorum-meta blob read + decode) on an
// EC object. Mirrors the single-node BenchmarkS3Head.
func BenchmarkClusterHead(b *testing.B) {
	bk := newECBenchmarkBackend(b)
	require.NoError(b, bk.CreateBucket(context.Background(), "bench"))
	_, err := bk.PutObject(context.Background(), "bench", "key", bytes.NewReader(bytes.Repeat([]byte("x"), 4<<10)), "application/octet-stream")
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bk.HeadObject(context.Background(), "bench", "key")
		require.NoError(b, err)
	}
}

// BenchmarkClusterDelete measures DeleteObject (meta tombstone + shard cleanup)
// on an EC object. Per-iter re-PUT is excluded via Stop/StartTimer, like the
// single-node benchDelete.
func BenchmarkClusterDelete(b *testing.B) {
	bk := newECBenchmarkBackend(b)
	require.NoError(b, bk.CreateBucket(context.Background(), "bench"))
	data := bytes.Repeat([]byte("x"), 4<<10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		key := fmt.Sprintf("k-%d", i)
		_, err := bk.PutObject(context.Background(), "bench", key, bytes.NewReader(data), "application/octet-stream")
		require.NoError(b, err)
		b.StartTimer()
		require.NoError(b, bk.DeleteObject(context.Background(), "bench", key))
	}
}

// BenchmarkClusterListObjectsPage measures one LIST page over a populated EC
// bucket (quorum-meta scan). populate is excluded via StopTimer. Fewer objects
// than the single-node bench (each PUT is a full EC stripe) but enough to fill
// one 100-key page.
func BenchmarkClusterListObjectsPage(b *testing.B) {
	const total = 300
	const pageSize = 100
	bk := newECBenchmarkBackend(b)
	require.NoError(b, bk.CreateBucket(context.Background(), "bench"))
	for i := 0; i < total; i++ {
		_, err := bk.PutObject(context.Background(), "bench", fmt.Sprintf("obj/%06d", i), bytes.NewReader([]byte("x")), "application/octet-stream")
		require.NoError(b, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		objs, _, err := bk.ListObjectsPage(context.Background(), "bench", "obj/", "", pageSize)
		require.NoError(b, err)
		require.Len(b, objs, pageSize)
	}
}

// BenchmarkClusterPutObjectTags measures the object-tagging write (control
// metadata, no object body rewrite) on an EC object.
func BenchmarkClusterPutObjectTags(b *testing.B) {
	bk := newECBenchmarkBackend(b)
	require.NoError(b, bk.CreateBucket(context.Background(), "bench"))
	_, err := bk.PutObject(context.Background(), "bench", "tagged", bytes.NewReader([]byte("x")), "application/octet-stream")
	require.NoError(b, err)
	tags := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "storage"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, bk.SetObjectTags("bench", "tagged", "", tags))
	}
}

// BenchmarkClusterGetObjectTags measures the object-tagging read on an EC object.
// It uses a VERSIONING-ENABLED bucket on purpose: under blob-authoritative reads
// (versioning Enabled) GetObjectTags derives tags from the per-version blob,
// which this backend-level harness populates via bk.PutObject. On a non-versioned
// bucket GetObjectTags instead reads the FSM ObjectMetaKey, which the bare
// bk.PutObject does NOT populate the way the full-stack server PUT does (it
// returns ErrObjectNotFound here even though SetObjectTags/HeadObject succeed) —
// confirmed NOT a product bug: the e2e Cluster4Node tagging round-trip passes.
// Measuring the non-versioned FSM-read path would need a full-stack server
// harness, out of scope for this micro-bench.
func BenchmarkClusterGetObjectTags(b *testing.B) {
	bk := newECBenchmarkBackend(b)
	require.NoError(b, bk.CreateBucket(context.Background(), "bench"))
	require.NoError(b, bk.SetBucketVersioning("bench", "Enabled"))
	_, err := bk.PutObject(context.Background(), "bench", "tagged", bytes.NewReader([]byte("x")), "application/octet-stream")
	require.NoError(b, err)
	require.NoError(b, bk.SetObjectTags("bench", "tagged", "", []storage.Tag{{Key: "env", Value: "prod"}}))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bk.GetObjectTags("bench", "tagged", "")
		require.NoError(b, err)
	}
}

// BenchmarkClusterCopy measures CopyObject on the EC path: Operations.CopyObject
// over a DistributedBackend reads the source (EC reconstruct) and re-encodes the
// destination (EC write). Mirrors the single-node BenchmarkS3Copy; the size
// sweep shows how copy allocation scales with object size on the EC path.
func BenchmarkClusterCopy(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"1MiB", 1 << 20},
		{"16MiB", 16 << 20},
	}
	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			bk := newECBenchmarkBackend(b)
			ops := storage.NewOperations(bk)
			require.NoError(b, bk.CreateBucket(context.Background(), "bench"))
			payload := bytes.Repeat([]byte("x"), sz.size)
			_, err := bk.PutObject(context.Background(), "bench", "src", bytes.NewReader(payload), "application/octet-stream")
			require.NoError(b, err)
			req := storage.CopyObjectRequest{
				Source:      storage.ObjectRef{Bucket: "bench", Key: "src"},
				Destination: storage.ObjectRef{Bucket: "bench", Key: "dst"},
			}
			b.SetBytes(int64(sz.size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ops.CopyObjectWithResult(context.Background(), req)
				require.NoError(b, err)
			}
		})
	}
}
