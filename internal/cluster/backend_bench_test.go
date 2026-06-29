package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// newECBenchmarkBackend builds an all-local EC 4+2 DistributedBackend. It takes
// clusterTestTB so both benchmarks (*testing.B) and round-trip tests (*testing.T)
// can exercise the same EC write/read wiring.
func newECBenchmarkBackend(tb clusterTestTB) *DistributedBackend {
	tb.Helper()

	bk := newTestDistributedBackend(tb)
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	bk.SetECConfig(cfg)

	keeper, clusterID := testDEKKeeper(tb)
	svc := NewShardService(bk.root, nil, WithShardDEKKeeper(keeper, clusterID))
	allNodes := make([]string, cfg.NumShards())
	for i := range allNodes {
		allNodes[i] = bk.selfAddr
	}
	bk.SetShardService(svc, allNodes)
	require.True(tb, bk.ECActive(), "EC setup must exercise the EC path")
	return bk
}

func newChunkedECBenchmarkBackend(tb clusterTestTB) *DistributedBackend {
	tb.Helper()
	bk := newECBenchmarkBackend(tb)
	cfg := bk.currentECConfig()
	peers := make([]string, cfg.NumShards())
	for i := range peers {
		peers[i] = bk.selfAddr
	}
	bk.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-a": {ID: "group-a", PeerIDs: peers},
	}})
	return bk
}

// BenchmarkPutObjectEC measures the local EC write path with a full 4+2 stripe.
func BenchmarkPutObjectEC_Sequential(b *testing.B) {
	cases := []struct {
		name string
		size int
	}{
		{"64KiB", 64 << 10},
		{"1MiB", 1 << 20},
		{"16MiB", 16 << 20},
		{"64MiB", 64 << 20},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			bk := newECBenchmarkBackend(b)
			require.NoError(b, bk.CreateBucket(context.Background(), "bench"))

			data := make([]byte, tc.size)
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				_, err := bk.PutObject(context.Background(), "bench", "key", bytes.NewReader(data), "application/octet-stream")
				require.NoError(b, err)
			}
		})
	}
}

func BenchmarkPutObjectEC_Chunked10MiB(b *testing.B) {
	for _, tc := range []struct {
		name      string
		exactHint bool
	}{
		{name: "spooled"},
		{name: "exact-size-stream", exactHint: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			bk := newChunkedECBenchmarkBackend(b)
			require.NoError(b, bk.CreateBucket(context.Background(), "bench"))

			data := make([]byte, 10<<20)
			size := int64(len(data))
			b.SetBytes(size)
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				_, err := bk.PutObjectWithRequest(context.Background(), storage.PutObjectRequest{
					Bucket:        "bench",
					Key:           "key",
					Body:          bytes.NewReader(data),
					SizeHint:      &size,
					SizeHintExact: tc.exactHint,
					ContentType:   "application/octet-stream",
				})
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkPutObjectEC_Spool2plus2_10MiB measures the all-local SPOOL write path
// at EC 2+2 / 10 MiB so it is config- and size-matched to
// putpipeline.BenchmarkPipelinePut10MiB (the STREAM path, also 2+2 / 10 MiB,
// all-local). The pair isolates the spool double-staging penalty (spool_object +
// spool_shards) with no network and no EC-width confound — the conservative
// lower bound on the streaming-EC win (multi-node adds network/ingest overlap on
// top, common to both). putPipeline is NOT enabled here, so a >1 MiB parity
// object takes putObjectECSpooled -> writeSpooledShards.
func BenchmarkPutObjectEC_Spool2plus2_10MiB(b *testing.B) {
	bk := newTestDistributedBackend(b)
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	bk.SetECConfig(cfg)
	keeper, clusterID := testDEKKeeper(b)
	svc := NewShardService(bk.root, nil, WithShardDEKKeeper(keeper, clusterID))
	allNodes := make([]string, cfg.NumShards())
	for i := range allNodes {
		allNodes[i] = bk.selfAddr
	}
	bk.SetShardService(svc, allNodes)
	require.True(b, bk.ECActive())
	require.NoError(b, bk.CreateBucket(context.Background(), "bench"))

	data := make([]byte, 10<<20)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, err := bk.PutObject(context.Background(), "bench", "key", bytes.NewReader(data), "application/octet-stream")
		require.NoError(b, err)
	}
}

func BenchmarkPutObjectSingleLocal5MiB(b *testing.B) {
	bk := newTestDistributedBackend(b)
	require.NoError(b, bk.CreateBucket(context.Background(), "bench"))

	payload := bytes.Repeat([]byte("x"), 5<<20)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bk.PutObject(context.Background(), "bench", "key", bytes.NewReader(payload), "application/octet-stream")
		require.NoError(b, err)
	}
}

// BenchmarkGetObjectEC measures EC read latency (sequential vs k-of-n parallel after Phase 1).
func BenchmarkGetObjectEC(b *testing.B) {
	cases := []struct {
		name string
		size int
	}{
		{"64KiB", 64 << 10},
		{"1MiB", 1 << 20},
		{"16MiB", 16 << 20},
		{"64MiB", 64 << 20},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			bk := newECBenchmarkBackend(b)
			require.NoError(b, bk.CreateBucket(context.Background(), "bench"))

			data := make([]byte, tc.size)
			_, err := bk.PutObject(context.Background(), "bench", "readkey", bytes.NewReader(data), "application/octet-stream")
			require.NoError(b, err)

			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				rc, _, err := bk.GetObject(context.Background(), "bench", "readkey")
				require.NoError(b, err)
				_, _ = io.Copy(io.Discard, rc)
				rc.Close()
			}
		})
	}
}

func BenchmarkGetObjectEC_ChunkedSegment10MiB(b *testing.B) {
	bk := newECBenchmarkBackend(b)
	bk.chunkedPutChunkSize = 10 << 20
	bk.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		"group-a": {ID: "group-a", PeerIDs: []string{bk.selfAddr, bk.selfAddr, bk.selfAddr, bk.selfAddr, bk.selfAddr, bk.selfAddr}},
	}})
	require.NoError(b, bk.CreateBucket(context.Background(), "bench"))

	data := make([]byte, 10<<20)
	sp := makeSpool(b, data)
	_, err := bk.putObjectChunked(context.Background(),
		"bench", "chunked-readkey", "v1", sp, "application/octet-stream",
		nil, "", 0, 0, false, "", nil, nil, nil)
	require.NoError(b, err)

	obj, err := bk.HeadObject(context.Background(), "bench", "chunked-readkey")
	require.NoError(b, err)
	require.Len(b, obj.Segments, 1)
	require.Greater(b, obj.Segments[0].Size, int64(maxECPooledReadObjectSize))

	store := &clusterSegmentStore{b: bk, bucket: "bench", key: "chunked-readkey", obj: obj}
	record, err := store.placementRecord(obj.Segments[0])
	require.NoError(b, err)
	shardKey := "chunked-readkey/segments/" + obj.Segments[0].BlobID

	for _, tc := range []struct {
		name string
		read func(*testing.B)
	}{
		{
			name: "open-segment-stream",
			read: func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					rc, err := store.OpenSegment(context.Background(), obj.Segments[0])
					require.NoError(b, err)
					_, err = io.Copy(io.Discard, rc)
					require.NoError(b, rc.Close())
					require.NoError(b, err)
				}
			},
		},
		{
			name: "legacy-readobject-buffered",
			read: func(b *testing.B) {
				reader := bk.newECObjectReader()
				for i := 0; i < b.N; i++ {
					got, err := reader.ReadObject(context.Background(), "bench", shardKey, record)
					require.NoError(b, err)
					require.Len(b, got, len(data))
				}
			},
		},
		{
			name: "getobject",
			read: func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					rc, _, err := bk.GetObject(context.Background(), "bench", "chunked-readkey")
					require.NoError(b, err)
					_, err = io.Copy(io.Discard, rc)
					require.NoError(b, rc.Close())
					require.NoError(b, err)
				}
			},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			tc.read(b)
		})
	}
}

func BenchmarkGetObjectEC_DirectReconstruct(b *testing.B) {
	cases := []struct {
		name string
		size int
	}{
		{"64KiB", 64 << 10},
		{"1MiB", 1 << 20},
		{"16MiB", 16 << 20},
		{"64MiB", 64 << 20},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			bk := newECBenchmarkBackend(b)
			require.NoError(b, bk.CreateBucket(context.Background(), "bench"))

			data := make([]byte, tc.size)
			_, err := bk.PutObject(context.Background(), "bench", "readkey", bytes.NewReader(data), "application/octet-stream")
			require.NoError(b, err)

			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				_, placementMeta, err := bk.headObjectMeta(context.Background(), "bench", "readkey")
				require.NoError(b, err)
				resolved, err := bk.ResolvePlacement(context.Background(), "bench", "readkey", placementMeta)
				require.NoError(b, err)
				er := bk.newECObjectReader()
				rc, err := er.OpenObject(context.Background(), "bench", resolved.ShardKey, resolved.Record, int64(len(data)))
				require.NoError(b, err)
				_, err = io.Copy(io.Discard, rc)
				_ = rc.Close()
				require.NoError(b, err)
			}
		})
	}
}

func BenchmarkDistributedBackend_ListMultipartUploads(b *testing.B) {
	cases := []struct {
		name       string
		uploads    int
		maxUploads int
	}{
		{"100_all", 100, 0},
		{"1000_all", 1000, 0},
		{"1000_max100", 1000, 100},
		{"10000_max100", 10000, 100},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			bk, db := newTestDistributedBackendWithDB(b)
			require.NoError(b, bk.CreateBucket(context.Background(), "bench"))
			for i := 0; i < tc.uploads; i++ {
				bucket := "bench"
				prefix := "listed/"
				if i%10 == 0 {
					bucket = "other"
				}
				if i%7 == 0 {
					prefix = "else/"
				}
				writeMultipartMeta(b, bk, db, fmt.Sprintf("upload-%06d", i), clusterMultipartMeta{
					Bucket:           bucket,
					Key:              fmt.Sprintf("%sobj-%06d.bin", prefix, i),
					CreatedAt:        int64(i),
					ContentType:      "application/octet-stream",
					PlacementGroupID: "group-1",
				})
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				uploads, err := bk.ListMultipartUploads(context.Background(), "bench", "listed/", tc.maxUploads)
				require.NoError(b, err)
				if tc.maxUploads > 0 && len(uploads) > tc.maxUploads {
					b.Fatalf("got %d uploads, max %d", len(uploads), tc.maxUploads)
				}
			}
		})
	}
}

func writeMultipartMeta(b testing.TB, bk *DistributedBackend, _ *badger.DB, uploadID string, meta clusterMultipartMeta) {
	b.Helper()
	// M2b: manifests live on the .qmeta_mpu blob, not the FSM mpu: key.
	raw, err := marshalClusterMultipartMeta(meta)
	require.NoError(b, err)
	require.NoError(b, bk.shardSvc.writeManifestBlobLocal(meta.Bucket, uploadID, raw))
}

func BenchmarkDistributedBackend_CompleteSinglePartMultipart64KiB(b *testing.B) {
	bk := newTestDistributedBackend(b)
	require.NoError(b, bk.CreateBucket(context.Background(), "bench"))
	data := bytes.Repeat([]byte("x"), 64<<10)

	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		key := fmt.Sprintf("single-part-%d", i)
		up, err := bk.CreateMultipartUpload(context.Background(), "bench", key, "application/octet-stream")
		require.NoError(b, err)
		part, err := bk.UploadPart(context.Background(), "bench", key, up.UploadID, 1, bytes.NewReader(data), "")
		require.NoError(b, err)
		b.StartTimer()

		_, err = bk.CompleteMultipartUpload(context.Background(), "bench", key, up.UploadID, []storage.Part{*part})
		require.NoError(b, err)
	}
}

func BenchmarkGetObjectEC_LocalDataShardRead(b *testing.B) {
	cases := []struct {
		name string
		size int
	}{
		{"64MiB", 64 << 20},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			bk := newECBenchmarkBackend(b)
			require.NoError(b, bk.CreateBucket(context.Background(), "bench"))

			data := make([]byte, tc.size)
			_, err := bk.PutObject(context.Background(), "bench", "readkey", bytes.NewReader(data), "application/octet-stream")
			require.NoError(b, err)
			_, placementMeta, err := bk.headObjectMeta(context.Background(), "bench", "readkey")
			require.NoError(b, err)
			resolved, err := bk.ResolvePlacement(context.Background(), "bench", "readkey", placementMeta)
			require.NoError(b, err)
			recCfg := resolved.Record.ECConfigOrFallback(bk.ecConfig)

			b.SetBytes(int64(len(data)))
			b.Run("sequential", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					require.NoError(b, readLocalECDataShardsSequential(bk, "bench", resolved.ShardKey, recCfg))
				}
			})
			b.Run("parallel_discard", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					require.NoError(b, readLocalECDataShardsParallel(bk, "bench", resolved.ShardKey, recCfg))
				}
			})
			b.Run("parallel_buffered_ordered", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					require.NoError(b, readLocalECDataShardsParallelBufferedOrdered(bk, "bench", resolved.ShardKey, recCfg))
				}
			})
			b.Run("parallel_prealloc_ordered", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					require.NoError(b, readLocalECDataShardsParallelPreallocOrdered(bk, "bench", resolved.ShardKey, recCfg, int64(len(data))))
				}
			})
			b.Run("parallel_pooled_ordered", func(b *testing.B) {
				payloadLen := shardHeaderSize + int((int64(len(data))+int64(recCfg.DataShards)-1)/int64(recCfg.DataShards))
				pool := sync.Pool{
					New: func() any {
						return make([]byte, payloadLen)
					},
				}
				b.ReportAllocs()
				for b.Loop() {
					require.NoError(b, readLocalECDataShardsParallelPooledOrdered(bk, "bench", resolved.ShardKey, recCfg, &pool))
				}
			})
			b.Run("parallel_raw_no_crc", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					require.NoError(b, readLocalECDataShardsParallelRawNoCRC(bk, "bench", resolved.ShardKey, recCfg))
				}
			})
			b.Run("parallel_reuse_open_crc", func(b *testing.B) {
				files, payloadLen, err := openLocalECDataShardFiles(bk, "bench", resolved.ShardKey, recCfg)
				require.NoError(b, err)
				b.Cleanup(func() {
					for _, f := range files {
						_ = f.Close()
					}
				})

				b.ReportAllocs()
				for b.Loop() {
					require.NoError(b, readLocalECDataShardsParallelReuseOpenCRC(files, payloadLen))
				}
			})
			b.Run("parallel_reuse_open_raw_no_crc", func(b *testing.B) {
				files, payloadLen, err := openLocalECDataShardFiles(bk, "bench", resolved.ShardKey, recCfg)
				require.NoError(b, err)
				b.Cleanup(func() {
					for _, f := range files {
						_ = f.Close()
					}
				})

				b.ReportAllocs()
				for b.Loop() {
					require.NoError(b, readLocalECDataShardsParallelReuseOpenRawNoCRC(files, payloadLen))
				}
			})
			b.Run("parallel_reuse_open_direct_crc", func(b *testing.B) {
				files, payloadLen, err := openLocalECDataShardFiles(bk, "bench", resolved.ShardKey, recCfg)
				require.NoError(b, err)
				b.Cleanup(func() {
					for _, f := range files {
						_ = f.Close()
					}
				})
				pool := sync.Pool{
					New: func() any {
						return make([]byte, payloadLen)
					},
				}

				b.ReportAllocs()
				for b.Loop() {
					require.NoError(b, readLocalECDataShardsParallelReuseOpenDirectCRC(files, payloadLen, &pool))
				}
			})
		})
	}
}

func readLocalECDataShardsSequential(bk *DistributedBackend, bucket, shardKey string, cfg ECConfig) error {
	for i := 0; i < cfg.DataShards; i++ {
		r, err := bk.shardSvc.OpenLocalShard(bucket, shardKey, i)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(io.Discard, r)
		closeErr := r.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func readLocalECDataShardsParallel(bk *DistributedBackend, bucket, shardKey string, cfg ECConfig) error {
	var g errgroup.Group
	for i := 0; i < cfg.DataShards; i++ {
		shardIdx := i
		g.Go(func() error {
			r, err := bk.shardSvc.OpenLocalShard(bucket, shardKey, shardIdx)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(io.Discard, r)
			closeErr := r.Close()
			if copyErr != nil {
				return copyErr
			}
			return closeErr
		})
	}
	return g.Wait()
}

func readLocalECDataShardsParallelBufferedOrdered(bk *DistributedBackend, bucket, shardKey string, cfg ECConfig) error {
	buffers := make([][]byte, cfg.DataShards)
	var g errgroup.Group
	for i := 0; i < cfg.DataShards; i++ {
		shardIdx := i
		g.Go(func() error {
			r, err := bk.shardSvc.OpenLocalShard(bucket, shardKey, shardIdx)
			if err != nil {
				return err
			}
			data, readErr := io.ReadAll(r)
			closeErr := r.Close()
			if readErr != nil {
				return readErr
			}
			if closeErr != nil {
				return closeErr
			}
			buffers[shardIdx] = data
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	for _, data := range buffers {
		if _, err := io.Copy(io.Discard, bytes.NewReader(data)); err != nil {
			return err
		}
	}
	return nil
}

func readLocalECDataShardsParallelPreallocOrdered(bk *DistributedBackend, bucket, shardKey string, cfg ECConfig, objectSize int64) error {
	payloadLen := shardHeaderSize + int((objectSize+int64(cfg.DataShards)-1)/int64(cfg.DataShards))
	buffers := make([][]byte, cfg.DataShards)
	var g errgroup.Group
	for i := 0; i < cfg.DataShards; i++ {
		shardIdx := i
		g.Go(func() error {
			r, err := bk.shardSvc.OpenLocalShard(bucket, shardKey, shardIdx)
			if err != nil {
				return err
			}
			data := make([]byte, payloadLen)
			_, readErr := io.ReadFull(r, data)
			closeErr := r.Close()
			if readErr != nil {
				return readErr
			}
			if closeErr != nil {
				return closeErr
			}
			buffers[shardIdx] = data
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	for _, data := range buffers {
		if _, err := io.Copy(io.Discard, bytes.NewReader(data)); err != nil {
			return err
		}
	}
	return nil
}

func readLocalECDataShardsParallelPooledOrdered(bk *DistributedBackend, bucket, shardKey string, cfg ECConfig, pool *sync.Pool) error {
	buffers := make([][]byte, cfg.DataShards)
	var g errgroup.Group
	for i := 0; i < cfg.DataShards; i++ {
		shardIdx := i
		g.Go(func() error {
			r, err := bk.shardSvc.OpenLocalShard(bucket, shardKey, shardIdx)
			if err != nil {
				return err
			}
			data := pool.Get().([]byte)
			_, readErr := io.ReadFull(r, data)
			closeErr := r.Close()
			if readErr != nil {
				pool.Put(data)
				return readErr
			}
			if closeErr != nil {
				pool.Put(data)
				return closeErr
			}
			buffers[shardIdx] = data
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		for _, data := range buffers {
			if data != nil {
				pool.Put(data)
			}
		}
		return err
	}
	for _, data := range buffers {
		if _, err := io.Copy(io.Discard, bytes.NewReader(data)); err != nil {
			for _, data := range buffers {
				if data != nil {
					pool.Put(data)
				}
			}
			return err
		}
		pool.Put(data)
	}
	return nil
}

func readLocalECDataShardsParallelRawNoCRC(bk *DistributedBackend, bucket, shardKey string, cfg ECConfig) error {
	var g errgroup.Group
	for i := 0; i < cfg.DataShards; i++ {
		shardIdx := i
		g.Go(func() error {
			path := mustShardPath(bk.shardSvc, bucket, shardKey, shardIdx)
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			info, statErr := f.Stat()
			if statErr != nil {
				_ = f.Close()
				return statErr
			}
			payloadLen := info.Size() - 8 - 4
			if payloadLen < 0 {
				_ = f.Close()
				return io.ErrUnexpectedEOF
			}
			if _, err := f.Seek(8, io.SeekStart); err != nil {
				_ = f.Close()
				return err
			}
			_, copyErr := io.CopyN(io.Discard, f, payloadLen)
			closeErr := f.Close()
			if copyErr != nil {
				return copyErr
			}
			return closeErr
		})
	}
	return g.Wait()
}

func openLocalECDataShardFiles(bk *DistributedBackend, bucket, shardKey string, cfg ECConfig) ([]*os.File, int64, error) {
	files := make([]*os.File, 0, cfg.DataShards)
	var payloadLen int64 = -1
	for i := 0; i < cfg.DataShards; i++ {
		path := mustShardPath(bk.shardSvc, bucket, shardKey, i)
		f, err := os.Open(path)
		if err != nil {
			for _, f := range files {
				_ = f.Close()
			}
			return nil, 0, err
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			for _, f := range files {
				_ = f.Close()
			}
			return nil, 0, err
		}
		shardPayloadLen := info.Size() - 8 - 4
		if shardPayloadLen < 0 {
			_ = f.Close()
			for _, f := range files {
				_ = f.Close()
			}
			return nil, 0, io.ErrUnexpectedEOF
		}
		if payloadLen < 0 {
			payloadLen = shardPayloadLen
		}
		files = append(files, f)
	}
	return files, payloadLen, nil
}

func readLocalECDataShardsParallelReuseOpenCRC(files []*os.File, payloadLen int64) error {
	var g errgroup.Group
	for _, f := range files {
		file := f
		g.Go(func() error {
			if _, err := file.Seek(8, io.SeekStart); err != nil {
				return err
			}
			r := eccodec.NewSizedShardReader(file, payloadLen)
			_, err := io.Copy(io.Discard, r)
			return err
		})
	}
	return g.Wait()
}

func readLocalECDataShardsParallelReuseOpenRawNoCRC(files []*os.File, payloadLen int64) error {
	var g errgroup.Group
	for _, f := range files {
		file := f
		g.Go(func() error {
			if _, err := file.Seek(8, io.SeekStart); err != nil {
				return err
			}
			_, err := io.CopyN(io.Discard, file, payloadLen)
			return err
		})
	}
	return g.Wait()
}

func readLocalECDataShardsParallelReuseOpenDirectCRC(files []*os.File, payloadLen int64, pool *sync.Pool) error {
	var g errgroup.Group
	for _, f := range files {
		file := f
		g.Go(func() error {
			buf := pool.Get().([]byte)
			if int64(len(buf)) != payloadLen {
				buf = buf[:payloadLen]
			}
			if _, err := file.ReadAt(buf, 8); err != nil {
				pool.Put(buf)
				return err
			}
			var footer [4]byte
			if _, err := file.ReadAt(footer[:], 8+payloadLen); err != nil {
				pool.Put(buf)
				return err
			}
			if crc32.ChecksumIEEE(buf) != binary.LittleEndian.Uint32(footer[:]) {
				pool.Put(buf)
				return eccodec.ErrCRCMismatch
			}
			pool.Put(buf)
			return nil
		})
	}
	return g.Wait()
}
