package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func newECBenchmarkBackend(b *testing.B) *DistributedBackend {
	b.Helper()

	bk := newTestDistributedBackend(b)
	cfg := ECConfig{DataShards: 4, ParityShards: 2}
	bk.SetECConfig(cfg)

	svc := NewShardService(bk.root, nil)
	allNodes := make([]string, cfg.NumShards())
	for i := range allNodes {
		allNodes[i] = bk.selfAddr
	}
	bk.SetShardService(svc, allNodes)
	require.True(b, bk.ECActive(), "benchmark setup must exercise the EC path")
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
				recCfg, shards, err := bk.getObjectECShardReadersAtShardKey(context.Background(), "bench", resolved.ShardKey, resolved.Record)
				require.NoError(b, err)
				readers := make([]io.Reader, len(shards))
				for i, shard := range shards {
					if shard != nil {
						readers[i] = shard
					}
				}
				err = ECReconstructStreamTo(io.Discard, recCfg, readers)
				closeECShardReaders(shards)
				require.NoError(b, err)
			}
		})
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
			path := filepath.Join(bk.shardSvc.dataDir, bucket, shardKey, fmt.Sprintf("shard_%d", shardIdx))
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
