package cluster

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
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
