package cluster

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkPutObjectEC measures EC write latency (sequential vs parallel after Phase 1).
// Single-node: all writes go to WriteLocalShard, so delta is minimal.
// Multi-node (real deployment): parallel writes reduce latency from Σ(shard) to max(shard).
func BenchmarkPutObjectEC_Sequential(b *testing.B) {
	bk := newTestDistributedBackend(b)
	require.NoError(b, bk.CreateBucket(context.Background(), "bench"))
	bk.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})

	svc := NewShardService(bk.root, nil)
	allNodes := []string{bk.selfAddr}
	bk.SetShardService(svc, allNodes)

	data := make([]byte, 64*1024) // 64KB object
	b.ResetTimer()
	for b.Loop() {
		_, err := bk.PutObject(context.Background(), "bench", "key", bytes.NewReader(data), "application/octet-stream")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetObjectEC measures EC read latency (sequential vs k-of-n parallel after Phase 1).
func BenchmarkGetObjectEC(b *testing.B) {
	bk := newTestDistributedBackend(b)
	require.NoError(b, bk.CreateBucket(context.Background(), "bench"))
	bk.SetECConfig(ECConfig{DataShards: 4, ParityShards: 2})

	svc := NewShardService(bk.root, nil)
	allNodes := []string{bk.selfAddr}
	bk.SetShardService(svc, allNodes)

	data := make([]byte, 64*1024)
	_, err := bk.PutObject(context.Background(), "bench", "readkey", bytes.NewReader(data), "application/octet-stream")
	require.NoError(b, err)

	b.ResetTimer()
	for b.Loop() {
		rc, _, err := bk.GetObject(context.Background(), "bench", "readkey")
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, rc)
		rc.Close()
	}
}
