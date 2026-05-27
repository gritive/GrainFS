package cluster

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/datawal"
)

// BenchmarkShardServiceWriteLocalShardStream_DataWAL_5MiB measures the
// allocation cost of the streaming write path when a data WAL is wired.
// The dataWAL branch currently funnels the body through io.ReadAll regardless
// of whether the caller knows the size; the sized variant should let us
// pre-allocate and skip Buffer doubling.
func BenchmarkShardServiceWriteLocalShardStream_DataWAL_5MiB(b *testing.B) {
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(b, err)
	payload := bytes.Repeat([]byte("x"), 5<<20)

	for _, tc := range []struct {
		name string
		put  func(context.Context, *ShardService, string, string, int, []byte) error
	}{
		{
			name: "unsized",
			put: func(ctx context.Context, svc *ShardService, bucket, key string, shardIdx int, body []byte) error {
				return svc.WriteLocalShardStreamContext(ctx, bucket, key, shardIdx, bytes.NewReader(body))
			},
		},
		{
			name: "sized",
			put: func(ctx context.Context, svc *ShardService, bucket, key string, shardIdx int, body []byte) error {
				return svc.WriteLocalShardStreamSizedContext(ctx, bucket, key, shardIdx, bytes.NewReader(body), int64(len(body)))
			},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			dwal, err := datawal.Open(filepath.Join(dir, "datawal"), storage.NewEncryptorAdapter(enc, make([]byte, 16)), "datawal")
			require.NoError(b, err)
			b.Cleanup(func() { _ = dwal.Close() })

			svc := NewShardService(
				dir,
				nil,
				WithEncryptor(enc),
				WithDataWAL(dwal),
				WithShardPackThreshold(65545),
			)

			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := tc.put(context.Background(), svc, "bench", fmt.Sprintf("obj-%d", i), 0, payload)
				require.NoError(b, err)
			}
		})
	}
}
