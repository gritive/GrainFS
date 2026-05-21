package cluster

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func BenchmarkShardServiceWriteLocalShardStream5MiBEncrypted(b *testing.B) {
	key := bytes.Repeat([]byte("k"), 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(b, err)
	payload := bytes.Repeat([]byte("x"), 5<<20)

	for _, tc := range []struct {
		name string
		put  func(context.Context, *ShardService, string, string, int, []byte) error
	}{
		{
			name: "pack-probe",
			put: func(ctx context.Context, svc *ShardService, bucket, key string, shardIdx int, body []byte) error {
				return svc.WriteLocalShardStreamContext(ctx, bucket, key, shardIdx, bytes.NewReader(body))
			},
		},
		{
			name: "sized-no-pack",
			put: func(ctx context.Context, svc *ShardService, bucket, key string, shardIdx int, body []byte) error {
				return svc.WriteLocalShardStreamSizedContext(ctx, bucket, key, shardIdx, bytes.NewReader(body), int64(len(body)))
			},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			svc := NewShardService(
				b.TempDir(),
				nil,
				WithEncryptor(enc),
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
