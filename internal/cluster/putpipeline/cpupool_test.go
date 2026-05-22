package putpipeline

import (
	"context"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/stretchr/testify/require"
)

func TestCPUPool_RegisterAndDispatch_OneShardPerChannel(t *testing.T) {
	in := make(chan StripePlaintext, 4)
	pool := &CPUPool{
		in:       in,
		enc:      testEncryptor(t),
		ecCfg:    cluster.ECConfig{DataShards: 2, ParityShards: 2},
		workers:  2,
		outByPut: make(map[uint64][]chan<- EncryptedShardChunk),
	}
	shardChans := make([]chan EncryptedShardChunk, 4)
	for i := range shardChans {
		shardChans[i] = make(chan EncryptedShardChunk, 4)
	}
	sends := make([]chan<- EncryptedShardChunk, 4)
	for i := range shardChans {
		sends[i] = shardChans[i]
	}
	pool.registerPut(1, sends)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Run(ctx)

	in <- StripePlaintext{
		PutID:     1,
		StripeIdx: 0,
		Data:      make([]byte, 1<<20),
		LastInPut: true,
	}

	for i := 0; i < 4; i++ {
		select {
		case chunk := <-shardChans[i]:
			require.Equal(t, i, chunk.ShardIdx)
			require.Equal(t, uint64(1), chunk.PutID)
			require.True(t, chunk.LastInPut)
		case <-time.After(2 * time.Second):
			t.Fatalf("shard %d did not receive a chunk", i)
		}
	}
}

func TestCPUPool_OrderedPerShard_FiveStripes(t *testing.T) {
	const stripe = 1 << 20
	in := make(chan StripePlaintext, 8)
	pool := &CPUPool{
		in:       in,
		enc:      testEncryptor(t),
		ecCfg:    cluster.ECConfig{DataShards: 2, ParityShards: 2},
		workers:  4,
		outByPut: make(map[uint64][]chan<- EncryptedShardChunk),
	}
	shardChans := make([]chan EncryptedShardChunk, 4)
	sends := make([]chan<- EncryptedShardChunk, 4)
	for i := range shardChans {
		shardChans[i] = make(chan EncryptedShardChunk, 8)
		sends[i] = shardChans[i]
	}
	pool.registerPut(7, sends)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Run(ctx)

	for i := 0; i < 5; i++ {
		in <- StripePlaintext{
			PutID:     7,
			StripeIdx: uint32(i),
			Data:      make([]byte, stripe),
			LastInPut: i == 4,
		}
	}
	for shard := 0; shard < 4; shard++ {
		for want := uint32(0); want < 5; want++ {
			select {
			case chunk := <-shardChans[shard]:
				require.Equal(t, want, chunk.StripeIdx, "shard %d expected stripe %d", shard, want)
			case <-time.After(2 * time.Second):
				t.Fatalf("shard %d did not receive stripe %d", shard, want)
			}
		}
	}
}
