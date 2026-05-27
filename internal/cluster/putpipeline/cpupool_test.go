package putpipeline

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
	"github.com/stretchr/testify/require"
)

func TestCPUPool_RegisterAndDispatch_OneShardPerChannel(t *testing.T) {
	in := make(chan StripePlaintext, 4)
	pool := &CPUPool{
		in:      in,
		enc:     testEncryptor(t),
		ecCfg:   cluster.ECConfig{DataShards: 2, ParityShards: 2},
		workers: 2,
	}
	shardChans := make([]chan EncryptedShardChunk, 4)
	for i := range shardChans {
		shardChans[i] = make(chan EncryptedShardChunk, 4)
	}
	sends := make([]chan<- EncryptedShardChunk, 4)
	for i := range shardChans {
		sends[i] = shardChans[i]
	}
	pool.registerPut(1, "testbucket", "testkey", 1<<20, sends)

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
		in:      in,
		enc:     testEncryptor(t),
		ecCfg:   cluster.ECConfig{DataShards: 2, ParityShards: 2},
		workers: 4,
	}
	shardChans := make([]chan EncryptedShardChunk, 4)
	sends := make([]chan<- EncryptedShardChunk, 4)
	for i := range shardChans {
		shardChans[i] = make(chan EncryptedShardChunk, 8)
		sends[i] = shardChans[i]
	}
	pool.registerPut(7, "testbucket", "testkey", 5<<20, sends)

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

func TestCPUPool_ConcatenatedShardIsValidGFSENC3(t *testing.T) {
	const stripe = 1 << 20
	enc := testEncryptor(t)
	in := make(chan StripePlaintext, 8)
	pool := &CPUPool{
		in:      in,
		enc:     enc,
		ecCfg:   cluster.ECConfig{DataShards: 2, ParityShards: 2},
		workers: 4,
	}
	shardChans := make([]chan EncryptedShardChunk, 4)
	sends := make([]chan<- EncryptedShardChunk, 4)
	for i := range shardChans {
		shardChans[i] = make(chan EncryptedShardChunk, 16)
		sends[i] = shardChans[i]
	}
	pool.registerPut(11, "testbucket", "testobj/v1", 3<<20, sends)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Run(ctx)

	// Three stripes so the shard file spans multiple stripes — the exact
	// case the per-stripe-writer bug corrupted.
	const stripes = 3
	for i := 0; i < stripes; i++ {
		data := make([]byte, stripe)
		for j := range data {
			data[j] = byte((i*7 + j) % 251)
		}
		in <- StripePlaintext{
			PutID:     11,
			StripeIdx: uint32(i),
			Data:      data,
			LastInPut: i == stripes-1,
		}
	}

	// For each shard, concatenate every chunk's ciphertext in stripe
	// order, then confirm the result is a single decodable GFSENC3
	// stream under that shard's AAD.
	for shard := 0; shard < 4; shard++ {
		var concatenated []byte
		for got := 0; got < stripes; got++ {
			select {
			case chunk := <-shardChans[shard]:
				require.Equal(t, uint32(got), chunk.StripeIdx)
				concatenated = append(concatenated, chunk.Ciphertext...)
			case <-time.After(2 * time.Second):
				t.Fatalf("shard %d missing chunk for stripe %d", shard, got)
			}
		}
		aad := []byte(fmt.Sprintf("testbucket/testobj/v1/%d", shard))
		var plain bytes.Buffer
		err := eccodec.DecodeEncryptedShard(&plain, bytes.NewReader(concatenated), enc, aad)
		require.NoError(t, err, "shard %d: concatenated chunks are not one valid GFSENC3 stream", shard)
		require.NotZero(t, plain.Len(), "shard %d decoded to empty", shard)
	}
}
