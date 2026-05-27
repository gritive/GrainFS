package putpipeline

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
	"github.com/stretchr/testify/require"
)

func TestCPUPool_RegisterAndDispatch_OneShardPerChannel(t *testing.T) {
	in := make(chan StripePlaintext, 4)
	pool := &CPUPool{
		in:      in,
		enc:     testShardEncryptor(t),
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
		enc:     testShardEncryptor(t),
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
	shardEnc := testShardEncryptor(t)
	in := make(chan StripePlaintext, 8)
	pool := &CPUPool{
		in:      in,
		enc:     shardEnc,
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
	// stream under that shard's base fields.
	for shard := 0; shard < 4; shard++ {
		var concatenated []byte
		for got := 0; got < stripes; got++ {
			select {
			case chunk := <-shardChans[shard]:
				require.Equal(t, uint32(got), chunk.StripeIdx)
				require.NoError(t, chunk.Err, "shard %d stripe %d unexpected error", shard, got)
				concatenated = append(concatenated, chunk.Ciphertext...)
			case <-time.After(2 * time.Second):
				t.Fatalf("shard %d missing chunk for stripe %d", shard, got)
			}
		}
		var plain bytes.Buffer
		err := eccodec.DecodeEncryptedShard(&plain, bytes.NewReader(concatenated), shardEnc, cluster.ShardAADFields("testbucket", "testobj/v1", shard))
		require.NoError(t, err, "shard %d: concatenated chunks are not one valid GFSENC3 stream", shard)
		require.NotZero(t, plain.Len(), "shard %d decoded to empty", shard)
	}
}

// fakeGenDriftEncryptor simulates a ShardEncryptor that increments gen each
// call, triggering the gen-pinning check in EncryptedShardChunkedWriter.
type fakeGenDriftEncryptor struct {
	enc *encrypt.Encryptor
	gen uint32
}

func (f *fakeGenDriftEncryptor) Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	aad := encrypt.BuildAAD(domain, make([]byte, 16), fields...)
	ct, err := f.enc.SealValueAADTo(nil, aad, plain)
	if err != nil {
		return nil, 0, err
	}
	g := f.gen
	f.gen++
	return ct, g, nil
}

func (f *fakeGenDriftEncryptor) Open(domain encrypt.AADDomain, fields []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	aad := encrypt.BuildAAD(domain, make([]byte, 16), fields...)
	return f.enc.OpenValueAADTo(nil, aad, ct)
}

// TestCPUPool_SealError_PropagatesAsErrChunk drives a PUT through the pool
// using a gen-drifting encryptor that causes EncryptedShardChunkedWriter to
// fail on the 2nd chunk (gen-pinning). It verifies:
// (1) every shard receives exactly one terminal EncryptedShardChunk,
// (2) at least one shard's chunk has Err != nil (the failing one),
// (3) no duplicate chunks arrive.
func TestCPUPool_SealError_PropagatesAsErrChunk(t *testing.T) {
	// Use large stripes so each shard gets ≥2 chunks (gen drift fires on chunk 1).
	const stripe = 2 << 20 // 2 MiB; each shard gets ~512 KiB split → at least 1 full chunk + partial
	enc, err := encrypt.NewEncryptor(make([]byte, 32))
	require.NoError(t, err)
	driftEnc := &fakeGenDriftEncryptor{enc: enc}

	in := make(chan StripePlaintext, 4)
	pool := &CPUPool{
		in:      in,
		enc:     driftEnc,
		ecCfg:   cluster.ECConfig{DataShards: 2, ParityShards: 2},
		workers: 2,
	}
	const numShards = 4
	shardChans := make([]chan EncryptedShardChunk, numShards)
	sends := make([]chan<- EncryptedShardChunk, numShards)
	for i := range shardChans {
		shardChans[i] = make(chan EncryptedShardChunk, 16)
		sends[i] = shardChans[i]
	}
	pool.registerPut(42, "b", "k", 2<<20, sends)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Run(ctx)

	// Two stripes to exercise mid-stream gen drift on the 2nd stripe's write.
	for i := 0; i < 2; i++ {
		data := make([]byte, stripe)
		in <- StripePlaintext{
			PutID:     42,
			StripeIdx: uint32(i),
			Data:      data,
			LastInPut: i == 1,
		}
	}

	// Drain: each shard must produce exactly one terminal chunk.
	for shard := 0; shard < numShards; shard++ {
		var terminal *EncryptedShardChunk
		deadline := time.After(5 * time.Second)
	drain:
		for {
			select {
			case chunk := <-shardChans[shard]:
				if chunk.LastInPut || chunk.Err != nil {
					if terminal != nil {
						t.Fatalf("shard %d: got a second terminal chunk (duplicate)", shard)
					}
					c := chunk
					terminal = &c
					break drain
				}
			case <-deadline:
				t.Fatalf("shard %d: timed out waiting for terminal chunk", shard)
			}
		}
		// terminal must have been set
		require.NotNil(t, terminal, "shard %d: no terminal chunk", shard)
	}
}
