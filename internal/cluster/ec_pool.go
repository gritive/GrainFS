package cluster

import (
	"sync"

	"github.com/klauspost/reedsolomon"
)

// encoderEntry caches a reedsolomon.Encoder for a given ECConfig.
type encoderEntry struct {
	enc reedsolomon.Encoder
}

// encoderCache maps ECConfig → *encoderEntry.
var encoderCache sync.Map

func getEncoder(cfg ECConfig) (reedsolomon.Encoder, error) {
	if v, ok := encoderCache.Load(cfg); ok {
		return v.(*encoderEntry).enc, nil
	}
	enc, err := reedsolomon.New(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return nil, err
	}
	entry := &encoderEntry{enc: enc}
	// Store-or-load to handle concurrent first calls for the same config.
	actual, _ := encoderCache.LoadOrStore(cfg, entry)
	return actual.(*encoderEntry).enc, nil
}

// streamEncoderKey caches a reedsolomon stream encoder per (config, block size).
type streamEncoderKey struct {
	cfg       ECConfig
	blockSize int
}

// streamEncoderCache maps streamEncoderKey → reedsolomon.StreamEncoder.
var streamEncoderCache sync.Map

const maxECSplitPaddingPoolBytes = 64 << 20

var ecSplitPaddingPool sync.Pool

func getECSplitPaddingShards(count, shardSize int) [][]byte {
	if count <= 0 || shardSize <= 0 {
		return nil
	}
	if v := ecSplitPaddingPool.Get(); v != nil {
		shardsp := v.(*[][]byte)
		shards := *shardsp
		if len(shards) == count {
			ok := true
			for i := range shards {
				if cap(shards[i]) < shardSize {
					ok = false
					break
				}
			}
			if ok {
				for i := range shards {
					shards[i] = shards[i][:shardSize]
				}
				return shards
			}
		}
	}
	return reedsolomon.AllocAligned(count, shardSize)
}

func putECSplitPaddingShards(shards [][]byte) {
	if len(shards) == 0 {
		return
	}
	totalCap := 0
	for i := range shards {
		totalCap += cap(shards[i])
		shards[i] = shards[i][:cap(shards[i])]
	}
	if totalCap > maxECSplitPaddingPoolBytes {
		return
	}
	ecSplitPaddingPool.Put(&shards)
}

// getStreamEncoder returns a reusable reedsolomon stream encoder for cfg at
// blockSize. A reedsolomon stream encoder owns an internal sync.Pool of aligned
// per-block scratch buffers (AllocAligned); building a fresh encoder per PUT (as
// spoolECShards did) means that pool never survives the call, so every encode
// re-AllocAligns its scratch — the largest single allocation on the EC write
// path. Sharing one encoder per key lets the internal pool be reused across
// PUTs (GC may still drop it between collections, so allocation drops a lot, not
// to zero). reedsolomon's stream Encode keeps no per-call state on the encoder
// (its only mutable state is the concurrent-safe internal pool), so one encoder
// is safe to share across concurrent PUTs — the same property getEncoder relies
// on, confirmed by TestStreamEncoderPool_ConcurrentRoundTrip under -race.
//
// Only the two clamped block sizes (minECStreamBlockSize for small objects,
// defaultECStreamBlockSize for objects whose per-shard size exceeds the default)
// are cached: ecStreamBlockSize returns a continuous value for the mid band, so
// caching those would grow the map unboundedly. Mid-band PUTs build a fresh
// encoder, exactly as before — no behavior change, and the block size is never
// altered (no byte-identity assumption).
func getStreamEncoder(cfg ECConfig, blockSize int) (reedsolomon.StreamEncoder, error) {
	newStream := func() (reedsolomon.StreamEncoder, error) {
		return reedsolomon.NewStream(cfg.DataShards, cfg.ParityShards, reedsolomon.WithStreamBlockSize(blockSize))
	}
	if blockSize != minECStreamBlockSize && blockSize != defaultECStreamBlockSize {
		return newStream() // mid-band continuous size: not cached (bounded map)
	}
	key := streamEncoderKey{cfg: cfg, blockSize: blockSize}
	if v, ok := streamEncoderCache.Load(key); ok {
		return v.(reedsolomon.StreamEncoder), nil
	}
	enc, err := newStream()
	if err != nil {
		return nil, err
	}
	// Store-or-load to handle concurrent first calls for the same key.
	actual, _ := streamEncoderCache.LoadOrStore(key, enc)
	return actual.(reedsolomon.StreamEncoder), nil
}
