// Package zstdpool provides shared, pooled zstd compression helpers so the
// packblob (small-object) path and the cluster EC (large-object) path reuse one
// encoder/decoder pool instead of duplicating it.
package zstdpool

import (
	"github.com/gritive/GrainFS/internal/pool"
	"github.com/klauspost/compress/zstd"
)

var (
	encoderPool = pool.New(func() *zstd.Encoder {
		enc, _ := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderConcurrency(1), // callers provide their own concurrency (segment writers)
			zstd.WithWindowSize(1<<20),     // 1 MiB window: caps cold-start table alloc while preserving ratio
		)
		return enc
	})
	decoderPool = pool.New(func() *zstd.Decoder {
		dec, _ := zstd.NewReader(nil)
		return dec
	})
)

// Compress returns the zstd-compressed form of data using a pooled encoder.
func Compress(data []byte) ([]byte, error) {
	enc := encoderPool.Get()
	out := enc.EncodeAll(data, nil)
	encoderPool.Put(enc)
	return out, nil
}

// Decompress returns the original bytes of a zstd frame using a pooled decoder.
func Decompress(data []byte) ([]byte, error) {
	dec := decoderPool.Get()
	out, err := dec.DecodeAll(data, nil)
	decoderPool.Put(dec)
	if err != nil {
		return nil, err
	}
	return out, nil
}
